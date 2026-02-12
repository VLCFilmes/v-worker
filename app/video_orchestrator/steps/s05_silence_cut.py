"""
Step 05: Corte de silêncios (hybrid ou simples).

v4.0: Multi-arquivo — gera placas tectônicas POR VÍDEO com paralelismo.
Lê configurações de state.options (do template).
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from ._base import *

MAX_CONCURRENT_CUT = int(os.environ.get('PIPELINE_MAX_CONCURRENT_CUT', '3'))


@register_step(
    name="silence_cut",
    description="Remove silêncios do vídeo (gera placas tectônicas se hybrid)",
    category="preprocessing",
    depends_on=["detect_silence"],
    produces=["phase1_video_url", "phase1_audio_url", "speech_segments",
              "cut_timestamps", "phase1_source", "total_duration_ms",
              "tectonic_plates"],
    optional=True,
    estimated_duration_s=30,
    cost_category="cpu",
    retryable=True,
    timeout_s=300,
    sse_step_name="SILENCE_CUT",
    tool_schema={
        "description": "Remove silêncios do vídeo, gerando segmentos de fala",
        "input": {"cut_mode": "str?"},
        "output": {"segments": "int", "source": "str"}
    }
)
def silence_cut_step(state: PipelineState, params: dict) -> PipelineState:
    opts = state.options or {}
    silence_enabled = opts.get('silence_removal', True) or opts.get('silence_cut', False)

    if not silence_enabled or not state.silence_detection:
        logger.info("⏭️ [SILENCE_CUT] Sem detecção de silêncio ou desabilitado")
        # Sem silence cut = marcar vídeos como prontos para fase 1
        return _skip_silence_cut(state)

    from ..services.silence_service import SilenceService
    service = SilenceService()

    # ─── Ler configurações de state.options ───
    cut_mode = opts.get('cut_mode', 'all_silences')
    # cut_mode do template: 'hybrid', 'all_silences', 'edges_only'

    detection = state.silence_detection or {}
    is_multi = detection.get('multi_file', False)

    if is_multi:
        return _cut_multi_file(state, service, detection, cut_mode)
    else:
        return _cut_single_file(state, service, detection, cut_mode)


def _skip_silence_cut(state: PipelineState) -> PipelineState:
    """Sem corte de silêncio — marcar vídeos como prontos."""
    videos = state.videos or []

    if len(videos) <= 1:
        video_url = state.normalized_video_url or state.get_video_url_for_processing()
        return state.with_updates(
            phase1_video_url=video_url,
            phase1_audio_url=video_url,
            phase1_source='normalized',
        )

    # Multi-arquivo: cada vídeo normalizado vira sua própria "placa"
    tectonic_plates = _build_passthrough_plates(videos)
    first_url = videos[0].get('normalized_url') or videos[0].get('url')

    return state.with_updates(
        phase1_video_url=first_url,
        phase1_audio_url=first_url,
        phase1_source='normalized',
        tectonic_plates=tectonic_plates,
    )


def _cut_single_file(state, service, detection, cut_mode):
    """Corte de silêncio para um único vídeo (backward compat)."""
    video_url = state.normalized_video_url or state.get_video_url_for_processing()

    speech_periods = detection.get('speech_periods', [])
    if speech_periods:
        logger.info(f"🔪 [SILENCE_CUT] Modo HYBRID: {len(speech_periods)} segmentos de fala")
        result = service.cut_silence_hybrid(
            input_file=video_url,
            output_prefix=f"hybrid_{state.job_id[:8]}",
            clips=speech_periods,
            clips_type='speech_periods',
            fast_copy=False,             # 🔧 v4.2: explícito — cortes precisos com re-encoding
            optimize_keyframes=False,    # 🔧 v4.2: desnecessário com re-encoding
        )

        if result.get('error'):
            raise Exception(f"Hybrid silence cut falhou: {result['error']}")

        segments = result.get('segments', [])
        audio_url = result.get('audio_url') or result.get('concatenated_audio_url')
        concat_url = result.get('concatenated_url')

        logger.info(f"✅ [SILENCE_CUT] Hybrid: {len(segments)} placas tectônicas")

        return state.with_updates(
            speech_segments=segments,
            phase1_video_url=video_url,
            phase1_audio_url=audio_url or concat_url or video_url,
            phase1_video_concatenated_url=concat_url,
            cut_timestamps=result.get('cut_timestamps'),
            phase1_source='tectonic',
            total_duration_ms=result.get('total_duration_ms') or state.total_duration_ms,
        )
    else:
        # Fallback: corte simples
        silence_periods = detection.get('silence_periods', [])
        logger.info(f"🔪 [SILENCE_CUT] Modo SIMPLES: {len(silence_periods)} silêncios")

        result = service.cut_silence(
            input_file=video_url,
            output_file=f"cut_{state.job_id[:8]}",
            clips=silence_periods,
        )

        if result.get('error'):
            raise Exception(f"Silence cut falhou: {result['error']}")

        output_url = result.get('output_url') or result.get('url')

        return state.with_updates(
            phase1_video_url=output_url,
            phase1_audio_url=output_url,
            cut_timestamps=result.get('cut_timestamps'),
            phase1_source='normalized',
            total_duration_ms=result.get('total_duration_ms') or state.total_duration_ms,
        )


def _cut_multi_file(state, service, detection, cut_mode):
    """
    Corte de silêncio multi-arquivo: gera placas tectônicas POR VÍDEO.

    Cada vídeo gera suas próprias placas, referenciando seu asset_id e
    URL de origem com timestamps corretos.
    """
    videos = state.videos or []
    per_video = detection.get('per_video', {})

    if not per_video:
        logger.warning("⚠️ [SILENCE_CUT] Multi-file sem per_video data, fallback")
        return _skip_silence_cut(state)

    logger.info(
        f"🔪 [SILENCE_CUT] Multi-arquivo: {len(per_video)} vídeos "
        f"(max_concurrent={MAX_CONCURRENT_CUT})"
    )

    all_plates = []
    total_segments = 0
    errors = []

    # Mapear vid_id → video dict para lookup
    vid_map = {}
    for v in videos:
        vid_id = v.get('asset_id') or v.get('upload_id')
        if vid_id:
            vid_map[vid_id] = v

    # Processar vídeos em paralelo
    items_to_process = []
    for vid_id, silence_result in per_video.items():
        video_dict = vid_map.get(vid_id, {})
        url = video_dict.get('retake_cut_url') or video_dict.get('normalized_url') or video_dict.get('url')
        if not url:
            logger.warning(f"⚠️ [SILENCE_CUT] Sem URL para {vid_id}")
            continue

        speech_periods = silence_result.get('speech_periods', [])
        if not speech_periods:
            # Sem speech_periods = vídeo inteiro é uma "placa"
            logger.info(f"⏭️ [SILENCE_CUT] {vid_id[:8]}: sem speech_periods, vídeo inteiro como placa")
            order = video_dict.get('order', 0)
            all_plates.append({
                'asset_id': vid_id,
                'source_url': url,
                'order': order,
                'plates': [{
                    'start_s': 0.0,
                    'end_s': silence_result.get('total_duration', 0),
                    'duration_s': silence_result.get('total_duration', 0),
                    'type': 'full_video',
                }],
                'cut_applied': False,
            })
            continue

        items_to_process.append((vid_id, video_dict, url, speech_periods))

    if items_to_process:
        with ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_CUT, len(items_to_process))) as executor:
            futures = {}
            for vid_id, video_dict, url, speech_periods in items_to_process:
                future = executor.submit(
                    _cut_single_video_hybrid,
                    service, url, vid_id, state.job_id, speech_periods,
                )
                futures[future] = (vid_id, video_dict)

            for future in as_completed(futures):
                vid_id, video_dict = futures[future]
                try:
                    result = future.result()
                    if result.get('error'):
                        errors.append(f"{vid_id}: {result['error']}")
                        logger.warning(f"⚠️ [SILENCE_CUT] {vid_id[:8]}: {result['error']}")
                        continue

                    segments = result.get('segments', result.get('video_segments', []))
                    order = video_dict.get('order', 0)
                    url = video_dict.get('normalized_url') or video_dict.get('url')

                    plates = []
                    for seg in segments:
                        plates.append({
                            'start_s': seg.get('original_start', seg.get('start', 0)),
                            'end_s': seg.get('original_end', seg.get('end', 0)),
                            'duration_s': seg.get('duration', 0),
                            'type': 'speech',
                            'url': seg.get('url'),  # URL do clip individual (se gerado)
                        })

                    all_plates.append({
                        'asset_id': vid_id,
                        'source_url': url,
                        'order': order,
                        'plates': plates,
                        'cut_applied': True,
                        'audio_url': result.get('audio_url') or result.get('concatenated_audio_url'),
                    })

                    video_dict['speech_segments'] = segments
                    total_segments += len(plates)

                    logger.info(
                        f"✅ [SILENCE_CUT] {vid_id[:8]}: {len(plates)} placas tectônicas"
                    )
                except Exception as e:
                    errors.append(f"{vid_id}: {e}")
                    logger.error(f"❌ [SILENCE_CUT] {vid_id[:8]}: {e}")

    # Ordenar placas pela ordem narrativa
    all_plates.sort(key=lambda p: p.get('order', 0))

    tectonic_plates = {
        'videos': all_plates,
        'total_plates': total_segments,
        'total_videos': len(all_plates),
        'multi_file': True,
    }

    # Para backward compat, usar primeiro vídeo como phase1
    first_url = None
    first_audio = None
    if all_plates:
        first_url = all_plates[0].get('source_url')
        first_audio = all_plates[0].get('audio_url') or first_url

    logger.info(
        f"✅ [SILENCE_CUT] Multi-arquivo concluído: "
        f"{len(all_plates)} vídeos, {total_segments} placas total"
    )

    return state.with_updates(
        tectonic_plates=tectonic_plates,
        phase1_video_url=first_url,
        phase1_audio_url=first_audio or first_url,
        phase1_source='tectonic_multi',
        videos=[v for v in videos],
    )


def _cut_single_video_hybrid(service, url, vid_id, job_id, speech_periods):
    """Wrapper para cortar silêncio de um único vídeo."""
    return service.cut_silence_hybrid(
        input_file=url,
        output_prefix=f"hybrid_{job_id[:8]}_{vid_id[:8]}",
        clips=speech_periods,
        clips_type='speech_periods',
        fast_copy=False,             # 🔧 v4.2: explícito — cortes precisos com re-encoding
        optimize_keyframes=False,    # 🔧 v4.2: desnecessário com re-encoding
    )


def _build_passthrough_plates(videos):
    """Constrói placas passthrough (sem corte) para multi-arquivo."""
    plates = []
    for v in videos:
        vid_id = v.get('asset_id') or v.get('upload_id') or ''
        url = v.get('normalized_url') or v.get('retake_cut_url') or v.get('url')
        order = v.get('order', 0)
        plates.append({
            'asset_id': vid_id,
            'source_url': url,
            'order': order,
            'plates': [],  # Sem corte = vídeo inteiro
            'cut_applied': False,
        })
    return {
        'videos': plates,
        'total_plates': 0,
        'total_videos': len(plates),
        'multi_file': True,
        'passthrough': True,
    }
