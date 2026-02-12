"""
Step 04: Detecção de silêncios no vídeo.

v4.0: Multi-arquivo — detecta silêncio POR VÍDEO com paralelismo.
Lê configurações de state.options (do template).
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from ._base import *

MAX_CONCURRENT_DETECT = int(os.environ.get('PIPELINE_MAX_CONCURRENT_DETECT', '4'))


@register_step(
    name="detect_silence",
    description="Detecta períodos de silêncio no vídeo (multi-arquivo)",
    category="preprocessing",
    depends_on=["normalize"],
    produces=["silence_detection"],
    optional=True,
    estimated_duration_s=15,
    cost_category="cpu",
    retryable=True,
    sse_step_name="SILENCE_CUT",
    tool_schema={
        "description": "Detecta silêncios no vídeo para corte",
        "input": {"silence_threshold": "float?", "min_silence_duration": "float?"},
        "output": {"silence_periods": "int", "speech_periods": "int"}
    }
)
def detect_silence_step(state: PipelineState, params: dict) -> PipelineState:
    # ─── Verificar se silence cut está habilitado ───
    opts = state.options or {}
    silence_enabled = opts.get('silence_removal', True)
    # Também checar 'silence_cut' (nome usado pelo chat_flask)
    if not silence_enabled and not opts.get('silence_cut', False):
        logger.info("⏭️ [DETECT_SILENCE] Silence removal desabilitado nas options")
        return state

    from ..services.silence_service import SilenceService
    service = SilenceService()

    # ─── Ler configurações de state.options (do template) ───
    # 🔧 v4.1: Defaults calibrados (09/Fev/2026) — multi-arquivo
    # Ver docs: 04-DETECT-SILENCE/README.md para guia de ajuste
    silence_threshold = opts.get('silence_threshold') or params.get('silence_threshold')
    min_silence_duration = opts.get('min_silence_duration', 0.5)
    silence_offset = opts.get('silence_offset', 0.0)          # v4.1: 0.3→0.0
    threshold_offset = opts.get('threshold_offset', 3)         # v4.1: 5→3
    # Template usa 'min_speech_duration', código original usava 'minimum_speech_duration'
    minimum_speech_duration = (
        opts.get('min_speech_duration')
        or opts.get('minimum_speech_duration')
        or 0.4                                                  # v4.1: 0.6→0.4
    )
    trim_start = opts.get('trim_start', 0.0)                   # v4.1: 0.3→0.0
    trim_end = opts.get('trim_end', 0.0)

    # ─── Multi-arquivo: detectar silêncio POR VÍDEO ───
    videos = state.videos or []
    videos_with_urls = []
    for v in videos:
        url = v.get('retake_cut_url') or v.get('normalized_url') or v.get('url')
        if url:
            videos_with_urls.append((v, url))

    if not videos_with_urls:
        # Fallback single-file (backward compat)
        video_url = state.normalized_video_url or state.get_video_url_for_processing()
        if not video_url:
            logger.warning("⚠️ [DETECT_SILENCE] Sem URL de vídeo")
            return state
        videos_with_urls = [({}, video_url)]

    if len(videos_with_urls) == 1:
        # ─── Single video: comportamento original ───
        video_dict, url = videos_with_urls[0]
        result = _detect_for_single_video(
            service, url, silence_threshold, min_silence_duration,
            silence_offset, threshold_offset, minimum_speech_duration,
            trim_start, trim_end, state.normalization_stats,
        )
        if result.get('error'):
            logger.warning(f"⚠️ [DETECT_SILENCE] Falhou: {result['error']}")
            return state

        _log_detection_result(result, url)

        # Enriquecer video dict
        if video_dict:
            video_dict['silence_detection'] = result

        return state.with_updates(
            silence_detection=result,
            videos=[v for v in videos] if videos else state.videos,
        )
    else:
        # ─── Multi-arquivo: paralelo com ThreadPoolExecutor ───
        logger.info(
            f"🔍 [DETECT_SILENCE] Multi-arquivo: {len(videos_with_urls)} vídeos "
            f"(max_concurrent={MAX_CONCURRENT_DETECT})"
        )

        results_map = {}
        errors = []

        with ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_DETECT, len(videos_with_urls))) as executor:
            futures = {}
            for video_dict, url in videos_with_urls:
                vid_id = video_dict.get('asset_id') or video_dict.get('upload_id') or url[-20:]
                # v4.1: Usar volume_analysis da normalização POR VÍDEO
                video_vol_analysis = video_dict.get('normalization_stats') or None
                if video_vol_analysis:
                    logger.info(
                        f"📊 [DETECT_SILENCE] {vid_id[:8]}: usando volume_analysis da normalização"
                    )
                future = executor.submit(
                    _detect_for_single_video,
                    service, url, silence_threshold, min_silence_duration,
                    silence_offset, threshold_offset, minimum_speech_duration,
                    trim_start, trim_end, video_vol_analysis,
                )
                futures[future] = (vid_id, video_dict, url)

            for future in as_completed(futures):
                vid_id, video_dict, url = futures[future]
                try:
                    result = future.result()
                    if result.get('error'):
                        errors.append(f"{vid_id}: {result['error']}")
                        logger.warning(f"⚠️ [DETECT_SILENCE] {vid_id}: {result['error']}")
                    else:
                        results_map[vid_id] = result
                        video_dict['silence_detection'] = result
                        _log_detection_result(result, url, vid_id)
                except Exception as e:
                    errors.append(f"{vid_id}: {e}")
                    logger.error(f"❌ [DETECT_SILENCE] {vid_id}: {e}")

        if not results_map:
            logger.warning(f"⚠️ [DETECT_SILENCE] Nenhuma detecção bem-sucedida")
            return state

        # Consolidar: silence_detection contém agregado (para backward compat)
        all_silence = []
        all_speech = []
        for r in results_map.values():
            all_silence.extend(r.get('silence_periods', []))
            all_speech.extend(r.get('speech_periods', []))

        consolidated = {
            'silence_periods': all_silence,
            'speech_periods': all_speech,
            'per_video': results_map,
            'multi_file': True,
            'videos_processed': len(results_map),
            'videos_failed': len(errors),
        }

        logger.info(
            f"✅ [DETECT_SILENCE] Multi-arquivo concluído: "
            f"{len(results_map)}/{len(videos_with_urls)} vídeos OK"
        )

        return state.with_updates(
            silence_detection=consolidated,
            videos=[v for v in videos],
        )


def _detect_for_single_video(
    service, url, silence_threshold, min_silence_duration,
    silence_offset, threshold_offset, minimum_speech_duration,
    trim_start, trim_end, volume_analysis,
):
    """Wrapper para detectar silêncio em um único vídeo."""
    return service.detect_silence(
        url=url,
        silence_threshold=silence_threshold,
        min_silence_duration=min_silence_duration,
        silence_offset=silence_offset,
        threshold_offset=threshold_offset,
        minimum_speech_duration=minimum_speech_duration,
        trim_start=trim_start,
        trim_end=trim_end,
        volume_analysis=volume_analysis,
    )


def _log_detection_result(result, url, vid_id=None):
    """Log resultado de detecção."""
    prefix = f"[{vid_id}] " if vid_id else ""
    silence_count = len(result.get('silence_periods', []))
    speech_count = len(result.get('speech_periods', []))
    logger.info(
        f"✅ [DETECT_SILENCE] {prefix}Detectados: "
        f"{silence_count} silêncios, {speech_count} falas"
    )
