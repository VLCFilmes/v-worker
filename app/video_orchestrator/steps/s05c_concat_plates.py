"""
Step 05c: Concatenação de placas tectônicas.

Após silence_cut (multi-arquivo), cada vídeo tem suas próprias placas
(speech segments). Este step concatena TODAS as placas na ordem narrativa
num único vídeo — que se torna o phase1_video_url para:
  1. Transcrição (timestamps alinhados ao vídeo final)
  2. Fraseamento, classify, matting, render

Condicional: só roda se tectonic_plates.multi_file = True.
Se single-file, o silence_cut já setou phase1_video_url corretamente.
"""

from ._base import *


@register_step(
    name="concat_plates",
    description="Concatena placas tectônicas de múltiplos vídeos na ordem narrativa",
    category="preprocessing",
    depends_on=["silence_cut"],
    produces=["phase1_video_url", "phase1_audio_url", "total_duration_ms"],
    optional=True,
    estimated_duration_s=30,
    cost_category="cpu",
    retryable=True,
    max_retries=2,
    timeout_s=300,
    sse_step_name="CONCAT",
    tool_schema={
        "description": "Concatena placas tectônicas na ordem narrativa",
        "input": {},
        "output": {"url": "str", "plates_count": "int", "duration_s": "float"}
    }
)
def concat_plates_step(state: PipelineState, params: dict) -> PipelineState:
    tectonic = state.tectonic_plates or {}

    # ─── Só rodar se multi-arquivo ───
    if not tectonic.get('multi_file'):
        logger.info("⏭️ [CONCAT_PLATES] Single-file — skip (phase1_video_url já definido)")
        return state

    # ─── Coletar URLs de todas as placas na ordem narrativa ───
    plate_urls = _collect_plate_urls(tectonic)

    if not plate_urls:
        # Se não há URLs de placas, usar os vídeos normalizados na ordem
        logger.warning("⚠️ [CONCAT_PLATES] Sem URLs de placas — concatenando vídeos normalizados")
        plate_urls = _fallback_normalized_urls(state)

    if not plate_urls:
        logger.warning("⚠️ [CONCAT_PLATES] Sem URLs para concatenar — skip")
        return state

    if len(plate_urls) == 1:
        logger.info(f"✅ [CONCAT_PLATES] Apenas 1 placa — usando diretamente")
        return state.with_updates(
            phase1_video_url=plate_urls[0],
            phase1_audio_url=plate_urls[0],
        )

    # ─── Concatenar via v-services ───
    from ..services.concat_service import ConcatService
    service = ConcatService()

    logger.info(
        f"🔗 [CONCAT_PLATES] Concatenando {len(plate_urls)} placas tectônicas "
        f"de {tectonic.get('total_videos', 0)} vídeos"
    )

    result = service.concat(
        urls=plate_urls,
        output_file=f"plates_{state.job_id[:8]}",
        force_copy=True,  # Stream copy: sem re-encoding, preserva qualidade
    )

    if result.get('error'):
        raise Exception(f"Concat plates falhou: {result['error']}")

    # Extrair URL do resultado (v-services pode retornar em diferentes keys)
    output_url = (
        result.get('output_url')
        or result.get('preview_url')
        or result.get('url')
    )

    if not output_url:
        # Tentar chaves dinâmicas como no normalize
        for key, val in result.items():
            if key.startswith('url') and isinstance(val, str) and val.startswith('http'):
                output_url = val
                break

    if not output_url:
        raise Exception(
            f"Concat plates: sem URL no resultado. Keys: {list(result.keys())}"
        )

    duration_s = result.get('duration_seconds') or result.get('duration', 0)
    duration_ms = int(float(duration_s) * 1000) if duration_s else None

    logger.info(
        f"✅ [CONCAT_PLATES] {len(plate_urls)} placas → 1 vídeo | "
        f"duração={duration_s}s | url={output_url[:80]}..."
    )

    return state.with_updates(
        phase1_video_url=output_url,
        phase1_audio_url=output_url,
        phase1_video_concatenated_url=output_url,
        total_duration_ms=duration_ms or state.total_duration_ms,
        phase1_source='tectonic_concat',
    )


def _collect_plate_urls(tectonic: dict) -> list:
    """
    Coleta URLs de placas tectônicas na ordem narrativa.

    Cada vídeo em tectonic['videos'] tem 'plates[]' com 'url' individual.
    Se 'url' não existe na placa, usa source_url do vídeo como fallback.
    """
    videos = tectonic.get('videos', [])
    urls = []

    for video in videos:
        plates = video.get('plates', [])
        source_url = video.get('source_url', '')
        audio_url = video.get('audio_url', '')

        if not plates:
            # Sem placas = vídeo inteiro (passthrough)
            if source_url:
                urls.append(source_url)
            continue

        for plate in plates:
            plate_url = plate.get('url')
            if plate_url:
                urls.append(plate_url)
            elif audio_url:
                # Se silence_cut gerou concatenated_audio_url
                # Não ideal, mas melhor que nada
                pass

        # Se as placas não têm URLs individuais, o silence_cut gerou
        # um audio_url concatenado para este vídeo. Usar esse.
        if not any(p.get('url') for p in plates) and audio_url:
            urls.append(audio_url)
        elif not any(p.get('url') for p in plates) and source_url:
            # Fallback final: usar source_url (vídeo sem corte)
            urls.append(source_url)

    return urls


def _fallback_normalized_urls(state: PipelineState) -> list:
    """Fallback: usar URLs normalizadas na ordem."""
    urls = []
    for v in (state.videos or []):
        url = (
            v.get('retake_cut_url')
            or v.get('normalized_url')
            or v.get('url')
        )
        if url:
            urls.append(url)
    return urls
