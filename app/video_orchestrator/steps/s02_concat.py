"""
Step 02: Concatenação de múltiplos vídeos.

v4.0: SKIP automático para STM talking_head/audio_narration.
Nesses modos, cada vídeo é processado individualmente e gera suas
próprias placas tectônicas. O v-editor/render monta na ordem do roteiro.

Mantido no registry para compatibilidade com outros STMs futuros.
"""

from ._base import *


@register_step(
    name="concat",
    description="Concatena múltiplos vídeos em um único arquivo",
    category="preprocessing",
    depends_on=["normalize"],
    produces=["concatenated_video_url"],
    optional=True,
    estimated_duration_s=20,
    cost_category="cpu",
    retryable=True,
    max_retries=2,
    timeout_s=120,
    sse_step_name="CONCAT",
    tool_schema={
        "description": "Concatena múltiplos vídeos em sequência",
        "input": {},
        "output": {"url": "str"}
    }
)
def concat_step(state: PipelineState, params: dict) -> PipelineState:
    opts = state.options or {}
    stm = opts.get('storytelling_mode', 'unknown')

    # ─── v4.0: SKIP para STMs que processam por vídeo ───
    # talking_head e audio_narration: cada vídeo gera suas placas individualmente.
    # A concatenação não é necessária — o v-editor monta na ordem do roteiro.
    skip_stms = {'talking_head', 'audio_narration', 'narration'}
    if stm in skip_stms:
        logger.info(
            f"⏭️ [CONCAT] Skip: STM={stm} — vídeos processados individualmente"
        )
        return state

    # ─── Verificar se concatenação está desabilitada nas options ───
    if not opts.get('concatenar', True):
        logger.info("⏭️ [CONCAT] Concatenação desabilitada nas options")
        return state

    # Só concatenar se tiver mais de 1 vídeo
    if len(state.videos) <= 1:
        logger.info("⏭️ [CONCAT] Apenas 1 vídeo, pulando concatenação")
        return state

    from ..services.concat_service import ConcatService

    # Usar URLs normalizadas se disponíveis
    urls = []
    for v in state.videos:
        url = v.get('normalized_url') or v.get('url')
        if url:
            urls.append(url)

    if not urls:
        logger.warning("⚠️ [CONCAT] Sem URLs para concatenar")
        return state

    service = ConcatService()
    result = service.concat(
        urls=urls,
        output_file=f"concat_{state.job_id[:8]}",
    )

    if result.get('error'):
        raise Exception(f"Concatenação falhou: {result['error']}")

    output_url = result.get('output_url') or result.get('url')
    logger.info(f"✅ [CONCAT] {len(urls)} vídeos concatenados")

    return state.with_updates(
        concatenated_video_url=output_url,
    )
