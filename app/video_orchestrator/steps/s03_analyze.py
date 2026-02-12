"""Step 03: Análise de vídeo (volume, FPS, resolução)."""

from ._base import *

@register_step(
    name="analyze",
    description="Analisa propriedades do vídeo (volume, FPS, resolução)",
    category="preprocessing",
    depends_on=["normalize"],
    produces=["normalization_stats"],
    optional=True,
    estimated_duration_s=10,
    cost_category="cpu",
    retryable=True,
    sse_step_name="NORMALIZE",
)
def analyze_step(state: PipelineState, params: dict) -> PipelineState:
    from ..services.analyze_service import AnalyzeService

    video_url = state.normalized_video_url or state.get_video_url_for_processing()
    if not video_url:
        logger.warning("⚠️ [ANALYZE] Sem URL de vídeo para analisar")
        return state

    service = AnalyzeService()
    result = service.analyze(url=video_url)

    if result.get('error'):
        logger.warning(f"⚠️ [ANALYZE] Análise falhou: {result['error']}")
        return state

    stats = result.get('stats') or result
    logger.info(f"✅ [ANALYZE] Análise completa")

    return state.with_updates(
        normalization_stats=stats,
    )
