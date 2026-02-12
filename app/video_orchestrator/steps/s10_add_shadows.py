"""Step 10: Adição de sombras aos PNGs."""

from ._base import *

@register_step(
    name="add_shadows",
    description="Adiciona sombras aos PNGs de legendas",
    category="rendering",
    depends_on=["generate_pngs"],
    produces=["shadow_results", "png_results"],
    optional=True,
    estimated_duration_s=15,
    cost_category="cpu",
    retryable=True,
    sse_step_name="SHADOW",
    tool_schema={
        "description": "Adiciona efeito de sombra aos PNGs gerados",
        "input": {},
        "output": {"total_processed": "int", "status": "str"}
    }
)
def add_shadows_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.png_results:
        logger.warning("⚠️ [SHADOWS] Sem png_results para adicionar sombras")
        return state
    if not state.template_config:
        logger.warning("⚠️ [SHADOWS] Sem template_config")
        return state

    from ..services.shadow_service import ShadowService

    service = ShadowService()
    result = service.add_shadows_to_phrases(
        png_results=state.png_results,
        template_config=state.template_config,
    )

    if result.get('status') == 'skipped':
        logger.info(f"⏭️ [SHADOWS] {result.get('message', 'Desabilitado')}")
        return state.with_updates(shadow_results=result)

    if result.get('status') in ('success', 'partial'):
        logger.info(f"✅ [SHADOWS] {result.get('total_processed', 0)} PNGs processados")
        # Shadows atualizam os png_results com as URLs dos PNGs com sombra
        updated_png_results = state.png_results.copy()
        if result.get('phrases'):
            updated_png_results['phrases'] = result['phrases']
        return state.with_updates(
            shadow_results=result,
            png_results=updated_png_results,
        )

    logger.warning(f"⚠️ [SHADOWS] Status inesperado: {result.get('status')}")
    return state.with_updates(shadow_results=result)
