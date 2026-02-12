"""Step 13: Geração de backgrounds (fundos visuais para frases)."""

from ._base import *

@register_step(
    name="generate_backgrounds",
    description="Gera imagens de fundo para frases (word/phrase/fullscreen)",
    category="rendering",
    depends_on=["calculate_positions"],
    produces=["background_results", "png_results"],
    optional=True,
    estimated_duration_s=15,
    cost_category="cpu",
    retryable=True,
    sse_step_name="BG_GEN",
    tool_schema={
        "description": "Gera backgrounds visuais para as legendas posicionadas",
        "input": {},
        "output": {"total_backgrounds": "int", "status": "str"}
    }
)
def generate_backgrounds_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.png_results:
        logger.warning("⚠️ [BG_GEN] Sem png_results")
        return state
    if not state.template_config:
        logger.warning("⚠️ [BG_GEN] Sem template_config")
        return state

    from ..services.background_generator_service import BackgroundGeneratorService

    # Verificar se algum estilo tem background habilitado
    text_styles = state.template_config.get('_text_styles') or state.text_styles or {}
    has_background = False
    for style_name, style_config in text_styles.items():
        if not style_config:
            continue
        bg = style_config.get('background', {})
        bg_enabled = bg.get('enabled', False)
        if isinstance(bg_enabled, dict):
            bg_enabled = bg_enabled.get('value', False)
        if bg_enabled:
            has_background = True
            break

    if not has_background:
        logger.info("⏭️ [BG_GEN] Nenhum estilo com background habilitado")
        return state.with_updates(background_results={'status': 'skipped', 'reason': 'disabled'})

    # Precisamos de positioned_sentences para backgrounds de frase
    positioned_sentences = state.png_results.get('positioned_sentences', [])
    if not positioned_sentences and state.positioning_results:
        positioned_sentences = state.positioning_results.get('sentences', [])

    if not positioned_sentences:
        logger.warning("⚠️ [BG_GEN] Sem positioned_sentences para backgrounds")
        return state.with_updates(background_results={'status': 'skipped', 'reason': 'no_positions'})

    canvas = {
        'width': state.video_width,
        'height': state.video_height,
    }

    service = BackgroundGeneratorService()

    logger.info(f"🎨 [BG_GEN] Gerando backgrounds ({len(positioned_sentences)} frases)...")

    result = service.generate_backgrounds(
        sentences=positioned_sentences,
        text_styles=text_styles,
        canvas=canvas,
        job_id=state.job_id,
    )

    if result.get('status') == 'skipped':
        logger.info(f"⏭️ [BG_GEN] {result.get('message', 'Sem backgrounds configurados')}")
        return state.with_updates(background_results=result)

    if result.get('status') in ('success', 'partial'):
        backgrounds = result.get('backgrounds', [])
        logger.info(f"✅ [BG_GEN] {len(backgrounds)} backgrounds gerados")

        # Adicionar backgrounds ao png_results
        updated_png_results = state.png_results.copy()
        updated_png_results['backgrounds'] = backgrounds

        return state.with_updates(
            background_results=result,
            png_results=updated_png_results,
        )

    logger.warning(f"⚠️ [BG_GEN] Falha: {result.get('error', 'desconhecido')}")
    return state.with_updates(background_results=result)
