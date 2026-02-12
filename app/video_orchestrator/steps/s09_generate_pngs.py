"""Step 09: Geração de PNGs das legendas."""

from ._base import *

@register_step(
    name="generate_pngs",
    description="Gera imagens PNG para cada palavra/letra das legendas",
    category="rendering",
    depends_on=["classify", "load_template"],
    produces=["png_results"],
    estimated_duration_s=30,
    cost_category="cpu",
    retryable=True,
    max_retries=2,
    timeout_s=180,
    sse_step_name="PNG_GEN",
    tool_schema={
        "description": "Gera PNGs de legendas com estilos visuais do template",
        "input": {},
        "output": {"total_pngs": "int", "style_stats": "dict"}
    }
)
def generate_pngs_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.phrase_groups:
        raise ValueError("Sem phrase_groups para gerar PNGs")
    if not state.template_config:
        raise ValueError("Sem template_config para gerar PNGs")

    from ..services.png_generator_service import PngGeneratorService
    from ..services.creative_layout_service import CreativeLayoutService, extract_creative_layout_config
    from ..services.cartela_service import CartelaService

    phrase_groups = list(state.phrase_groups)  # cópia para não mutar state
    template_config = state.template_config

    # ═══ Step 10.5: Creative Layout (variação de tamanhos) ═══
    creative_layout_config = extract_creative_layout_config(template_config)
    if creative_layout_config.get('enabled', False):
        logger.info("🎨 [CREATIVE_LAYOUT] Aplicando variação de tamanhos...")
        creative_service = CreativeLayoutService()
        sentences_for_creative = [
            {
                'style_type': pg.get('style_type', 'default'),
                'words': pg.get('words', []),
                'text': pg.get('text', ''),
            }
            for pg in phrase_groups
        ]
        processed = creative_service.process(
            sentences=sentences_for_creative,
            creative_layout_config=creative_layout_config,
            job_id=state.job_id,
        )
        for i, pg in enumerate(phrase_groups):
            if i < len(processed):
                pg['words'] = processed[i].get('words', pg.get('words', []))
        logger.info("✅ [CREATIVE_LAYOUT] Aplicado")

    # ═══ Step 10.6: Cartelas (fundos visuais) ═══
    text_styles = template_config.get('_text_styles') or {}
    has_cartela = any(
        (style or {}).get('cartela_config', {}).get('enabled')
        for style in text_styles.values()
    )
    cartela_results = None

    if has_cartela:
        logger.info("🎬 [CARTELAS] Gerando cartelas...")
        cartela_service = CartelaService()
        cartela_result = cartela_service.generate_cartelas(
            sentences=phrase_groups,
            template_config=template_config,
            canvas_width=state.video_width,
            canvas_height=state.video_height,
            job_id=state.job_id,
        )
        if cartela_result.get('status') == 'success':
            phrase_classification = template_config.get('phrase_classification', {})
            phrase_groups = cartela_service.assign_cartelas_to_phrases(
                sentences=phrase_groups,
                generated_cartelas=cartela_result.get('generated_cartelas', []),
                phrase_classification=phrase_classification,
            )
            cartela_results = cartela_result
            logger.info(f"✅ [CARTELAS] Geradas: {cartela_result.get('stats', {})}")
        else:
            logger.warning(f"⚠️ [CARTELAS] {cartela_result.get('error', 'erro')}")

    # ═══ Geração de PNGs ═══
    logger.info(f"🖼️ [PNG_GEN] Gerando PNGs para {len(phrase_groups)} frases...")
    png_service = PngGeneratorService()
    result = png_service.generate_pngs_for_phrases(
        phrase_groups=phrase_groups,
        template_config=template_config,
        video_height=state.video_height,
        job_id=state.job_id,
    )

    if result.get('status') not in ('success', 'partial'):
        raise Exception(f"Geração de PNGs falhou: {result.get('error', 'desconhecido')}")

    logger.info(f"✅ [PNG_GEN] {result.get('total_pngs', 0)} PNGs | "
                f"Stats: {result.get('style_stats', {})}")

    return state.with_updates(
        png_results=result,
        phrase_groups=phrase_groups,
        cartela_results=cartela_results,
    )
