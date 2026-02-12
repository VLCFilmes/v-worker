"""Step 11: Aplicação de animações (metadados, não altera PNGs)."""

from ._base import *

@register_step(
    name="apply_animations",
    description="Adiciona metadados de animação às frases (stagger, entrada, saída)",
    category="rendering",
    depends_on=["add_shadows"],
    produces=["png_results"],
    optional=True,
    estimated_duration_s=5,
    cost_category="free",
    retryable=True,
    sse_step_name="POSITION",
    tool_schema={
        "description": "Configura animações de entrada/saída para as legendas",
        "input": {},
        "output": {"phrases_processed": "int", "status": "str"}
    }
)
def apply_animations_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.png_results:
        logger.warning("⚠️ [ANIMATIONS] Sem png_results")
        return state
    if not state.template_config:
        logger.warning("⚠️ [ANIMATIONS] Sem template_config")
        return state

    from ..services.animation_service import AnimationService

    text_styles = state.template_config.get('_text_styles') or {}
    animation_config = {}

    # Extrair animações PER-STYLE dos text_styles
    per_style_animations = {}
    for style_name, style_config in text_styles.items():
        if not style_config or not style_config.get('animation_config'):
            continue
        anim = style_config['animation_config']
        is_enabled = anim.get('enabled', False)
        if isinstance(is_enabled, dict):
            is_enabled = is_enabled.get('value', False)
        if is_enabled:
            per_style_animations[style_name] = anim
            logger.info(f"   ✅ {style_name}: animação HABILITADA")

    if not per_style_animations:
        logger.info("⏭️ [ANIMATIONS] Todas animações desabilitadas no template")
        return state

    animation_config['per_style_animations'] = per_style_animations

    # Extrair stagger config do primeiro estilo
    first_style = list(per_style_animations.values())[0]
    if first_style.get('stagger_preset'):
        animation_config['animation_preset'] = first_style['stagger_preset']

    stagger = first_style.get('stagger', {})
    stagger_enabled = stagger.get('enabled', False)
    if isinstance(stagger_enabled, dict):
        stagger_enabled = stagger_enabled.get('value', False)
    if stagger_enabled:
        animation_config['stagger_and_opacity'] = {
            'enabled': True,
            'stagger_config': {
                'delay_ms': stagger.get('delay_ms', 50),
                'direction': 'left_to_right',
            }
        }

    phrases = state.png_results.get('phrases', [])
    if not phrases:
        logger.warning("⚠️ [ANIMATIONS] Nenhuma frase nos png_results")
        return state

    service = AnimationService()
    processed_phrases = service.apply_animations(
        phrase_groups=phrases,
        animation_config=animation_config,
        job_id=state.job_id,
    )

    total_words = sum(len(p.get('words', [])) for p in processed_phrases)
    logger.info(f"✅ [ANIMATIONS] {len(processed_phrases)} frases, {total_words} palavras")

    updated_png_results = state.png_results.copy()
    updated_png_results['phrases'] = processed_phrases

    return state.with_updates(png_results=updated_png_results)
