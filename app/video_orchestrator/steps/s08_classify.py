"""Step 08: Classificação de frases (LLM ou heurística).

🆕 v5.0: STM-aware — comportamento varia por storytelling_mode:
  - talking_head: classificação completa (style_type + cartela + matting)
  - text_video:   SÓ style_type + aplica scene_overrides do roteiro
"""

from ._base import *

@register_step(
    name="classify",
    description="Classifica frases em default/emphasis/letter_effect (STM-aware)",
    category="preprocessing",
    depends_on=["fraseamento"],
    produces=["phrase_groups"],
    estimated_duration_s=5,
    cost_category="llm",
    retryable=True,
    sse_step_name="CLASSIFY",
    tool_schema={
        "description": "Classifica frases por estilo visual (default, emphasis, letter_effect)",
        "input": {"force_reclassify": "bool?"},
        "output": {"stats": "dict", "phrase_count": "int"}
    }
)
def classify_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.phrase_groups:
        logger.warning("⚠️ [CLASSIFY] Sem phrase_groups para classificar")
        return state

    stm = state.storytelling_mode or "talking_head"

    # Verificar se já tem classificação manual (editada pelo usuário no revisor)
    has_manual_style = any(
        pg.get('style_type') and pg.get('style_type') != 'default'
        for pg in state.phrase_groups
    )
    has_manual_cartela = any(
        pg.get('use_cartela', False) for pg in state.phrase_groups
    )
    has_manual_matting = any(
        pg.get('person_overlay_enabled', False) for pg in state.phrase_groups
    )
    has_manual = has_manual_style or has_manual_cartela or has_manual_matting

    if has_manual and not params.get('force_reclassify'):
        logger.info(f"⏭️ [CLASSIFY] Frases já classificadas manualmente "
                     f"(style={has_manual_style}, cartela={has_manual_cartela}, "
                     f"matting={has_manual_matting})")
        # Mesmo com classificação manual, aplicar scene_overrides se text_video
        if stm == "text_video" and state.scene_overrides:
            result = _apply_scene_overrides(state.phrase_groups, state.scene_overrides)
            return state.with_updates(phrase_groups=result)
        return state

    from ..services.phrase_classifier_service import PhraseClassifierService

    classifier = PhraseClassifierService()

    # enabled_types vem do state (carregado pelo load_template step)
    enabled_types = state.enabled_types or ['default']

    logger.info(f"🎯 [CLASSIFY] Classificando {len(state.phrase_groups)} frases "
                f"(stm={stm}, enabled_types={enabled_types})")

    # 🆕 STM text_video: passar storytelling_mode no context para que o classifier
    # NÃO decida cartela/matting (overrides vêm do roteiro)
    context = {
        'template_id': state.template_id,
        'enabled_types': enabled_types,
        'storytelling_mode': stm,
    }

    result = classifier.classify_phrases(
        state.phrase_groups,
        context=context,
    )

    if result and isinstance(result, list):
        # 🆕 STM text_video: forçar use_cartela=False e person_overlay_enabled=False
        # (overrides vêm do roteiro via scene_overrides, não do classify)
        if stm == "text_video":
            for pg in result:
                pg['use_cartela'] = False
                pg['person_overlay_enabled'] = False

            # Aplicar scene_overrides (cartela/bg) por cena
            if state.scene_overrides:
                result = _apply_scene_overrides(result, state.scene_overrides)

        # Calcular stats
        stats = {}
        for p in result:
            st = p.get('style_type', 'default')
            stats[st] = stats.get(st, 0) + 1
        cartela_count = sum(1 for p in result if p.get('use_cartela'))
        logger.info(f"✅ [CLASSIFY] Classificação: {stats}, cartelas={cartela_count} (stm={stm})")

        return state.with_updates(phrase_groups=result)

    logger.warning("⚠️ [CLASSIFY] Classificação retornou resultado inválido")
    return state


def _apply_scene_overrides(
    phrase_groups: list,
    scene_overrides: list,
) -> list:
    """
    Aplica overrides visuais do roteiro (cartela, background) nas phrase_groups.
    
    Mapeia cada phrase_group para sua cena correspondente pelo texto,
    e aplica os overrides definidos no roteiro.
    """
    if not scene_overrides:
        return phrase_groups

    # Construir lookup de overrides por cena
    # Cada cena tem clean_text; mapear phrase_groups para cenas pelo conteúdo
    scene_texts = []
    for scene in scene_overrides:
        clean = (scene.get("clean_text") or "").strip().lower()
        scene_texts.append({
            "text": clean,
            "overrides": scene.get("overrides", {}),
        })

    updated = []
    for pg in phrase_groups:
        pg_copy = dict(pg)
        pg_text = (pg_copy.get("text") or "").strip().lower()

        # Encontrar cena que contém este texto
        matched_overrides = {}
        for scene in scene_texts:
            if scene["text"] and pg_text and pg_text in scene["text"]:
                matched_overrides = scene["overrides"]
                break

        # Aplicar override de cartela
        cartela = matched_overrides.get("cartela")
        if cartela and cartela.get("enabled"):
            pg_copy["use_cartela"] = True
            pg_copy["cartela_override"] = cartela  # cor, tipo, opacidade

        # Aplicar override de background
        bg = matched_overrides.get("background")
        if bg:
            pg_copy["background_override"] = bg  # type, color

        updated.append(pg_copy)

    override_count = sum(1 for pg in updated if pg.get("cartela_override") or pg.get("background_override"))
    logger.info(f"   📝 [CLASSIFY] scene_overrides aplicados: {override_count}/{len(updated)} frases")

    return updated
