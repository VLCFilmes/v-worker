"""
Step: format_script — Parseia tags visuais do roteiro.

Exclusivo do STM text_video. Separa o roteiro bruto em:
- clean_text (texto sem tags, para timestamps/fraseamento)
- scene_overrides (cartela/background por cena)

Se não houver tags, retorna texto integral como 1 cena sem overrides.
"""

from ._base import *


@register_step(
    name="format_script",
    description="Parseia tags visuais do roteiro (cartela, background por cena)",
    category="preprocessing",
    depends_on=["load_template"],
    produces=["clean_text", "scene_overrides"],
    estimated_duration_s=1,
    cost_category="cpu",
    retryable=True,
    sse_step_name="FORMAT_SCRIPT",
    tool_schema={
        "description": "Parseia anotações visuais [CARTELA:...] [BG:...] do roteiro",
        "input": {},
        "output": {"scene_count": "int", "has_overrides": "bool"}
    }
)
def format_script_step(state: PipelineState, params: dict) -> PipelineState:
    # Obter texto do roteiro — pode vir de transcription_text (setado pelo bridge)
    # ou de options.text (passado pelo start_processing)
    raw_text = (
        state.transcription_text
        or (state.options or {}).get('text', '')
    )

    if not raw_text or not raw_text.strip():
        logger.warning("⚠️ [FORMAT_SCRIPT] Sem texto para formatar")
        return state

    from ..services.script_formatter_service import ScriptFormatterService

    formatter = ScriptFormatterService()
    result = formatter.format(raw_text)

    logger.info(
        f"✅ [FORMAT_SCRIPT] {result['scene_count']} cenas, "
        f"has_overrides={result['has_overrides']}, "
        f"clean_text_len={len(result['clean_text'])}"
    )

    return state.with_updates(
        clean_text=result["clean_text"],
        scene_overrides=result["scenes"],
        # Também atualizar transcription_text com o texto limpo
        # para que fraseamento e outros steps downstream usem texto sem tags
        transcription_text=result["clean_text"],
    )
