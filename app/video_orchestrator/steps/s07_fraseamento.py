"""Step 07: Fraseamento (agrupamento de palavras em frases)."""

from ._base import *

@register_step(
    name="fraseamento",
    description="Agrupa palavras transcritas em frases usando NLP",
    category="preprocessing",
    depends_on=["transcribe"],
    produces=["phrase_groups"],
    estimated_duration_s=10,
    cost_category="cpu",
    retryable=True,
    sse_step_name="PHRASE_GROUP",
    tool_schema={
        "description": "Agrupa palavras em frases com regras de quebra",
        "input": {"conservative_mode": "bool?"},
        "output": {"phrase_count": "int"}
    }
)
def fraseamento_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.transcription_words:
        raise ValueError("Sem transcription_words para fraseamento")

    from ..services.fraseamento_service import FraseamentoService

    service = FraseamentoService()

    # Extrair enhanced-phrase-rules do template_config já carregado no state
    # v-services trata None/vazio com defaults — sem necessidade de lógica complexa aqui
    rules = None
    if state.template_config:
        rules = state.template_config.get('enhanced-phrase-rules')

    result = service.process(
        words=state.transcription_words,
        rules=rules,
        conservative_mode=params.get('conservative_mode', False),
        template_id=state.template_id,
    )

    if result.get('error'):
        raise Exception(f"Fraseamento falhou: {result['error']}")

    phrase_groups = result.get('phrase_groups', [])
    logger.info(f"✅ [FRASEAMENTO] {len(phrase_groups)} frases agrupadas")

    return state.with_updates(
        phrase_groups=phrase_groups,
    )
