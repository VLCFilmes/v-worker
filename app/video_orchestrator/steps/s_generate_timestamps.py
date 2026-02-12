"""
Step: generate_timestamps — Gera timestamps virtuais para texto puro.

Exclusivo do STM text_video. Converte texto limpo em transcription_words
com timestamps calculados por velocidade de leitura (WPM).

Usa o TimestampGeneratorService já existente.

Saída compatível com o formato AssemblyAI (start/end em segundos),
garantindo que todo o pipeline downstream funcione identicamente.
"""

from ._base import *


@register_step(
    name="generate_timestamps",
    description="Gera timestamps virtuais para texto (text_video mode)",
    category="preprocessing",
    depends_on=["format_script"],
    produces=["transcription_words", "total_duration_ms"],
    estimated_duration_s=2,
    cost_category="cpu",
    retryable=True,
    sse_step_name="GENERATE_TIMESTAMPS",
    tool_schema={
        "description": "Gera timestamps artificiais para texto sem áudio",
        "input": {"speed": "str? (very_slow|slow|normal|fast|very_fast)"},
        "output": {"word_count": "int", "total_duration_ms": "int"}
    }
)
def generate_timestamps_step(state: PipelineState, params: dict) -> PipelineState:
    # Usar clean_text (do format_script) ou transcription_text como fallback
    text = state.clean_text or state.transcription_text or ""

    if not text.strip():
        raise ValueError("Sem texto para gerar timestamps (clean_text e transcription_text vazios)")

    from ..services.timestamp_generator_service import TimestampGeneratorService

    speed = params.get('speed') or (state.options or {}).get('reading_speed', 'normal')
    service = TimestampGeneratorService(speed=speed)

    result = service.generate_timestamps(text)

    if not result.get("phrases"):
        raise ValueError("TimestampGeneratorService retornou 0 frases")

    # Converter phrases em transcription_words (formato flat, compatível com fraseamento)
    # O fraseamento espera uma lista flat de words [{text, start, end}]
    all_words = []
    for phrase in result["phrases"]:
        for word in phrase.get("words", []):
            all_words.append({
                "text": word["text"],
                "start": word["start"],
                "end": word["end"],
            })

    total_duration_ms = result.get("total_duration_ms", 0)

    logger.info(
        f"✅ [GENERATE_TIMESTAMPS] {len(all_words)} palavras, "
        f"{result['phrase_count']} frases, "
        f"duração={total_duration_ms}ms ({total_duration_ms / 1000:.1f}s), "
        f"speed={speed}"
    )

    return state.with_updates(
        transcription_words=all_words,
        transcription_text=text,
        total_duration_ms=total_duration_ms,
    )
