"""
Step 06: Transcrição de áudio (AssemblyAI ou Whisper).

v4.1: SEMPRE transcreve o vídeo processado (phase1_video_url).
Não reutiliza transcrições do upload porque os timestamps precisam
corresponder ao vídeo após silence_cut + concat_plates.
"""

from ._base import *


@register_step(
    name="transcribe",
    description="Transcreve áudio do vídeo para texto com word-level timestamps",
    category="preprocessing",
    depends_on=["silence_cut"],
    produces=["transcription_text", "transcription_words", "total_duration_ms"],
    estimated_duration_s=60,
    cost_category="cpu",
    retryable=True,
    max_retries=3,
    timeout_s=300,
    sse_step_name="TRANSCRIBE",
    tool_schema={
        "description": "Transcreve áudio para texto com timestamps por palavra",
        "input": {"language": "str?"},
        "output": {"text_length": "int", "word_count": "int", "duration_ms": "int"}
    }
)
def transcribe_step(state: PipelineState, params: dict) -> PipelineState:
    # ─── v4.1: Sempre transcrever o vídeo processado ───
    audio_url = state.get_audio_url_for_transcription()
    if not audio_url:
        raise ValueError("Nenhuma URL de áudio para transcrição")

    logger.info(f"🎙️ [TRANSCRIBE] URL: {audio_url[:80]}...")

    language = params.get('language', 'pt')

    # Tentar AssemblyAI primeiro, fallback para Whisper
    assembly_key = get_env('ASSEMBLY_API_KEY')
    if assembly_key:
        return _transcribe_assembly(state, audio_url, language)
    else:
        return _transcribe_whisper(state, audio_url, language)


def _transcribe_assembly(state: PipelineState, audio_url: str, language: str) -> PipelineState:
    """Transcrição via AssemblyAI."""
    from ..services.assembly_service import AssemblyAIService

    service = AssemblyAIService()
    result = service.transcribe(audio_url)

    if not result or result.get('error'):
        raise Exception(f"AssemblyAI falhou: {result.get('error', 'sem resultado')}")

    text = result.get('text', '')
    words = result.get('words', [])
    duration_ms = result.get('audio_duration', 0)
    if isinstance(duration_ms, float):
        duration_ms = int(duration_ms * 1000)

    logger.info(f"✅ [TRANSCRIBE] AssemblyAI: {len(words)} palavras, {len(text)} chars")

    return state.with_updates(
        transcription_text=text,
        transcription_words=words,
        total_duration_ms=duration_ms or state.total_duration_ms,
    )


def _transcribe_whisper(state: PipelineState, audio_url: str, language: str) -> PipelineState:
    """Transcrição via Whisper (fallback)."""
    from ..services.transcription_service import TranscriptionService

    service = TranscriptionService()
    result = service.transcribe_sync(audio_url=audio_url, language=language)

    if not result or result.get('error'):
        raise Exception(f"Whisper falhou: {result.get('error', 'sem resultado')}")

    text = result.get('text', '')
    words = result.get('words', [])
    duration_ms = result.get('duration_ms', 0)

    logger.info(f"✅ [TRANSCRIBE] Whisper: {len(words)} palavras, {len(text)} chars")

    return state.with_updates(
        transcription_text=text,
        transcription_words=words,
        total_duration_ms=duration_ms or state.total_duration_ms,
    )
