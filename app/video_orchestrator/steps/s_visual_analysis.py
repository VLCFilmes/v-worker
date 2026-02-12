"""Step: Visual Analysis — v-vision-diretor (Modal GPU).

Envia URL do vídeo normalizado para o v-vision-diretor no Modal.
O Modal faz download, extrai frames (ffmpeg), analisa com
Qwen2.5-VL-7B-Instruct e retorna análise visual estruturada.

Nenhum processamento de vídeo é feito na v-api.

Este step é OPCIONAL. Ativado quando vision_analysis=true
no project_config (opt-in pelo chatbot pré-pipeline).

Posição no pipeline: após silence_cut, antes de transcribe
(roda na lista ALL_STEPS_WITH_VISUAL do auto_runner).
"""

from ._base import *

@register_step(
    name="visual_analysis",
    description="Analisa vídeo com Vision LLM (Modal GPU): enquadramento, câmera, conteúdo, cortes",
    category="creative",
    depends_on=["normalize"],
    produces=["visual_analysis", "shot_list", "edit_decision_list", "content_type_detected"],
    optional=True,
    estimated_duration_s=60,
    cost_category="llm",
    retryable=True,
    max_retries=2,
    timeout_s=300,
    sse_step_name="VISUAL_ANALYSIS",
    tool_schema={
        "description": (
            "Envia vídeo para o v-vision-diretor (Modal GPU). Analisa frames com "
            "Qwen2.5-VL-7B: enquadramento (close-up, medium, wide), câmera "
            "(pan, tilt, zoom, static), conteúdo (pessoa, produto, tela), "
            "sugere pontos de corte e detecta tipo de conteúdo."
        ),
        "input": {
            "num_frames": "int? (default: 8, frames extraídos pelo Modal)",
            "include_transcription": "bool? (default: true, envia transcrição para melhorar análise)",
        },
        "output": {
            "shots": "int (quantidade de shots detectados)",
            "cuts": "int (sugestões de corte)",
            "content_type": "str (tipo de conteúdo detectado)",
            "gpu_seconds": "float (tempo GPU no Modal)",
            "status": "str",
        }
    }
)
def visual_analysis_step(state: PipelineState, params: dict) -> PipelineState:
    # Determinar URL do vídeo para análise
    video_url = (
        state.normalized_video_url
        or state.get_video_url_for_processing()
    )
    if not video_url:
        raise ValueError("Sem URL de vídeo para análise visual")

    from ..services.visual_director_service import VisualDirectorService

    service = VisualDirectorService()

    # Duração
    duration_ms = state.total_duration_ms or 0

    # Transcrição (opcional, melhora qualidade da análise)
    transcription_text = None
    transcription_words = None
    include_transcription = params.get('include_transcription', True)

    if include_transcription and state.transcription_text:
        transcription_text = state.transcription_text
        transcription_words = state.transcription_words
        logger.info(f"👁️ [VISUAL] Incluindo transcrição na análise "
                     f"({len(transcription_text)} chars)")

    # Opções extras
    options = {}
    num_frames = params.get('num_frames')
    if num_frames:
        options['num_frames'] = num_frames

    logger.info(f"👁️ [VISUAL] Enviando vídeo para Modal (v-vision-diretor)...")
    logger.info(f"   Vídeo: {video_url[:80]}...")
    logger.info(f"   Duração: {duration_ms}ms")

    result = service.analyze(
        video_url=video_url,
        duration_ms=duration_ms,
        transcription_text=transcription_text,
        transcription_words=transcription_words,
        options=options if options else None,
    )

    if result.get('status') != 'success':
        error = result.get('error', 'desconhecido')
        details = result.get('details', '')
        raise Exception(f"Análise visual falhou: {error} | {details}")

    shots = result.get('shots', [])
    cuts = result.get('cut_suggestions', [])
    edl = result.get('edit_decision_list', [])
    content_type = result.get('content_type', 'unknown')

    logger.info(f"✅ [VISUAL] Análise concluída (Modal GPU):")
    logger.info(f"   Content type: {content_type}")
    logger.info(f"   Shots: {len(shots)}")
    logger.info(f"   Cortes sugeridos: {len(cuts)}")
    logger.info(f"   EDL: {len(edl)} entradas")
    logger.info(f"   GPU: {result.get('gpu_seconds', 0):.1f}s")
    logger.info(f"   Tokens: {result.get('tokens_in', 0)}→{result.get('tokens_out', 0)}")
    logger.info(f"   Resumo: {result.get('visual_summary', '')[:120]}...")

    return state.with_updates(
        visual_analysis=result,
        shot_list=shots,
        edit_decision_list=edl,
        content_type_detected=content_type,
    )
