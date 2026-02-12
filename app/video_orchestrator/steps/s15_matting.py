"""Step 15: Matting (background removal / person overlay)."""

import hashlib
import json
from ._base import *

@register_step(
    name="matting",
    description="Remove fundo das frases com person_overlay (foreground extraction)",
    category="rendering",
    depends_on=["classify"],
    produces=["matting_segments", "foreground_segments", "matting_config_hash"],
    optional=True,
    estimated_duration_s=120,
    cost_category="gpu",
    retryable=True,
    max_retries=2,
    timeout_s=600,
    sse_step_name="MATTING",
    async_mode=True,  # 🆕 v4.3.0: Roda em thread separada (GPU Modal, IO-bound)
    tool_schema={
        "description": "Extrai foreground (pessoa) do vídeo para person overlay",
        "input": {"force_reprocess": "bool?"},
        "output": {"clips_processed": "int", "mode": "str", "status": "str"}
    }
)
def matting_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.phrase_groups:
        logger.info("⏭️ [MATTING] Sem phrase_groups")
        return state

    # Verificar se há frases com person_overlay
    phrases_with_matting = [
        p for p in state.phrase_groups
        if p.get('person_overlay_enabled', False)
    ]

    if not phrases_with_matting:
        logger.info("⏭️ [MATTING] Nenhuma frase com person_overlay_enabled")
        return state

    # Verificar cache via matting_config_hash
    if not params.get('force_reprocess') and state.matting_config_hash:
        current_hash = _compute_matting_hash(state)
        if current_hash == state.matting_config_hash:
            logger.info("⏭️ [MATTING] Cache válido (matting_config_hash inalterado)")
            return state

    from ..services.matting_orchestrator.matting_orchestrator_service import MattingOrchestratorService
    from ..jobs import get_job_manager

    job_manager = get_job_manager()  # Singleton com db_connection_func
    orchestrator = MattingOrchestratorService(
        job_manager=job_manager,
    )

    logger.info(f"🎭 [MATTING] Processando {len(phrases_with_matting)} frases com person_overlay...")

    # Obter o job para o MattingOrchestratorService (compatibilidade)
    job = job_manager.get_job(state.job_id)
    if not job:
        raise ValueError(f"Job {state.job_id} não encontrado para matting")

    result = orchestrator.execute_matting(
        job_id=state.job_id,
        phrase_groups=state.phrase_groups,
        job=job,
        template_config=state.template_config or {},
        speech_segments=state.speech_segments,
    )

    if result.get('status') == 'error':
        raise Exception(f"Matting falhou: {result.get('error', 'desconhecido')}")

    new_hash = _compute_matting_hash(state)

    logger.info(f"✅ [MATTING] {result.get('clips_processed', 0)} clips | "
                f"Modo: {result.get('mode', '?')} | "
                f"Tempo: {result.get('time', 0):.1f}s")

    # Obter matting data do job atualizado (MattingOrchestratorService salva no job)
    job_updated = job_manager.get_job(state.job_id, force_reload=True)

    # 🔧 v3.5.0: Ler AMBOS matting_segments (timing) e foreground_segments (URLs do Modal)
    # Antes só lia matting_segments e copiava para foreground_segments (bug!)
    job_matting_segments = getattr(job_updated, 'matting_segments', None) or []
    job_foreground_segments = getattr(job_updated, 'foreground_segments', None) or []

    logger.info(f"📊 [MATTING] Job data: matting_segments={len(job_matting_segments)}, "
                f"foreground_segments={len(job_foreground_segments)}")

    return state.with_updates(
        matting_segments=job_matting_segments,
        foreground_segments=job_foreground_segments,
        matted_video_url=getattr(job_updated, 'matted_video_url', None),
        matting_config_hash=new_hash,
    )


def _compute_matting_hash(state: PipelineState) -> str:
    """Computa hash da configuração de matting para invalidação de cache."""
    data = {
        'phrases_with_overlay': [
            {
                'idx': i,
                'person_overlay_enabled': p.get('person_overlay_enabled'),
                'start_time': p.get('start_time'),
                'end_time': p.get('end_time'),
            }
            for i, p in enumerate(state.phrase_groups or [])
            if p.get('person_overlay_enabled', False)
        ],
        'template_id': state.template_id,
    }
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()[:12]
