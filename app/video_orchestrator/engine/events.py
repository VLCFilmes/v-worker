"""
Engine Events - Wrapper do sistema SSE existente (pipeline_events.py).

Fornece interface limpa para o PipelineEngine emitir eventos
sem acoplar ao sistema SSE diretamente.
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class EngineEvents:
    """
    Wrapper para emissão de eventos SSE.
    
    Delega para pipeline_events.py existente.
    Todos os métodos são fire-and-forget (não propagam exceções).
    """

    def job_start(self, job_id: str, total_steps: int = None,
                  metadata: Dict[str, Any] = None) -> None:
        """Emite evento de início do job."""
        try:
            from ..pipeline_events import emit_job_start
            emit_job_start(job_id, total_steps=total_steps, metadata=metadata)
        except Exception as e:
            logger.warning(f"⚠️ [EVENTS] Falha em job_start: {e}")

    def job_complete(self, job_id: str, video_url: str,
                     duration_ms: int = None,
                     metadata: Dict[str, Any] = None) -> None:
        """Emite evento de conclusão do job."""
        try:
            from ..pipeline_events import emit_job_complete
            emit_job_complete(job_id, video_url, duration_ms=duration_ms,
                              metadata=metadata)
        except Exception as e:
            logger.warning(f"⚠️ [EVENTS] Falha em job_complete: {e}")

    def job_error(self, job_id: str, error: str, step: str = None) -> None:
        """Emite evento de erro do job."""
        try:
            from ..pipeline_events import emit_job_error
            emit_job_error(job_id, error, step=step)
        except Exception as e:
            logger.warning(f"⚠️ [EVENTS] Falha em job_error: {e}")

    def step_start(self, job_id: str, step: str,
                   metadata: Dict[str, Any] = None) -> None:
        """Emite evento de início de step."""
        try:
            from ..pipeline_events import emit_step_start
            emit_step_start(job_id, step, metadata=metadata)
        except Exception as e:
            logger.warning(f"⚠️ [EVENTS] Falha em step_start: {e}")

    def step_progress(self, job_id: str, step: str,
                      current: int = None, total: int = None,
                      percent: int = None,
                      metadata: Dict[str, Any] = None) -> None:
        """Emite evento de progresso de step."""
        try:
            from ..pipeline_events import emit_step_progress
            emit_step_progress(job_id, step, current=current, total=total,
                               percent=percent, metadata=metadata)
        except Exception as e:
            logger.warning(f"⚠️ [EVENTS] Falha em step_progress: {e}")

    def step_complete(self, job_id: str, step: str,
                      duration_ms: int = None,
                      metadata: Dict[str, Any] = None) -> None:
        """Emite evento de conclusão de step."""
        try:
            from ..pipeline_events import emit_step_complete
            emit_step_complete(job_id, step, duration_ms=duration_ms,
                               metadata=metadata)
        except Exception as e:
            logger.warning(f"⚠️ [EVENTS] Falha em step_complete: {e}")

    def step_error(self, job_id: str, step: str, error: str,
                   metadata: Dict[str, Any] = None) -> None:
        """Emite evento de erro em step."""
        try:
            from ..pipeline_events import emit_step_error
            emit_step_error(job_id, step, error, metadata=metadata)
        except Exception as e:
            logger.warning(f"⚠️ [EVENTS] Falha em step_error: {e}")
