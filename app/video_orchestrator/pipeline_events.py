"""
📺 Pipeline Events - Helper para emitir eventos SSE do Pipeline

Este módulo é a interface entre o orchestrator e o sistema SSE.
Usa display_config para sanitizar e formatar as mensagens.

Uso:
    from .pipeline_events import emit_step_start, emit_step_progress, emit_step_complete

    emit_step_start(job_id, 'PNG_GEN')
    emit_step_progress(job_id, 'PNG_GEN', current=44, total=88, metadata={'word': 'Fala'})
    emit_step_complete(job_id, 'PNG_GEN', metadata={'count': 88})
"""

import logging
from typing import Dict, Any, Optional

from ..routes.sse_stream import emit_job_event
from .display_config import create_display_event, get_log_lines, PIPELINE_STEPS

logger = logging.getLogger(__name__)

# =============================================================================
# 📤 FUNÇÕES DE EMISSÃO
# =============================================================================

def emit_step_start(job_id: str, step: str, metadata: Dict[str, Any] = None) -> None:
    """
    Emite evento de início de etapa.
    
    Args:
        job_id: ID do job
        step: Nome da etapa (ex: 'PNG_GEN', 'TRANSCRIBE')
        metadata: Dados extras opcionais
    """
    try:
        event = create_display_event(
            step=step,
            event_type='start',
            metadata=metadata
        )
        
        emit_job_event(job_id, 'step_start', event)
        logger.info(f"📡 [SSE] {job_id[:8]}... step_start: {step}")
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir step_start: {e}")


def emit_step_progress(
    job_id: str,
    step: str,
    current: int = None,
    total: int = None,
    percent: int = None,
    metadata: Dict[str, Any] = None
) -> None:
    """
    Emite evento de progresso de etapa.
    
    Args:
        job_id: ID do job
        step: Nome da etapa
        current: Item atual
        total: Total de itens
        percent: Porcentagem (0-100)
        metadata: Dados extras (word, preview_url, etc.)
    """
    try:
        event = create_display_event(
            step=step,
            event_type='progress',
            current=current,
            total=total,
            percent=percent,
            metadata=metadata
        )
        
        emit_job_event(job_id, 'step_progress', event)
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir step_progress: {e}")


def emit_step_complete(
    job_id: str,
    step: str,
    duration_ms: int = None,
    metadata: Dict[str, Any] = None
) -> None:
    """
    Emite evento de conclusão de etapa.
    
    Args:
        job_id: ID do job
        step: Nome da etapa
        duration_ms: Duração em milissegundos
        metadata: Dados extras (count, stats, etc.)
    """
    try:
        event = create_display_event(
            step=step,
            event_type='complete',
            metadata=metadata
        )
        
        if duration_ms:
            event['duration_ms'] = duration_ms
        
        emit_job_event(job_id, 'step_complete', event)
        logger.info(f"📡 [SSE] {job_id[:8]}... step_complete: {step}")
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir step_complete: {e}")


def emit_step_error(
    job_id: str,
    step: str,
    error: str,
    metadata: Dict[str, Any] = None
) -> None:
    """
    Emite evento de erro em etapa.
    
    Args:
        job_id: ID do job
        step: Nome da etapa
        error: Mensagem de erro (será sanitizada)
        metadata: Dados extras
    """
    try:
        event = create_display_event(
            step=step,
            event_type='error',
            metadata={'error': error, **(metadata or {})}
        )
        
        emit_job_event(job_id, 'step_error', event)
        logger.debug(f"📡 [SSE] {job_id[:8]}... step_error: {step}")
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir step_error: {e}")


def emit_job_start(job_id: str, total_steps: int = None, metadata: Dict[str, Any] = None) -> None:
    """
    Emite evento de início do job.
    
    Args:
        job_id: ID do job
        total_steps: Número total de etapas
        metadata: Dados extras
    """
    try:
        event = {
            'job_id': job_id,
            'status': 'processing',
            'message': '🎬 Iniciando processamento...',
        }
        
        if total_steps:
            event['total_steps'] = total_steps
        
        if metadata:
            event['metadata'] = metadata
        
        emit_job_event(job_id, 'job_start', event)
        logger.info(f"📡 [SSE] Job iniciado: {job_id[:8]}...")
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir job_start: {e}")


def emit_job_complete(
    job_id: str,
    video_url: str,
    duration_ms: int = None,
    metadata: Dict[str, Any] = None
) -> None:
    """
    Emite evento de conclusão do job.
    
    Args:
        job_id: ID do job
        video_url: URL do vídeo final
        duration_ms: Duração total em milissegundos
        metadata: Dados extras
    """
    try:
        event = {
            'job_id': job_id,
            'status': 'completed',
            'video_url': video_url,
            'message': '🎉 Vídeo pronto!',
        }
        
        if duration_ms:
            event['duration_ms'] = duration_ms
        
        if metadata:
            event['metadata'] = metadata
        
        emit_job_event(job_id, 'job_complete', event)
        logger.info(f"📡 [SSE] Job completo: {job_id[:8]}...")
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir job_complete: {e}")


def emit_job_error(job_id: str, error: str, step: str = None) -> None:
    """
    Emite evento de erro do job.
    
    Args:
        job_id: ID do job
        error: Mensagem de erro (será sanitizada)
        step: Etapa onde ocorreu o erro
    """
    try:
        # Sanitizar erro para não expor detalhes técnicos
        safe_error = error
        if len(error) > 200:
            safe_error = error[:200] + '...'
        
        # Mensagem amigável para erros comuns
        if 'timeout' in error.lower():
            safe_error = 'O processamento demorou mais que o esperado. Tente novamente.'
        elif 'connection' in error.lower():
            safe_error = 'Erro de conexão com servidor. Tente novamente.'
        elif 'memory' in error.lower():
            safe_error = 'Vídeo muito grande. Tente com um vídeo menor.'
        
        event = {
            'job_id': job_id,
            'status': 'error',
            'error': safe_error,
            'message': '❌ Algo deu errado',
        }
        
        if step:
            event['step'] = step
        
        emit_job_event(job_id, 'job_error', event)
        logger.info(f"📡 [SSE] Job erro: {job_id[:8]}...")
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir job_error: {e}")


def emit_log_line(job_id: str, step: str, line: str) -> None:
    """
    Emite linha de log estilizada (Code Theatre).
    
    Args:
        job_id: ID do job
        step: Nome da etapa
        line: Linha de log formatada
    """
    try:
        event = {
            'step': step,
            'log_line': line,
        }
        
        emit_job_event(job_id, 'log_line', event)
        
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir log_line: {e}")


# =============================================================================
# 🎭 HELPERS PARA CODE THEATRE
# =============================================================================

def emit_code_theatre(job_id: str, step: str, data: Dict[str, Any] = None) -> None:
    """
    Emite linhas de log estilizadas para Code Theatre.
    
    Args:
        job_id: ID do job
        step: Nome da etapa
        data: Dados para interpolação
    """
    try:
        lines = get_log_lines(step, data=data)
        
        for line in lines:
            emit_log_line(job_id, step, line)
            
    except Exception as e:
        logger.warning(f"⚠️ [SSE] Erro ao emitir code_theatre: {e}")
