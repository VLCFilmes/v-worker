"""
📺 SSE Stream Endpoint
Endpoint Server-Sent Events para Pipeline Visualizer em tempo real.

Envia eventos:
- step_start: Início de um step
- step_progress: Progresso de um step
- step_complete: Step finalizado
- step_error: Erro em um step
- job_complete: Job finalizado com sucesso
- job_error: Job falhou

Frontend conecta via:
const eventSource = new EventSource('/api/video/job/{jobId}/stream');
"""

import json
import time
import queue
import threading
from typing import Generator, Dict, Any, Optional
from flask import Blueprint, Response, request, g
from functools import wraps
import logging
import redis

logger = logging.getLogger(__name__)

# Blueprint
sse_bp = Blueprint('sse', __name__)

# Redis para pub/sub (se disponível)
try:
    import os
    REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
    REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
    REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
    redis_client = redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    redis_client.ping()
    REDIS_AVAILABLE = True
    logger.info("✅ SSE: Redis disponível para pub/sub")
except Exception as e:
    REDIS_AVAILABLE = False
    logger.warning(f"⚠️ SSE: Redis não disponível, usando fallback polling: {e}")

# In-memory event storage (fallback quando Redis não disponível)
# job_id -> list of events
_event_store: Dict[str, list] = {}
_event_store_lock = threading.Lock()


def format_sse(data: Dict[str, Any], event: Optional[str] = None) -> str:
    """
    Formata dados para SSE.
    
    Args:
        data: Dados a enviar
        event: Nome do evento (opcional)
    
    Returns:
        String formatada para SSE
    """
    msg = ""
    if event:
        msg += f"event: {event}\n"
    msg += f"data: {json.dumps(data)}\n\n"
    return msg


def emit_job_event(job_id: str, event_type: str, data: Dict[str, Any]) -> None:
    """
    Emite evento para um job específico.
    
    Args:
        job_id: ID do job
        event_type: Tipo do evento (step_start, step_progress, etc.)
        data: Dados do evento
    """
    event_data = {
        "event": event_type,
        "timestamp": time.time(),
        **data
    }
    
    if REDIS_AVAILABLE:
        # Publicar no Redis
        channel = f"job:{job_id}:events"
        try:
            redis_client.publish(channel, json.dumps(event_data))
            logger.info(f"📡 SSE event published to Redis: {event_type} for job {job_id[:8]}...")
        except Exception as e:
            logger.error(f"❌ Erro ao publicar evento SSE: {e}")
    else:
        # Fallback: armazenar em memória
        with _event_store_lock:
            if job_id not in _event_store:
                _event_store[job_id] = []
            _event_store[job_id].append(event_data)
            # Limitar a 1000 eventos por job
            if len(_event_store[job_id]) > 1000:
                _event_store[job_id] = _event_store[job_id][-500:]


def get_job_events_generator(job_id: str, timeout: int = 300) -> Generator[str, None, None]:
    """
    Generator que produz eventos SSE para um job.
    
    Args:
        job_id: ID do job
        timeout: Timeout em segundos
    
    Yields:
        Strings formatadas para SSE
    """
    start_time = time.time()
    last_event_index = 0
    
    # Enviar evento de conexão
    yield format_sse({"status": "connected", "job_id": job_id}, "connection")
    
    if REDIS_AVAILABLE:
        # Usar Redis pub/sub
        pubsub = redis_client.pubsub()
        channel = f"job:{job_id}:events"
        pubsub.subscribe(channel)
        
        try:
            while time.time() - start_time < timeout:
                message = pubsub.get_message(timeout=1.0)
                
                if message and message['type'] == 'message':
                    event_data = json.loads(message['data'])
                    yield format_sse(event_data, event_data.get('event', 'message'))
                    
                    # Se job completou ou erro, encerrar
                    if event_data.get('event') in ('job_complete', 'job_error'):
                        break
                else:
                    # Enviar heartbeat a cada 15 segundos
                    yield format_sse({"heartbeat": True}, "heartbeat")
        finally:
            pubsub.unsubscribe(channel)
            pubsub.close()
    else:
        # Fallback: polling da memória
        while time.time() - start_time < timeout:
            with _event_store_lock:
                events = _event_store.get(job_id, [])
                new_events = events[last_event_index:]
                last_event_index = len(events)
            
            for event_data in new_events:
                yield format_sse(event_data, event_data.get('event', 'message'))
                
                # Se job completou ou erro, encerrar
                if event_data.get('event') in ('job_complete', 'job_error'):
                    return
            
            # Heartbeat
            yield format_sse({"heartbeat": True}, "heartbeat")
            time.sleep(1)
    
    # Timeout
    yield format_sse({"status": "timeout"}, "timeout")


def verify_api_key(func):
    """
    Decorator para verificar autenticação via apikey.
    Aceita service_role key do Supabase/Kong.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        apikey = request.headers.get('apikey')
        auth_header = request.headers.get('Authorization')
        
        # Verificar se tem alguma forma de autenticação
        if not apikey and not auth_header:
            logger.warning("🔒 SSE: Tentativa de conexão sem autenticação")
            return Response(
                format_sse({"error": "Unauthorized", "message": "apikey header required"}, "error"),
                status=401,
                mimetype='text/event-stream'
            )
        
        # Extrair token do Bearer se necessário
        token = apikey
        if not token and auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]
        
        if not token:
            logger.warning("🔒 SSE: Token vazio ou inválido")
            return Response(
                format_sse({"error": "Unauthorized", "message": "Invalid token"}, "error"),
                status=401,
                mimetype='text/event-stream'
            )
        
        # Verificar se é um token JWT válido (estrutura básica)
        # Em produção, Kong já valida o token antes de chegar aqui
        # Aqui fazemos apenas verificação básica de formato
        parts = token.split('.')
        if len(parts) != 3:
            logger.warning("🔒 SSE: Token com formato inválido")
            return Response(
                format_sse({"error": "Unauthorized", "message": "Invalid token format"}, "error"),
                status=401,
                mimetype='text/event-stream'
            )
        
        # Autenticação OK
        g.authenticated = True
        g.token = token
        
        return func(*args, **kwargs)
    return wrapper


@sse_bp.route('/api/video/job/<job_id>/stream', methods=['GET'])
@verify_api_key
def stream_job_events(job_id: str):
    """
    Endpoint SSE para streaming de eventos de um job.
    
    GET /api/video/job/{job_id}/stream
    
    Headers:
        apikey: Service role key (obrigatório)
        Accept: text/event-stream
    
    Events:
        - connection: Conexão estabelecida
        - heartbeat: Keep-alive
        - step_start: Início de step
        - step_progress: Progresso de step
        - step_complete: Step finalizado
        - step_error: Erro em step
        - job_complete: Job finalizado
        - job_error: Job falhou
        - timeout: Conexão expirou
    """
    logger.info(f"📺 SSE connection opened for job: {job_id} (authenticated)")
    
    # TODO: Verificar se o usuário tem acesso ao job_id
    # Isso requer consultar o banco para ver o owner do job
    # Por enquanto, confiamos que Kong já validou o token
    
    return Response(
        get_job_events_generator(job_id),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',  # Desabilita buffering no nginx
            'Access-Control-Allow-Origin': '*',
        }
    )


# === Helper functions para emitir eventos de cada step ===

def emit_step_start(job_id: str, step: str, total: Optional[int] = None):
    """Emite evento de início de step."""
    emit_job_event(job_id, "step_start", {
        "step": step,
        "total": total
    })


def emit_step_progress(
    job_id: str, 
    step: str, 
    current: int, 
    total: int, 
    preview_url: Optional[str] = None
):
    """Emite evento de progresso."""
    data = {
        "step": step,
        "current": current,
        "total": total,
        "percent": round((current / total) * 100, 1) if total > 0 else 0
    }
    if preview_url:
        data["preview_url"] = preview_url
    
    emit_job_event(job_id, "step_progress", data)


def emit_step_complete(job_id: str, step: str, duration_ms: float):
    """Emite evento de step completo."""
    emit_job_event(job_id, "step_complete", {
        "step": step,
        "duration_ms": duration_ms
    })


def emit_step_error(job_id: str, step: str, error: str, details: Optional[str] = None):
    """Emite evento de erro em step."""
    emit_job_event(job_id, "step_error", {
        "step": step,
        "error": error,
        "details": details
    })


def emit_job_complete(job_id: str, video_url: str, duration_ms: float):
    """Emite evento de job completo."""
    emit_job_event(job_id, "job_complete", {
        "video_url": video_url,
        "duration_ms": duration_ms
    })


def emit_job_error(job_id: str, error: str, step: Optional[str] = None):
    """Emite evento de erro no job."""
    emit_job_event(job_id, "job_error", {
        "error": error,
        "step": step
    })


# === Cleanup ===

def cleanup_job_events(job_id: str):
    """Remove eventos de um job da memória."""
    with _event_store_lock:
        if job_id in _event_store:
            del _event_store[job_id]
