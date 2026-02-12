"""
Utilitários de fila Redis para o Video Orchestrator.

Funções:
- enqueue_job: enfileira job para execute_pipeline (Fase 1)
- enqueue_continue_job: enfileira job para continue_pipeline (Fase 2)
- enqueue_replay_job: enfileira job para replay_pipeline (Pipeline Replay)
- get_redis_client: obtém conexão Redis
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

QUEUE_NAME = 'video_orchestrator'


def get_redis_client():
    """
    Obtém conexão Redis.
    
    Returns:
        Redis client conectado, ou None se indisponível
    """
    try:
        from redis import Redis

        redis_host = os.environ.get('REDIS_HOST', 'localhost')
        redis_port = int(os.environ.get('REDIS_PORT', 6379))
        redis_password = os.environ.get('REDIS_PASSWORD', None)

        client = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )
        client.ping()
        return client

    except Exception as e:
        logger.warning(f"⚠️ Falha ao conectar ao Redis: {e}")
        return None


def enqueue_job(job_id: str) -> bool:
    """
    Enfileira job para execute_pipeline (Fase 1 ou pipeline completo).
    
    Mensagem: string pura com job_id (retrocompatível com worker).
    
    Returns:
        True se enfileirou com sucesso, False caso contrário
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        client.rpush(QUEUE_NAME, job_id)
        queue_size = client.llen(QUEUE_NAME)
        logger.info(f"📤 Job {job_id[:8]}... enfileirado no Redis "
                     f"(fila: {queue_size})")
        return True

    except Exception as e:
        logger.warning(f"⚠️ Falha ao enfileirar job no Redis: {e}")
        return False


def enqueue_continue_job(job_id: str) -> bool:
    """
    Enfileira job para continue_pipeline (Fase 2).
    
    Mensagem: JSON com action e job_id, para que o worker diferencie
    de um job normal (execute_pipeline).
    
    Returns:
        True se enfileirou com sucesso, False caso contrário
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        message = json.dumps({
            'action': 'continue_pipeline',
            'job_id': job_id
        })

        client.rpush(QUEUE_NAME, message)
        queue_size = client.llen(QUEUE_NAME)
        logger.info(f"📤 [CONTINUE] Job {job_id[:8]}... enfileirado no Redis "
                     f"(fila: {queue_size})")
        return True

    except Exception as e:
        logger.warning(f"⚠️ Falha ao enfileirar continue job no Redis: {e}")
        return False


def enqueue_replay_job(job_id: str) -> bool:
    """
    🆕 v3.10.0: Enfileira job para replay_pipeline (Pipeline Replay).
    
    O worker reconhece a action 'replay_pipeline' e chama
    bridge.replay_pipeline() com os parâmetros salvos no job.options.
    
    Returns:
        True se enfileirou com sucesso, False caso contrário
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        message = json.dumps({
            'action': 'replay_pipeline',
            'job_id': job_id
        })

        client.rpush(QUEUE_NAME, message)
        queue_size = client.llen(QUEUE_NAME)
        logger.info(f"📤 [REPLAY] Job {job_id[:8]}... enfileirado no Redis "
                     f"(fila: {queue_size})")
        return True

    except Exception as e:
        logger.warning(f"⚠️ Falha ao enfileirar replay job no Redis: {e}")
        return False
