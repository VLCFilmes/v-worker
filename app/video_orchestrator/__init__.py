"""
🎬 Video Orchestrator - Pipeline Engine v3

Motor declarativo de steps para processamento de vídeo:
1. Steps modulares via decorator (@register_step)
2. AutoRunner para fluxo automático (sem LLM)
3. EngineBridge como ponto de entrada unificado
4. JobManager para persistência de jobs
"""

from .endpoints import video_orchestrator_bp
from .jobs import JobManager, JobStatus, get_job_manager
from .engine.bridge import EngineBridge, get_engine_bridge

__all__ = [
    'video_orchestrator_bp',
    'JobManager',
    'JobStatus',
    'get_job_manager',
    'EngineBridge',
    'get_engine_bridge',
]

