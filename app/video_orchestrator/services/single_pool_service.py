"""
🎬 Single Pool Service - Pool de workers single para jobs simultâneos

Este serviço gerencia múltiplos containers v-editor-single para processar
jobs em paralelo. Cada container processa 1 job inteiro sozinho.

Diferente do WorkerPoolService que distribui CHUNKS de um job entre workers,
este serviço distribui JOBS inteiros entre workers.

Uso:
    pool = SinglePoolService()
    result = pool.submit_render_job(job_id, payload, user_id, project_id)
"""

import os
import json
import logging
import requests
import threading
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class SingleWorkerInfo:
    """Informações de um worker single"""
    id: str
    name: str
    url: str
    port: int
    is_busy: bool = False
    current_job_id: Optional[str] = None
    last_health_check: Optional[datetime] = None
    is_healthy: bool = True


class SinglePoolService:
    """
    Gerencia pool de workers v-editor-single para jobs simultâneos.
    
    Cada worker processa 1 job inteiro. Distribuição round-robin.
    """
    
    # Configuração dos workers single
    DEFAULT_SINGLES = [
        {"id": "single-1", "name": "v-editor-single-1", "port": 5010},
        {"id": "single-2", "name": "v-editor-single-2", "port": 5011},
        {"id": "single-3", "name": "v-editor-single-3", "port": 5012},
    ]
    
    # 🆕 Suporte a workers remotos via Tailscale
    REMOTE_BASE_URL = os.environ.get('V_EDITOR_BASE_URL', None)
    
    # Callback URL para webhooks
    WEBHOOK_BASE_URL = os.environ.get('WEBHOOK_INTERNAL_URL') or \
                       os.environ.get('CALLBACK_BASE_URL') or \
                       'https://api.vinicius.ai'
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self.workers: List[SingleWorkerInfo] = []
        self._round_robin_index = 0
        self._rr_lock = threading.Lock()
        
        # Inicializar workers
        for w in self.DEFAULT_SINGLES:
            if self.REMOTE_BASE_URL:
                url = f"{self.REMOTE_BASE_URL}:{w['port']}"
            else:
                url = f"http://localhost:{w['port']}"
            
            self.workers.append(SingleWorkerInfo(
                id=w['id'],
                name=w['name'],
                url=url,
                port=w['port']
            ))
        
        logger.info(f"🎬 SinglePoolService inicializado com {len(self.workers)} workers")
        for w in self.workers:
            logger.info(f"   Single {w.id}: {w.name} @ {w.url}")
        
        self._initialized = True
    
    def _get_next_worker(self) -> SingleWorkerInfo:
        """Retorna o próximo worker disponível (round-robin)"""
        with self._rr_lock:
            # Tentar encontrar um worker livre
            for _ in range(len(self.workers)):
                worker = self.workers[self._round_robin_index]
                self._round_robin_index = (self._round_robin_index + 1) % len(self.workers)
                
                if not worker.is_busy and worker.is_healthy:
                    return worker
            
            # Se todos estão ocupados, retorna o próximo mesmo assim
            # (o v-editor vai enfileirar internamente)
            worker = self.workers[self._round_robin_index]
            self._round_robin_index = (self._round_robin_index + 1) % len(self.workers)
            return worker
    
    def _health_check(self, worker: SingleWorkerInfo) -> bool:
        """Verifica saúde de um worker"""
        try:
            response = requests.get(f"{worker.url}/health", timeout=5)
            worker.is_healthy = response.status_code == 200
            worker.last_health_check = datetime.now()
            return worker.is_healthy
        except Exception as e:
            logger.warning(f"⚠️ Health check falhou para {worker.name}: {e}")
            worker.is_healthy = False
            return False
    
    def check_all_health(self) -> Dict[str, bool]:
        """Verifica saúde de todos os workers"""
        results = {}
        for worker in self.workers:
            results[worker.name] = self._health_check(worker)
        return results
    
    def submit_render_job(
        self,
        job_id: str,
        payload: Dict[str, Any],
        user_id: str,
        project_id: str,
        template_id: str = None,
        callback_endpoint: str = "/api/webhook/render-complete"
    ) -> Dict[str, Any]:
        """
        Submete um job para renderização em um dos workers single.
        
        Args:
            job_id: ID do job
            payload: Payload completo para o v-editor
            user_id: ID do usuário
            project_id: ID do projeto
            template_id: ID do template (opcional)
            callback_endpoint: Endpoint para webhook de conclusão
            
        Returns:
            Dict com status e informações do job
        """
        logger.info(f"📤 [SINGLE POOL] Submetendo job {job_id[:8]}...")
        
        # Selecionar worker
        worker = self._get_next_worker()
        logger.info(f"   🎯 Worker selecionado: {worker.name} @ {worker.url}")
        
        # Marcar como ocupado
        worker.is_busy = True
        worker.current_job_id = job_id
        
        try:
            # Construir webhook URL
            webhook_url = f"{self.WEBHOOK_BASE_URL}{callback_endpoint}"
            
            # 🆕 v2.9.105: Usar RenderService para formatar payload corretamente
            # O v-editor espera project_settings e tracks, não payload aninhado
            from .render_service import RenderService
            render_service = RenderService(v_editor_url=worker.url)
            
            # Usar o método _build_render_payload do RenderService
            render_payload = render_service._build_render_payload(
                job_id=job_id,
                payload=payload,
                user_id=user_id,
                project_id=project_id,
                template_id=template_id,
                webhook_url=webhook_url
            )
            
            # Adicionar identificação do worker
            render_payload["worker_name"] = worker.name
            
            # Enviar para o worker
            response = requests.post(
                f"{worker.url}/render-video",
                json=render_payload,
                timeout=30
            )
            
            if response.status_code in [200, 202]:
                result = response.json()
                logger.info(f"   ✅ Job aceito por {worker.name}")
                return {
                    "success": True,
                    "job_id": job_id,
                    "worker": worker.name,
                    "worker_url": worker.url,
                    "response": result
                }
            else:
                logger.error(f"   ❌ Erro do worker: {response.status_code} - {response.text[:200]}")
                worker.is_busy = False
                worker.current_job_id = None
                return {
                    "success": False,
                    "job_id": job_id,
                    "worker": worker.name,
                    "error": f"HTTP {response.status_code}: {response.text[:200]}"
                }
                
        except Exception as e:
            logger.error(f"   ❌ Erro ao enviar para {worker.name}: {e}")
            worker.is_busy = False
            worker.current_job_id = None
            
            # Tentar outro worker
            for backup_worker in self.workers:
                if backup_worker.id != worker.id and not backup_worker.is_busy:
                    logger.info(f"   🔄 Tentando worker backup: {backup_worker.name}")
                    return self._submit_to_worker(backup_worker, job_id, payload, user_id, project_id, template_id, callback_endpoint)
            
            return {
                "success": False,
                "job_id": job_id,
                "worker": worker.name,
                "error": str(e)
            }
    
    def _submit_to_worker(
        self,
        worker: SingleWorkerInfo,
        job_id: str,
        payload: Dict[str, Any],
        user_id: str,
        project_id: str,
        template_id: str,
        callback_endpoint: str
    ) -> Dict[str, Any]:
        """Helper para submeter a um worker específico"""
        worker.is_busy = True
        worker.current_job_id = job_id
        
        try:
            webhook_url = f"{self.WEBHOOK_BASE_URL}{callback_endpoint}"
            
            # 🆕 v2.9.105: Usar RenderService para formatar payload corretamente
            from .render_service import RenderService
            render_service = RenderService(v_editor_url=worker.url)
            
            render_payload = render_service._build_render_payload(
                job_id=job_id,
                payload=payload,
                user_id=user_id,
                project_id=project_id,
                template_id=template_id,
                webhook_url=webhook_url
            )
            render_payload["worker_name"] = worker.name
            
            response = requests.post(
                f"{worker.url}/render-video",
                json=render_payload,
                timeout=30
            )
            
            if response.status_code in [200, 202]:
                result = response.json()
                logger.info(f"   ✅ Job aceito por {worker.name} (backup)")
                return {
                    "success": True,
                    "job_id": job_id,
                    "worker": worker.name,
                    "worker_url": worker.url,
                    "response": result
                }
            else:
                worker.is_busy = False
                worker.current_job_id = None
                return {
                    "success": False,
                    "job_id": job_id,
                    "worker": worker.name,
                    "error": f"HTTP {response.status_code}"
                }
        except Exception as e:
            worker.is_busy = False
            worker.current_job_id = None
            return {
                "success": False,
                "job_id": job_id,
                "worker": worker.name,
                "error": str(e)
            }
    
    def mark_job_complete(self, job_id: str):
        """Marca um job como completo, liberando o worker"""
        for worker in self.workers:
            if worker.current_job_id == job_id:
                logger.info(f"   ✅ Worker {worker.name} liberado (job {job_id[:8]} completo)")
                worker.is_busy = False
                worker.current_job_id = None
                return
    
    def get_status(self) -> Dict[str, Any]:
        """Retorna status do pool"""
        return {
            "total_workers": len(self.workers),
            "busy_workers": sum(1 for w in self.workers if w.is_busy),
            "healthy_workers": sum(1 for w in self.workers if w.is_healthy),
            "workers": [
                {
                    "id": w.id,
                    "name": w.name,
                    "url": w.url,
                    "is_busy": w.is_busy,
                    "current_job": w.current_job_id[:8] if w.current_job_id else None,
                    "is_healthy": w.is_healthy
                }
                for w in self.workers
            ]
        }


# Singleton instance
_single_pool_instance = None

def get_single_pool() -> SinglePoolService:
    """Retorna instância singleton do SinglePoolService"""
    global _single_pool_instance
    if _single_pool_instance is None:
        _single_pool_instance = SinglePoolService()
    return _single_pool_instance
