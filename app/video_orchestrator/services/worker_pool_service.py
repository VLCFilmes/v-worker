"""
🆕 v2.9.87: Worker Pool Service - Gerencia múltiplos workers v-editor

Este serviço distribui a renderização de vídeos entre múltiplos workers
para paralelizar o processamento e reduzir o tempo de render.

ARQUITETURA (6 workers):
┌──────────────────────────────────────────────────────────────────┐
│                      WorkerPoolService                           │
│               (divide vídeo em N partes)                         │
└──────┬─────┬─────┬─────┬─────┬─────┬─────────────────────────────┘
       │     │     │     │     │     │
  ┌────▼──┐┌─▼───┐┌─▼───┐┌─▼───┐┌─▼───┐┌─▼───┐
  │ w-1   ││ w-2 ││ w-3 ││ w-4 ││ w-5 ││ w-6 │
  │:5018  ││:5019││:5022││:5021││:5023││:5024│
  └───────┘└─────┘└─────┘└─────┘└─────┘└─────┘

FUNCIONAMENTO:
1. Recebe payload completo de renderização
2. Divide em chunks baseado no número de workers disponíveis
3. Envia cada chunk para um worker em paralelo
4. Monitora progresso de todos os workers
5. Concatena chunks finais via v-services
6. Retorna vídeo final
"""

import os
import logging
import requests
import asyncio
import re
from typing import Dict, Any, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import time

logger = logging.getLogger(__name__)

# 🆕 v2.9.67: Mapeamento de URLs externas para internas
# 🔧 v2.9.80: Adicionado services-home.vinicius.ai (Linux Home tunnel)
EXTERNAL_TO_INTERNAL_URL_MAP = {
    "https://services-home.vinicius.ai": "http://v-services:5000",
    "http://services-home.vinicius.ai": "http://v-services:5000",
    "https://services.vinicius.ai": "http://v-services:5000",
    "http://services.vinicius.ai": "http://v-services:5000",
    "https://api.vinicius.ai": "http://supabase-custom-api:5000",
    "http://api.vinicius.ai": "http://supabase-custom-api:5000",
}


@dataclass
class WorkerInfo:
    """Informações de um worker v-editor."""
    id: str
    name: str
    url: str
    port: int
    status: str = "unknown"  # unknown, healthy, busy, unhealthy
    current_job: Optional[str] = None
    last_health_check: Optional[datetime] = None


class WorkerPoolService:
    """
    Gerencia pool de workers v-editor para renderização paralela.
    
    Uso:
        pool = WorkerPoolService()
        result = pool.render_distributed(job_id, payload, user_id, project_id)
    """
    
    # Configuração dos workers
    # Nota: porta 5020 é usada pelo supabase-studio, então worker 3 usa 5022
    # 🆕 v2.9.87: 6 workers para melhor paralelização (limite do servidor: 15GB RAM)
    DEFAULT_WORKERS = [
        {"id": "1", "name": "v-editor-1", "port": 5018},
        {"id": "2", "name": "v-editor-2", "port": 5019},
        {"id": "3", "name": "v-editor-3", "port": 5022},  # 5020 ocupada pelo supabase-studio
        {"id": "4", "name": "v-editor-4", "port": 5021},
        {"id": "5", "name": "v-editor-5", "port": 5023},
        {"id": "6", "name": "v-editor-6", "port": 5024},
    ]
    
    # Usar nomes Docker quando em container, IPs quando fora
    DOCKER_NETWORK_MODE = os.environ.get('DOCKER_NETWORK_MODE', 'true').lower() == 'true'
    
    # 🆕 v2.9.103: Suporte a workers remotos via Tailscale
    # Se V_EDITOR_BASE_URL estiver definido, usa ele como base para todos os workers
    # Exemplo: V_EDITOR_BASE_URL=http://100.89.162.39 -> workers em :5018, :5019, etc.
    REMOTE_BASE_URL = os.environ.get('V_EDITOR_BASE_URL', None)
    
    def __init__(self, workers: List[Dict] = None, max_concurrent_chunks: int = 6):
        """
        Inicializa o pool de workers.
        
        Args:
            workers: Lista de workers customizada (opcional)
            max_concurrent_chunks: Máximo de chunks em paralelo
        """
        self.workers: List[WorkerInfo] = []
        self.max_concurrent_chunks = max_concurrent_chunks
        self.v_services_url = os.environ.get('V_SERVICES_URL', 'http://v-services:5000')
        
        # Inicializar workers
        worker_configs = workers or self.DEFAULT_WORKERS
        for w in worker_configs:
            # 🆕 v2.9.103: Prioridade de URLs
            # 1. Remote via Tailscale (V_EDITOR_BASE_URL)
            # 2. Docker network (nomes de container)
            # 3. Localhost (desenvolvimento)
            if self.REMOTE_BASE_URL:
                url = f"{self.REMOTE_BASE_URL}:{w['port']}"
                logger.info(f"   🌐 Worker {w['id']} usando Tailscale: {url}")
            elif self.DOCKER_NETWORK_MODE:
                url = f"http://{w['name']}:5018"  # Porta interna sempre 5018
            else:
                url = f"http://localhost:{w['port']}"
            
            self.workers.append(WorkerInfo(
                id=w['id'],
                name=w['name'],
                url=url,
                port=w['port']
            ))
        
        logger.info(f"🎬 WorkerPoolService inicializado com {len(self.workers)} workers")
        for w in self.workers:
            logger.info(f"   Worker {w.id}: {w.name} @ {w.url}")
    
    def _cleanup_old_chunks(self, job_id: str) -> None:
        """
        🆕 v2.9.77: Limpa chunks antigos de um job antes de iniciar novo render.
        
        Isso evita que o polling encontre arquivos de renderizações anteriores
        do mesmo job e retorne "completed" prematuramente.
        """
        try:
            # Chamar v-services para limpar os chunks
            response = requests.post(
                f"{self.v_services_url}/ffmpeg/cleanup-chunks",
                json={"job_id": job_id},
                timeout=10
            )
            if response.status_code == 200:
                result = response.json()
                deleted = result.get("deleted_count", 0)
                if deleted > 0:
                    logger.info(f"🧹 [CLEANUP] Removidos {deleted} chunks antigos do job {job_id}")
            else:
                logger.warning(f"⚠️ [CLEANUP] Falhou: {response.status_code}")
        except Exception as e:
            # Não falhar o render por causa de limpeza
            logger.warning(f"⚠️ [CLEANUP] Erro ao limpar chunks: {e}")
    
    def health_check_all(self) -> Dict[str, Any]:
        """
        Verifica saúde de todos os workers.
        
        Returns:
            {
                "healthy_count": N,
                "total_count": M,
                "workers": [
                    {"id": "1", "name": "v-editor-1", "status": "healthy", ...},
                    ...
                ]
            }
        """
        results = {"healthy_count": 0, "total_count": len(self.workers), "workers": []}
        
        for worker in self.workers:
            try:
                response = requests.get(f"{worker.url}/health", timeout=5)
                if response.status_code == 200:
                    worker.status = "healthy"
                    worker.last_health_check = datetime.now()
                    results["healthy_count"] += 1
                else:
                    worker.status = "unhealthy"
            except Exception as e:
                worker.status = "unhealthy"
                logger.warning(f"⚠️ Worker {worker.name} não respondeu: {e}")
            
            results["workers"].append({
                "id": worker.id,
                "name": worker.name,
                "url": worker.url,
                "status": worker.status,
                "last_health_check": worker.last_health_check.isoformat() if worker.last_health_check else None
            })
        
        logger.info(f"🏥 Health check: {results['healthy_count']}/{results['total_count']} workers healthy")
        return results
    
    def get_healthy_workers(self) -> List[WorkerInfo]:
        """Retorna lista de workers saudáveis."""
        self.health_check_all()
        return [w for w in self.workers if w.status == "healthy"]
    
    def _convert_external_to_internal_url(self, url: str) -> str:
        """
        🆕 v2.9.67: Converte URLs externas para URLs internas Docker.
        
        Workers rodam em containers e não conseguem acessar URLs externas
        como services.vinicius.ai - precisam usar nomes de containers.
        
        Args:
            url: URL potencialmente externa
            
        Returns:
            URL convertida para rede interna Docker
        """
        if not url:
            return url
        
        for external, internal in EXTERNAL_TO_INTERNAL_URL_MAP.items():
            if external in url:
                converted = url.replace(external, internal)
                logger.debug(f"🔄 URL convertida: {url[:50]}... → {converted[:50]}...")
                return converted
        
        return url
    
    def _convert_payload_urls_recursive(self, obj: Any) -> Any:
        """
        🆕 v2.9.67: Converte URLs recursivamente em qualquer estrutura de dados.
        
        Procura por campos que parecem ser URLs (contém 'services.vinicius.ai')
        e converte para URLs internas.
        """
        if isinstance(obj, str):
            # Se é uma string, verificar se é URL externa
            return self._convert_external_to_internal_url(obj)
        elif isinstance(obj, dict):
            # Se é dict, processar cada valor
            return {k: self._convert_payload_urls_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            # Se é lista, processar cada item
            return [self._convert_payload_urls_recursive(item) for item in obj]
        else:
            # Outros tipos (int, float, bool, None) - retornar como está
            return obj
    
    def _convert_payload_urls(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        🆕 v2.9.67: Converte todas as URLs no payload para URLs internas.
        
        Usa conversão recursiva para pegar todos os campos, incluindo:
        - src (usado pelo Remotion)
        - video_url, audio_url, image_url, url
        - Qualquer campo aninhado em qualquer nível
        """
        logger.info("🔄 [URL CONVERT] Convertendo URLs externas para internas...")
        
        converted = self._convert_payload_urls_recursive(payload)
        
        logger.info("✅ [URL CONVERT] Conversão concluída")
        
        return converted
    
    def calculate_frame_ranges(
        self, 
        duration_in_frames: int, 
        num_workers: int
    ) -> List[Dict[str, int]]:
        """
        Divide os frames entre os workers de forma equilibrada.
        
        🆕 v2.9.67: Garante que NUNCA excede duration_in_frames - 1
        O último frame válido é sempre (duration_in_frames - 1), já que começa em 0.
        
        Args:
            duration_in_frames: Total de frames do vídeo
            num_workers: Número de workers disponíveis
            
        Returns:
            Lista de ranges: [{"start": 0, "end": 250}, {"start": 251, "end": 500}, ...]
        """
        # 🛡️ CORREÇÃO: O frame máximo é duration_in_frames - 1 (índice base 0)
        max_frame = duration_in_frames - 1
        
        frames_per_worker = duration_in_frames // num_workers
        remainder = duration_in_frames % num_workers
        
        ranges = []
        current_frame = 0
        
        for i in range(num_workers):
            # Distribuir frames extras entre os primeiros workers
            extra = 1 if i < remainder else 0
            end_frame = current_frame + frames_per_worker + extra - 1
            
            # 🛡️ CORREÇÃO: Garantir que end_frame nunca excede max_frame
            end_frame = min(end_frame, max_frame)
            
            # Só adicionar range se for válido
            if current_frame <= max_frame:
                actual_frame_count = end_frame - current_frame + 1
                ranges.append({
                    "worker_index": i,
                    "start_frame": current_frame,
                    "end_frame": end_frame,
                    "frame_count": actual_frame_count
                })
            
            current_frame = end_frame + 1
            
            # Se já passou do máximo, parar
            if current_frame > max_frame:
                break
        
        logger.info(f"📐 [FRAME RANGES] duration={duration_in_frames}, max_frame={max_frame}, chunks={len(ranges)}")
        
        return ranges
    
    def _render_chunk_on_worker(
        self,
        worker: WorkerInfo,
        job_id: str,
        chunk_index: int,
        frame_range: Dict[str, int],
        payload: Dict[str, Any],
        user_id: str,
        project_id: str
    ) -> Dict[str, Any]:
        """
        Envia um chunk para um worker específico.
        
        Args:
            worker: Worker que processará o chunk
            job_id: ID do job original
            chunk_index: Índice do chunk
            frame_range: Range de frames (start_frame, end_frame)
            payload: Payload original
            user_id: ID do usuário
            project_id: ID do projeto
            
        Returns:
            {"status": "success"|"error", "chunk_path": "...", ...}
        """
        chunk_job_id = f"{job_id}_chunk_{chunk_index}"
        start_time = time.time()
        
        logger.info(f"🎬 [CHUNK {chunk_index}] Enviando para {worker.name}")
        logger.info(f"   Frames: {frame_range['start_frame']}-{frame_range['end_frame']} ({frame_range['frame_count']} frames)")
        
        try:
            # Modificar payload para este chunk
            chunk_payload = self._prepare_chunk_payload(
                payload=payload,
                job_id=chunk_job_id,
                frame_range=frame_range,
                user_id=user_id,
                project_id=project_id
            )
            
            # Enviar para o worker
            response = requests.post(
                f"{worker.url}/render-video",
                json=chunk_payload,
                timeout=300  # 5 minutos para iniciar
            )
            
            if response.status_code not in [200, 202]:
                raise Exception(f"Worker retornou {response.status_code}: {response.text[:200]}")
            
            result = response.json()
            worker.current_job = chunk_job_id
            
            # Aguardar conclusão (polling)
            final_result = self._wait_for_chunk_completion(
                worker=worker,
                job_id=chunk_job_id,
                timeout_seconds=600  # 10 minutos por chunk
            )
            
            duration = time.time() - start_time
            logger.info(f"✅ [CHUNK {chunk_index}] Concluído em {duration:.2f}s no {worker.name}")
            
            return {
                "status": "success",
                "chunk_index": chunk_index,
                "worker": worker.name,
                "duration_seconds": duration,
                # 🔧 v2.9.77: Priorizar shared_path (caminho no volume compartilhado)
                "chunk_path": final_result.get("shared_path") or final_result.get("output_path"),
                "b2_url": final_result.get("b2_url")
            }
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"❌ [CHUNK {chunk_index}] Falhou no {worker.name} após {duration:.2f}s: {e}")
            return {
                "status": "error",
                "chunk_index": chunk_index,
                "worker": worker.name,
                "duration_seconds": duration,
                "error": str(e)
            }
        finally:
            worker.current_job = None
    
    def _prepare_chunk_payload(
        self,
        payload: Dict[str, Any],
        job_id: str,
        frame_range: Dict[str, int],
        user_id: str,
        project_id: str
    ) -> Dict[str, Any]:
        """
        Prepara o payload para um chunk específico.
        
        Adiciona frame_range para que o worker renderize apenas esse trecho.
        IMPORTANTE: Monta project_settings.video_settings igual ao render_service.py
        """
        # Extrair dados do payload (igual render_service.py)
        canvas = payload.get("canvas", {"width": 1080, "height": 1920})
        fps = payload.get("fps", 30)
        duration_in_frames = payload.get("duration_in_frames", 0)
        
        # 🎯 Montar project_settings no formato que v-editor espera
        # O render-engine.js EXIGE project_settings.video_settings com width, height, fps
        project_settings = {
            "video_settings": {
                "width": canvas.get("width", 1080),
                "height": canvas.get("height", 1920),
                "fps": fps,
                "duration_in_frames": frame_range["end_frame"] - frame_range["start_frame"] + 1  # Duração do chunk
            }
        }
        
        # 🔧 v2.9.85: Obter video_url corretamente
        # O payload do subtitle_pipeline pode ter base_layer com asset_id ao invés da URL normalizada
        # Precisamos usar video_url do payload (que é a URL normalizada) para construir base_layer
        video_url = payload.get("video_url", "")
        
        # 🔧 v2.9.85: Construir base_layer com video_url correto
        # Igual ao render_service.py, não usar base_layer do template (que pode ter asset_id)
        if video_url and video_url not in ["__TEXT_VIDEO_NO_BASE__", "__HYBRID_MODE_USE_SPEECH_SEGMENTS__"]:
            # Modo vídeo: usar URL normalizada
            base_layer = {
                "video_base": {
                    "urls": [video_url]
                }
            }
            base_type = "video"
            logger.info(f"🎬 [CHUNK] Base layer construído com video_url: {video_url[:60]}...")
        else:
            # Modo text_video ou hybrid: usar base_layer original
            base_layer = payload.get("base_layer", {})
            base_type = payload.get("base_type", "solid")
            logger.info(f"📝 [CHUNK] Usando base_layer original (modo {base_type})")
        
        # Copiar payload base
        chunk_payload = {
            "jobId": job_id,
            "job_id": job_id,
            "user_id": user_id,
            "project_id": project_id,
            
            # 🎯 Configurações do projeto no formato correto para v-editor
            "project_settings": project_settings,
            
            # Dados originais do payload (v-editor também usa alguns direto)
            "canvas": canvas,
            "fps": fps,
            "duration_in_frames": frame_range["end_frame"] - frame_range["start_frame"] + 1,
            "video_url": video_url,
            
            # Tracks (mantém todas, o Remotion filtra pelos frames)
            "tracks": payload.get("tracks", {}),
            
            # 🔧 v2.9.85: Base layer com URL correta (não do template!)
            "base_type": base_type,
            "base_layer": base_layer,
            
            # Render settings
            "render_settings": payload.get("render_settings", {}),
            "quality_settings": payload.get("quality_settings", {}),
            
            # 🆕 Frame range específico para este chunk
            "frame_range": {
                "start_frame": frame_range["start_frame"],
                "end_frame": frame_range["end_frame"]
            },
            
            # Configurações especiais para chunk
            "is_chunk": True,
            "skip_upload": True,  # Não fazer upload, salvar localmente
            "output_to_shared": True,  # Salvar em /app/shared para concat
            
            # Sem webhook (o orquestrador monitora por polling)
            "webhook_url": None
        }
        
        logger.debug(f"📦 [CHUNK] Payload preparado: fps={fps}, canvas={canvas}, duration_chunk={chunk_payload['duration_in_frames']} frames")
        
        # 🆕 v2.9.67: Converter URLs externas para internas
        chunk_payload = self._convert_payload_urls(chunk_payload)
        
        return chunk_payload
    
    def _wait_for_chunk_completion(
        self,
        worker: WorkerInfo,
        job_id: str,
        timeout_seconds: int = 600,
        poll_interval: float = 5.0,  # 🔧 v2.9.75: Polling mais espaçado
        max_consecutive_errors: int = 5,
        initial_wait: float = 10.0  # 🔧 v2.9.75: Esperar mais antes de começar polling
    ) -> Dict[str, Any]:
        """
        Aguarda conclusão de um job em um worker (polling).
        
        🆕 v2.9.74: Correções para evitar falsos positivos/negativos:
        - Espera inicial antes do primeiro poll (job pode estar iniciando)
        - 404 não falha imediatamente - espera mais tentativas
        - Valida timestamp do arquivo para evitar cache antigo
        - Timeout aumentado para vídeos longos
        
        Args:
            worker: Worker onde o job está rodando
            job_id: ID do job
            timeout_seconds: Timeout em segundos
            poll_interval: Intervalo entre polls
            max_consecutive_errors: Máximo de erros consecutivos antes de falhar
            initial_wait: Tempo para esperar antes do primeiro poll
            
        Returns:
            Resultado final do job
        """
        start_time = time.time()
        consecutive_errors = 0
        consecutive_404s = 0  # 🆕 v2.9.74: Contador separado para 404s
        # 🔧 v2.9.80: Aumentado para 150 (750s = 12+ min)
        # Com REMOTION_CONCURRENCY=1, chunks podem levar muito tempo
        max_404s_before_fail = 150
        last_status = None
        job_started = False  # 🆕 v2.9.74: Flag para saber se job foi detectado
        
        # 🆕 v2.9.74: Espera inicial para job iniciar
        logger.info(f"⏳ [POLL] Aguardando {initial_wait}s para job {job_id} iniciar no {worker.name}...")
        time.sleep(initial_wait)
        
        while (time.time() - start_time) < timeout_seconds:
            try:
                response = requests.get(
                    f"{worker.url}/job/{job_id}",
                    timeout=15  # 🔧 v2.9.74: Aumentado de 10s para 15s
                )
                
                if response.status_code == 200:
                    consecutive_errors = 0  # Reset contador de erros
                    consecutive_404s = 0  # Reset contador de 404s
                    job_started = True  # Job foi detectado
                    
                    result = response.json()
                    status = result.get("status")
                    
                    # Log apenas quando status muda
                    if status != last_status:
                        elapsed = time.time() - start_time
                        logger.info(f"📊 [POLL] {worker.name} job {job_id}: {status} ({elapsed:.1f}s)")
                        last_status = status
                    
                    if status == "completed":
                        # 🔧 v2.9.75: Aceitar resultado diretamente, sem validação de timestamp
                        # O path do arquivo já é único por job_id, então não há risco de cache
                        elapsed = time.time() - start_time
                        logger.info(f"✅ [POLL] Job {job_id} completado em {elapsed:.1f}s")
                        return result
                    elif status in ["failed", "error"]:
                        error_msg = result.get('error', 'Unknown error')
                        logger.error(f"❌ [POLL] Job falhou: {error_msg}")
                        raise Exception(f"Job falhou: {error_msg}")
                    # else: continua polling (queued, rendering, etc)
                    
                elif response.status_code == 404:
                    # 🔧 v2.9.74: Não falhar imediatamente - job pode estar iniciando
                    consecutive_404s += 1
                    elapsed = time.time() - start_time
                    
                    if job_started:
                        # Job já tinha sido detectado mas agora retorna 404 = erro real
                        logger.error(f"❌ [POLL] Job {job_id} desapareceu do {worker.name} (404) após {elapsed:.1f}s")
                        raise Exception(f"Job {job_id} desapareceu do worker (404)")
                    elif consecutive_404s >= max_404s_before_fail:
                        logger.error(f"❌ [POLL] Job {job_id} não apareceu no {worker.name} após {consecutive_404s} tentativas ({elapsed:.1f}s)")
                        raise Exception(f"Job {job_id} não iniciou no worker após {elapsed:.1f}s")
                    else:
                        if consecutive_404s % 10 == 1:  # Log a cada 10 tentativas
                            logger.info(f"⏳ [POLL] Aguardando job {job_id} aparecer no {worker.name} ({consecutive_404s}/{max_404s_before_fail})...")
                    
                elif response.status_code >= 500:
                    consecutive_errors += 1
                    logger.warning(f"⚠️ [POLL] Erro {response.status_code} no {worker.name} ({consecutive_errors}/{max_consecutive_errors})")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        raise Exception(f"Worker {worker.name} retornou {max_consecutive_errors} erros 5xx consecutivos")
                        
                else:
                    consecutive_errors += 1
                    logger.warning(f"⚠️ [POLL] Status inesperado {response.status_code} do {worker.name}")
                    
            except requests.RequestException as e:
                consecutive_errors += 1
                logger.warning(f"⚠️ [POLL] Erro de conexão com {worker.name}: {e} ({consecutive_errors}/{max_consecutive_errors})")
                
                if consecutive_errors >= max_consecutive_errors:
                    raise Exception(f"Worker {worker.name} inacessível após {max_consecutive_errors} tentativas")
            
            time.sleep(poll_interval)
        
        elapsed = time.time() - start_time
        raise Exception(f"Timeout ({elapsed:.1f}s) aguardando job {job_id} no {worker.name}")
    
    def _concatenate_chunks(
        self,
        chunk_paths: List[str],
        job_id: str,
        output_filename: str = None
    ) -> Dict[str, Any]:
        """
        Concatena chunks usando v-services.
        
        Args:
            chunk_paths: Lista de paths dos chunks (em /app/shared)
            job_id: ID do job
            output_filename: Nome do arquivo final
            
        Returns:
            {"status": "success", "output_path": "...", "duration_seconds": ...}
        """
        logger.info(f"🔗 [CONCAT] Concatenando {len(chunk_paths)} chunks...")
        
        start_time = time.time()
        
        try:
            # Chamar endpoint de concatenação no v-services
            response = requests.post(
                f"{self.v_services_url}/ffmpeg/concat-chunks",
                json={
                    "chunk_paths": chunk_paths,
                    "output_filename": output_filename or f"{job_id}_final.mp4",
                    "job_id": job_id
                },
                timeout=120  # 2 minutos para concat
            )
            
            if response.status_code != 200:
                raise Exception(f"Concat falhou: {response.status_code} - {response.text[:200]}")
            
            result = response.json()
            duration = time.time() - start_time
            
            logger.info(f"✅ [CONCAT] Concluído em {duration:.2f}s")
            
            return {
                "status": "success",
                "output_path": result.get("output_path"),
                "output_url": result.get("output_url"),
                "duration_seconds": duration
            }
            
        except Exception as e:
            logger.error(f"❌ [CONCAT] Falhou: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _calculate_duration_from_tectonic_plates(
        self,
        speech_segments: List[Dict[str, Any]],
        fps: int = 30
    ) -> int:
        """
        🆕 v2.9.69: Calcula duration_in_frames somando as placas tectônicas.
        
        Para storytelling_mode='vlog', a duração total é a soma das durações
        de cada speech_segment (placa tectônica).
        
        Args:
            speech_segments: Lista de placas tectônicas
            fps: Frames por segundo
            
        Returns:
            duration_in_frames calculado
        """
        if not speech_segments:
            logger.warning("⚠️ [DURATION] Sem speech_segments para calcular duração")
            return 0
        
        total_duration_seconds = 0
        for seg in speech_segments:
            # Tentar pegar duration, ou calcular de end_time - start_time
            seg_duration = seg.get('duration')
            if seg_duration is None:
                start = seg.get('original_start', seg.get('start_time', seg.get('start', 0)))
                end = seg.get('original_end', seg.get('end_time', seg.get('end', 0)))
                seg_duration = end - start
            
            total_duration_seconds += seg_duration
        
        duration_in_frames = int(total_duration_seconds * fps)
        
        logger.info(f"📐 [DURATION] Calculado de {len(speech_segments)} placas tectônicas:")
        logger.info(f"   Total: {total_duration_seconds:.2f}s = {duration_in_frames} frames @ {fps}fps")
        
        return duration_in_frames
    
    def render_distributed(
        self,
        job_id: str,
        payload: Dict[str, Any],
        user_id: str,
        project_id: str,
        template_id: str = None,
        speech_segments: List[Dict[str, Any]] = None,
        matting_segments: List[Dict[str, Any]] = None,
        storytelling_mode: str = None,
        total_duration_ms: int = None  # 🆕 v2.9.76: Duração real do job (da fase 1)
    ) -> Dict[str, Any]:
        """
        Renderiza vídeo distribuído entre múltiplos workers.
        
        Esta é a função principal que:
        1. Verifica workers saudáveis
        2. Divide o trabalho em chunks
        3. Envia chunks para workers em paralelo
        4. Aguarda conclusão de todos
        5. Concatena resultado final
        
        🆕 v2.9.76: Usa total_duration_ms do job como fonte primária de duração
        
        Args:
            job_id: ID do job
            payload: Payload completo de renderização
            user_id: ID do usuário
            project_id: ID do projeto
            template_id: ID do template (opcional)
            total_duration_ms: Duração real do vídeo em ms (da fase 1)
            
        Returns:
            {
                "status": "success" | "error",
                "output_path": "...",
                "output_url": "...",
                "total_duration_seconds": ...,
                "workers_used": N,
                "chunks": [...]
            }
        """
        total_start = time.time()
        
        logger.info(f"🚀 [DISTRIBUTED RENDER] Iniciando job {job_id}")
        
        # 🆕 v2.9.77: Limpar chunks antigos deste job antes de iniciar
        self._cleanup_old_chunks(job_id)
        
        # 1. Verificar workers saudáveis
        healthy_workers = self.get_healthy_workers()
        
        if not healthy_workers:
            logger.error("❌ Nenhum worker disponível!")
            return {
                "status": "error",
                "error": "Nenhum worker v-editor disponível"
            }
        
        num_workers = min(len(healthy_workers), self.max_concurrent_chunks)
        logger.info(f"   {num_workers} workers disponíveis")
        
        # 2. Extrair informações do vídeo
        fps = (
            payload.get("fps") or  # Raiz do payload
            payload.get("project_settings", {}).get("video_settings", {}).get("fps") or  # Aninhado
            payload.get("project_settings", {}).get("video_settings", {}).get("fps", {}).get("value") or
            30
        )
        
        # 🔧 v2.9.82: CALCULAR DURAÇÃO DOS VIDEO_SEGMENTS (PLACAS TECTÔNICAS)
        # PRIORIDADE 1: video_segments (soma dos durations) - fonte de verdade ABSOLUTA
        # PRIORIDADE 2: maior end_time das tracks (subtitles, highlights)
        # PRIORIDADE 3: payload.duration_in_frames (último recurso, pode estar errado)
        
        tracks = payload.get("tracks", {})
        video_segments = tracks.get("video_segments", [])
        duration_in_frames = 0
        duration_source = None
        
        # PRIORIDADE 1: Somar durations dos video_segments (placas tectônicas)
        if video_segments and len(video_segments) > 0:
            total_duration_ms = 0
            for seg in video_segments:
                # duration pode estar em segundos ou ms
                duration = seg.get("duration", 0)
                if duration < 1000:  # Provavelmente em segundos
                    total_duration_ms += int(duration * 1000)
                else:
                    total_duration_ms += int(duration)
            
            if total_duration_ms > 0:
                # Adicionar 500ms de margem
                total_duration_ms += 500
                duration_in_frames = int((total_duration_ms / 1000) * fps)
                duration_source = f"video_segments ({len(video_segments)} segs)"
                logger.info(f"📐 [DURATION] ✅ SOMA dos {len(video_segments)} video_segments = {total_duration_ms-500}ms + 500ms margem = {duration_in_frames} frames")
        
        # PRIORIDADE 2: Maior end_time das tracks (subtitles, highlights)
        if not duration_in_frames:
            max_end_time_ms = 0
            for track_name, track_items in tracks.items():
                if isinstance(track_items, list):
                    for item in track_items:
                        end_time = item.get("end_time", 0)
                        if end_time > max_end_time_ms:
                            max_end_time_ms = end_time
            
            if max_end_time_ms > 0:
                real_duration_ms = max_end_time_ms + 500
                duration_in_frames = int((real_duration_ms / 1000) * fps)
                duration_source = f"tracks.max_end_time ({max_end_time_ms}ms)"
                logger.info(f"📐 [DURATION] Calculado do max end_time das TRACKS: {max_end_time_ms}ms + 500ms = {duration_in_frames} frames")
        
        # PRIORIDADE 3: payload.duration_in_frames (último recurso)
        if not duration_in_frames:
            duration_in_frames = (
                payload.get("duration_in_frames") or
                payload.get("project_settings", {}).get("video_settings", {}).get("duration_in_frames") or
                0
            )
            if duration_in_frames:
                duration_source = "payload.duration_in_frames (FALLBACK)"
                logger.warning(f"⚠️ [DURATION] Usando FALLBACK payload.duration_in_frames={duration_in_frames}")
            else:
                logger.error("❌ [DURATION] Nenhuma fonte de duração disponível!")
                logger.error(f"   video_segments: {len(video_segments)}")
                logger.error(f"   tracks: {list(tracks.keys())}")
                return {
                    "status": "error",
                    "error": "duration_in_frames não encontrado - sem video_segments, tracks vazios, payload sem duração"
                }
        
        estimated_seconds = duration_in_frames / fps
        logger.info(f"📐 [DURATION] FONTE: {duration_source}")
        logger.info(f"   Vídeo: {duration_in_frames} frames @ {fps}fps = {estimated_seconds:.2f}s")
        
        # 3. Dividir frames entre workers
        frame_ranges = self.calculate_frame_ranges(duration_in_frames, num_workers)
        
        logger.info(f"📦 Dividindo em {len(frame_ranges)} chunks:")
        for r in frame_ranges:
            chunk_seconds = r['frame_count'] / fps
            logger.info(f"   Chunk {r['worker_index']}: frames {r['start_frame']}-{r['end_frame']} ({chunk_seconds:.2f}s)")
        
        # 4. Enviar chunks para workers em paralelo
        chunk_results = []
        
        # 🔧 v2.9.80: Rotação de workers para diagnóstico
        # WORKER_ROTATION=0 (padrão): ED1→chunk0, ED2→chunk1, ED3→chunk2, ED4→chunk3
        # WORKER_ROTATION=1: ED2→chunk0, ED3→chunk1, ED4→chunk2, ED1→chunk3
        # WORKER_ROTATION=2: ED3→chunk0, ED4→chunk1, ED1→chunk2, ED2→chunk3
        import os
        rotation = int(os.environ.get('WORKER_ROTATION', '0'))
        if rotation > 0:
            # Rotacionar lista de workers
            rotated_workers = healthy_workers[rotation:] + healthy_workers[:rotation]
            logger.info(f"🔄 [ROTATION] Workers rotacionados por {rotation}: {[w.name for w in rotated_workers]}")
        else:
            rotated_workers = healthy_workers
            logger.info(f"🔄 [ROTATION] Sem rotação: {[w.name for w in rotated_workers]}")
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {}
            
            for i, frame_range in enumerate(frame_ranges):
                worker = rotated_workers[i]
                future = executor.submit(
                    self._render_chunk_on_worker,
                    worker=worker,
                    job_id=job_id,
                    chunk_index=i,
                    frame_range=frame_range,
                    payload=payload,
                    user_id=user_id,
                    project_id=project_id
                )
                futures[future] = i
            
            # Coletar resultados
            for future in as_completed(futures):
                result = future.result()
                chunk_results.append(result)
        
        # Ordenar por índice
        chunk_results.sort(key=lambda x: x.get("chunk_index", 0))
        
        # 5. Verificar se todos os chunks foram bem-sucedidos
        failed_chunks = [r for r in chunk_results if r.get("status") != "success"]
        
        if failed_chunks:
            logger.error(f"❌ {len(failed_chunks)} chunks falharam")
            return {
                "status": "error",
                "error": f"{len(failed_chunks)} chunks falharam",
                "failed_chunks": failed_chunks,
                "successful_chunks": [r for r in chunk_results if r.get("status") == "success"]
            }
        
        # 6. Concatenar chunks
        chunk_paths = [r.get("chunk_path") for r in chunk_results if r.get("chunk_path")]
        
        if len(chunk_paths) != len(chunk_results):
            logger.error("❌ Alguns chunks não retornaram path")
            return {
                "status": "error",
                "error": "Alguns chunks não retornaram path",
                "chunks": chunk_results
            }
        
        concat_result = self._concatenate_chunks(chunk_paths, job_id)
        
        if concat_result.get("status") != "success":
            return {
                "status": "error",
                "error": f"Concatenação falhou: {concat_result.get('error')}",
                "chunks": chunk_results
            }
        
        # 7. Upload para B2
        b2_url = None
        try:
            from app.utils.b2_client import get_b2_client
            
            output_path = concat_result.get("output_path")
            if output_path:
                # Definir path no B2
                b2_destination = f"users/{user_id}/projects/{project_id}/renders/{job_id}_final.mp4"
                
                logger.info(f"📤 [B2 UPLOAD] Enviando para {b2_destination}...")
                
                b2_client = get_b2_client()
                # 🔧 v2.9.74: Corrigido nome do método (era upload_from_url)
                upload_result = b2_client.upload_file_from_url(
                    source_url=concat_result.get("output_url"),
                    destination_path=b2_destination,
                    content_type="video/mp4"
                )
                b2_url = upload_result.get("public_url") if upload_result else None
                
                logger.info(f"✅ [B2 UPLOAD] Concluído: {b2_url[:60]}...")
        except Exception as e:
            logger.warning(f"⚠️ [B2 UPLOAD] Falhou: {e}, usando URL do v-services")
        
        # 8. Resultado final
        total_duration = time.time() - total_start
        
        # Calcular soma dos tempos de render (seria sequencial)
        sequential_time = sum(r.get("duration_seconds", 0) for r in chunk_results)
        time_saved = sequential_time - total_duration
        speedup = sequential_time / total_duration if total_duration > 0 else 1
        
        final_url = b2_url or concat_result.get("output_url")
        
        logger.info(f"✅ [DISTRIBUTED RENDER] Concluído em {total_duration:.2f}s")
        logger.info(f"   ⏱️ Tempo sequencial estimado: {sequential_time:.2f}s")
        logger.info(f"   🚀 Speedup: {speedup:.2f}x (economizou {time_saved:.2f}s)")
        logger.info(f"   🔗 URL final: {final_url[:80]}...")
        
        return {
            "status": "success",
            "render_status": "completed",
            "output_path": concat_result.get("output_path"),
            "output_url": final_url,
            "b2_url": b2_url,
            "total_duration_seconds": total_duration,
            "sequential_time_estimate": sequential_time,
            "speedup": round(speedup, 2),
            "time_saved_seconds": round(time_saved, 2),
            "workers_used": num_workers,
            "chunks": chunk_results,
            "concat_duration": concat_result.get("duration_seconds", 0)
        }


# Singleton
_worker_pool = None


def get_worker_pool() -> WorkerPoolService:
    """Retorna instância singleton do WorkerPoolService."""
    global _worker_pool
    if _worker_pool is None:
        _worker_pool = WorkerPoolService()
    return _worker_pool

