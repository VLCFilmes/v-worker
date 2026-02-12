"""
🆕 v2.9.97: Modal Matting Service
Serviço para chamar v-matting no Modal.com com suporte a processamento paralelo de segmentos.

v2.9.284 - Luma Matte para v-editor-python:
- Quando output_format="alpha_only", captura DOIS arquivos:
  - foreground_url: máscara grayscale (H.264)
  - base_video_url: vídeo RGB sincronizado (H.264)
- NÃO faz merge para PNG quando for para v-editor-python
"""

import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

class ModalMattingService:
    """
    Serviço para processar matting no Modal.com
    Suporta processamento paralelo de múltiplos segmentos.
    """
    
    # 🆕 v2.9.261: Mapeamento de workers para endpoints Modal
    WORKER_ENDPOINTS = {
        'modal': 'https://fotovinicius2--v-matting-matting-t4-sync.modal.run',  # T4 GPU
        'modal-t4': 'https://fotovinicius2--v-matting-matting-t4-sync.modal.run',  # T4 GPU
        'modal-cpu-light': 'https://fotovinicius2--v-matting-matting-cpu-light-sync.modal.run',  # CPU Light (barato)
        'modal-cpu': 'https://fotovinicius2--v-matting-matting-cpu-sync.modal.run',  # CPU
    }
    
    def __init__(self, worker_id: str = None):
        # 🆕 v2.9.261: Selecionar endpoint baseado no worker
        self.worker_id = worker_id or 'modal-cpu-light'  # Default: CPU Light (mais barato)
        
        # Usar mapeamento ou fallback para env var
        if self.worker_id in self.WORKER_ENDPOINTS:
            self.endpoint_url = self.WORKER_ENDPOINTS[self.worker_id]
        else:
            self.endpoint_url = os.environ.get('MODAL_ENDPOINT_URL') or os.environ.get(
                'MODAL_MATTING_ENDPOINT',
                'https://fotovinicius2--v-matting-matting-cpu-light-sync.modal.run'
            )
        
        self.enabled = os.environ.get('MODAL_MATTING_ENABLED', 'false').lower() == 'true'
        self.max_parallel_jobs = int(os.environ.get('MODAL_MAX_PARALLEL_JOBS', '5'))
        self.timeout = int(os.environ.get('MODAL_MATTING_TIMEOUT', '600'))
        
        # Formato de saída: 'webm', 'alpha_only', 'png_sequence'
        self.output_format = os.environ.get('MODAL_OUTPUT_FORMAT', 'alpha_only')
        
        logger.info(f"🔧 [MODAL] ModalMattingService inicializado")
        logger.info(f"   - Worker: {self.worker_id}")
        logger.info(f"   - Endpoint: {self.endpoint_url}")
        logger.info(f"   - Enabled: {self.enabled}")
        logger.info(f"   - Max Parallel Jobs: {self.max_parallel_jobs}")
        logger.info(f"   - Output Format: {self.output_format}")
    
    def is_enabled(self) -> bool:
        """Verifica se o Modal está habilitado"""
        return self.enabled
    
    def process_segment(
        self,
        video_url: str,
        job_id: str,
        segment_index: int,
        user_id: str = None,
        project_path: str = None,
        output_format: str = None,
        # 🆕 v2.9.140: Parâmetros de timing para passar de volta ao orchestrator
        original_start: float = 0,
        original_end: float = 0,
        audio_offset: float = 0,
        duration: float = 0,
        # 🆕 v2.9.284: Skip merge para v-editor-python (luma matte direto)
        skip_merge: bool = False
    ) -> Dict[str, Any]:
        """
        Processa um único segmento de vídeo no Modal.
        
        Args:
            video_url: URL do vídeo do segmento (já cortado)
            job_id: ID do job principal
            segment_index: Índice do segmento
            user_id: ID do usuário
            project_path: Caminho do projeto no B2
            output_format: Formato de saída ('webm', 'alpha_only')
            original_start: Timestamp de início no vídeo original (segundos)
            original_end: Timestamp de fim no vídeo original (segundos)
            audio_offset: Offset no áudio concatenado (segundos)
            duration: Duração do segmento (segundos)
        
        Returns:
            Dict com resultado do processamento incluindo tempos
        """
        segment_job_id = f"{job_id}_seg{segment_index:03d}"
        format_to_use = output_format or self.output_format
        
        payload = {
            "video_url": video_url,
            "job_id": segment_job_id,
            "user_id": user_id,
            "project_path": project_path,
            "output_format": format_to_use,
            "upload_to_b2": True,
            "downsample_ratio": 0.25,
            "use_fp16": True
        }
        
        start_time = time.time()
        logger.info(f"🚀 [MODAL] Enviando segmento {segment_index} para Modal...")
        logger.info(f"   - Job ID: {segment_job_id}")
        logger.info(f"   - Video URL: {video_url[:80]}...")
        logger.info(f"   - Format: {format_to_use}")
        
        try:
            response = requests.post(
                self.endpoint_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            result = response.json()
            
            elapsed = time.time() - start_time
            
            # 🔧 v2.9.131: Endpoint síncrono retorna resultado completo com foreground_url
            if result.get('status') == 'success' and result.get('foreground_url'):
                logger.info(f"✅ [MODAL] Segmento {segment_index} concluído em {elapsed:.2f}s")
                logger.info(f"   - Foreground URL: {result.get('foreground_url')[:80]}...")
                
                foreground_url = result.get('foreground_url')
                # 🆕 v2.9.284: Capturar base_video_url para luma matte
                base_video_url = result.get('base_video_url')
                if base_video_url:
                    logger.info(f"   - Base Video URL: {base_video_url[:80]}...")
                
                output_format_result = format_to_use  # webm, alpha_only, etc.
                png_base_url = None
                frames_count = 0
                
                # 🆕 v2.9.284: Se skip_merge=True (v-editor-python), retornar direto sem merge
                if skip_merge and format_to_use == 'alpha_only':
                    logger.info(f"   🎭 [LUMA MATTE] skip_merge=True, retornando alpha + base para v-editor-python")
                    return {
                        "status": "success",
                        "segment_index": segment_index,
                        "job_id": segment_job_id,
                        "foreground_url": foreground_url,  # Máscara grayscale (mask_url)
                        "base_video_url": base_video_url,  # Vídeo RGB sincronizado (original_video_url)
                        "output_format": "luma_matte",  # Novo formato para v-editor-python
                        "metrics": result.get('metrics', {}),
                        "elapsed": elapsed,
                        "original_start": original_start,
                        "original_end": original_end,
                        "audio_offset": audio_offset,
                        "duration": duration
                    }
                
                # 🆕 v2.9.141: Se alpha_only (sem skip), fazer merge para PNG sequence no Hetzner
                if format_to_use == 'alpha_only':
                    logger.info(f"   🔄 [MERGE] alpha_only detectado, chamando merge-to-png-sequence...")
                    try:
                        merge_endpoint = os.environ.get('V_MATTING_URL', 'http://v-matting:5000') + '/merge-to-png-sequence'
                        merge_payload = {
                            "original_video_url": video_url,  # URL da placa original
                            "alpha_mask_url": foreground_url,  # Alpha mask do Modal
                            "job_id": job_id,
                            "segment_id": f"seg_{segment_index:03d}",
                            "user_id": user_id,
                            "project_path": project_path,
                            "upload_to_b2": False,  # 🆕 v2.9.150: Usar shared volume local!
                            "target_fps": 30
                        }
                        
                        merge_response = requests.post(merge_endpoint, json=merge_payload, timeout=300)
                        merge_response.raise_for_status()
                        merge_result = merge_response.json()
                        
                        if merge_result.get('status') == 'success':
                            # 🔧 v2.9.161: Corrigir nomes dos campos (png_base_url, frame_count)
                            png_base_url = merge_result.get('png_base_url')
                            frames_count = merge_result.get('frame_count', 0)
                            output_format_result = 'png_sequence'
                            logger.info(f"   ✅ [MERGE] PNG sequence gerado: {frames_count} frames")
                            logger.info(f"   📂 Base URL: {png_base_url[:80] if png_base_url else 'None'}...")
                        else:
                            logger.warning(f"   ⚠️ [MERGE] Falhou: {merge_result.get('error')}, usando alpha_only")
                    except Exception as merge_error:
                        logger.error(f"   ❌ [MERGE] Erro: {merge_error}, usando alpha_only")
                
                return {
                    "status": "success",
                    "segment_index": segment_index,
                    "job_id": segment_job_id,
                    "foreground_url": png_base_url or foreground_url,
                    "base_video_url": base_video_url,  # 🆕 v2.9.284: Incluir sempre
                    "output_format": output_format_result,
                    "frames_count": frames_count,
                    "metrics": result.get('metrics', {}),
                    "elapsed": elapsed,
                    # 🆕 v2.9.140: Passar tempos de volta para o orchestrator
                    "original_start": original_start,
                    "original_end": original_end,
                    "audio_offset": audio_offset,
                    "duration": duration
                }
            elif result.get('call_id'):
                # Resposta assíncrona (apenas call_id)
                logger.info(f"✅ [MODAL] Segmento {segment_index} aceito em {elapsed:.2f}s (assíncrono)")
                logger.info(f"   - Call ID: {result.get('call_id')}")
                return {
                    "status": "accepted",
                    "segment_index": segment_index,
                    "job_id": segment_job_id,
                    "call_id": result.get("call_id"),
                    "elapsed": elapsed
                }
            else:
                logger.error(f"❌ [MODAL] Resposta inesperada para segmento {segment_index}: {result}")
                return {
                    "status": "error",
                    "segment_index": segment_index,
                    "error": f"Resposta inesperada: {result.get('status')}",
                    "elapsed": elapsed
                }
            
        except requests.exceptions.Timeout:
            elapsed = time.time() - start_time
            logger.error(f"❌ [MODAL] Timeout no segmento {segment_index} após {elapsed:.2f}s")
            return {
                "status": "error",
                "segment_index": segment_index,
                "error": "Timeout",
                "elapsed": elapsed
            }
        except requests.exceptions.RequestException as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ [MODAL] Erro no segmento {segment_index}: {e}")
            return {
                "status": "error",
                "segment_index": segment_index,
                "error": str(e),
                "elapsed": elapsed
            }
    
    def process_segments_parallel(
        self,
        segments: List[Dict[str, Any]],
        job_id: str,
        user_id: str = None,
        project_path: str = None,
        output_format: str = None,
        # 🆕 v2.9.284: Skip merge para v-editor-python
        skip_merge: bool = False
    ) -> Dict[str, Any]:
        """
        Processa múltiplos segmentos em paralelo no Modal.
        
        Args:
            segments: Lista de segmentos, cada um com 'video_url' e 'index'
            job_id: ID do job principal
            user_id: ID do usuário
            project_path: Caminho do projeto no B2
            output_format: Formato de saída
        
        Returns:
            Dict com resultados de todos os segmentos
        """
        if not segments:
            logger.warning("⚠️ [MODAL] Nenhum segmento para processar")
            return {"status": "success", "results": [], "total_segments": 0}
        
        start_time = time.time()
        total_segments = len(segments)
        
        logger.info(f"🚀 [MODAL] Iniciando processamento paralelo de {total_segments} segmentos")
        logger.info(f"   - Max parallel: {self.max_parallel_jobs}")
        
        results = []
        successful = 0
        failed = 0
        
        # Usar ThreadPoolExecutor para processamento paralelo
        with ThreadPoolExecutor(max_workers=self.max_parallel_jobs) as executor:
            futures = {}
            
            for segment in segments:
                video_url = segment.get('video_url') or segment.get('url')
                segment_index = segment.get('index', segment.get('segment_index', 0))
                
                if not video_url:
                    logger.warning(f"⚠️ [MODAL] Segmento {segment_index} sem video_url, pulando...")
                    continue
                
                # 🆕 v2.9.140: Extrair parâmetros de timing do segmento
                original_start = segment.get('original_start', segment.get('start', 0))
                original_end = segment.get('original_end', segment.get('end', 0))
                audio_offset = segment.get('audio_offset', segment.get('_audio_offset', 0))
                duration = segment.get('duration', original_end - original_start)
                
                future = executor.submit(
                    self.process_segment,
                    video_url=video_url,
                    job_id=job_id,
                    segment_index=segment_index,
                    user_id=user_id,
                    project_path=project_path,
                    output_format=output_format,
                    # 🆕 v2.9.140: Passar tempos para retornar ao orchestrator
                    original_start=original_start,
                    original_end=original_end,
                    audio_offset=audio_offset,
                    duration=duration,
                    # 🆕 v2.9.284: Skip merge para v-editor-python
                    skip_merge=skip_merge
                )
                futures[future] = segment_index
            
            # Coletar resultados
            for future in as_completed(futures):
                segment_index = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # 🔧 v2.9.132: Verificar "success" (sync) ou "accepted" (async)
                    if result.get("status") in ["success", "accepted"]:
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"❌ [MODAL] Exceção no segmento {segment_index}: {e}")
                    results.append({
                        "status": "error",
                        "segment_index": segment_index,
                        "error": str(e)
                    })
                    failed += 1
        
        total_time = time.time() - start_time
        
        logger.info(f"✅ [MODAL] Processamento paralelo concluído em {total_time:.2f}s")
        logger.info(f"   - Total: {total_segments}")
        logger.info(f"   - Sucesso: {successful}")
        logger.info(f"   - Falhas: {failed}")
        
        return {
            "status": "success" if failed == 0 else "partial",
            "results": sorted(results, key=lambda x: x.get('segment_index', 0)),
            "total_segments": total_segments,
            "successful": successful,
            "failed": failed,
            "total_time": total_time
        }
    
    def poll_job_status(self, call_id: str, timeout: int = 600) -> Dict[str, Any]:
        """
        Aguarda a conclusão de um job no Modal (polling).
        
        NOTA: O Modal não tem um endpoint nativo de status, então
        usamos o resultado síncrono ou webhook.
        """
        # Por enquanto, o Modal retorna resultado síncrono
        # Para jobs longos, implementar webhook
        logger.warning("⚠️ [MODAL] poll_job_status ainda não implementado - usar webhook")
        return {"status": "unknown", "call_id": call_id}
    
    def process_segment_sync(
        self,
        video_url: str,
        original_video_url: str,
        job_id: str,
        segment_id: str,
        user_id: str = None,
        project_path: str = None
    ) -> Dict[str, Any]:
        """
        🆕 v2.9.102: Processa um segmento de forma SÍNCRONA e gera PNG sequence.
        
        Fluxo:
        1. Modal processa matting → retorna alpha_mask_url
        2. Hetzner merge-to-png-sequence → combina original + alpha → PNGs
        3. Retorna png_base_url para o v-editor
        
        Args:
            video_url: URL do vídeo do segmento (para Modal)
            original_video_url: URL do vídeo original (para merge)
            job_id: ID do job principal
            segment_id: ID do segmento
            user_id: ID do usuário
            project_path: Caminho do projeto no B2
        
        Returns:
            Dict com png_base_url e metadados
        """
        start_time = time.time()
        segment_job_id = f"{job_id}_{segment_id}"
        
        logger.info(f"")
        logger.info(f"╔══════════════════════════════════════════════════════════════╗")
        logger.info(f"║  🚀 MODAL + MERGE PIPELINE v2.9.102                          ║")
        logger.info(f"║  Job: {job_id[:8]}... Segment: {segment_id}                  ║")
        logger.info(f"╚══════════════════════════════════════════════════════════════╝")
        
        # === STEP 1: Chamar Modal para gerar alpha_mask ===
        logger.info(f"📤 [STEP 1/2] Enviando para Modal (GPU)...")
        
        # Usar endpoint SÍNCRONO do Modal
        sync_endpoint = self.endpoint_url.replace('matting-endpoint', 'matting-sync')
        
        modal_payload = {
            "video_url": video_url,
            "job_id": segment_job_id,
            "user_id": user_id,
            "project_path": project_path,
            "output_format": "alpha_only",  # Sempre alpha_only para merge local
            "upload_to_b2": True,
            "downsample_ratio": 0.25,
            "target_fps": 30
        }
        
        try:
            modal_start = time.time()
            response = requests.post(
                sync_endpoint,
                json=modal_payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            modal_result = response.json()
            modal_time = time.time() - modal_start
            
            if modal_result.get("status") != "success":
                raise Exception(f"Modal error: {modal_result.get('error')}")
            
            alpha_mask_url = modal_result.get("foreground_url")
            if not alpha_mask_url:
                raise Exception("Modal não retornou alpha_mask_url")
            
            logger.info(f"   ✅ Modal concluído em {modal_time:.1f}s")
            logger.info(f"   📥 Alpha mask: {alpha_mask_url[:60]}...")
            
        except Exception as e:
            logger.error(f"❌ [MODAL] Erro: {e}")
            return {
                "status": "error",
                "segment_id": segment_id,
                "error": str(e),
                "stage": "modal"
            }
        
        # === STEP 2: Chamar Hetzner para merge-to-png-sequence ===
        logger.info(f"📤 [STEP 2/2] Chamando Hetzner merge-to-png-sequence...")
        
        merge_endpoint = os.environ.get(
            'V_MATTING_URL',
            'http://v-matting:5000'
        ) + '/merge-to-png-sequence'
        
        merge_payload = {
            "original_video_url": original_video_url,
            "alpha_mask_url": alpha_mask_url,
            "job_id": job_id,
            "segment_id": segment_id,
            "user_id": user_id,
            "project_path": project_path,
            "upload_to_b2": False,  # 🆕 v2.9.150: Usar shared volume local!
            "target_fps": 30
        }
        
        try:
            merge_start = time.time()
            response = requests.post(
                merge_endpoint,
                json=merge_payload,
                timeout=600  # 10 minutos para merge + upload
            )
            response.raise_for_status()
            merge_result = response.json()
            merge_time = time.time() - merge_start
            
            if merge_result.get("status") != "success":
                raise Exception(f"Merge error: {merge_result.get('error')}")
            
            png_base_url = merge_result.get("png_base_url")
            frame_count = merge_result.get("frame_count", 0)
            
            logger.info(f"   ✅ Merge concluído em {merge_time:.1f}s")
            logger.info(f"   🖼️ PNG base: {png_base_url[:60]}...")
            logger.info(f"   📊 Frames: {frame_count}")
            
        except Exception as e:
            logger.error(f"❌ [MERGE] Erro: {e}")
            return {
                "status": "error",
                "segment_id": segment_id,
                "error": str(e),
                "stage": "merge"
            }
        
        total_time = time.time() - start_time
        
        logger.info(f"")
        logger.info(f"╔══════════════════════════════════════════════════════════════╗")
        logger.info(f"║  ✅ PIPELINE CONCLUÍDO!                                      ║")
        logger.info(f"║  Modal: {modal_time:.1f}s | Merge: {merge_time:.1f}s | Total: {total_time:.1f}s  ║")
        logger.info(f"╚══════════════════════════════════════════════════════════════╝")
        
        return {
            "status": "success",
            "segment_id": segment_id,
            "png_base_url": png_base_url,
            "frame_count": frame_count,
            "frame_pattern": merge_result.get("frame_pattern", "frame_%06d.png"),
            "alpha_mask_url": alpha_mask_url,
            "timings": {
                "modal": round(modal_time, 2),
                "merge": round(merge_time, 2),
                "total": round(total_time, 2)
            }
        }
    
    def process_segments_with_merge(
        self,
        segments: List[Dict[str, Any]],
        job_id: str,
        user_id: str = None,
        project_path: str = None
    ) -> Dict[str, Any]:
        """
        🆕 v2.9.102: Processa múltiplos segmentos em paralelo com merge para PNG.
        
        Cada segmento passa por:
        1. Modal (GPU) → alpha_only
        2. Hetzner → merge-to-png-sequence
        
        Os segmentos são processados em paralelo no Modal, mas sequencialmente no merge.
        """
        if not segments:
            logger.warning("⚠️ [MODAL] Nenhum segmento para processar")
            return {"status": "success", "results": [], "total_segments": 0}
        
        start_time = time.time()
        total_segments = len(segments)
        
        logger.info(f"🚀 [MODAL+MERGE] Iniciando pipeline para {total_segments} segmentos")
        
        results = []
        successful = 0
        failed = 0
        
        # Processar segmentos em paralelo com ThreadPool
        with ThreadPoolExecutor(max_workers=self.max_parallel_jobs) as executor:
            futures = {}
            
            for i, segment in enumerate(segments):
                video_url = segment.get('video_url') or segment.get('url')
                original_url = segment.get('original_video_url') or video_url
                segment_id = segment.get('segment_id', f"seg_{i:03d}")
                
                if not video_url:
                    logger.warning(f"⚠️ Segmento {segment_id} sem video_url, pulando...")
                    continue
                
                future = executor.submit(
                    self.process_segment_sync,
                    video_url=video_url,
                    original_video_url=original_url,
                    job_id=job_id,
                    segment_id=segment_id,
                    user_id=user_id,
                    project_path=project_path
                )
                futures[future] = segment_id
            
            # Coletar resultados
            for future in as_completed(futures):
                segment_id = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.get("status") == "success":
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"❌ Exceção no segmento {segment_id}: {e}")
                    results.append({
                        "status": "error",
                        "segment_id": segment_id,
                        "error": str(e)
                    })
                    failed += 1
        
        total_time = time.time() - start_time
        
        logger.info(f"✅ [MODAL+MERGE] Pipeline concluído em {total_time:.1f}s")
        logger.info(f"   - Total: {total_segments}")
        logger.info(f"   - Sucesso: {successful}")
        logger.info(f"   - Falhas: {failed}")
        
        return {
            "status": "success" if failed == 0 else "partial",
            "results": sorted(results, key=lambda x: x.get('segment_id', '')),
            "total_segments": total_segments,
            "successful": successful,
            "failed": failed,
            "total_time": round(total_time, 2)
        }


# 🆕 v2.9.261: Cache de instâncias por worker_id
_modal_matting_services: Dict[str, ModalMattingService] = {}

def get_modal_matting_service(worker_id: str = 'modal-cpu-light') -> ModalMattingService:
    """
    Retorna instância do ModalMattingService para o worker especificado.
    
    Args:
        worker_id: ID do worker ('modal', 'modal-cpu-light', 'modal-t4', etc)
    
    Returns:
        ModalMattingService configurado para o endpoint correto
    """
    global _modal_matting_services
    
    if worker_id not in _modal_matting_services:
        _modal_matting_services[worker_id] = ModalMattingService(worker_id=worker_id)
        logger.info(f"🔧 [MODAL] Nova instância criada para worker: {worker_id}")
    
    return _modal_matting_services[worker_id]
