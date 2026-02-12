"""
EC2 Unified Service - Roteia jobs para EC2 Spot Unificada

Esta EC2 contém v-services, v-matting e v-editor em uma única instância.
Ideal para processar o pipeline completo sem transferências entre ambientes.

🆕 v2.9.110: Criado em 2026-01-20
"""

import os
import requests
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class EC2UnifiedService:
    """
    Serviço para rotear jobs para EC2 Spot Unificada.
    
    A EC2 Unificada contém:
    - v-services (porta 5000)
    - v-matting (porta 5100)  
    - v-editor (porta 5018)
    
    Todos na mesma instância, com shared storage local.
    """
    
    # IP da EC2 Unificada (pode ser configurado via env)
    EC2_UNIFIED_IP = os.environ.get('EC2_UNIFIED_IP', '18.204.215.226')
    
    # URLs dos serviços
    V_SERVICES_PORT = 5000
    V_MATTING_PORT = 5100
    V_EDITOR_PORT = 5018
    
    # Webhook base URL
    WEBHOOK_BASE_URL = os.environ.get('CALLBACK_BASE_URL', 'https://api.vinicius.ai')
    
    def __init__(self):
        self.base_url = f"http://{self.EC2_UNIFIED_IP}"
        self.v_services_url = f"{self.base_url}:{self.V_SERVICES_PORT}"
        self.v_matting_url = f"{self.base_url}:{self.V_MATTING_PORT}"
        self.v_editor_url = f"{self.base_url}:{self.V_EDITOR_PORT}"
        
        logger.info(f"🖥️ EC2UnifiedService inicializado")
        logger.info(f"   IP: {self.EC2_UNIFIED_IP}")
        logger.info(f"   v-services: {self.v_services_url}")
        logger.info(f"   v-matting: {self.v_matting_url}")
        logger.info(f"   v-editor: {self.v_editor_url}")
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica saúde de todos os serviços na EC2"""
        results = {
            "ec2_ip": self.EC2_UNIFIED_IP,
            "services": {}
        }
        
        for service_name, url in [
            ("v-services", self.v_services_url),
            ("v-matting", self.v_matting_url),
            ("v-editor", self.v_editor_url)
        ]:
            try:
                response = requests.get(f"{url}/health", timeout=5)
                results["services"][service_name] = {
                    "status": "healthy" if response.status_code == 200 else "unhealthy",
                    "url": url
                }
            except Exception as e:
                results["services"][service_name] = {
                    "status": "unreachable",
                    "error": str(e),
                    "url": url
                }
        
        # EC2 está saudável se todos os serviços estão healthy
        all_healthy = all(
            s.get("status") == "healthy" 
            for s in results["services"].values()
        )
        results["overall_status"] = "healthy" if all_healthy else "degraded"
        
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
        Envia job de renderização para o v-editor na EC2 Unificada.
        
        O v-editor na EC2 já está configurado para usar os v-services e v-matting
        locais (mesma instância), então todo o processamento acontece lá.
        """
        logger.info(f"📤 [EC2 UNIFIED] Enviando job {job_id[:8]} para EC2 Unificada...")
        logger.info(f"   IP: {self.EC2_UNIFIED_IP}")
        
        try:
            # Verificar saúde primeiro
            health = self.health_check()
            if health["overall_status"] != "healthy":
                logger.warning(f"⚠️ [EC2 UNIFIED] EC2 não está 100% saudável: {health}")
                # Continuar mesmo assim, pode ser que o v-editor funcione
            
            # Montar webhook URL
            webhook_url = f"{self.WEBHOOK_BASE_URL}{callback_endpoint}"
            
            # Preparar payload para o v-editor
            render_payload = self._build_render_payload(
                job_id=job_id,
                payload=payload,
                user_id=user_id,
                project_id=project_id,
                template_id=template_id,
                webhook_url=webhook_url
            )
            
            # Enviar para v-editor
            logger.info(f"   🚀 POST {self.v_editor_url}/render-video")
            
            response = requests.post(
                f"{self.v_editor_url}/render-video",
                json=render_payload,
                timeout=30  # Timeout inicial apenas para aceitar o job
            )
            
            # v-editor retorna 200 (OK) ou 202 (Accepted) quando aceita o job
            if response.status_code in [200, 202]:
                result = response.json()
                logger.info(f"✅ [EC2 UNIFIED] Job aceito pelo v-editor (status {response.status_code})")
                return {
                    "status": "success",
                    "render_id": result.get("render_id", result.get("jobId", job_id)),
                    "message": "Job enviado para EC2 Unificada",
                    "ec2_ip": self.EC2_UNIFIED_IP,
                    "worker": "ec2-unified-v-editor"
                }
            else:
                error_msg = f"v-editor retornou {response.status_code}: {response.text[:200]}"
                logger.error(f"❌ [EC2 UNIFIED] {error_msg}")
                return {
                    "status": "error",
                    "error": error_msg
                }
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ [EC2 UNIFIED] Timeout ao conectar com v-editor")
            return {
                "status": "error",
                "error": "Timeout ao conectar com EC2 Unificada"
            }
        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ [EC2 UNIFIED] Erro de conexão: {e}")
            return {
                "status": "error", 
                "error": f"Não foi possível conectar à EC2 Unificada ({self.EC2_UNIFIED_IP})"
            }
        except Exception as e:
            logger.error(f"❌ [EC2 UNIFIED] Erro inesperado: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _build_render_payload(
        self,
        job_id: str,
        payload: Dict[str, Any],
        user_id: str,
        project_id: str,
        template_id: str,
        webhook_url: str
    ) -> Dict[str, Any]:
        """
        Monta o payload no formato esperado pelo v-editor.
        
        🆕 v2.9.111: Replicar lógica completa do render_service para
        garantir que project_settings e tracks estão no formato correto.
        """
        # Extrair dados do payload (formato do PayloadBuilderService)
        tracks = payload.get("tracks", {})
        canvas = payload.get("canvas", {"width": 720, "height": 1280})
        fps = payload.get("fps", 30)
        duration_in_frames = payload.get("duration_in_frames", 0)
        video_url = payload.get("video_url", "")
        
        # Montar project_settings no formato esperado pelo v-editor
        project_settings = {
            "video_settings": {
                "width": canvas.get("width", 720),
                "height": canvas.get("height", 1280),
                "fps": fps,
                "duration_in_frames": duration_in_frames
            }
        }
        
        # Quality settings
        template_quality = payload.get("quality", "high")
        template_preset = payload.get("preset", "medium")
        
        quality_to_crf = {"ultra": 15, "high": 18, "medium": 23, "low": 28, "draft": 32}
        base_crf = quality_to_crf.get(template_quality, 23)
        
        quality_settings = {
            "crf": base_crf,
            "codec": "h264",
            "pixel_format": "yuv420p",
            "audio_bitrate": "192k" if template_quality in ["ultra", "high"] else "128k",
            "preset": template_preset
        }
        
        logger.info(f"🎬 [EC2 UNIFIED] Payload montado:")
        logger.info(f"   - Canvas: {canvas}")
        logger.info(f"   - FPS: {fps}")
        logger.info(f"   - Duration: {duration_in_frames} frames")
        logger.info(f"   - Video URL: {video_url[:80] if video_url else 'N/A'}...")
        logger.info(f"   - Quality: {template_quality}, Preset: {template_preset}, CRF: {base_crf}")
        
        return {
            "jobId": job_id,
            "job_id": job_id,
            "user_id": user_id,
            "project_id": project_id,
            "template_id": template_id,
            "webhook_url": webhook_url,
            "webhook_metadata": {
                "job_id": job_id,
                "user_id": user_id,
                "project_id": project_id,
                "template_id": template_id,
                "source": "ec2_unified"
            },
            "project_settings": project_settings,
            "tracks": tracks,
            "base_type": "video" if video_url else "solid",
            "base_layer": {
                "video_base": {
                    "urls": [video_url] if video_url else []
                }
            } if video_url else {
                "solid_base": {
                    "color": "#000000",
                    "opacity": 1
                }
            },
            "render_settings": payload.get("render_settings", {
                "solid_background": not video_url,
                "background_color": "#000000"
            }),
            "quality_settings": quality_settings,
            "worker_name": "ec2_unified",
            "render_source": "ec2_unified"
        }
