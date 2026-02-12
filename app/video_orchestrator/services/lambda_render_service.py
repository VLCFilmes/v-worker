"""
🚀 Lambda Render Service - Renderização serverless via AWS Lambda

Este serviço envia jobs de renderização para o v-editor-lambda API,
que usa @remotion/lambda SDK para renderização serverless.

Suporta 3 modos de performance:
- lambda_slow: 1024MB RAM, baixa concurrency (~$0.0015/render)
- lambda_medium: 2048MB RAM, média concurrency (~$0.0013/render)
- lambda_fast: 3008MB RAM, alta concurrency (~$0.0008/render)

🆕 v2.9.101: Usa v-editor-lambda API com @remotion/lambda SDK
"""

import os
import json
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# v-editor-lambda API URL (serviço que usa @remotion/lambda SDK)
V_EDITOR_LAMBDA_URL = os.environ.get('V_EDITOR_LAMBDA_URL', 'http://v-editor-lambda:5050')

# URL do webhook de callback
WEBHOOK_BASE_URL = os.environ.get('CALLBACK_BASE_URL') or \
                   os.environ.get('WEBHOOK_INTERNAL_URL') or \
                   'https://api.vinicius.ai'


class LambdaRenderService:
    """
    Serviço de renderização via AWS Lambda (Remotion).
    
    Usa o v-editor-lambda API que implementa @remotion/lambda SDK.
    """
    
    def __init__(self):
        self.api_url = V_EDITOR_LAMBDA_URL
        self.webhook_base_url = WEBHOOK_BASE_URL
        self.timeout = 30  # Timeout para chamada inicial (render é async)
        
        logger.info(f"🚀 LambdaRenderService inicializado")
        logger.info(f"   API URL: {self.api_url}")
        logger.info(f"   Webhook Base: {self.webhook_base_url}")
    
    def is_configured(self) -> bool:
        """Verifica se Lambda está configurado"""
        try:
            response = requests.get(f"{self.api_url}/health", timeout=5)
            if response.status_code == 200:
                data = response.json()
                return data.get('configured', {}).get('function', False)
        except Exception:
            pass
        return False
    
    def submit_render_job(
        self,
        job_id: str,
        payload: Dict[str, Any],
        user_id: str,
        project_id: str,
        template_id: str = None,
        lambda_config: Dict[str, Any] = None,
        callback_endpoint: str = "/api/webhook/render-complete"
    ) -> Dict[str, Any]:
        """
        Envia job de renderização para v-editor-lambda API.
        
        O v-editor-lambda usa @remotion/lambda SDK para invocar o Lambda.
        
        Args:
            job_id: ID do job
            payload: Payload Remotion completo
            user_id: ID do usuário
            project_id: ID do projeto
            template_id: ID do template
            lambda_config: Configuração de memória/concurrency
            callback_endpoint: Endpoint para webhook
            
        Returns:
            {
                "status": "success" | "error",
                "render_id": "...",
                "render_status": "rendering",
                ...
            }
        """
        # Determinar modo baseado no lambda_config
        mode = 'lambda_medium'
        if lambda_config:
            memory = lambda_config.get('memory', 2048)
            if memory <= 1024:
                mode = 'lambda_slow'
            elif memory >= 3008:
                mode = 'lambda_fast'
        
        logger.info(f"🚀 [LAMBDA] Enviando job {job_id} para v-editor-lambda...")
        logger.info(f"   Mode: {mode}")
        logger.info(f"   API: {self.api_url}")
        
        # Construir webhook URL
        webhook_url = f"{self.webhook_base_url}{callback_endpoint}"
        
        # Extrair dimensões do payload para logging
        canvas = payload.get("canvas", {"width": 720, "height": 1280})
        fps = payload.get("fps", 30)
        duration_in_frames = payload.get("duration_in_frames", 0)
        
        logger.info(f"   Canvas: {canvas.get('width', 720)}x{canvas.get('height', 1280)} @ {fps}fps")
        logger.info(f"   Duration: {duration_in_frames} frames ({duration_in_frames / fps:.1f}s)")
        logger.info(f"   Payload keys: {list(payload.keys())}")
        
        # 🆕 v2.9.103: Renovar signed URL do video_url antes de enviar ao Lambda
        # Isso evita que URLs expiradas causem vídeos sem base
        payload = self._refresh_video_url_if_needed(payload, user_id, project_id)
        
        # 🆕 v2.9.102: Passar payload COMPLETO para v-editor-lambda
        # A composição VideoComposition espera o formato exato do v-editor local:
        # { projectSettings, baseVideo, layers, renderSettings, ... }
        api_payload = {
            "jobId": job_id,
            "composition": "VideoComposition",
            # Passar payload COMPLETO como inputProps (mesmo formato do v-editor local)
            "inputProps": payload,
            "webhookUrl": webhook_url,
            "mode": mode,
            "userId": user_id,
            "projectId": project_id,
            "templateId": template_id,
        }
        
        try:
            response = requests.post(
                f"{self.api_url}/render",
                json=api_payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout
            )
            
            if response.status_code in [200, 202]:
                result = response.json()
                logger.info(f"✅ [LAMBDA] Render iniciado: {result.get('renderId', job_id)}")
                return {
                    "status": "success",
                    "render_id": result.get("renderId", job_id),
                    "render_status": "rendering",
                    "queue_system": "aws_lambda",
                    "mode": mode,
                    "message": result.get("message", "Render iniciado via Lambda")
                }
            else:
                error_text = response.text[:500]
                logger.error(f"❌ [LAMBDA] Erro {response.status_code}: {error_text}")
                return {
                    "status": "error",
                    "error": f"v-editor-lambda retornou {response.status_code}: {error_text}"
                }
                
        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ [LAMBDA] Conexão falhou: {e}")
            return {
                "status": "error",
                "error": f"v-editor-lambda não acessível: {self.api_url}"
            }
        except Exception as e:
            logger.error(f"❌ [LAMBDA] Erro: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _refresh_video_url_if_needed(
        self, 
        payload: Dict[str, Any], 
        user_id: str, 
        project_id: str
    ) -> Dict[str, Any]:
        """
        🆕 v2.9.103: Gera nova signed URL para o video_url se necessário.
        
        Isso evita que URLs do Backblaze B2 expirem antes do Lambda renderizar.
        Signed URLs são geradas com validade de 24 horas.
        """
        video_url = payload.get("video_url", "")
        
        if not video_url:
            logger.info("   📹 Sem video_url no payload")
            return payload
        
        # Verificar se é URL do Backblaze B2 (precisa de signed URL)
        if "backblazeb2.com" not in video_url and "vinicius-ai-" not in video_url:
            logger.info(f"   📹 video_url não é Backblaze, mantendo: {video_url[:60]}...")
            return payload
        
        try:
            from app.utils.b2_client import get_b2_client
            
            # Extrair path do arquivo da URL
            # Formato: https://f001.backblazeb2.com/file/bucket-name/path/to/file.mp4?Authorization=...
            base_url = video_url.split('?')[0]  # Remove query params
            
            # Tentar extrair o bucket e path
            bucket_name = None
            file_path = None
            
            # Padrão 1: /file/bucket-name/path
            if "/file/" in base_url:
                parts = base_url.split("/file/")[1].split("/", 1)
                if len(parts) == 2:
                    bucket_name = parts[0]
                    file_path = parts[1]
            
            if not file_path:
                logger.warning(f"   ⚠️ Não foi possível extrair path de: {video_url[:80]}...")
                return payload
            
            logger.info(f"   🔐 Gerando nova signed URL para video_url...")
            logger.info(f"      Bucket: {bucket_name}")
            logger.info(f"      Path: {file_path[:60]}...")
            
            b2_client = get_b2_client()
            
            # Gerar URL assinada com validade de 24 horas (86400 segundos)
            new_signed_url = b2_client.generate_signed_url(
                file_path, 
                valid_duration_seconds=86400  # 24 horas
            )
            
            if new_signed_url:
                # Atualizar payload com nova URL
                payload = dict(payload)  # Criar cópia para não modificar original
                payload["video_url"] = new_signed_url
                logger.info(f"   ✅ Nova signed URL gerada (válida por 24h)")
                logger.info(f"      Nova URL: {new_signed_url[:80]}...")
            else:
                logger.warning(f"   ⚠️ generate_signed_url retornou None, mantendo URL original")
                
        except ImportError:
            logger.warning(f"   ⚠️ b2_client não disponível, mantendo URL original")
        except Exception as e:
            logger.error(f"   ❌ Erro ao gerar nova signed URL: {e}")
            # Manter URL original em caso de erro
        
        return payload
    
    def get_render_status(self, render_id: str) -> Dict[str, Any]:
        """Obtém status de um render em andamento"""
        try:
            response = requests.get(
                f"{self.api_url}/status/{render_id}",
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"status": "not_found"}
            else:
                return {"status": "error", "error": f"API retornou {response.status_code}"}
                
        except Exception as e:
            return {"status": "error", "error": str(e)}


# Singleton
_lambda_render_service = None


def get_lambda_render_service() -> LambdaRenderService:
    """Retorna instância singleton do LambdaRenderService"""
    global _lambda_render_service
    if _lambda_render_service is None:
        _lambda_render_service = LambdaRenderService()
    return _lambda_render_service
