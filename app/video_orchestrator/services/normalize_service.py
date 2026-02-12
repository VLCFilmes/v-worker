"""
🔊 Normalize Service - Wrapper para v-services/normalizacao

Encapsula chamadas para normalização de ÁUDIO via v-services.

📝 Nota: A normalização de FPS/resolução é feita pelo v-matting (GPU NVENC).
"""

import os
import logging
import requests
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# URL base do v-services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')
# Header Host para roteamento no ALB (se usando ALB interno)
V_SERVICES_HOST = os.environ.get('V_SERVICES_HOST', 'services.vinicius.ai')
# Token de autenticação para o serviço de normalização
V_SERVICES_SECRET_TOKEN = os.environ.get('V_SERVICES_SECRET_TOKEN', '')


class NormalizeService:
    """
    Wrapper para o serviço de normalização de áudio e vídeo do v-services.
    
    Endpoint: /normalizacao/audio_normalizer
    
    Nota: Este serviço EXIGE autenticação via Bearer token.
    """
    
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or V_SERVICES_URL
        self.host = V_SERVICES_HOST
        self.token = V_SERVICES_SECRET_TOKEN
        self.endpoint = f"{self.base_url}/normalizacao/audio_normalizer"
        self.timeout = 600  # 🆕 10 minutos (re-encoding pode demorar)
        self.headers = {
            "Content-Type": "application/json",
            "Host": self.host,
            "Authorization": f"Bearer {self.token}"  # 🔑 Token obrigatório!
        }
    
    def normalize(
        self,
        urls: List[str],
        true_peak: float = 0.0,  # 🔧 v2.7.0: Normaliza para 0dB (máximo)
        output_filename: str = "normalized",
        analyze_volume: bool = True,
        target_fps: Optional[int] = None,
        target_resolution: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Normaliza áudio e vídeo de um ou mais vídeos.
        
        Args:
            urls: Lista de URLs dos vídeos
            true_peak: Peak máximo em dB (padrão: 0.0)
            output_filename: Nome base do arquivo de saída
            analyze_volume: Se True, retorna análise de volume
            target_fps: 🆕 FPS alvo (ex: 30). None = manter original.
            target_resolution: 🆕 Resolução "WxH" (ex: "1080x1920"). None = manter original.
            
        Returns:
            Dict com resultados da normalização:
            {
                "status": "completed",
                "results": [{
                    "original_url": "...",
                    "output_url": "...",
                    "shared_path": "...",
                    "volume_analysis": {...},
                    "video_info": {...}
                }]
            }
            
        Note:
            A normalização de FPS usa duplicação/remoção de frames, NÃO altera
            a velocidade do vídeo. Ideal para garantir CFR (Constant Frame Rate).
        """
        payload = {
            "urls": urls,
            "true_peak": true_peak,
            "output_filename": output_filename,
            "analyze_volume_levels": analyze_volume
        }
        
        # 🆕 Adicionar parâmetros de normalização de vídeo
        if target_fps:
            payload["target_fps"] = target_fps
        if target_resolution:
            payload["target_resolution"] = target_resolution
        
        logger.info(f"🔊 [v3.0.0] Normalizando ÁUDIO de {len(urls)} vídeo(s) com true_peak={true_peak}")
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                timeout=self.timeout,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"✅ Normalização concluída: {len(result.get('results', []))} arquivo(s)")
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na normalização após {self.timeout}s")
            return {"error": "Timeout na normalização", "status": "failed"}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na normalização: {e}")
            return {"error": str(e), "status": "failed"}
    
    def health_check(self) -> bool:
        """Verifica se o serviço está disponível"""
        try:
            response = requests.get(
                f"{self.base_url}/normalizacao/health",
                timeout=10,
                headers={"Host": self.host}
            )
            return response.status_code == 200
        except:
            return False

