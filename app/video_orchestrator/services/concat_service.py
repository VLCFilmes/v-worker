"""
🎬 Concat Service - Wrapper para v-services/ffmpeg/concat

Encapsula chamadas para concatenação de vídeos via v-services.
"""

import os
import logging
import requests
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# URL base do v-services (usar ALB interno para bypass Cloudflare)
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')
V_SERVICES_HOST = os.environ.get('V_SERVICES_HOST', 'services.vinicius.ai')
V_SERVICES_TOKEN = os.environ.get('V_SERVICES_AUTH_TOKEN', '612d13aee901126f5101611fc5d1a53a348c6407b8653b5428fb9cca5ffe9d21')


class ConcatService:
    """
    Wrapper para o serviço de concatenação do v-services.
    
    Endpoint: /ffmpeg/concat
    """
    
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or V_SERVICES_URL
        self.host = V_SERVICES_HOST
        self.endpoint = f"{self.base_url}/ffmpeg/concat"
        self.timeout = 600  # 10 minutos
        self.headers = {
            "Content-Type": "application/json",
            "Host": self.host,
            "Authorization": f"Bearer {V_SERVICES_TOKEN}"
        }
    
    def concat(
        self,
        urls: List[str],
        output_file: str,
        analyze_volume: bool = True,
        force_copy: bool = True  # IMPORTANTE: Evita re-encoding e perda de qualidade
    ) -> Dict[str, Any]:
        """
        Concatena múltiplos vídeos em um único arquivo.
        
        Args:
            urls: Lista de URLs dos vídeos (na ordem de concatenação)
            output_file: Nome do arquivo de saída (sem extensão)
            analyze_volume: Se True, retorna análise de volume
            force_copy: Se True, usa stream copy (sem perda de qualidade)
            
        Returns:
            Dict com resultado da concatenação:
            {
                "status": "completed",
                "output_url": "https://...",
                "shared_path": "/app/shared-storage/...",
                "preview_url": "https://...",
                "volume_analysis": {
                    "mean_volume": -24.5,
                    "max_volume": -3.0
                },
                "duration": 120.5,
                "processing_time_seconds": 15.3
            }
        """
        payload = {
            "urls": urls,
            "output_file": output_file,
            "analyze_volume": analyze_volume,
            "force_copy": force_copy  # Preserva qualidade original
        }
        
        logger.info(f"🎬 Concatenando {len(urls)} vídeo(s)")
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                timeout=self.timeout,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            # v-services retorna 'duration_seconds', não 'duration'
            duration = result.get('duration_seconds') or result.get('duration', 0)
            logger.info(f"✅ Concatenação concluída: {duration}s de vídeo")
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na concatenação após {self.timeout}s")
            return {"error": "Timeout na concatenação", "status": "failed"}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na concatenação: {e}")
            return {"error": str(e), "status": "failed"}
    
    def health_check(self) -> bool:
        """Verifica se o serviço está disponível"""
        try:
            response = requests.get(
                f"{self.base_url}/ffmpeg/health",
                timeout=10,
                headers={"Host": self.host}
            )
            return response.status_code == 200
        except:
            return False

