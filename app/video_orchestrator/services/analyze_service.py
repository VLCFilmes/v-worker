"""
🔬 Analyze Service - Wrapper para v-services/normalizacao/analyze

Normalização agressiva para melhorar detecção de silêncios.
O áudio resultante NÃO é para escuta final (pode soar "processado").
"""

import os
import logging
import requests
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

# URL base do v-services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')
# Header Host para roteamento no ALB (se usando ALB interno)
V_SERVICES_HOST = os.environ.get('V_SERVICES_HOST', 'services.vinicius.ai')
# Token de autenticação
V_SERVICES_SECRET_TOKEN = os.environ.get('V_SERVICES_SECRET_TOKEN', '')


class AnalyzeService:
    """
    Wrapper para o serviço de análise de áudio do v-services.
    
    Endpoint: /normalizacao/analyze
    
    Este serviço aplica normalização AGRESSIVA para melhorar
    a precisão da detecção de silêncios. O áudio resultante
    NÃO deve ser usado para escuta final.
    
    Presets disponíveis:
    - conservative: Mínima alteração, preserva nuances
    - balanced: Equilibrado (default), bom para maioria
    - aggressive: Máxima separação fala/silêncio
    """
    
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or V_SERVICES_URL
        self.host = V_SERVICES_HOST
        self.token = V_SERVICES_SECRET_TOKEN
        self.endpoint = f"{self.base_url}/normalizacao/analyze"
        self.timeout = 300  # 5 minutos
        self.headers = {
            "Content-Type": "application/json",
            "Host": self.host,
            "Authorization": f"Bearer {self.token}"
        }
    
    def analyze(
        self,
        url: str,
        preset: str = "balanced",
        output_filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Aplica normalização agressiva para análise de silêncios.
        
        Args:
            url: URL do vídeo/áudio
            preset: "conservative", "balanced" (default), "aggressive"
            output_filename: Nome do arquivo de saída (opcional)
            
        Returns:
            Dict com resultado:
            {
                "status": "success",
                "output_url": "https://services.vinicius.ai/normalizacao/files/...",
                "shared_path": "/app/shared-storage/normalizacao/analysis/...",
                "suggested_silence_threshold": -28.5,
                "analysis_metrics": {
                    "preset_used": "balanced",
                    "volume_stats": {...}
                }
            }
        """
        payload = {
            "url": url,
            "preset": preset
        }
        
        if output_filename:
            payload["output_filename"] = output_filename
        
        logger.info(f"🔬 Analisando áudio com preset '{preset}'")
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                timeout=self.timeout,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            threshold = result.get('suggested_silence_threshold')
            logger.info(f"✅ Análise concluída - Threshold sugerido: {threshold}dB")
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na análise após {self.timeout}s")
            return {"error": "Timeout na análise", "status": "failed"}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na análise: {e}")
            return {"error": str(e), "status": "failed"}
    
    def get_presets(self) -> Dict[str, Any]:
        """Lista os presets disponíveis"""
        try:
            response = requests.get(
                f"{self.base_url}/normalizacao/analyze/presets",
                timeout=10,
                headers={"Host": self.host}
            )
            return response.json()
        except:
            return {"error": "Falha ao obter presets"}
    
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

