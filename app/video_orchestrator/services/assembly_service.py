"""
🎤 AssemblyAI Service - Transcrição com word-level timestamps

Substitui Whisper para transcrições. Retorna words[] no formato:
[{"text": "Olá", "start": 0.0, "end": 0.5, "confidence": 0.98}, ...]

Usa ai_config para gerenciamento centralizado de API keys.
Service key: 'video_transcription'
"""

import os
import time
import logging
import requests
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Fallback para variável de ambiente (desenvolvimento local)
ASSEMBLY_API_KEY_ENV = os.environ.get('ASSEMBLY_API_KEY')
ASSEMBLY_BASE_URL = "https://api.assemblyai.com/v2"

# Service key no ai_config
AI_CONFIG_SERVICE_KEY = 'video_transcription'


def _get_assembly_key_from_ai_config() -> Optional[str]:
    """
    Busca API key do AssemblyAI via ai_config (centralizado).
    
    O ai_config busca da tabela ai_service_configs onde:
    - service_key = 'video_transcription'
    - provider = 'assemblyai'
    """
    try:
        from app.ai_config import get_ai_config
        
        config = get_ai_config(AI_CONFIG_SERVICE_KEY)
        if config and config.get('api_key'):
            logger.info(f"✅ AssemblyAI key carregada via ai_config")
            return config['api_key']
        
        logger.debug(f"⚠️ Serviço '{AI_CONFIG_SERVICE_KEY}' não configurado no ai_config")
        return None
        
    except Exception as e:
        logger.debug(f"⚠️ Erro ao buscar ai_config: {e}")
        return None


class AssemblyAIService:
    """
    Serviço de transcrição usando AssemblyAI.
    
    Fluxo:
    1. POST /transcript - Inicia transcrição (retorna ID)
    2. GET /transcript/{id} - Polling até status = completed
    3. Retorna words[] com timestamps (formato compatível com fraseamento)
    
    Configuração:
    1. ai_config (banco) - service_key: 'video_transcription' (RECOMENDADO)
    2. Variável de ambiente ASSEMBLY_API_KEY (fallback para dev local)
    
    Vantagens sobre Whisper:
    - Word-level timestamps nativo
    - Pontuação automática
    - Melhor qualidade para PT-BR
    """
    
    def __init__(self):
        self.base_url = ASSEMBLY_BASE_URL
        
        # Prioridade: ai_config > env > None
        self.api_key = _get_assembly_key_from_ai_config()
        self.key_source = "ai_config"
        
        if not self.api_key and ASSEMBLY_API_KEY_ENV:
            self.api_key = ASSEMBLY_API_KEY_ENV
            self.key_source = "env"
        
        if self.api_key:
            logger.info(f"✅ AssemblyAI configurado (fonte: {self.key_source})")
        else:
            logger.warning("⚠️ AssemblyAI não configurado - usando Whisper como fallback")
            logger.warning("   Configure via ai_config (service_key: 'video_transcription')")
            self.key_source = None
        
        self.headers = {
            "Authorization": self.api_key or "",
            "Content-Type": "application/json"
        }
    
    def is_available(self) -> bool:
        """Verifica se o serviço está disponível (API key configurada)"""
        return bool(self.api_key)
    
    def transcribe(
        self,
        audio_url: str,
        language_code: str = "pt",
        poll_interval: int = 3,
        max_wait_time: int = 600
    ) -> Dict[str, Any]:
        """
        Transcreve áudio usando AssemblyAI.
        
        Args:
            audio_url: URL pública do áudio/vídeo
            language_code: Código do idioma (pt, en, es, etc)
            poll_interval: Intervalo de polling em segundos
            max_wait_time: Tempo máximo de espera em segundos
            
        Returns:
            {
                "status": "completed",
                "transcript": "Texto completo...",
                "transcript_partial": "Texto completo...",  # Para compatibilidade
                "words": [
                    {"text": "Olá", "start": 0.0, "end": 0.5, "confidence": 0.98},
                    ...
                ],
                "confidence_avg": 0.95,
                "duration_s": 120,
                "word_count": 150
            }
        """
        if not self.api_key:
            return {
                "error": "ASSEMBLY_API_KEY não configurada",
                "status": "failed"
            }
        
        logger.info(f"🎤 Iniciando transcrição AssemblyAI (idioma: {language_code})")
        logger.info(f"🔗 URL: {audio_url[:80]}...")
        
        try:
            # 1. POST para iniciar transcrição
            response = requests.post(
                f"{self.base_url}/transcript",
                headers=self.headers,
                json={
                    "audio_url": audio_url,
                    "language_code": language_code,
                    "format_text": True,
                    "punctuate": True,
                    "speech_threshold": 0.2
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            transcript_id = result.get("id")
            
            if not transcript_id:
                return {
                    "error": "Falha ao iniciar transcrição - ID não retornado",
                    "status": "failed"
                }
            
            logger.info(f"📝 Transcrição iniciada: {transcript_id}")
            
            # 2. Polling até completar
            elapsed = 0
            while elapsed < max_wait_time:
                time.sleep(poll_interval)
                elapsed += poll_interval
                
                status_response = requests.get(
                    f"{self.base_url}/transcript/{transcript_id}",
                    headers=self.headers,
                    timeout=30
                )
                status_response.raise_for_status()
                status_data = status_response.json()
                
                status = status_data.get("status")
                
                if status == "completed":
                    logger.info(f"✅ Transcrição completa após {elapsed}s")
                    return self._format_response(status_data)
                
                elif status == "error":
                    error_msg = status_data.get("error", "Erro desconhecido")
                    logger.error(f"❌ Transcrição falhou: {error_msg}")
                    return {
                        "error": error_msg,
                        "status": "failed"
                    }
                
                # Ainda processando
                if elapsed % 15 == 0:  # Log a cada 15s
                    logger.info(f"⏳ Aguardando transcrição... ({elapsed}s)")
            
            # Timeout
            logger.error(f"⏱️ Timeout após {max_wait_time}s")
            return {
                "error": f"Timeout após {max_wait_time}s",
                "status": "timeout"
            }
            
        except requests.exceptions.Timeout:
            logger.error("⏱️ Timeout na requisição HTTP")
            return {"error": "Timeout na requisição", "status": "failed"}
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na requisição: {e}")
            return {"error": str(e), "status": "failed"}
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {e}")
            return {"error": str(e), "status": "failed"}
    
    def _format_response(self, assembly_response: Dict) -> Dict[str, Any]:
        """
        Formata resposta do AssemblyAI para nosso padrão.
        
        Converte:
        - Timestamps de milissegundos para segundos
        - Adiciona campos de compatibilidade
        """
        words_raw = assembly_response.get("words", [])
        
        # Converter formato (ms → seconds)
        words = []
        for w in words_raw:
            words.append({
                "text": w.get("text", "").strip(),
                "start": w.get("start", 0) / 1000,  # ms → seconds
                "end": w.get("end", 0) / 1000,      # ms → seconds
                "confidence": w.get("confidence", 0.8)
            })
        
        transcript = assembly_response.get("text", "")
        # 🐛 FIX v2: audio_duration do AssemblyAI é INTEGER (sem decimais)!
        # Para precisão, usar timestamp da última palavra (que é float preciso)
        audio_duration_api = assembly_response.get("audio_duration", 0)
        confidence = assembly_response.get("confidence", 0.8)
        
        # 🎯 Duração PRECISA: usar end da última palavra (já convertido para segundos)
        # Isso evita erro de arredondamento (17.234s → 17s)
        if words:
            last_word_end = words[-1].get("end", 0)
            # Usar o maior valor entre API e última palavra (segurança)
            precise_duration = max(last_word_end, float(audio_duration_api))
        else:
            precise_duration = float(audio_duration_api)
        
        # 💰 Cálculo de custo estimado
        # AssemblyAI cobra $0.15/hora (modelo Universal)
        # $0.15/hora ÷ 3600 segundos = $0.0000417/segundo
        # Fonte: https://www.assemblyai.com/pricing
        COST_PER_HOUR = 0.15  # USD
        COST_PER_SECOND = COST_PER_HOUR / 3600  # ~$0.0000417/segundo
        estimated_cost_usd = precise_duration * COST_PER_SECOND
        
        logger.info(f"📊 {len(words)} palavras transcritas, duração: {precise_duration:.3f}s (API: {audio_duration_api}s)")
        logger.info(f"💰 Custo estimado: ${estimated_cost_usd:.6f} USD ({precise_duration:.1f}s × ${COST_PER_SECOND:.7f}/s = ~${COST_PER_HOUR}/hora)")
        
        return {
            "status": "completed",
            "transcript": transcript,
            "transcript_partial": transcript,  # Compatibilidade com Whisper
            "words": words,
            "confidence_avg": confidence,
            "duration_s": precise_duration,  # ✅ Agora preciso (float)!
            "word_count": len(words),
            "provider": "assemblyai",
            # 💰 Métricas de custo (para tracking)
            "cost_estimate": {
                "duration_seconds": precise_duration,
                "cost_per_hour_usd": COST_PER_HOUR,
                "cost_per_second_usd": round(COST_PER_SECOND, 8),
                "estimated_cost_usd": round(estimated_cost_usd, 6),
                "pricing_source": "https://www.assemblyai.com/pricing (Universal model)"
            }
        }
    
    def health_check(self) -> bool:
        """Verifica se o serviço está disponível e API key é válida"""
        if not self.api_key:
            return False
        
        try:
            # Faz uma chamada simples para verificar a API key
            # GET /transcript retorna lista de transcrições (vazia se nova conta)
            response = requests.get(
                f"{self.base_url}/transcript",
                headers=self.headers,
                params={"limit": 1},
                timeout=10
            )
            # 401 = key inválida, 200 = ok
            return response.status_code == 200
        except Exception as e:
            logger.error(f"❌ Health check falhou: {e}")
            return False

