"""
🎤 Transcription Service - Wrapper para v-services/whisper

Encapsula chamadas para transcrição via Whisper no v-services.
"""

import os
import logging
import requests
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

# URL base do v-services (usar ALB interno para bypass Cloudflare)
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')
V_SERVICES_HOST = os.environ.get('V_SERVICES_HOST', 'services.vinicius.ai')
V_SERVICES_TOKEN = os.environ.get('V_SERVICES_AUTH_TOKEN', '612d13aee901126f5101611fc5d1a53a348c6407b8653b5428fb9cca5ffe9d21')


class TranscriptionService:
    """
    Wrapper para o serviço de transcrição Whisper do v-services.
    
    Endpoints:
    - /whisper/transcribe/fast (síncrono)
    - /whisper/transcribe/async (assíncrono com webhook)
    """
    
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or V_SERVICES_URL
        self.host = V_SERVICES_HOST
        self.sync_endpoint = f"{self.base_url}/whisper/transcribe/fast"
        self.async_endpoint = f"{self.base_url}/whisper/transcribe/async"
        self.timeout = 600  # 10 minutos para sync
        self.headers = {
            "Content-Type": "application/json",
            "Host": self.host,
            "Authorization": f"Bearer {V_SERVICES_TOKEN}"
        }
    
    def transcribe_sync(
        self,
        audio_url: str,
        language: str = "pt",
        max_duration_s: int = 120
    ) -> Dict[str, Any]:
        """
        Transcreve áudio de forma síncrona (aguarda resultado).
        
        Recomendado para vídeos curtos (< 5 minutos).
        
        Args:
            audio_url: URL do áudio/vídeo
            language: Código do idioma (pt, en, es, etc)
            max_duration_s: Duração máxima para transcrever (10-120s)
            
        Returns:
            Dict com transcrição:
            {
                "status": "completed",
                "transcript": "Texto completo da transcrição...",
                "transcript_partial": "Texto completo...",
                "words": [
                    {"start": 0.0, "end": 0.5, "word": "Olá", "confidence": 0.98},
                    ...
                ],
                "language": "pt",
                "confidence_avg": 0.95,
                "duration_s": 120.5,
                "processing_time_seconds": 45.6
            }
        """
        # NOTA: O endpoint /whisper/transcribe/fast espera:
        # - source_url (não audio_url)
        # - language_hint (não language)
        # - max_duration_s (obrigatório, 10-120)
        payload = {
            "source_url": audio_url,
            "language_hint": language,
            "max_duration_s": max_duration_s
        }
        
        logger.info(f"🎤 Iniciando transcrição síncrona (idioma: {language})")
        
        try:
            response = requests.post(
                self.sync_endpoint,
                json=payload,
                timeout=self.timeout,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            word_count = len(result.get('words', []))
            logger.info(f"✅ Transcrição concluída: {word_count} palavras")
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na transcrição após {self.timeout}s")
            return {"error": "Timeout na transcrição", "status": "failed"}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na transcrição: {e}")
            return {"error": str(e), "status": "failed"}
    
    def transcribe_async(
        self,
        audio_url: str,
        language: str = "pt",
        webhook_url: str = None,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Inicia transcrição assíncrona (retorna job_id, resultado via webhook).
        
        Recomendado para vídeos longos (> 5 minutos).
        
        Args:
            audio_url: URL do áudio/vídeo
            language: Código do idioma
            webhook_url: URL para receber resultado
            metadata: Metadados adicionais para incluir no webhook
            
        Returns:
            Dict com job_id:
            {
                "job_id": "uuid",
                "status": "queued",
                "message": "Transcrição enfileirada"
            }
        """
        # NOTA: O endpoint /whisper/transcribe/async espera:
        # - source_url (não audio_url)
        # - language_hint (não language)
        # - max_duration_s
        payload = {
            "source_url": audio_url,
            "language_hint": language,
            "max_duration_s": 120
        }
        
        if webhook_url:
            payload["webhook_url"] = webhook_url
        
        if metadata:
            payload["metadata"] = metadata
        
        logger.info(f"🎤 Iniciando transcrição assíncrona (idioma: {language})")
        
        try:
            response = requests.post(
                self.async_endpoint,
                json=payload,
                timeout=30,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            job_id = result.get('job_id')
            logger.info(f"✅ Transcrição enfileirada: job_id={job_id}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao enfileirar transcrição: {e}")
            return {"error": str(e), "status": "failed"}
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Verifica status de um job de transcrição assíncrono.
        
        Args:
            job_id: ID do job
            
        Returns:
            Dict com status atual do job
        """
        try:
            response = requests.get(
                f"{self.base_url}/whisper/job/{job_id}",
                timeout=10,
                headers={"Host": self.host}
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao verificar job {job_id}: {e}")
            return {"error": str(e), "status": "unknown"}
    
    def health_check(self) -> bool:
        """Verifica se o serviço está disponível"""
        try:
            response = requests.get(
                f"{self.base_url}/whisper/health",
                timeout=10,
                headers={"Host": self.host}
            )
            return response.status_code == 200
        except:
            return False

