"""
🔇 Silence Service - Wrapper para v-services/ffmpeg/detect_silence e silence_cut

Encapsula chamadas para detecção e corte de silêncios via v-services.
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


class SilenceService:
    """
    Wrapper para os serviços de silêncio do v-services.
    
    Endpoints:
    - /ffmpeg/detect_silence
    - /ffmpeg/silence_cut
    """
    
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or V_SERVICES_URL
        self.host = V_SERVICES_HOST
        self.detect_endpoint = f"{self.base_url}/ffmpeg/detect_silence"
        self.cut_endpoint = f"{self.base_url}/ffmpeg/silence_cut"
        self.timeout = 600  # 10 minutos
        self.headers = {
            "Content-Type": "application/json",
            "Host": self.host,
            "Authorization": f"Bearer {V_SERVICES_TOKEN}"
        }
    
    def detect_silence(
        self,
        url: str,
        silence_threshold: Optional[float] = None,
        min_silence_duration: float = 0.5,
        detect_speech: bool = True,
        vad_aggressiveness: int = 2,
        silence_offset: float = 0.0,   # 🔧 v4.1: 0.3→0.0 (multi-arquivo)
        threshold_offset: float = 3,   # 🔧 v4.1: 5→3 (menos agressivo)
        minimum_speech_duration: float = 0.4,  # 🔧 v4.1: 0.6→0.4
        trim_start: float = 0.0,       # 🔧 v4.1: 0.3→0.0 (multi-arquivo: trim por vídeo cortava fala)
        trim_end: float = 0.0,         # 🔧 v4.1: 0→0 (explícito)
        volume_analysis: Optional[Dict[str, Any]] = None  # 🆕 v2.8.0: Dados do normalizador
    ) -> Dict[str, Any]:
        """
        Detecta períodos de silêncio em um vídeo.
        
        Args:
            url: URL do vídeo
            silence_threshold: Threshold em dB (se None, usa volume_analysis ou auto-detect)
            min_silence_duration: Duração mínima do silêncio em segundos
            detect_speech: Usar VAD para detectar fala
            vad_aggressiveness: Agressividade do VAD (1-3)
            silence_offset: Offset em segundos
            threshold_offset: Offset do threshold em dB
            minimum_speech_duration: Duração mínima da fala
            trim_start: Trim no início em segundos
            trim_end: Trim no final em segundos
            volume_analysis: 🆕 v2.8.0 - Dados de análise de volume do normalizador
                             Se fornecido, usa min_speech_level para calcular threshold
            
        Returns:
            Dict com períodos de silêncio detectados:
            {
                "silence_periods": [
                    {"start": 0.0, "end": 1.5, "duration": 1.5},
                    ...
                ],
                "speech_periods": [
                    {"start": 1.5, "end": 5.0, "duration": 3.5},
                    ...
                ],
                "total_duration": 120.5,
                "total_silence_duration": 25.3,
                "processing_time_seconds": 5.2,
                "threshold_info": {...}  # 🆕 v2.8.0: Info de como threshold foi calculado
            }
        """
        payload = {
            "url": url,
            "min_silence_duration": min_silence_duration,
            "detect_speech": detect_speech,
            "vad_aggressiveness": vad_aggressiveness,
            "silence_offset": silence_offset,
            "threshold_offset": threshold_offset,
            "minimum_speech_duration": minimum_speech_duration,
            "trim_start": trim_start,
            "trim_end": trim_end
        }
        
        # Threshold pode ser dinâmico (calculado do mean_volume ou min_speech_level)
        if silence_threshold is not None:
            payload["silence_threshold"] = silence_threshold
        
        # 🆕 v2.8.0: Passar volume_analysis para usar min_speech_level
        if volume_analysis:
            payload["volume_analysis"] = volume_analysis
            min_speech_level = volume_analysis.get('min_speech_level')
            noise_floor = volume_analysis.get('noise_floor')
            suggested_threshold = volume_analysis.get('suggested_silence_threshold')
            logger.info(f"📊 [DETECT_SILENCE] Usando volume_analysis do normalizador:")
            logger.info(f"   🗣️ min_speech_level: {min_speech_level}dB" if min_speech_level else "   🗣️ min_speech_level: N/A")
            logger.info(f"   📉 noise_floor: {noise_floor}dB" if noise_floor else "   📉 noise_floor: N/A")
            logger.info(f"   🎯 suggested_threshold: {suggested_threshold}dB" if suggested_threshold else "   🎯 suggested_threshold: N/A")
        
        # 🆕 v2.7.5: LOG DETALHADO do payload para debug
        logger.info(f"🔍 [DETECT_SILENCE] Payload completo:")
        logger.info(f"   📹 URL: ...{url[-50:] if url else 'None'}")
        logger.info(f"   ⏱️ min_silence_duration: {min_silence_duration}s")
        logger.info(f"   🗣️ minimum_speech_duration: {minimum_speech_duration}s")
        logger.info(f"   📐 silence_offset: {silence_offset}s")
        logger.info(f"   🔊 silence_threshold: {silence_threshold or 'AUTO (from volume_analysis or auto-detect)'}")
        logger.info(f"   📊 threshold_offset: {threshold_offset}dB")
        logger.info(f"   ✂️ trim_start: {trim_start}s, trim_end: {trim_end}s")
        
        try:
            response = requests.post(
                self.detect_endpoint,
                json=payload,
                timeout=self.timeout,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            silence_count = len(result.get('silence_periods', []))
            logger.info(f"✅ Detecção concluída: {silence_count} períodos de silêncio")
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na detecção após {self.timeout}s")
            return {"error": "Timeout na detecção de silêncio", "status": "failed"}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na detecção: {e}")
            return {"error": str(e), "status": "failed"}
    
    def cut_silence(
        self,
        input_file: str,
        output_file: str,
        clips: List[Dict],
        clips_type: str = "silence_periods",
        cut_mode: str = "all_silences",
        fast_copy: bool = False,         # 🔧 v4.2: True→False (cortes precisos, evita perda por keyframe)
        optimize_keyframes: bool = False, # 🔧 v4.2: True→False (desnecessário com re-encoding)
        keyframe_precision: str = "medium",
        parallel_processing: bool = True,
        preset: str = "ultrafast",
        quality: int = 22,
        target_fps: int = None  # 🆕 Converter para FPS específico (ex: 30)
    ) -> Dict[str, Any]:
        """
        Corta silêncios de um vídeo baseado nos clips detectados.
        
        Args:
            input_file: URL ou caminho do vídeo de entrada
            output_file: Nome do arquivo de saída (sem extensão)
            clips: Lista de períodos (do detect_silence)
            clips_type: "silence_periods" (remove) ou "speech_periods" (mantém)
            cut_mode: "all_silences" ou "only_edges"
            fast_copy: Usar stream copy para velocidade
            optimize_keyframes: Otimizar keyframes
            keyframe_precision: "ultra", "high", "medium"
            parallel_processing: Processar em paralelo
            preset: Preset do FFmpeg
            quality: CRF (0=melhor, 51=pior)
            target_fps: 🆕 FPS de destino (None = manter original, 30 = converter para 30fps)
            
        Returns:
            Dict com resultado do corte:
            {
                "status": "completed",
                "output_url": "https://...",
                "shared_path": "/app/shared-storage/...",
                "original_duration": 120.5,
                "final_duration": 95.2,
                "removed_duration": 25.3,
                "processing_time_seconds": 45.6
            }
        """
        payload = {
            "input_file": input_file,
            "output_file": output_file,
            "clips": clips,
            "clips_type": clips_type,
            "cut_mode": cut_mode,
            "fast_copy": fast_copy,
            "optimize_keyframes": optimize_keyframes,
            "keyframe_precision": keyframe_precision,
            "parallel_processing": parallel_processing,
            "preset": preset,
            "quality": quality
        }
        
        # 🆕 Adicionar target_fps se especificado
        if target_fps:
            payload["target_fps"] = target_fps
            # Não usar fast_copy quando converter FPS (precisa re-codificar)
            payload["fast_copy"] = False
            # 🔧 NVENC não suporta preset "ultrafast", usar "fast" que funciona com ambos
            payload["preset"] = "fast"
            # 🔧 Forçar libx264 para evitar problemas de preset com NVENC
            payload["hardware_accel"] = "cpu"
            logger.info(f"   📐 Convertendo para {target_fps} FPS (usando libx264 para compatibilidade)")
        
        logger.info(f"✂️ Cortando silêncios do vídeo (mode: {cut_mode})")
        
        try:
            response = requests.post(
                self.cut_endpoint,
                json=payload,
                timeout=self.timeout,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            removed = result.get('removed_duration', 0)
            logger.info(f"✅ Corte concluído: {removed}s de silêncio removido")
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout no corte após {self.timeout}s")
            return {"error": "Timeout no corte de silêncio", "status": "failed"}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro no corte: {e}")
            return {"error": str(e), "status": "failed"}
    
    def cut_silence_hybrid(
        self,
        input_file: str,
        output_prefix: str,
        clips: List[Dict],
        clips_type: str = "speech_periods",
        fast_copy: bool = False,
        optimize_keyframes: bool = False,
        keyframe_precision: str = "medium",
        preset: str = "ultrafast",
        quality: int = 23,
        min_speech_duration: float = 0.0  # 🆕 v2.9.40: Filtro de duração mínima
    ) -> Dict[str, Any]:
        """
        🆕 v2.9.0: HYBRID SILENCE CUT
        
        Corta silêncios gerando:
        1. Clips de vídeo SEPARADOS (não concatenados) - para v-editor
        2. Áudio CONCATENADO único - para transcrição (Whisper/AssemblyAI)
        
        Isso resolve problemas de sync mantendo flexibilidade para o v-editor.
        
        🔧 v2.9.50: Defaults alterados para cortes PRECISOS (re-encoding)
        fast_copy=False evita imprecisão por keyframe alignment do FFmpeg.
        
        Args:
            input_file: URL ou caminho do vídeo de entrada
            output_prefix: Prefixo para nomes dos arquivos
            clips: Lista de períodos (do detect_silence)
            clips_type: "silence_periods" (remove) ou "speech_periods" (mantém)
            fast_copy: Usar stream copy (rápido mas impreciso nos cortes)
            optimize_keyframes: Otimizar keyframes antes (só útil com fast_copy)
            keyframe_precision: "ultra", "high", "medium"
            preset: Preset do FFmpeg (para re-encoding)
            quality: CRF (0=melhor, 51=pior)
            
        Returns:
            Dict com resultado do corte híbrido:
            {
                "status": "completed",
                "audio_concatenated": {
                    "url": "https://...",
                    "shared_path": "ffmpeg/audio/...",
                    "duration_seconds": 95.2
                },
                "video_segments": [
                    {
                        "url": "https://...",
                        "shared_path": "ffmpeg/videos/...",
                        "original_start": 0.5,
                        "original_end": 5.2,
                        "audio_offset": 0.0,
                        "duration": 4.7
                    },
                    ...
                ],
                "total_segments": 15,
                "original_duration": 120.5,
                "final_audio_duration": 95.2,
                "removed_duration": 25.3
            }
        """
        hybrid_endpoint = f"{self.base_url}/ffmpeg/silence_cut_hybrid"
        
        payload = {
            "input_file": input_file,
            "output_prefix": output_prefix,
            "clips": clips,
            "clips_type": clips_type,
            "fast_copy": fast_copy,
            "optimize_keyframes": optimize_keyframes,
            "keyframe_precision": keyframe_precision,
            "preset": preset,
            "quality": quality,
            "min_speech_duration": min_speech_duration  # 🆕 v2.9.40
        }
        
        cut_mode_label = "FAST COPY (keyframe)" if fast_copy else "PRECISE (re-encoding)"
        logger.info(f"🔀 [HYBRID CUT] Cortando silêncios (modo híbrido)")
        logger.info(f"   📊 {len(clips)} clips ({clips_type})")
        logger.info(f"   🎯 Modo de corte: {cut_mode_label}")
        logger.info(f"   🗣️ min_speech_duration: {min_speech_duration}s")
        
        try:
            response = requests.post(
                hybrid_endpoint,
                json=payload,
                timeout=self.timeout,
                headers=self.headers
            )
            
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") == "completed":
                segments_count = result.get('total_segments', 0)
                audio_duration = result.get('audio_concatenated', {}).get('duration_seconds', 0)
                logger.info(f"✅ Corte híbrido concluído: {segments_count} clips, {audio_duration:.2f}s de áudio")
            
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout no corte híbrido após {self.timeout}s")
            return {"error": "Timeout no corte híbrido de silêncio", "status": "failed"}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro no corte híbrido: {e}")
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

