"""
🎨 Background Generator Service - Step 13 (HTTP Client)

Cliente HTTP que chama o v-services para gerar PNGs de backgrounds.
Toda a geração acontece no v-services, que já tem o volume EFS montado.

Arquitetura:
- custom-api (orchestrator) → HTTP → v-services → EFS → v-editor

Fluxo:
1. Recebe sentences do Step 12 (Positioning) com bounding box
2. Chama POST /backgrounds/generate no v-services
3. v-services gera PNGs e salva em /app/shared-storage/temp_frames/{job_id}/backgrounds/
4. Retorna lista de backgrounds com paths
"""

import os
import logging
import requests
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# 🆕 Importar debug_logger para salvar payloads no banco
try:
    from app.video_orchestrator.debug_logger import debug_logger
except ImportError:
    debug_logger = None
    logger.warning("⚠️ debug_logger não disponível - logs de auditoria desabilitados")

# URL do v-services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'http://v-services:5000')


class BackgroundGeneratorService:
    """
    Cliente HTTP para o serviço de geração de backgrounds.
    
    Toda a geração acontece no v-services, este serviço apenas
    faz a chamada HTTP e retorna os resultados.
    """
    
    def __init__(self):
        self.v_services_url = V_SERVICES_URL
        logger.info(f"🎨 BackgroundGeneratorService (HTTP Client) inicializado")
        logger.info(f"   • V-Services URL: {self.v_services_url}")
    
    def generate_backgrounds(
        self,
        sentences: List[Dict[str, Any]],
        text_styles: Dict[str, Any],
        canvas: Dict[str, int],
        job_id: str
    ) -> Dict[str, Any]:
        """
        Gera backgrounds para todas as sentences via HTTP.
        
        Args:
            sentences: Lista de sentences do Positioning (com layout/bounding box)
            text_styles: Dict com configs de cada estilo (ts_*_background)
            canvas: {"width": 720, "height": 1280}
            job_id: ID do job para nomear arquivos
            
        Returns:
            {
                "status": "success",
                "backgrounds": [...],
                "total": 12,
                "stats": {"word_bgs": 0, "phrase_bgs": 10, "fullscreen_bgs": 2}
            }
        """
        logger.info(f"🎨 [BACKGROUNDS] Gerando backgrounds para {len(sentences)} sentences")
        logger.info(f"   • Job: {job_id}")
        logger.info(f"   • Canvas: {canvas.get('width')}x{canvas.get('height')}")
        logger.info(f"   • V-Services: {self.v_services_url}/backgrounds/generate")
        
        # 📝 DEBUG: Salvar input do generate_backgrounds
        if debug_logger:
            debug_logger.log_step(job_id, "generate_backgrounds", "input", {
                "sentences_count": len(sentences),
                "text_styles_keys": list(text_styles.keys()) if text_styles else [],
                "canvas": canvas,
                "first_sentence_style": sentences[0].get('style_type') if sentences else None,
                "first_sentence_layout": sentences[0].get('layout') if sentences else None
            })
        
        try:
            # Chamar v-services
            response = requests.post(
                f"{self.v_services_url}/backgrounds/generate",
                json={
                    "sentences": sentences,
                    "text_styles": text_styles,
                    "canvas": canvas,
                    "job_id": job_id
                },
                timeout=120  # 2 minutos de timeout
            )
            
            if response.status_code != 200:
                error_msg = f"V-Services retornou status {response.status_code}: {response.text}"
                logger.error(f"❌ [BACKGROUNDS] {error_msg}")
                return {
                    "status": "error",
                    "error": error_msg,
                    "backgrounds": [],
                    "total": 0
                }
            
            result = response.json()
            
            total = result.get('total', 0)
            stats = result.get('stats', {})
            
            logger.info(f"✅ [BACKGROUNDS] Gerados {total} backgrounds via v-services")
            logger.info(f"   • Word BGs: {stats.get('word_bgs', 0)}")
            logger.info(f"   • Phrase BGs: {stats.get('phrase_bgs', 0)}")
            logger.info(f"   • Fullscreen BGs: {stats.get('fullscreen_bgs', 0)}")
            
            # 📝 DEBUG: Salvar output do generate_backgrounds
            if debug_logger:
                debug_logger.log_step(job_id, "generate_backgrounds", "output", {
                    "status": result.get('status'),
                    "total": total,
                    "stats": stats,
                    "first_bg": result.get('backgrounds', [None])[0] if result.get('backgrounds') else None
                })
            
            return result
            
        except requests.exceptions.Timeout:
            error_msg = "Timeout ao chamar v-services (>120s)"
            logger.error(f"❌ [BACKGROUNDS] {error_msg}")
            return {
                "status": "error",
                "error": error_msg,
                "backgrounds": [],
                "total": 0
            }
            
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Erro de conexão com v-services: {str(e)}"
            logger.error(f"❌ [BACKGROUNDS] {error_msg}")
            return {
                "status": "error",
                "error": error_msg,
                "backgrounds": [],
                "total": 0
            }
            
        except Exception as e:
            error_msg = f"Erro inesperado: {str(e)}"
            logger.error(f"❌ [BACKGROUNDS] {error_msg}")
            import traceback
            traceback.print_exc()
            return {
                "status": "error",
                "error": error_msg,
                "backgrounds": [],
                "total": 0
            }
