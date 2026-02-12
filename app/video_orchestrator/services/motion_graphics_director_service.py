"""
🎬 Motion Graphics Director Service

Serviço para integrar com v-llm-directors (MotionGraphicsDirector0).
Planeja motion graphics baseado em contexto completo do vídeo.

🔧 v3.1.0: Agora busca catálogo de templates do v-services para
           enviar lista dinâmica à LLM (em vez de lista hardcoded).
"""

import os
import requests
import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)


class MotionGraphicsDirectorService:
    """
    Serviço para comunicação com v-llm-directors.
    
    Responsável por:
    - Planejar motion graphics via LLM Director (Level 0)
    - Montar contexto otimizado para LLM
    - Buscar catálogo de templates disponíveis do v-services
    - Retornar plano estruturado para execução
    """
    
    def __init__(self):
        self.base_url = os.getenv('V_LLM_DIRECTORS_URL', 'http://v-llm-directors:5025')
        self.plan_endpoint = f"{self.base_url}/directors/level-0/motion-graphics/plan-simple"
        self.timeout = int(os.getenv('LLM_DIRECTOR_TIMEOUT', '90'))
        # 🆕 v3.1.0: URL do v-services para buscar catálogo de templates
        self.v_services_url = os.getenv('V_SERVICES_URL', 'http://v-services:5000')
        self._template_catalog = None  # Cache
        logger.info(f"🎬 MotionGraphicsDirectorService inicializado")
        logger.info(f"   URL: {self.base_url}")
        logger.info(f"   v-services: {self.v_services_url}")
    
    def fetch_template_catalog(self) -> Optional[Dict]:
        """
        🆕 v3.1.0: Busca catálogo de templates do v-services.
        Usa cache em memória para evitar chamadas repetidas.
        
        Returns:
            Dict com catálogo ou None se falhar
        """
        if self._template_catalog is not None:
            return self._template_catalog
        
        try:
            catalog_url = f"{self.v_services_url}/motion-graphics/templates/catalog"
            logger.info(f"📚 [DIRECTOR0] Buscando catálogo de templates: {catalog_url}")
            
            response = requests.get(catalog_url, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success':
                    self._template_catalog = result.get('catalog', {})
                    logger.info(f"✅ [DIRECTOR0] Catálogo carregado: {len(self._template_catalog)} templates")
                    return self._template_catalog
            
            logger.warning(f"⚠️ [DIRECTOR0] Falha ao buscar catálogo: HTTP {response.status_code}")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ [DIRECTOR0] Erro ao buscar catálogo: {e}")
            return None
    
    def plan_motion_graphics(
        self,
        user_prompt: str,
        transcription: str,
        words_with_timestamps: List[Dict],
        text_layout: List[Dict],
        canvas: Dict[str, int],
        duration: float,
        style: Optional[str] = "modern"
    ) -> Dict:
        """
        Chama v-llm-directors para planejar motion graphics.
        
        Args:
            user_prompt: Prompt do usuário (ex: "Crie setas apontando")
            transcription: Transcrição completa do vídeo
            words_with_timestamps: Lista de palavras com timings
            text_layout: Layout de texto com posições (positioned_sentences)
            canvas: Dimensões do canvas (width, height)
            duration: Duração total do vídeo
            style: Estilo visual do vídeo
        
        Returns:
            {
                "status": "success",
                "plan": {
                    "motion_graphics": [...],
                    "reasoning": "..."
                },
                "llm_usage": {...}
            }
        """
        logger.info(f"🎬 [DIRECTOR0] Planejando motion graphics...")
        logger.info(f"   Prompt: {user_prompt[:100]}...")
        logger.info(f"   Context: {len(words_with_timestamps)} words, {len(text_layout)} groups")
        
        try:
            # 🆕 v3.1.0: Buscar catálogo de templates disponíveis
            template_catalog = self.fetch_template_catalog()
            
            # Montar contexto
            context = {
                "transcription": transcription,
                "words": words_with_timestamps,
                "text_layout": text_layout,
                "canvas": canvas,
                "duration": duration,
                "style": style
            }
            
            # 🆕 v3.1.0: Incluir templates disponíveis no contexto
            if template_catalog:
                context["available_templates"] = template_catalog
                logger.info(f"   📚 Templates disponíveis enviados à LLM: {list(template_catalog.keys())}")
            
            # Payload para v-llm-directors
            payload = {
                "user_prompt": user_prompt,
                "context": context
            }
            
            # Chamar v-llm-directors
            logger.info(f"🌐 Chamando v-llm-directors: {self.plan_endpoint}")
            response = requests.post(
                self.plan_endpoint,
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if result.get('status') == 'success':
                    plan = result.get('plan', {})
                    mgs = plan.get('motion_graphics', [])
                    
                    logger.info(f"✅ [DIRECTOR0] Plano criado: {len(mgs)} motion graphics")
                    for mg in mgs:
                        logger.info(f"   - {mg.get('id')}: {mg.get('type')} → {mg.get('target_word')}")
                    
                    # Log de custo (se disponível)
                    llm_usage = result.get('llm_usage', {})
                    if llm_usage:
                        logger.info(f"💰 [DIRECTOR0] LLM Usage: {llm_usage.get('total_tokens')} tokens")
                    
                    return {
                        "status": "success",
                        "plan": plan,
                        "llm_usage": llm_usage,
                        "total_mgs": len(mgs)
                    }
                else:
                    error_msg = result.get('error', 'Unknown error')
                    logger.error(f"❌ [DIRECTOR0] Erro no planejamento: {error_msg}")
                    return {
                        "status": "error",
                        "error": error_msg,
                        "details": result.get('details', '')
                    }
            else:
                logger.error(f"❌ [DIRECTOR0] HTTP Error {response.status_code}: {response.text[:200]}")
                return {
                    "status": "error",
                    "error": f"HTTP {response.status_code}",
                    "details": response.text[:200]
                }
        
        except requests.exceptions.Timeout:
            logger.error(f"❌ [DIRECTOR0] Timeout ao chamar v-llm-directors (>{self.timeout}s)")
            return {
                "status": "error",
                "error": "timeout",
                "details": f"Request timeout after {self.timeout}s"
            }
        except Exception as e:
            logger.error(f"❌ [DIRECTOR0] Erro ao chamar v-llm-directors: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "details": str(type(e).__name__)
            }
    
    def health_check(self) -> bool:
        """
        Verifica se v-llm-directors está disponível.
        """
        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"⚠️ v-llm-directors health check failed: {e}")
            return False


# Singleton instance
_service_instance = None


def get_motion_graphics_director_service() -> MotionGraphicsDirectorService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = MotionGraphicsDirectorService()
    return _service_instance
