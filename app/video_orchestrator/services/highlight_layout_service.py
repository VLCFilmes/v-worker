"""
📐 Highlight Layout Service - Step 11.5

Serviço para reposicionamento dinâmico de palavras quando há highlights maiores.

Problema:
- Quando highlight é maior que palavra normal, pode sobrepor palavras adjacentes

Solução:
- Recalcula posições para manter espaçamento uniforme
- Gera múltiplas layers com posições e tempos diferentes

Fluxo:
1. Recebe sentences já posicionadas (saída do PositioningService)
2. Chama V-Services /highlight-layout/calculate
3. Retorna sentences com layers expandidas

Feature Flag:
- ENABLE_HIGHLIGHT_LAYOUT = True/False (default: False)
"""

import os
import logging
import requests
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# URL do V-Services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')

# Feature flag global - Controle fino é feito por template via UI do Generator
# (layout_spacing.enable_dynamic_highlight_layout)
ENABLE_HIGHLIGHT_LAYOUT_GLOBAL = os.environ.get('ENABLE_HIGHLIGHT_LAYOUT', 'true').lower() == 'true'


class HighlightLayoutService:
    """
    Serviço de reposicionamento dinâmico para highlights maiores.
    
    Garante espaçamento uniforme entre palavras mesmo quando
    uma palavra é highlighted com tamanho maior.
    
    Controle em dois níveis:
    1. Flag global: ENABLE_HIGHLIGHT_LAYOUT (env var) - habilita/desabilita globalmente
    2. Por template: layout_spacing.enable_dynamic_highlight_layout - controle fino
    """
    
    def __init__(
        self, 
        v_services_url: str = None,
        debug_logger = None
    ):
        self.base_url = v_services_url or V_SERVICES_URL
        self.endpoint = f"{self.base_url}/highlight-layout/calculate"
        self.debug_logger = debug_logger
        self.global_enabled = ENABLE_HIGHLIGHT_LAYOUT_GLOBAL
        
        status = "✅ GLOBAL ATIVO" if self.global_enabled else "❌ GLOBAL DESATIVADO"
        logger.info(f"📐 HighlightLayoutService inicializado: {status}")
        logger.info(f"   • Endpoint: {self.endpoint}")
        logger.info(f"   • Controle por template: layout_spacing.enable_dynamic_highlight_layout")
    
    def process_sentences(
        self,
        sentences: list,
        canvas: Optional[Dict[str, int]] = None,
        uniform_spacing: float = None,
        min_diff_threshold: float = 5.0,
        job_id: str = None,
        template_enabled: bool = None
    ) -> Dict[str, Any]:
        """
        Processa sentences e expande layers quando há highlights maiores.
        
        Args:
            sentences: Lista de sentences já posicionadas pelo PositioningService
            canvas: Dimensões do canvas {width, height}
            uniform_spacing: Espaçamento fixo em pixels (opcional)
            min_diff_threshold: Diferença mínima para considerar "maior" (default 5px)
            job_id: ID do job para debug logging
            template_enabled: Valor de layout_spacing.enable_dynamic_highlight_layout do template
            
        Returns:
            {
                "sentences": [...],  # Sentences com layers expandidas
                "stats": {
                    "total_sentences": N,
                    "sentences_with_larger_highlights": M,
                    "original_layers": X,
                    "expanded_layers": Y
                },
                "enabled": True/False
            }
        """
        # Verificar se está habilitado (global E template)
        # Se template_enabled for None, usar False (opt-in)
        is_enabled = self.global_enabled and (template_enabled == True)
        
        if not is_enabled:
            reason = "global desabilitado" if not self.global_enabled else "template não habilitou"
            logger.info(f"📐 [HIGHLIGHT_LAYOUT] Feature desabilitada ({reason}) - retornando sentences originais")
            return {
                "sentences": sentences,
                "stats": {
                    "total_sentences": len(sentences),
                    "sentences_with_larger_highlights": 0,
                    "original_layers": sum(len(s.get('words', [])) for s in sentences),
                    "expanded_layers": sum(len(s.get('words', [])) for s in sentences),
                    "skipped": True
                },
                "enabled": False
            }
        
        # Verificar se há sentences
        if not sentences:
            logger.warning("📐 [HIGHLIGHT_LAYOUT] Nenhuma sentence para processar")
            return {
                "sentences": [],
                "stats": {"total_sentences": 0},
                "enabled": True
            }
        
        # Montar payload
        payload = {
            "sentences": sentences,
            "canvas": canvas or {"width": 720, "height": 1280},
            "min_diff_threshold": min_diff_threshold
        }
        
        if uniform_spacing is not None:
            payload["uniform_spacing"] = uniform_spacing
        
        # Log de debug - entrada
        if self.debug_logger and job_id:
            try:
                self.debug_logger.log(
                    job_id=job_id,
                    step_name="highlight_layout",
                    direction="input",
                    payload={
                        "sentences_count": len(sentences),
                        "canvas": canvas,
                        "min_diff_threshold": min_diff_threshold,
                        "uniform_spacing": uniform_spacing
                    },
                    extracted_fields={
                        "sentences_count": len(sentences),
                        "original_layers": sum(len(s.get('words', [])) for s in sentences)
                    }
                )
            except Exception as e:
                logger.warning(f"📐 [HIGHLIGHT_LAYOUT] Erro ao salvar debug log: {e}")
        
        logger.info(f"📐 [HIGHLIGHT_LAYOUT] Enviando {len(sentences)} sentences para v-services")
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                error_msg = f"V-Services highlight-layout retornou {response.status_code}: {response.text[:200]}"
                logger.error(f"❌ [HIGHLIGHT_LAYOUT] {error_msg}")
                
                # Fallback: retornar sentences originais
                return {
                    "sentences": sentences,
                    "stats": {
                        "error": error_msg,
                        "fallback": True
                    },
                    "enabled": True
                }
            
            result = response.json()
            
            # Log de debug - saída
            if self.debug_logger and job_id:
                try:
                    stats = result.get('stats', {})
                    self.debug_logger.log(
                        job_id=job_id,
                        step_name="highlight_layout",
                        direction="output",
                        payload={
                            "stats": stats
                        },
                        extracted_fields={
                            "original_words": stats.get('original_layers', 0),
                            "expanded_layers": stats.get('expanded_layers', 0),
                            "sentences_with_larger_hl": stats.get('sentences_with_larger_highlights', 0),
                            "expansion_factor": round(
                                stats.get('expanded_layers', 1) / max(1, stats.get('original_layers', 1)),
                                2
                            )
                        }
                    )
                except Exception as e:
                    logger.warning(f"📐 [HIGHLIGHT_LAYOUT] Erro ao salvar debug log: {e}")
            
            stats = result.get('stats', {})
            logger.info(f"✅ [HIGHLIGHT_LAYOUT] Processamento concluído:")
            logger.info(f"   • Sentences com HL maior: {stats.get('sentences_with_larger_highlights', 0)}")
            logger.info(f"   • Layers: {stats.get('original_layers', '?')} → {stats.get('expanded_layers', '?')}")
            
            result["enabled"] = True
            return result
            
        except requests.Timeout:
            error_msg = "Timeout ao chamar highlight-layout service"
            logger.error(f"❌ [HIGHLIGHT_LAYOUT] {error_msg}")
            return {
                "sentences": sentences,
                "stats": {"error": error_msg, "fallback": True},
                "enabled": True
            }
            
        except requests.RequestException as e:
            error_msg = f"Erro de conexão: {str(e)}"
            logger.error(f"❌ [HIGHLIGHT_LAYOUT] {error_msg}")
            return {
                "sentences": sentences,
                "stats": {"error": error_msg, "fallback": True},
                "enabled": True
            }
    
    def is_globally_enabled(self) -> bool:
        """Verifica se o serviço está globalmente habilitado."""
        return self.global_enabled
    
    def should_process(self, template_enabled: bool = None) -> bool:
        """
        Verifica se deve processar baseado em flags global e de template.
        
        Args:
            template_enabled: Valor de layout_spacing.enable_dynamic_highlight_layout
        
        Returns:
            True se global E template estiverem habilitados
        """
        return self.global_enabled and (template_enabled == True)

