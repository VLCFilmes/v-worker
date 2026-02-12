"""
📍 Positioning Service - Step 11

Serviço INDEPENDENTE para calcular posições de palavras e bounding boxes.
Este serviço foi extraído do subtitle_pipeline_service para permitir que
backgrounds sejam gerados com as dimensões corretas.

Fluxo:
1. Recebe png_results (com dimensões dos PNGs)
2. Chama V-Services /positioning/calculate
3. Retorna sentences com layout (bounding boxes) preenchido

O layout é usado pelo BackgroundGeneratorService para:
- Dimensionar phrase backgrounds (group_width, group_height)
- Posicionar backgrounds (center_x, center_y)
"""

import os
import logging
import requests
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# URL do V-Services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')


class PositioningService:
    """
    Serviço de posicionamento de palavras e frases.
    
    Calcula:
    - Posição X,Y de cada palavra no canvas
    - Bounding box de cada frase (group_width, group_height)
    - Centro de cada frase (center_x, center_y)
    
    Esses valores são essenciais para gerar backgrounds de frase.
    """
    
    def __init__(self, v_services_url: str = None):
        self.base_url = v_services_url or V_SERVICES_URL
        self.positioning_endpoint = f"{self.base_url}/positioning/calculate"
        logger.info(f"📍 PositioningService inicializado: {self.base_url}")
    
    def calculate_positions(
        self,
        png_results: Dict[str, Any],
        template_config: Dict[str, Any],
        canvas: Optional[Dict[str, int]] = None
    ) -> Dict[str, Any]:
        """
        Calcula posições de todas as palavras e bounding boxes das frases.
        
        Args:
            png_results: Resultado do PngGeneratorService (frases com PNGs e dimensões)
            template_config: Configuração do template (positioning, text-alignment, etc)
            canvas: Opcional - dimensões do canvas (width, height, padding)
            
        Returns:
            {
                "status": "success",
                "sentences": [
                    {
                        "group_index": 0,
                        "style_type": "default",
                        "words": [...],
                        "layout": {
                            "group_width": 340,
                            "group_height": 120,
                            "center_x": 360,
                            "center_y": 640,
                            "number_of_lines": 2
                        }
                    }
                ]
            }
        """
        logger.info(f"📍 [POSITIONING] Calculando posições...")
        
        # Extrair configurações do template (usar 'or {}' para tratar None explícito)
        text_styles = template_config.get("_text_styles") or {}
        project_settings = template_config.get("project-settings") or {}
        video_settings = project_settings.get("video_settings") or {}
        
        # Usar canvas fornecido ou extrair do template
        if canvas is None:
            canvas = {
                "width": self._get_value(video_settings, "width", 720),
                "height": self._get_value(video_settings, "height", 1280),
                "padding": self._get_value(video_settings, "padding", 20)
            }
        
        # ═══════════════════════════════════════════════════════════════════
        # Extrair positioning PER-STYLE das colunas ts_*_positioning
        # ═══════════════════════════════════════════════════════════════════
        per_style_positioning = {}
        
        for style_name in ['default', 'emphasis', 'letter_effect', 'cartela']:
            style_config = text_styles.get(style_name) or {}
            positioning = style_config.get('positioning') or {}
            alignment = style_config.get('alignment') or {}
            
            per_style_positioning[style_name] = {
                # Posição no canvas (0-1)
                "position_x": self._get_value(positioning, "position_x", 0.5),
                "position_y": self._get_value(positioning, "position_y", self._get_default_y(style_name)),
                # Âncora do grupo (0-1)
                "anchor_x": self._get_value(positioning, "anchor_x", 0.5),
                "anchor_y": self._get_value(positioning, "anchor_y", 0.5),
                # Escala
                "scale": self._get_value(positioning, "scale", 1.0),
                # Largura máxima em % do canvas
                "max_width_percent": self._get_value(positioning, "max_width_percent", 80),
                # Alinhamento de texto
                "text_align": self._get_value(alignment, "horizontal", "center"),
                # Habilitado?
                "enabled": self._get_value(positioning, "enabled", True)
            }
            
            if positioning:
                logger.info(f"   • Positioning {style_name}: x={per_style_positioning[style_name]['position_x']:.2f}, y={per_style_positioning[style_name]['position_y']:.2f}")
        
        # ═══════════════════════════════════════════════════════════════════
        # Extrair layout_spacing do template (nova arquitetura)
        # ═══════════════════════════════════════════════════════════════════
        layout_spacing = template_config.get("layout_spacing") or {}
        
        # Log layout_spacing se existir
        if layout_spacing:
            word_sp = self._get_value(layout_spacing, "word_spacing_percent", 2)
            line_sp = self._get_value(layout_spacing, "line_spacing_percent", 3)
            max_w = self._get_value(layout_spacing, "max_line_width_percent", 80)
            logger.info(f"📐 [LAYOUT] word_spacing={word_sp}%, line_spacing={line_sp}%, max_width={max_w}%")
        
        # ═══════════════════════════════════════════════════════════════════
        # Montar payload para o V-Services positioning
        # ═══════════════════════════════════════════════════════════════════
        payload = {
            "png_results": png_results,
            "canvas": canvas,
            # NOVA ARQUITETURA: layout_spacing com % do canvas
            "layout_spacing": layout_spacing if layout_spacing else {
                "word_spacing_percent": {"value": 2},
                "word_spacing_multiplier": {"value": 1.0},
                "line_spacing_percent": {"value": 3},
                "max_line_width_percent": {"value": 80}
            },
            # Legado: layout_settings (para retrocompatibilidade)
            "layout_settings": {
                "spacing": {
                    "height_percent": 35,
                    "multiplier": 1.0
                },
                "max_line_width_percent": self._get_value(layout_spacing, "max_line_width_percent", 80),
                "line_spacing_px": 20
            },
            # Posicionamento per-style (nova arquitetura)
            "per_style_positioning": per_style_positioning,
            # Legado: posicionamento global (para retrocompatibilidade)
            "positioning": {
                "group_position_x": per_style_positioning.get('default', {}).get('position_x', 0.5),
                "group_position_y": per_style_positioning.get('default', {}).get('position_y', 0.75),
                "group_anchor_x": per_style_positioning.get('default', {}).get('anchor_x', 0.5),
                "group_anchor_y": per_style_positioning.get('default', {}).get('anchor_y', 0.5),
                "group_scale": per_style_positioning.get('default', {}).get('scale', 1.0)
            },
            "text_alignment": {
                "default": per_style_positioning.get('default', {}).get('text_align', 'center'),
                "emphasis": per_style_positioning.get('emphasis', {}).get('text_align', 'center'),
                "letter_effect": per_style_positioning.get('letter_effect', {}).get('text_align', 'center')
            }
        }
        
        try:
            phrases_count = len(png_results.get('phrases', [])) if isinstance(png_results, dict) else 0
            logger.info(f"   • Enviando {phrases_count} frases para V-Services")
            logger.info(f"   • Canvas: {canvas.get('width')}x{canvas.get('height')}")
            
            response = requests.post(
                self.positioning_endpoint,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                error_msg = f"V-Services positioning retornou {response.status_code}: {response.text[:200]}"
                logger.error(f"❌ {error_msg}")
                return {"status": "error", "error": error_msg}
            
            result = response.json()
            
            # Validar resultado
            sentences = result.get("sentences", [])
            if not sentences:
                logger.warning("⚠️ V-Services retornou 0 sentences posicionadas")
                return {
                    "status": "success",
                    "sentences": [],
                    "total": 0
                }
            
            # Log de sucesso
            total = len(sentences)
            with_layout = sum(1 for s in sentences if s.get('layout', {}).get('group_width', 0) > 0)
            
            logger.info(f"✅ [POSITIONING] Posicionamento concluído:")
            logger.info(f"   • Sentences: {total}")
            logger.info(f"   • Com layout (bounding box): {with_layout}")
            
            # Debug: mostrar primeiro layout
            if sentences:
                first_layout = sentences[0].get('layout', {})
                logger.info(f"   • Exemplo layout #0: {first_layout.get('group_width', 0)}x{first_layout.get('group_height', 0)} @ ({first_layout.get('center_x', 0)}, {first_layout.get('center_y', 0)})")
            
            return {
                "status": "success",
                "sentences": sentences,
                "total": total,
                "canvas": canvas
            }
            
        except requests.Timeout:
            logger.error("❌ Timeout no V-Services positioning (60s)")
            return {"status": "error", "error": "Timeout no positioning service"}
        except requests.RequestException as e:
            logger.error(f"❌ Erro de conexão com V-Services: {e}")
            return {"status": "error", "error": f"Erro de conexão: {str(e)}"}
    
    def _get_value(self, obj: Any, key: str, default: Any = None) -> Any:
        """
        Extrai valor de objeto, suportando formato {value: x}.
        """
        if not obj or not isinstance(obj, dict):
            return default
        
        val = obj.get(key)
        
        if val is None:
            return default
        
        # Se for objeto com .value, extrair
        if isinstance(val, dict) and 'value' in val:
            return val['value']
        
        return val
    
    def _get_default_y(self, style_name: str) -> float:
        """
        Retorna posição Y padrão baseada no tipo de estilo.
        
        - default: 0.75 (parte inferior - legendas tradicionais)
        - emphasis: 0.5 (centro)
        - letter_effect: 0.3 (parte superior)
        - cartela: 0.5 (centro)
        """
        defaults = {
            'default': 0.75,      # Legendas no rodapé
            'emphasis': 0.5,      # Ênfase no centro
            'letter_effect': 0.3, # Efeitos no topo
            'cartela': 0.5        # Cartela no centro
        }
        return defaults.get(style_name, 0.5)


# Singleton instance
_positioning_service = None


def get_positioning_service() -> PositioningService:
    """Retorna instância singleton do PositioningService."""
    global _positioning_service
    if _positioning_service is None:
        _positioning_service = PositioningService()
    return _positioning_service

