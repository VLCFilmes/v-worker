"""
🌑 Shadow Service - Adiciona sombras aos PNGs de legendas

Processa PNGs gerados pelo PngGeneratorService e adiciona sombras
usando o endpoint /png-subtitles/add_shadow_batch do V-Services.

Fluxo:
1. Recebe lista de frases com PNGs gerados
2. Busca configuração de sombra do template
3. Chama V-Services para adicionar sombras
4. Retorna PNGs atualizados com sombras
"""

import os
import json
import logging
import requests
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# URL do V-Services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')


class ShadowService:
    """
    Serviço de adição de sombras aos PNGs.
    
    Usa o endpoint /png-subtitles/add_shadow_batch do V-Services.
    """
    
    def __init__(self, v_services_url: str = None):
        self.base_url = v_services_url or V_SERVICES_URL
        self.endpoint = f"{self.base_url}/png-subtitles/add_shadow_batch"
        logger.info(f"🌑 Shadow Service inicializado: {self.endpoint}")
    
    def add_shadows_to_phrases(
        self,
        png_results: Dict[str, Any],
        template_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Adiciona sombras a todos os PNGs das frases.
        
        Args:
            png_results: Resultado do PngGeneratorService
                {
                    "phrases": [
                        {"phrase_index": 0, "words": [{"url": "...", ...}], ...},
                        ...
                    ]
                }
            template_config: Configuração do template (com shadow config)
            
        Returns:
            {
                "status": "success",
                "phrases": [...],  # Frases com URLs atualizadas (com sombra)
                "total_processed": 45,
                "shadow_config_used": {...}
            }
        """
        phrases = png_results.get('phrases', [])
        
        if not phrases:
            return {
                "status": "success",
                "phrases": [],
                "total_processed": 0,
                "message": "Nenhuma frase para processar"
            }
        
        # 🆕 NOVA ARQUITETURA: Shadow PER-STYLE
        # O shadow está em _text_styles[style_type].shadow
        # 🐛 FIX: Usar 'or {}' para tratar valores None explícitos
        text_styles = template_config.get('_text_styles') or {}
        
        # Extrair dimensões do canvas para conversão de %
        project_settings = template_config.get('project-settings') or {}
        video_settings = project_settings.get('video_settings') or {}
        canvas_height = video_settings.get('height', 1280)
        canvas_width = video_settings.get('width', 720)
        if isinstance(canvas_height, dict):
            canvas_height = canvas_height.get('value', 1280)
        if isinstance(canvas_width, dict):
            canvas_width = canvas_width.get('value', 720)
        
        logger.info(f"🌑 Adicionando sombras a {len(phrases)} frases (per-style)...")
        logger.info(f"   • Canvas: {canvas_width}x{canvas_height}")
        logger.info(f"   • Estilos disponíveis: {list(text_styles.keys())}")
        
        # Processar cada frase com seu shadow específico
        updated_phrases = []
        total_processed = 0
        errors = []
        styles_processed = {}
        shadow_config = {}  # Inicializar para evitar erro se nenhum shadow estiver habilitado
        
        for phrase in phrases:
            try:
                style_type = phrase.get('style_type', 'default')
                
                # Buscar shadow config do estilo específico
                # 🐛 FIX: Usar 'or {}' para tratar valores None explícitos
                style_config = text_styles.get(style_type) or {}
                shadow_raw = style_config.get('shadow') or {}
                
                # Verificar se shadow está habilitado para este estilo
                shadow_enabled = self._get_value(shadow_raw, 'enabled', False)
                
                if not shadow_enabled:
                    # Shadow desabilitado para este estilo - manter frase original
                    updated_phrases.append(phrase)
                    if style_type not in styles_processed:
                        styles_processed[style_type] = 'skipped'
                        logger.info(f"   ⏭️ {style_type}: shadow DESABILITADO")
                    continue
                
                # Normalizar shadow config para este estilo
                shadow_config = self._normalize_shadow_config(shadow_raw, canvas_height, canvas_width)
                
                if style_type not in styles_processed:
                    styles_processed[style_type] = 'enabled'
                    logger.info(f"   ✅ {style_type}: shadow HABILITADO (blur={shadow_config.get('blur')})")
                
                result = self._add_shadow_to_phrase(phrase, shadow_config)
                
                if "error" in result:
                    errors.append({
                        "phrase_index": phrase.get('phrase_index', 0),
                        "error": result["error"]
                    })
                    updated_phrases.append(phrase)  # Manter original
                else:
                    updated_phrases.append(result)
                    total_processed += len(result.get('words', []))
                    
            except Exception as e:
                logger.error(f"❌ Erro ao adicionar sombra na frase {phrase.get('phrase_index', '?')}: {e}")
                errors.append({
                    "phrase_index": phrase.get('phrase_index', 0),
                    "error": str(e)
                })
                updated_phrases.append(phrase)
        
        logger.info(f"✅ Sombras adicionadas: {total_processed} PNGs processados")
        
        return {
            "status": "success" if not errors else "partial",
            "phrases": updated_phrases,
            "total_processed": total_processed,
            "shadow_config_used": shadow_config,
            "errors": errors if errors else None
        }
    
    def _add_shadow_to_phrase(
        self,
        phrase: Dict[str, Any],
        shadow_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Adiciona sombra aos PNGs de uma frase.
        
        Args:
            phrase: Frase com words (PNGs)
            shadow_config: Configuração de sombra
            
        Returns:
            Frase atualizada com URLs dos PNGs com sombra
        """
        words = phrase.get('words', [])
        
        if not words:
            return phrase
        
        # Montar payload para V-Services
        payload = {
            "words": words,
            "shadow_config": {
                "blur": shadow_config.get('blur', 3),
                "distance_x": shadow_config.get('offset_x', 4),
                "distance_y": shadow_config.get('offset_y', 4),
                "color": shadow_config.get('color', [0, 0, 0, 255])
            }
        }
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                timeout=120
            )
            
            if response.status_code != 200:
                return {"error": f"V-Services retornou {response.status_code}: {response.text[:200]}"}
            
            result = response.json()
            
            if result.get('status') != 'success':
                return {"error": result.get('error', 'Erro desconhecido')}
            
            # Atualizar phrase com os words que agora têm sombra
            updated_phrase = {
                **phrase,
                "words": result.get('words', words),
                "shadow_applied": True
            }
            
            return updated_phrase
            
        except requests.Timeout:
            return {"error": "Timeout ao chamar V-Services"}
        except requests.RequestException as e:
            return {"error": f"Erro de conexão: {str(e)}"}
    
    def _extract_shadow_config(self, template_config: Dict) -> Dict:
        """
        Extrai configuração de sombra do template.
        
        🆕 Ordem de prioridade (NOVA ARQUITETURA):
        1. template_config['_shadow'] (COLUNA DEDICADA - preferencial)
        2. template_config['shadow'] (params JSONB - retrocompatibilidade)
        3. template_config['multi-text-styling']['shadow'] (dentro do styling)
        4. Valores default
        
        🚨 IMPORTANTE: A coluna dedicada tem prioridade sobre params!
        """
        # Extrair dimensões do canvas para conversão de %
        project_settings = template_config.get('project-settings', {})
        video_settings = project_settings.get('video_settings', {})
        
        canvas_width = video_settings.get('width', 720)
        if isinstance(canvas_width, dict):
            canvas_width = canvas_width.get('value', 720)
        
        canvas_height = video_settings.get('height', 1280)
        if isinstance(canvas_height, dict):
            canvas_height = canvas_height.get('value', 1280)
        
        # 🆕 1. PRIORIDADE: Coluna dedicada (_shadow)
        shadow = template_config.get('_shadow')
        if shadow:
            logger.info(f"✅ [SHADOW] Usando config da COLUNA DEDICADA (_shadow)")
            return self._normalize_shadow_config(shadow, canvas_height, canvas_width)
        
        # 2. Fallback: params JSONB (retrocompatibilidade)
        shadow = template_config.get('shadow', {})
        if shadow:
            logger.info(f"⚠️ [SHADOW] Usando config do params JSONB (shadow)")
            return self._normalize_shadow_config(shadow, canvas_height, canvas_width)
        
        # 3. Fallback: dentro do multi-text-styling
        mts = template_config.get('multi-text-styling', {})
        shadow = mts.get('shadow', {})
        if shadow:
            logger.info(f"⚠️ [SHADOW] Usando config de multi-text-styling.shadow")
            return self._normalize_shadow_config(shadow, canvas_height, canvas_width)
        
        # 4. Retornar defaults
        logger.warning(f"⚠️ [SHADOW] NENHUM shadow config encontrado - usando defaults!")
        return self._get_default_shadow_config()
    
    def _get_value(self, obj: Dict, key: str, default: Any = None) -> Any:
        """
        Extrai valor de objeto, suportando formato {value: x} ou valor direto.
        """
        if not obj or not isinstance(obj, dict):
            return default
        val = obj.get(key)
        if val is None:
            return default
        if isinstance(val, dict) and 'value' in val:
            return val['value']
        return val
    
    def _normalize_shadow_config(self, config: Dict, canvas_height: int = 1280, canvas_width: int = 720) -> Dict:
        """
        Normaliza configuração de sombra, suportando formato { value: x }.
        
        🆕 Suporta novo formato em % (blur_percent, offset_x_percent, offset_y_percent)
        que é convertido para pixels baseado nas dimensões do canvas.
        """
        def get_val(obj, key, default):
            if not obj or not isinstance(obj, dict):
                return default
            val = obj.get(key)
            if val is None:
                return default
            if isinstance(val, dict) and 'value' in val:
                return val['value']
            return val
        
        enabled = get_val(config, 'enabled', True)
        
        # Se desabilitado, retornar imediatamente
        if not enabled:
            return {'enabled': False}
        
        # 🆕 Verificar novo formato em %
        blur_percent = get_val(config, 'blur_percent', None)
        offset_x_percent = get_val(config, 'offset_x_percent', None)
        offset_y_percent = get_val(config, 'offset_y_percent', None)
        
        if blur_percent is not None or offset_x_percent is not None:
            # Novo formato em % - converter para pixels
            blur = int((blur_percent or 0.3) / 100 * canvas_height)
            offset_x = int((offset_x_percent or 0.3) / 100 * canvas_width)
            offset_y = int((offset_y_percent or 0.3) / 100 * canvas_height)
            
            logger.info(f"🌑 [SHADOW] Convertendo % para px: blur={blur_percent}%→{blur}px, offset=({offset_x_percent}%,{offset_y_percent}%)→({offset_x},{offset_y})px")
        else:
            # Formato legado em pixels
            blur = get_val(config, 'blur', 3)
            offset_x = get_val(config, 'offset_x', 4) or get_val(config, 'distance_x', 4)
            offset_y = get_val(config, 'offset_y', 4) or get_val(config, 'distance_y', 4)
        
        return {
            'enabled': True,
            'blur': blur,
            'offset_x': offset_x,
            'offset_y': offset_y,
            'color': get_val(config, 'color', [0, 0, 0, 255]),
            'opacity': get_val(config, 'opacity', 0.8)
        }
    
    def _get_default_shadow_config(self) -> Dict:
        """
        Retorna configuração de sombra padrão.
        """
        return {
            'enabled': True,
            'blur': 3,
            'offset_x': 4,
            'offset_y': 4,
            'color': [0, 0, 0, 255],
            'opacity': 0.8
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica status do serviço."""
        try:
            response = requests.get(f"{self.base_url}/png-subtitles/health", timeout=5)
            return {
                "available": response.status_code == 200,
                "endpoint": self.endpoint,
                "v_services_status": response.json() if response.ok else None
            }
        except Exception as e:
            return {
                "available": False,
                "endpoint": self.endpoint,
                "error": str(e)
            }

