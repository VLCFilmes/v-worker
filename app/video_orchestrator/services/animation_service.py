"""
🎬 Animation Service - Step 11

Calcula e adiciona metadados de animação às palavras/frases.
Este serviço NÃO modifica imagens PNG, apenas enriquece os dados
com informações de timing e animação para o v-editor.

Schemas Utilizados:
- stagger_and_opacity: Timing/delays de entrada e controle de opacidade
- multi_animations: Animações visuais de texto (in/out/middle)
- asset_animations: Animações de outros assets

Presets Disponíveis:
- typewriter, cascade, pop_bounce, wave, instant, vhs_step
- fade, slide, scale_bounce, elastic_pulse, vhs_snap_fade
- karaoke, spotlight, fade_phrase, vhs_flicker
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# 🆕 Importar debug_logger para salvar payloads no banco
try:
    from app.video_orchestrator.debug_logger import debug_logger
except ImportError:
    debug_logger = None
    logger.warning("⚠️ debug_logger não disponível - logs de auditoria desabilitados")

# Carregar presets de animação
PRESETS_PATH = os.path.join(
    os.path.dirname(__file__), 
    '..', '..', 'data', 'template-master-v3', 'presets', 'animation-presets.json'
)

def load_animation_presets() -> Dict[str, Any]:
    """Carrega presets de animação do arquivo JSON."""
    try:
        with open(PRESETS_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"⚠️ [AnimationService] Não conseguiu carregar presets: {e}")
        return {}

ANIMATION_PRESETS = load_animation_presets()


class AnimationService:
    """
    Serviço de animações para legendas.
    
    Adiciona metadados de:
    - Stagger (delays de entrada por palavra)
    - Opacity (opacidade por estado: future/active/past)
    - Animações visuais (in/out/middle)
    """
    
    def __init__(self):
        self.presets = ANIMATION_PRESETS
        logger.info(f"🎬 AnimationService inicializado com {len(self.presets.get('stagger_presets', {}))} presets de stagger")
    
    def apply_animations(
        self,
        phrase_groups: List[Dict[str, Any]],
        animation_config: Dict[str, Any],
        job_id: str = None
    ) -> List[Dict[str, Any]]:
        """
        Aplica configurações de animação às frases.
        
        Args:
            phrase_groups: Lista de frases com words e timestamps
            animation_config: Config do template com:
                - stagger_and_opacity
                - multi_animations
                - asset_animations
                - animation_preset
            job_id: ID do job para logs
            
        Returns:
            phrase_groups enriquecidos com animation_metadata
        """
        if not phrase_groups:
            logger.warning("⚠️ [AnimationService] phrase_groups vazio")
            return []
        
        logger.info(f"🎬 [AnimationService] Aplicando animações a {len(phrase_groups)} frases")
        
        # 📝 DEBUG: Salvar input do apply_animations
        if debug_logger and job_id:
            debug_logger.log_step(job_id, "apply_animations", "input", {
                "phrase_count": len(phrase_groups),
                "animation_config_keys": list(animation_config.keys()) if animation_config else [],
                "stagger_and_opacity": animation_config.get('stagger_and_opacity'),
                "multi_animations_keys": list(animation_config.get('multi_animations', {}).keys()) if animation_config.get('multi_animations') else [],
                "per_style_animations_keys": list(animation_config.get('per_style_animations', {}).keys()) if animation_config.get('per_style_animations') else [],
                "animation_preset": animation_config.get('animation_preset')
            })
        
        # Extrair configs
        stagger_config = animation_config.get('stagger_and_opacity', {}) or {}
        multi_animations = animation_config.get('multi_animations', {})
        preset_name = animation_config.get('animation_preset')
        per_style_animations = animation_config.get('per_style_animations', {})
        
        # 🆕 Se tem per_style_animations, incluir no stagger_config para acesso no _process_phrase
        if per_style_animations:
            stagger_config = dict(stagger_config)  # Não modificar original
            stagger_config['per_style_animations'] = per_style_animations
            logger.info(f"   • Per-style animations: {list(per_style_animations.keys())}")
        
        # Se tem preset, aplicar config do preset
        if preset_name:
            stagger_config = self._apply_preset(preset_name, stagger_config)
            logger.info(f"   • Usando preset: {preset_name}")
        
        # Verificar se stagger está habilitado
        stagger_enabled = self._get_value(stagger_config, 'enabled', False)
        logger.info(f"   • Stagger enabled: {stagger_enabled}")
        
        # Processar cada frase
        enriched_phrases = []
        
        for phrase in phrase_groups:
            enriched = self._process_phrase(
                phrase=phrase,
                stagger_config=stagger_config,
                multi_animations=multi_animations,
                stagger_enabled=stagger_enabled
            )
            enriched_phrases.append(enriched)
        
        # Estatísticas
        total_words = sum(len(p.get('words', [])) for p in enriched_phrases)
        animated_words = sum(
            len([w for w in p.get('words', []) if w.get('animation_metadata')])
            for p in enriched_phrases
        )
        
        logger.info(f"✅ [AnimationService] Animações aplicadas:")
        logger.info(f"   • Frases: {len(enriched_phrases)}")
        logger.info(f"   • Palavras com animação: {animated_words}/{total_words}")
        
        # 📝 DEBUG: Salvar output do apply_animations
        if debug_logger and job_id:
            # Extrair info de animação da primeira frase para debug
            first_phrase_anim = enriched_phrases[0].get('animation_metadata') if enriched_phrases else None
            first_word_anim = None
            if enriched_phrases and enriched_phrases[0].get('words'):
                first_word_anim = enriched_phrases[0]['words'][0].get('animation_metadata')
            
            debug_logger.log_step(job_id, "apply_animations", "output", {
                "phrase_count": len(enriched_phrases),
                "total_words": total_words,
                "animated_words": animated_words,
                "stagger_enabled": stagger_enabled,
                "first_phrase_animation": first_phrase_anim,
                "first_word_animation": first_word_anim
            })
        
        return enriched_phrases
    
    def _process_phrase(
        self,
        phrase: Dict[str, Any],
        stagger_config: Dict[str, Any],
        multi_animations: Dict[str, Any],
        stagger_enabled: bool
    ) -> Dict[str, Any]:
        """
        Processa uma frase, adicionando animation_metadata às words.
        """
        words = phrase.get('words', [])
        if not words:
            return phrase
        
        # Calcular duração da frase
        # Suportar dois formatos:
        # 1. AssemblyAI: 'start'/'end' em segundos
        # 2. script_data: 'start_ms'/'end_ms' em milissegundos
        if 'start_ms' in words[0]:
            phrase_start_ms = words[0].get('start_ms', 0)
            phrase_end_ms = words[-1].get('end_ms', 0)
            phrase_duration_ms = phrase_end_ms - phrase_start_ms
        else:
            phrase_start = words[0].get('start', 0)
            phrase_end = words[-1].get('end', 0)
            phrase_duration_ms = (phrase_end - phrase_start) * 1000
        
        # Extrair config de stagger
        delay_ms = self._get_value(stagger_config.get('stagger_config', {}), 'delay_ms', 0)
        direction = self._get_value(stagger_config.get('stagger_config', {}), 'direction', 'left_to_right')
        
        # Safety: ajustar delay se necessário
        safety = stagger_config.get('stagger_config', {}).get('safety', {})
        if safety.get('auto_adjust', True) and delay_ms > 0:
            max_percent = safety.get('max_delay_percent', 50)
            max_total_delay = phrase_duration_ms * (max_percent / 100)
            total_stagger = delay_ms * (len(words) - 1)
            
            if total_stagger > max_total_delay and len(words) > 1:
                adjusted_delay = max_total_delay / (len(words) - 1)
                logger.debug(f"   • Safety: delay ajustado de {delay_ms}ms para {adjusted_delay:.1f}ms")
                delay_ms = adjusted_delay
        
        # Extrair config de opacity
        opacity_config = stagger_config.get('opacity_config', {})
        opacity_mode = self._get_value(opacity_config, 'mode', 'none')
        word_opacity = opacity_config.get('word_opacity', {})
        
        # Extrair animações visuais - APENAS formato PER-STYLE (Generator V3)
        # 🚨 SEM FALLBACK para formato global - formato global está DESCONTINUADO
        style_type = phrase.get('style_type', 'default')
        
        style_animations = {}
        per_style = stagger_config.get('per_style_animations', {})
        
        # Usar APENAS formato PER-STYLE (ts_*_animation)
        if per_style and per_style.get(style_type):
            per_style_anim = per_style[style_type]
            entry_config = per_style_anim.get('entry', {})
            if entry_config:
                style_animations = {
                    'in_animation': {
                        'type': entry_config.get('type', 'none'),
                        'duration': entry_config.get('duration_ms', 350),
                        'easing': entry_config.get('easing', 'ease-out')
                    },
                    'out_animation': {},
                    'middle_animation': {}
                }
        elif per_style and per_style.get('default'):
            # Fallback para 'default' se style_type específico não existir
            per_style_anim = per_style['default']
            entry_config = per_style_anim.get('entry', {})
            if entry_config:
                style_animations = {
                    'in_animation': {
                        'type': entry_config.get('type', 'none'),
                        'duration': entry_config.get('duration_ms', 350),
                        'easing': entry_config.get('easing', 'ease-out')
                    },
                    'out_animation': {},
                    'middle_animation': {}
                }
                logger.debug(f"   • Usando animação 'default' para style_type '{style_type}'")
        
        # Processar cada palavra
        enriched_words = []
        word_count = len(words)
        
        for i, word in enumerate(words):
            # Calcular entry_delay (stagger)
            if stagger_enabled and delay_ms > 0:
                if direction == 'right_to_left':
                    entry_delay = (word_count - 1 - i) * delay_ms
                else:  # left_to_right
                    entry_delay = i * delay_ms
            else:
                entry_delay = 0
            
            # Animation metadata
            animation_metadata = {
                "entry_delay_ms": round(entry_delay, 1),
                "word_index": i,
                "total_words": word_count
            }
            
            # Adicionar opacity config
            if opacity_mode == 'word':
                animation_metadata['opacity'] = {
                    "future": self._get_value(word_opacity.get('future_words', {}), 'opacity', 30),
                    "active": self._get_value(word_opacity.get('active_word', {}), 'opacity', 100),
                    "past": self._get_value(word_opacity.get('past_words', {}), 'opacity', 70)
                }
            
            # Adicionar animações visuais
            if style_animations:
                in_anim = style_animations.get('in_animation', {})
                out_anim = style_animations.get('out_animation', {})
                middle_anim = style_animations.get('middle_animation', {})
                
                animation_metadata['visual'] = {
                    "in": {
                        "type": self._get_value(in_anim, 'type', 'none'),
                        "duration": self._get_value(in_anim, 'duration', 0),
                        "easing": self._get_value(in_anim, 'easing', 'ease-out')
                    },
                    "out": {
                        "type": self._get_value(out_anim, 'type', 'none'),
                        "duration": self._get_value(out_anim, 'duration', 0),
                        "easing": self._get_value(out_anim, 'easing', 'ease-in')
                    }
                }
                
                if middle_anim.get('enabled'):
                    animation_metadata['visual']['middle'] = {
                        "type": self._get_value(middle_anim, 'type', 'none'),
                        "duration": self._get_value(middle_anim, 'duration', 0),
                        "intensity": self._get_value(middle_anim, 'intensity', 0.5)
                    }
            
            # Copiar word e adicionar metadata
            enriched_word = dict(word)
            enriched_word['animation_metadata'] = animation_metadata
            enriched_words.append(enriched_word)
        
        # Retornar phrase enriquecida
        enriched_phrase = dict(phrase)
        enriched_phrase['words'] = enriched_words
        
        # Adicionar metadata no nível da frase
        enriched_phrase['phrase_animation'] = {
            "stagger_enabled": stagger_enabled,
            "delay_ms": delay_ms,
            "direction": direction,
            "opacity_mode": opacity_mode
        }
        
        return enriched_phrase
    
    def _apply_preset(self, preset_name: str, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Aplica um preset ao config existente.
        
        Args:
            preset_name: Nome do preset (ex: 'typewriter', 'karaoke')
            current_config: Config atual
            
        Returns:
            Config com preset aplicado
        """
        # Buscar preset de stagger
        stagger_presets = self.presets.get('stagger_presets', {})
        if preset_name in stagger_presets:
            preset = stagger_presets[preset_name]
            logger.info(f"   • Aplicando stagger preset: {preset.get('name', preset_name)}")
            
            # Merge: preset como base, config atual sobrescreve
            merged = dict(preset.get('config', {}))
            
            # Se current_config tem stagger_config, merge
            if current_config.get('stagger_config'):
                for key, value in current_config['stagger_config'].items():
                    if value is not None:
                        merged[key] = value
            
            return {
                **current_config,
                'enabled': merged.get('enabled', True),
                'stagger_config': merged
            }
        
        # Buscar preset de opacity
        opacity_presets = self.presets.get('opacity_presets', {})
        if preset_name in opacity_presets:
            preset = opacity_presets[preset_name]
            logger.info(f"   • Aplicando opacity preset: {preset.get('name', preset_name)}")
            
            return {
                **current_config,
                'opacity_config': preset.get('config', {})
            }
        
        logger.warning(f"⚠️ [AnimationService] Preset '{preset_name}' não encontrado")
        return current_config
    
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
    
    def get_available_presets(self) -> Dict[str, List[str]]:
        """Retorna lista de presets disponíveis por categoria."""
        return {
            "stagger": list(self.presets.get('stagger_presets', {}).keys()),
            "text_animation": list(self.presets.get('text_animation_presets', {}).keys()),
            "opacity": list(self.presets.get('opacity_presets', {}).keys()),
            "camera": list(self.presets.get('camera_presets', {}).keys())
        }


# Singleton para uso global
_animation_service = None

def get_animation_service() -> AnimationService:
    """Retorna instância singleton do AnimationService."""
    global _animation_service
    if _animation_service is None:
        _animation_service = AnimationService()
    return _animation_service
