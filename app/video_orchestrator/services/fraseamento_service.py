"""
🔤 Fraseamento Service - Agrupamento Inteligente de Palavras

Wrapper para chamar o serviço de fraseamento no v-services.
Recebe words[] da transcrição + rules{} do template.
Retorna phrase_groups[] com frases agrupadas, tipos e níveis de ênfase.

Fluxo:
1. Transcrição (AssemblyAI) → words[]
2. Template (enhanced-phrase-rules.json) → rules{}
3. Fraseamento → phrase_groups[]

Endpoint v-services: /fraseamento/process
"""

import os
import logging
import requests
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# URLs e autenticação
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')
V_SERVICES_HOST = os.environ.get('V_SERVICES_HOST', 'services.vinicius.ai')
V_SERVICES_TOKEN = os.environ.get('V_SERVICES_TOKEN', '')


class FraseamentoService:
    """
    Serviço de agrupamento de palavras em frases.
    
    Usa SpaCy para análise POS e regras configuráveis para:
    - Determinar quebras de frase (pontuação, pausa, semântica)
    - Classificar tipos (single, double, phrase, emphasis)
    - Definir níveis de ênfase (normal, medium, high)
    - Indicar fullscreen background para destaque visual
    
    Configuração via template:
    - phrase_rules: min/max words, pause_threshold, etc
    - emphasis_controls: regras de ênfase automática
    - font_size_config: tamanhos por tipo de frase
    - letter_effect_detection: efeitos especiais
    """
    
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or V_SERVICES_URL
        self.timeout = 120  # Fraseamento pode demorar para textos longos
        
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {V_SERVICES_TOKEN}',
            'Host': V_SERVICES_HOST
        }
        
        logger.info(f"✅ FraseamentoService inicializado: {self.base_url}")
    
    def process(
        self,
        words: List[Dict[str, Any]],
        rules: Optional[Dict[str, Any]] = None,
        conservative_mode: bool = False,
        disable_fullscreen_bg: bool = False,
        template_id: Optional[str] = None,
        template_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Processa lista de palavras e retorna grupos de frases.
        
        Args:
            words: Lista de palavras com timestamps
                   [{"text": "Olá", "start": 0.0, "end": 0.5, "confidence": 0.98}, ...]
            
            rules: Regras de fraseamento do template (enhanced-phrase-rules.json)
                   Se None, usa defaults do serviço
            
            conservative_mode: Se True, usa tamanho uniforme de frases
            
            disable_fullscreen_bg: Se True, desabilita backgrounds fullscreen
            
            template_id: UUID do template para debug/tracking
            
            template_name: Nome do template para debug/tracking
            
        Returns:
            {
                "status": "success",
                "phrase_groups": [
                    {
                        "phrase_index": 0,
                        "text": "Descubra como",
                        "words": [...],
                        "word_count": 2,
                        "start_time": 0.0,
                        "end_time": 1.2,
                        "duration": 1200,
                        "phrase_type": "double",
                        "emphasis_level": "normal",
                        "needs_fullscreen_bg": false,
                        "recommended_font_size": 32,
                        "has_punctuation_break": false
                    },
                    ...
                ],
                "total_phrases": 15,
                "total_words": 45,
                "processing_time_ms": 234.5
            }
        """
        if not words:
            return {
                "error": "Lista de palavras vazia",
                "status": "failed",
                "phrase_groups": []
            }
        
        logger.info(f"🔤 Iniciando fraseamento: {len(words)} palavras")
        
        # Usar template_id e template_name dos parâmetros (não extrair de rules)
        # Se não fornecidos, tentar extrair do rules como fallback
        if template_id is None and rules:
            template_id = rules.get('_template_id')
        if template_name is None and rules:
            template_name = rules.get('_template_name')
        
        # ⚠️ IMPORTANTE: v-services espera rules no formato ORIGINAL do template (com .value)
        # NÃO converter com _prepare_rules() - enviar formato original!
        # O v-services faz a extração de .value internamente
        
        # LOG CRÍTICO: Mostrar as regras que estão sendo enviadas
        phrase_rules = rules.get('phrase_rules', {}) if rules else {}
        logger.info(f"📋 [FRASEAMENTO] Regras sendo enviadas ao v-services:")
        logger.info(f"   • template_id: {template_id}")
        logger.info(f"   • template_name: {template_name}")
        logger.info(f"   • default_min_words: {phrase_rules.get('default_min_words', {}).get('value')}")
        logger.info(f"   • default_max_words: {phrase_rules.get('default_max_words', {}).get('value')}")
        logger.info(f"   • pause_threshold_ms: {phrase_rules.get('pause_threshold_ms', {}).get('value')}")
        logger.info(f"   • duration_threshold_ms: {phrase_rules.get('duration_threshold_ms', {}).get('value')}")
        logger.info(f"   • punctuation_rules.enabled: {phrase_rules.get('punctuation_rules', {}).get('enabled', {}).get('value')}")
        
        # 🆕 Log instruções customizadas
        custom_instructions = phrase_rules.get('custom_phrase_instructions', {}).get('value', '')
        if custom_instructions:
            logger.info(f"   📝 custom_phrase_instructions: {custom_instructions[:100]}...")
        else:
            logger.info(f"   📝 custom_phrase_instructions: (vazio)")
        
        try:
            # ✅ Enviar rules no formato ORIGINAL (com .value) para o v-services
            payload = {
                "words": words,
                "rules": rules,  # ✅ ORIGINAL, não convertido!
                "template_id": template_id,  # Para debug e rastreabilidade
                "template_name": template_name  # Para debug e rastreabilidade
            }
            
            response = requests.post(
                f"{self.base_url}/fraseamento/process",
                json=payload,
                headers=self.headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            
            phrase_count = len(result.get('phrase_groups', []))
            logger.info(f"✅ Fraseamento concluído: {phrase_count} frases")
            
            return {
                "status": "success",
                "phrase_groups": result.get('phrase_groups', []),
                "total_phrases": result.get('total_phrases', phrase_count),
                "total_words": result.get('total_words', len(words)),
                "processing_time_ms": result.get('processing_time_ms', 0),
                "conservative_mode_used": result.get('conservative_mode_used', conservative_mode),
                "punctuation_breaks_count": result.get('punctuation_breaks_count', 0),
                "font_config_applied": result.get('font_config_applied', {}),
                # ✅ RASTREABILIDADE: Incluir template info no OUTPUT
                "template_id": result.get('template_id', template_id),
                "template_name": result.get('template_name', template_name)
            }
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout no fraseamento após {self.timeout}s")
            return {
                "error": f"Timeout após {self.timeout}s",
                "status": "timeout",
                "phrase_groups": []
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro no fraseamento: {e}")
            return {
                "error": str(e),
                "status": "failed",
                "phrase_groups": []
            }
        except Exception as e:
            logger.error(f"❌ Erro inesperado no fraseamento: {e}")
            return {
                "error": str(e),
                "status": "failed",
                "phrase_groups": []
            }
    
    def _prepare_rules(
        self,
        rules: Optional[Dict[str, Any]],
        conservative_mode: bool,
        disable_fullscreen_bg: bool
    ) -> Dict[str, Any]:
        """
        Prepara regras de fraseamento, convertendo do formato do template.
        
        O template enhanced-phrase-rules.json usa estrutura com "value":
        { "default_max_words": { "value": 5, "sidecar_id": "..." } }
        
        O serviço de fraseamento espera valores diretos:
        { "default_phrase_max_words": 5 }
        """
        # Defaults se não houver regras
        if not rules:
            rules = {}
        
        # Extrair valores do formato template (com .value) ou usar direto
        def get_value(obj, key, default=None):
            """Extrai valor de objeto que pode ser {value: X} ou X diretamente"""
            if obj is None:
                return default
            if isinstance(obj, dict) and 'value' in obj:
                return obj['value']
            return obj
        
        # Converter phrase_rules do template para formato do serviço
        phrase_rules = rules.get('phrase_rules', {})
        
        final_rules = {
            # Regras básicas de frase
            'default_phrase_min_words': get_value(phrase_rules.get('default_min_words'), None, 2),
            'default_phrase_max_words': get_value(phrase_rules.get('default_max_words'), None, 5),
            'pause_threshold_ms': get_value(phrase_rules.get('pause_threshold_ms'), None, 400),
            'duration_threshold_ms': get_value(phrase_rules.get('duration_threshold_ms'), None, 900),
            
            # Regras de pontuação
            'punctuation_rules': self._convert_punctuation_rules(phrase_rules.get('punctuation_rules', {})),
            
            # Configuração de tamanho de fonte
            'font_size_config': self._convert_font_size_config(rules.get('font_size_config', {})),
            
            # Controles de ênfase
            'emphasis_controls': self._convert_emphasis_controls(rules.get('emphasis_controls', {})),
            
            # Controles de fullscreen background
            'fullscreen_bg_controls': self._convert_fullscreen_controls(rules),
            
            # Flags especiais
            'global_disable_fullscreen_bg': disable_fullscreen_bg,
        }
        
        # Modo conservador (usa tamanho uniforme fixo de 4 palavras)
        if conservative_mode:
            final_rules['conservative_mode'] = {
                'enabled': True,
                'uniform_phrase_length': 4,  # Valor fixo - modo conservador usa tamanho uniforme
                'disable_emphasis_detection': True
            }
        
        # Modo emphasis_without_bg (do template)
        emphasis_without_bg = rules.get('emphasis_without_bg_full_screen', {})
        if get_value(emphasis_without_bg.get('enabled'), None, False):
            final_rules['emphasis_without_bg_full_screen'] = {
                'enabled': True,
                'force_disable_fullscreen_bg': get_value(
                    emphasis_without_bg.get('force_disable_fullscreen_bg'), None, True
                )
            }
        
        return final_rules
    
    def _convert_punctuation_rules(self, punct_rules: Dict) -> Dict:
        """Converte regras de pontuação do formato template"""
        def get_val(obj, default=None):
            if isinstance(obj, dict) and 'value' in obj:
                return obj['value']
            return obj if obj is not None else default
        
        return {
            'enabled': get_val(punct_rules.get('enabled'), True),
            'max_words_before_punctuation_break': get_val(
                punct_rules.get('max_words_before_break'), 6
            ),
            'strong_break_chars': ['.', '!', '?'],
            'weak_break_chars': [',', ';', ':'],
            'prefer_punctuation_breaks': True
        }
    
    def _convert_font_size_config(self, font_config: Dict) -> Dict:
        """Converte configuração de tamanho de fonte do formato template"""
        def get_val(obj, default=None):
            if isinstance(obj, dict) and 'value' in obj:
                return obj['value']
            return obj if obj is not None else default
        
        return {
            'single_word_size': get_val(font_config.get('single_word_size_percent'), 5),
            'double_word_size': get_val(font_config.get('double_word_size_percent'), 3.2),
            'phrase_size': get_val(font_config.get('phrase_size_percent'), 2.5),
            'emphasis_size': get_val(font_config.get('highlight_size_percent'), 20)
        }
    
    def _convert_emphasis_controls(self, emphasis: Dict) -> Dict:
        """Converte controles de ênfase do formato template"""
        def get_val(obj, default=None):
            if isinstance(obj, dict) and 'value' in obj:
                return obj['value']
            return obj if obj is not None else default
        
        enabled = get_val(emphasis.get('enabled'), False)
        if not enabled:
            return {'enabled': False}
        
        auto_rules = emphasis.get('automatic_rules', {})
        text_based = auto_rules.get('text_based', {})
        timing_based = auto_rules.get('timing_based', {})
        
        return {
            'enabled': True,
            'automatic_rules': {
                'enabled': get_val(auto_rules.get('enabled'), True),
                'text_based': {
                    'detect_questions': get_val(text_based.get('detect_questions'), True),
                    'detect_exclamations': get_val(text_based.get('detect_exclamations'), True),
                    'detect_interjections': get_val(text_based.get('detect_interjections'), True),
                    'detect_caps': get_val(text_based.get('detect_caps'), True)
                },
                'timing_based': {
                    'long_pause_after_ms': get_val(timing_based.get('long_pause_after_ms'), 600),
                    'long_word_duration_ms': get_val(timing_based.get('long_word_duration_ms'), 800)
                }
            }
        }
    
    def _convert_fullscreen_controls(self, rules: Dict) -> Dict:
        """Converte controles de fullscreen background"""
        def get_val(obj, default=None):
            if isinstance(obj, dict) and 'value' in obj:
                return obj['value']
            return obj if obj is not None else default
        
        emphasis = rules.get('emphasis_controls', {})
        behavior = emphasis.get('behavior', {})
        fullscreen = behavior.get('fullscreen_bg', {})
        
        return {
            'enabled': get_val(fullscreen.get('enabled'), True),
            'max_fullscreen_percentage': get_val(fullscreen.get('max_percentage'), 0.35),
            'max_consecutive': get_val(fullscreen.get('max_consecutive'), 2),
            'min_phrases_between_fullscreen': get_val(fullscreen.get('min_distance_between'), 2),
            'anti_ping_pong_enabled': True,
            'priority_questions': True,
            'priority_interjections': True,
            'priority_emphasis': True
        }
    
    def health_check(self) -> bool:
        """Verifica se o serviço de fraseamento está disponível"""
        try:
            response = requests.get(
                f"{self.base_url}/fraseamento/health",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('status') == 'healthy'
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Health check fraseamento falhou: {e}")
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Retorna informações sobre o modelo SpaCy usado"""
        try:
            response = requests.get(
                f"{self.base_url}/fraseamento/health",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "status": "available",
                    "spacy_model": data.get('spacy_model'),
                    "version": data.get('version'),
                    "framework": data.get('framework')
                }
            
            return {"status": "unavailable"}
            
        except Exception as e:
            return {"status": "error", "error": str(e)}


