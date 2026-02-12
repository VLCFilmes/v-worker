"""
🖼️ PNG Generator Service - Gera PNGs de legendas via V-Services

Converte frases classificadas (com style_type) para payload do png_service.py
e chama o endpoint /png-subtitles/generate_subtitles.

Fluxo:
1. Receber phrase_groups (com style_type de cada frase)
2. Buscar estilos visuais do template (multi-text-styling)
3. Para cada frase, montar payload baseado no style_type
4. Chamar V-Services para gerar PNGs
5. Retornar URLs dos PNGs gerados
"""

import os
import json
import logging
import requests
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# 🆕 Importar debug_logger para salvar payloads no banco
try:
    from app.video_orchestrator.debug_logger import debug_logger
    DEBUG_LOGGER_AVAILABLE = True
except ImportError:
    DEBUG_LOGGER_AVAILABLE = False
    logger.warning("⚠️ debug_logger não disponível - logs de auditoria desabilitados")

# URL do V-Services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')

# Tipos de estilo válidos
VALID_STYLE_TYPES = ['default', 'emphasis', 'letter_effect']


class PngGeneratorService:
    """
    Serviço de geração de PNGs para legendas.
    
    Converte frases classificadas + estilos do template → PNGs via V-Services.
    """
    
    def __init__(self, v_services_url: str = None):
        self.base_url = v_services_url or V_SERVICES_URL
        self.endpoint = f"{self.base_url}/png-subtitles/generate_subtitles"
        logger.info(f"🖼️ PNG Generator inicializado: {self.endpoint}")
    
    def generate_pngs_for_phrases(
        self,
        phrase_groups: List[Dict[str, Any]],
        template_config: Dict[str, Any],
        video_height: int = 1280,
        job_id: str = None  # 🆕 Para salvar logs de auditoria
    ) -> Dict[str, Any]:
        """
        Gera PNGs para todas as frases classificadas.
        
        Args:
            phrase_groups: Lista de frases com style_type
                [{"text": "...", "style_type": "default", "words": [...], ...}, ...]
            template_config: Configuração do template (com multi-text-styling)
            video_height: Altura do vídeo em pixels (para % de fonte)
            job_id: ID do job para logs de auditoria (opcional)
            
        Returns:
            {
                "status": "success",
                "phrases": [
                    {
                        "phrase_index": 0,
                        "style_type": "default",
                        "words": [{"url": "...", "text": "...", ...}]
                    },
                    ...
                ],
                "total_pngs": 45,
                "total_phrases": 15
            }
        """
        if not phrase_groups:
            return {"status": "success", "phrases": [], "total_pngs": 0, "total_phrases": 0}
        
        logger.info(f"🖼️ Gerando PNGs para {len(phrase_groups)} frases...")
        
        # Extrair estilos do template
        text_styles = self._extract_text_styles(template_config)
        enhanced_rules = self._extract_enhanced_rules(template_config)
        
        # 🆕 Guardar job_id para logs de auditoria
        self._current_job_id = job_id
        
        # 🚀 v2.9.91: BATCH MODE - Enviar TODAS as palavras em UMA chamada HTTP
        # Isso elimina o overhead de ~1s por frase (12 frases = 12s perdidos)
        import time
        batch_start = time.time()
        
        try:
            results, total_pngs, errors = self._generate_pngs_batched(
                phrase_groups=phrase_groups,
                text_styles=text_styles,
                enhanced_rules=enhanced_rules,
                video_height=video_height
            )
            batch_elapsed = time.time() - batch_start
            logger.info(f"⚡ [BATCH MODE] {len(phrase_groups)} frases processadas em {batch_elapsed:.2f}s")
        except Exception as e:
            logger.warning(f"⚠️ Batch mode falhou ({e}), usando fallback sequencial...")
            results = []
            total_pngs = 0
            errors = []
            
            # Fallback para modo sequencial (compatibilidade)
            for phrase in phrase_groups:
                try:
                    phrase_result = self._generate_pngs_for_phrase(
                        phrase=phrase,
                        text_styles=text_styles,
                        enhanced_rules=enhanced_rules,
                        video_height=video_height
                    )
                    
                    if "error" in phrase_result:
                        errors.append({
                            "phrase_index": phrase.get('phrase_index', 0),
                            "error": phrase_result["error"]
                        })
                    else:
                        results.append(phrase_result)
                        total_pngs += len(phrase_result.get("words", []))
                        
                except Exception as e2:
                    logger.error(f"❌ Erro ao gerar PNGs para frase {phrase.get('phrase_index', '?')}: {e2}")
                    errors.append({
                        "phrase_index": phrase.get('phrase_index', 0),
                        "error": str(e2)
                    })
        
        # Estatísticas
        stats = {}
        for r in results:
            st = r.get('style_type', 'default')
            stats[st] = stats.get(st, 0) + 1
        
        logger.info(f"✅ PNGs gerados: {total_pngs} imagens para {len(results)} frases | Stats: {stats}")
        
        return {
            "status": "success" if not errors else "partial",
            "phrases": results,
            "total_pngs": total_pngs,
            "total_phrases": len(results),
            "style_stats": stats,
            "errors": errors if errors else None
        }
    
    def _generate_pngs_batched(
        self,
        phrase_groups: List[Dict[str, Any]],
        text_styles: Dict[str, Any],
        enhanced_rules: Dict[str, Any],
        video_height: int
    ) -> tuple:
        """
        🚀 v2.9.91: BATCH MODE - Gera PNGs para TODAS as frases em UMA chamada HTTP.
        
        Isso elimina o overhead de rede (~1s por chamada).
        Antes: 12 frases = 12 chamadas HTTP = ~12s de overhead
        Agora: 12 frases = 1 chamada HTTP = ~0.1s de overhead
        
        Returns:
            tuple: (results, total_pngs, errors)
        """
        import time
        
        # 1. Coletar TODAS as palavras de TODAS as frases
        all_words = []
        phrase_word_ranges = []  # [(start_idx, end_idx, phrase), ...]
        
        for phrase in phrase_groups:
            phrase_index = phrase.get('phrase_index', len(phrase_word_ranges))
            style_type = phrase.get('style_type', 'default')
            
            # Validar style_type
            if style_type not in VALID_STYLE_TYPES:
                style_type = 'default'
            
            # Buscar estilo específico
            style_config = text_styles.get(style_type) or text_styles.get('default', {})
            
            # Montar payload para esta frase
            phrase_payload = self._build_vservices_payload(
                phrase=phrase,
                style_config=style_config,
                enhanced_rules=enhanced_rules,
                video_height=video_height,
                style_type=style_type
            )
            
            words = phrase_payload.get('words', [])
            
            # Registrar range desta frase no batch
            start_idx = len(all_words)
            end_idx = start_idx + len(words)
            phrase_word_ranges.append({
                'start': start_idx,
                'end': end_idx,
                'phrase': phrase,
                'style_type': style_type
            })
            
            # Adicionar ao batch
            all_words.extend(words)
        
        if not all_words:
            return [], 0, []
        
        logger.info(f"🚀 [BATCH] Enviando {len(all_words)} palavras de {len(phrase_groups)} frases em 1 chamada HTTP")
        
        # 2. Fazer UMA chamada HTTP com TODAS as palavras
        batch_payload = {
            'words': all_words,
            'video_height': video_height
        }
        
        response = requests.post(
            self.endpoint,
            json=batch_payload,
            timeout=180  # 3 minutos para batches grandes
        )
        
        if response.status_code != 200:
            raise Exception(f"V-Services retornou {response.status_code}: {response.text[:200]}")
        
        result = response.json()
        
        if result.get('status') != 'success':
            raise Exception(result.get('error', 'Erro desconhecido'))
        
        result_words = result.get('words', [])
        logger.info(f"✅ [BATCH] V-Services retornou {len(result_words)} PNGs")
        
        # 3. Distribuir resultados de volta para cada frase
        results = []
        total_pngs = 0
        errors = []
        
        for range_info in phrase_word_ranges:
            phrase = range_info['phrase']
            phrase_index = phrase.get('phrase_index', 0)
            style_type = range_info['style_type']
            start_idx = range_info['start']
            end_idx = range_info['end']
            
            # Extrair palavras desta frase do resultado
            phrase_result_words = result_words[start_idx:end_idx]
            
            # Mesclar com dados originais (preservar timings)
            original_words = phrase.get('words', [])
            merged_words = []
            
            for i, rw in enumerate(phrase_result_words):
                merged_word = dict(rw)
                
                # Encontrar palavra original
                if i < len(original_words):
                    orig_word = original_words[i]
                    for audio_key in ['_audio_start', '_audio_end', 'start', 'end']:
                        if orig_word.get(audio_key) is not None:
                            merged_word[audio_key] = orig_word[audio_key]
                
                merged_words.append(merged_word)
            
            # Montar resultado da frase
            phrase_result = {
                "phrase_index": phrase_index,
                "style_type": style_type,
                "text": phrase.get('text', ''),
                "words": merged_words
            }
            
            # Preservar cartela_info
            if phrase.get('cartela_info'):
                phrase_result['cartela_info'] = phrase['cartela_info']
            
            # Preservar timings
            for timing_key in [
                'start', 'end', 'group_start_time', 'group_end_time',
                'start_time', 'end_time', '_audio_start_time', '_audio_end_time'
            ]:
                if phrase.get(timing_key) is not None:
                    phrase_result[timing_key] = phrase[timing_key]
            
            results.append(phrase_result)
            total_pngs += len(merged_words)
        
        return results, total_pngs, errors
    
    def _generate_pngs_for_phrase(
        self,
        phrase: Dict[str, Any],
        text_styles: Dict[str, Any],
        enhanced_rules: Dict[str, Any],
        video_height: int
    ) -> Dict[str, Any]:
        """
        Gera PNGs para uma única frase.
        
        Args:
            phrase: Frase com style_type e words
            text_styles: Estilos do multi-text-styling
            enhanced_rules: Regras do enhanced-phrase-rules
            video_height: Altura do vídeo
            
        Returns:
            {
                "phrase_index": 0,
                "style_type": "emphasis",
                "text": "Fala galera!",
                "words": [{"url": "...", "text": "FALA", ...}, ...]
            }
        """
        phrase_index = phrase.get('phrase_index', 0)
        style_type = phrase.get('style_type', 'default')
        
        # Validar style_type
        if style_type not in VALID_STYLE_TYPES:
            style_type = 'default'
        
        # Buscar estilo específico
        style_config = text_styles.get(style_type) or text_styles.get('default', {})
        
        # Montar payload para V-Services
        payload = self._build_vservices_payload(
            phrase=phrase,
            style_config=style_config,
            enhanced_rules=enhanced_rules,
            video_height=video_height,
            style_type=style_type
        )
        
        # 🆕 LOG DO PAYLOAD REAL enviado para V-Services
        # Este é o payload TRANSFORMADO após _build_vservices_payload()
        logger.info(f"📤 [PNG_GENERATOR] Payload para V-Services (frase #{phrase_index}):")
        logger.info(f"   • Total words no payload: {len(payload.get('words', []))}")
        
        # Contar base vs highlight
        base_words = [w for w in payload.get('words', []) if not w.get('is_highlight')]
        hl_words = [w for w in payload.get('words', []) if w.get('is_highlight')]
        logger.info(f"   • Words BASE (is_highlight=false): {len(base_words)}")
        logger.info(f"   • Words HIGHLIGHT (is_highlight=true): {len(hl_words)}")
        
        # Mostrar primeira word de cada tipo para debug
        if base_words:
            first_base = base_words[0]
            logger.info(f"   • Exemplo BASE: text='{first_base.get('text')}', size='{first_base.get('size')}', has_text_style={bool(first_base.get('text_style'))}")
        if hl_words:
            first_hl = hl_words[0]
            logger.info(f"   • Exemplo HIGHLIGHT: text='{first_hl.get('text')}', size='{first_hl.get('size')}', text_style={first_hl.get('text_style')}")
        
        # 📝 AUDITORIA: Salvar payload EXATO que vai para V-Services
        # Este é o payload que REALMENTE entra no png_service.py
        if DEBUG_LOGGER_AVAILABLE and hasattr(self, '_current_job_id') and self._current_job_id:
            debug_logger.log_step(
                job_id=self._current_job_id,
                step_name="generate_pngs",
                direction="vservices_request",  # 🆕 Novo tipo de direction!
                payload={
                    "phrase_index": phrase_index,
                    "style_type": style_type,
                    "endpoint": self.endpoint,
                    "payload_sent_to_vservices": payload,  # O payload EXATO
                    "stats": {
                        "total_words": len(payload.get('words', [])),
                        "base_words": len(base_words),
                        "highlight_words": len(hl_words)
                    }
                }
            )
            logger.info(f"   📝 Payload salvo em pipeline_debug_logs (direction='vservices_request')")
        
        # Chamar V-Services
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                timeout=120  # 2 minutos para frases grandes
            )
            
            if response.status_code != 200:
                return {"error": f"V-Services retornou {response.status_code}: {response.text[:200]}"}
            
            result = response.json()
            
            if result.get('status') != 'success':
                return {"error": result.get('error', 'Erro desconhecido')}
            
            # 🆕 Construir resultado preservando dados originais da phrase
            # 🔧 v2.9.33: Mesclar dados das palavras originais com os resultados do v-services
            # Isso preserva _audio_start, _audio_end para o modo HYBRID
            result_words = result.get('words', [])
            original_words = phrase.get('words', [])
            
            # Criar mapeamento por índice ou texto para mesclar
            merged_words = []
            for i, rw in enumerate(result_words):
                merged_word = dict(rw)  # Copiar dados do resultado (URLs, positions, etc)
                
                # Encontrar palavra original correspondente
                word_text = rw.get('text', '').lower().strip()
                orig_word = None
                
                # Tentar encontrar por índice primeiro
                if i < len(original_words):
                    orig_word = original_words[i]
                
                # Fallback: buscar por texto
                if not orig_word:
                    for ow in original_words:
                        if ow.get('text', '').lower().strip() == word_text:
                            orig_word = ow
                            break
                
                # Mesclar campos de timing de áudio se encontrou a palavra original
                if orig_word:
                    for audio_key in ['_audio_start', '_audio_end', 'start', 'end']:
                        if orig_word.get(audio_key) is not None:
                            merged_word[audio_key] = orig_word[audio_key]
                
                merged_words.append(merged_word)
            
            phrase_result = {
                "phrase_index": phrase_index,
                "style_type": style_type,
                "text": phrase.get('text', ''),
                "words": merged_words,
                "preview_url": result.get('preview_html_url')
            }
            
            # 🆕 Preservar cartela_info se existir na phrase original
            # Isso é crucial para o subtitle_pipeline extrair as cartelas!
            if phrase.get('cartela_info'):
                phrase_result['cartela_info'] = phrase['cartela_info']
                logger.debug(f"🎬 [PNG_GENERATOR] Preservando cartela_info para frase #{phrase_index}")
            
            # 🆕 Preservar timings originais também (necessários para cartela)
            # 🔧 v2.9.33: Adicionar _audio_* para modo HYBRID (legendas em tempo virtual)
            for timing_key in [
                'start', 'end', 'group_start_time', 'group_end_time',
                'start_time', 'end_time',  # Timestamps mapeados
                '_audio_start_time', '_audio_end_time'  # 🆕 Timestamps de áudio para HYBRID
            ]:
                if phrase.get(timing_key) is not None:
                    phrase_result[timing_key] = phrase[timing_key]
            
            return phrase_result
            
        except requests.Timeout:
            return {"error": "Timeout ao chamar V-Services"}
        except requests.RequestException as e:
            return {"error": f"Erro de conexão: {str(e)}"}
    
    def _build_vservices_payload(
        self,
        phrase: Dict[str, Any],
        style_config: Dict[str, Any],
        enhanced_rules: Dict[str, Any],
        video_height: int,
        style_type: str
    ) -> Dict[str, Any]:
        """
        Monta o payload no formato esperado pelo png_service.py.
        
        Campos obrigatórios:
        - words: Lista de palavras com configurações visuais
        - video_height: Altura do vídeo para cálculo de % de fonte
        """
        words = phrase.get('words', [])
        
        # 🆕 Extrair configurações do estilo (com tratamento para JSON strings)
        font_config = self._ensure_dict(style_config.get('font_config', {}))
        render_config = self._ensure_dict(style_config.get('render_config', {}))
        padding_config = self._ensure_dict(style_config.get('padding', {}))
        borders = self._ensure_list(style_config.get('borders', []))
        highlight_config = self._ensure_dict(style_config.get('highlight', {}))
        
        # 🔍 DEBUG: Log do style_config e font_config recebidos
        logger.info(f"📦 [PNG_GENERATOR] style_type='{style_type}' | font_config recebido: {font_config}")
        
        # Configurações de fonte
        # 🆕 NOVA LÓGICA: Combinar family + weight no formato "Family-Weight"
        # O PNG_service espera: "Poppins-Bold", "Montserrat-Regular", etc.
        
        # 🚨 FAIL LOUD: Se font_config está vazio, algo está errado!
        if not font_config or not font_config.get('family'):
            error_msg = (
                f"❌ FAIL LOUD: font_config vazio ou sem 'family' para style_type='{style_type}'!\n"
                f"   font_config recebido: {font_config}\n"
                f"   style_config keys: {list(style_config.keys()) if style_config else 'None'}\n"
                f"   BUG: TemplateLoader não está carregando ts_{style_type}_font corretamente!"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        font_family_raw = self._get_value(font_config, 'family', None)  # 🚨 Sem fallback!
        font_weight = self._get_value(font_config, 'weight', 700)
        
        # Mapear peso numérico para nome do variant
        weight_map = {
            100: 'Thin',
            200: 'ExtraLight',
            300: 'Light',
            400: 'Regular',
            500: 'Medium',
            600: 'SemiBold',
            700: 'Bold',
            800: 'ExtraBold',
            900: 'Black'
        }
        
        weight_name = weight_map.get(font_weight, 'Bold')
        
        # 🔧 CORREÇÃO: Enviar no formato Pango diretamente!
        # O v-services espera formato "Family:style=Weight" (sem hífen no meio da família)
        # 
        # Exemplos corretos:
        #   - "Poppins:style=Bold"
        #   - "Bebas Neue:style=Regular"
        #   - "Montserrat:style=Bold"
        #
        # O Generator V3 já salva sem hífen (ex: "Bebas Neue"), então mantemos assim.
        
        # Se já está no formato Pango (contém ':'), usar direto
        if ':' in str(font_family_raw):
            font_family = font_family_raw
            logger.debug(f"🔤 [PNG_GENERATOR] Font já em formato Pango: {font_family}")
        else:
            # Combinar no formato Pango: "Family:style=Weight"
            # Não usar hífen! O fontconfig usa o nome INTERNO do TTF (com espaço)
            font_family = f"{font_family_raw}:style={weight_name}"
            logger.debug(f"🔤 [PNG_GENERATOR] Font: '{font_family_raw}' + weight {font_weight} → '{font_family}'")
        
        uppercase = self._get_value(font_config, 'uppercase', True)
        line_join = self._get_value(font_config, 'line_join', 'round')
        
        # Configurações de tamanho
        # PRIORIDADE:
        # 1. font_config.size_percent (novo formato ts_*_font)
        # 2. enhanced_rules.font_size_config (formato legado)
        # 
        # 🆕 Garantir que size_cfg é um dict
        font_size_percent = self._get_value(font_config, 'size_percent', None)
        
        if font_size_percent:
            # Novo formato: tamanho fixo em % do canvas
            base_size = font_size_percent
            logger.debug(f"📏 [PNG_GENERATOR] Usando size_percent de font_config: {base_size}%")
        else:
            # Formato legado: tamanhos dinâmicos baseados na contagem de palavras
            size_cfg = self._ensure_dict(enhanced_rules.get('font_size_config', {}))
            word_count = len(words)
            
            if word_count == 1:
                base_size = self._get_value(size_cfg, 'single_word_size_percent', 5)
            elif word_count == 2:
                base_size = self._get_value(size_cfg, 'double_word_size_percent', 3.2)
            else:
                base_size = self._get_value(size_cfg, 'phrase_size_percent', 2.5)
        
        # 🆕 Ler size_increase_percent do highlight_config (novo campo no schema)
        highlight_size_increase_pct = self._get_value(highlight_config, 'size_increase_percent', 20)
        
        # Padding
        padding_unit = self._get_value(padding_config, 'unit', 'percent')
        padding_x_pct = self._get_value(padding_config, 'x_percent', 20)
        padding_y_pct = self._get_value(padding_config, 'y_percent', 20)
        
        # Text style (cor/gradiente) - passa font_config para novo formato
        text_style = self._build_text_style(render_config, font_config=font_config)
        
        # Border config
        text_border_config = self._build_border_config(borders, line_join)
        
        # Highlight config
        highlight_text_style = None
        highlight_border_config = None
        highlight_enabled = self._get_value(highlight_config, 'enabled', False)
        
        # 🔍 LOG DEBUG: Verificar se highlight está habilitado
        logger.info(f"🔍 [PNG_GENERATOR] Verificando HIGHLIGHT para style_type='{style_type}'")
        logger.info(f"   • highlight_config keys: {list(highlight_config.keys()) if highlight_config else 'VAZIO'}")
        logger.info(f"   • highlight.enabled RAW: {highlight_config.get('enabled') if highlight_config else 'N/A'}")
        logger.info(f"   • highlight.enabled EXTRAÍDO: {highlight_enabled}")
        
        if highlight_enabled:
            logger.info(f"✅ [PNG_GENERATOR] Highlight HABILITADO! Gerando PNGs base + highlight")
            # 🆕 Garantir que campos internos do highlight também sejam dicts/lists
            highlight_render = self._ensure_dict(highlight_config.get('render_config', {}))
            highlight_font = self._ensure_dict(highlight_config.get('font_config', {}))
            
            # 🔧 FIX 07/Feb/2026: Ler cor do highlight.color.value (formato Generator V3)
            # O schema do Generator V3 salva a cor como highlight.color.value = [R, G, B, A]
            # Mas _build_text_style espera render_config.solid_color_rgb = "R,G,B"
            # Se render_config está vazio mas highlight.color.value existe, injetar a cor
            if not highlight_render.get('solid_color_rgb'):
                hl_color = self._ensure_dict(highlight_config.get('color', {}))
                hl_color_value = hl_color.get('value') if isinstance(hl_color, dict) else hl_color
                if hl_color_value:
                    if isinstance(hl_color_value, (list, tuple)) and len(hl_color_value) >= 3:
                        # Converter [R, G, B, A] → "R,G,B"
                        highlight_render['solid_color_rgb'] = f"{hl_color_value[0]},{hl_color_value[1]},{hl_color_value[2]}"
                        logger.info(f"   🔧 [FIX] Cor do highlight.color.value → solid_color_rgb: {highlight_render['solid_color_rgb']}")
                    elif isinstance(hl_color_value, str) and ',' in hl_color_value:
                        highlight_render['solid_color_rgb'] = hl_color_value
                        logger.info(f"   🔧 [FIX] Cor do highlight.color.value (string): {hl_color_value}")
            
            highlight_text_style = self._build_text_style(highlight_render, is_highlight=True, font_config=highlight_font)
            highlight_borders = self._ensure_list(highlight_config.get('borders', []))
            # 🔥 Se não houver borders no highlight, deixar vazio (designer não quer borders)
            highlight_border_config = self._build_border_config(highlight_borders, line_join)
            # 🆕 Tratar solid_color_rgb que pode ser string
            solid_color = self._ensure_dict(highlight_render.get('solid_color_rgb', {}))
            color_value = solid_color.get('value', 'N/A') if isinstance(solid_color, dict) else solid_color
            logger.info(f"   • Cor highlight: {color_value}")
            logger.info(f"   • Borders: {len(highlight_borders)} configurados")
        else:
            logger.warning(f"⚠️ [PNG_GENERATOR] Highlight DESABILITADO para style_type='{style_type}'!")
        
        # Montar lista de palavras
        # 🆕 IMPORTANTE: Gerar 2 PNGs por palavra (base + highlight)
        payload_words = []
        base_count = 0
        highlight_count = 0
        
        for i, word in enumerate(words):
            word_text = word.get('text', '')
            if uppercase:
                word_text = word_text.upper()
            
            # Timestamps da palavra
            # Suportar dois formatos:
            # 1. AssemblyAI: 'start'/'end' em segundos (precisa * 1000)
            # 2. script_data: 'start_ms'/'end_ms' já em milissegundos
            if 'start_ms' in word:
                # Formato script_data (já em ms)
                start_ms = word.get('start_ms', 0)
                end_ms = word.get('end_ms', 0)
            else:
                # Formato AssemblyAI (segundos → ms)
                start_ms = word.get('start', 0) * 1000
                end_ms = word.get('end', 0) * 1000
            
            # 🔍 DEBUG v2.9.59: Log timestamps das palavras
            if i == 0:  # Apenas primeira palavra de cada frase para não poluir logs
                logger.info(f"   🔍 Word 0 timestamps: start_ms={start_ms}ms, end_ms={end_ms}ms (raw: start={word.get('start')}, start_ms={word.get('start_ms')})")
            
            # 🎨 CREATIVE LAYOUT: Aplicar size_scale se disponível
            # O creative_layout é aplicado ANTES do PNG generation e adiciona size_scale a cada palavra
            creative_layout_info = word.get('creative_layout', {})
            size_scale = creative_layout_info.get('size_scale', 1.0)
            
            # Calcular tamanho base (aplicando scale do creative layout)
            word_base_size = base_size * size_scale
            font_px = (video_height * word_base_size) / 100
            if padding_unit == 'percent':
                pad_x_base = max(1, round(font_px * (padding_x_pct / 100)))
                pad_y_base = max(1, round(font_px * (padding_y_pct / 100)))
            else:
                pad_x_base = max(1, round(padding_x_pct))
                pad_y_base = max(1, round(padding_y_pct))
            
            # 1️⃣ PNG BASE (subtitle)
            word_payload = {
                "text": word_text,
                "fontFamily": font_family,
                "size": f"{word_base_size}%",  # 🎨 Usa tamanho com scale aplicado
                "padding_x": pad_x_base,
                "padding_y": pad_y_base,
                "uppercase": uppercase,
                "is_highlight": False,  # BASE
                "letter_by_letter": style_type == 'letter_effect',
                "text_style": text_style,
                "text_border_config": text_border_config,
                "quality": 100,
                "dpi": 300,  # 🆕 Aumentado de 150 para 300 - qualidade profissional
                "start_time": start_ms,
                "end_time": end_ms,
                "word_index": i,
                "phrase_info": {
                    "index": phrase.get('phrase_index', 0),
                    "text": phrase.get('text', '')
                },
                "style_type": style_type
            }
            
            # 🎨 Propagar creative_layout info para uso posterior (positioning)
            if creative_layout_info:
                word_payload['creative_layout'] = creative_layout_info
            
            payload_words.append(word_payload)
            base_count += 1
            
            # 2️⃣ PNG HIGHLIGHT (karaokê) - SOMENTE SE highlight estiver habilitado
            # 🔥 IMPORTANTE: Aceitar highlight mesmo sem borders (borders são opcionais)
            if highlight_text_style:
                # Calcular tamanho do highlight usando size_increase_percent
                # Ex: word_base_size=3%, size_increase_pct=20 → highlight=3.6% (20% maior)
                # 🎨 CREATIVE LAYOUT: Aplica scale também ao highlight
                highlight_size_pct = word_base_size * (1 + (highlight_size_increase_pct / 100))
                
                font_px_hl = (video_height * highlight_size_pct) / 100
                if padding_unit == 'percent':
                    pad_x_hl = max(1, round(font_px_hl * (padding_x_pct / 100)))
                    pad_y_hl = max(1, round(font_px_hl * (padding_y_pct / 100)))
                else:
                    pad_x_hl = max(1, round(padding_x_pct))
                    pad_y_hl = max(1, round(padding_y_pct))
                
                # 🔧 FIX v2.9.61: Usar nomes de campo corretos para v-services
                # v-services espera highlight_text_style e highlight_border_config
                highlight_payload = {
                    "text": word_text,
                    "fontFamily": font_family,
                    "size": f"{highlight_size_pct}%",
                    "padding_x": pad_x_hl,
                    "padding_y": pad_y_hl,
                    "uppercase": uppercase,
                    "is_highlight": True,  # HIGHLIGHT
                    "letter_by_letter": style_type == 'letter_effect',
                    "highlight_text_style": highlight_text_style,  # 🔧 FIX: Renamed from text_style
                    "highlight_border_config": highlight_border_config,  # 🔧 FIX: Renamed from text_border_config
                    "quality": 100,
                    "dpi": 300,  # 🆕 Aumentado de 150 para 300 - qualidade profissional
                    "start_time": start_ms,
                    "end_time": end_ms,
                    "word_index": i,
                    "phrase_info": {
                        "index": phrase.get('phrase_index', 0),
                        "text": phrase.get('text', '')
                    },
                    "style_type": style_type
                }
                
                # 🎨 Propagar creative_layout info para highlight também
                if creative_layout_info:
                    highlight_payload['creative_layout'] = creative_layout_info
                
                payload_words.append(highlight_payload)
                highlight_count += 1
        
        # 📊 LOG FINAL: Resumo de PNGs gerados
        logger.info(f"📊 [PNG_GENERATOR] Resumo para frase #{phrase.get('phrase_index', 0)} (style='{style_type}'):")
        logger.info(f"   • Total de palavras originais: {len(words)}")
        logger.info(f"   • PNGs BASE gerados: {base_count}")
        logger.info(f"   • PNGs HIGHLIGHT gerados: {highlight_count}")
        logger.info(f"   • TOTAL de PNGs no payload: {len(payload_words)}")
        
        if highlight_count == 0 and highlight_enabled:
            logger.error(f"❌ [PNG_GENERATOR] BUG DETECTADO! Highlight habilitado mas NENHUM PNG highlight gerado!")
        
        return {
            "words": payload_words,
            "video_height": video_height
        }
    
    def _build_text_style(self, render_config: Dict, is_highlight: bool = False, font_config: Dict = None) -> Dict:
        """
        Monta configuração de cor/gradiente do texto.
        
        Suporta dois formatos:
        1. Legado: render_config com solid_color_rgb, render_type
        2. Novo (ts_*_font): font_config com color_rgb
        
        Args:
            render_config: Config de renderização (legado)
            is_highlight: Se é highlight (karaokê)
            font_config: Config de fonte (novo formato)
        """
        # 🆕 Garantir que são dicts
        render_config = self._ensure_dict(render_config)
        font_config = self._ensure_dict(font_config or {})
        
        render_type = self._get_value(render_config, 'render_type', 'solid')
        
        style = {"render_type": render_type}
        
        if render_type == 'solid':
            default_color = '238,255,0' if is_highlight else '255,255,255'
            
            # 🆕 PRIORIDADE: 
            # 1. render_config.solid_color_rgb (formato legado/schema completo)
            # 2. font_config.color_rgb (novo formato ts_*_font)
            # 3. default_color
            solid_color = self._get_value(render_config, 'solid_color_rgb', None)
            if not solid_color:
                solid_color = self._get_value(font_config, 'color_rgb', None)
            if not solid_color:
                solid_color = default_color
            
            style['solid_color_rgb'] = solid_color
            logger.debug(f"🎨 [_build_text_style] Cor sólida: {solid_color} (is_highlight={is_highlight})")
            
        elif render_type == 'gradient':
            # 🆕 Garantir que gradient_cfg é um dict
            gradient_cfg = self._ensure_dict(render_config.get('gradient_config', {}))
            default_start = '238,255,0' if is_highlight else '255,255,255'
            default_end = '255,255,255' if is_highlight else '200,200,200'
            
            style['gradient_start_color_rgb'] = self._get_value(gradient_cfg, 'start_rgb', default_start)
            style['gradient_end_color_rgb'] = self._get_value(gradient_cfg, 'end_rgb', default_end)
            style['gradient_text_direction'] = self._get_value(gradient_cfg, 'direction', 'vertical_text')
        
        return style
    
    def _build_border_config(self, borders: List, line_join: str) -> Dict:
        """Monta configuração de bordas (até 3 camadas)."""
        # 🆕 Garantir que borders é uma lista
        borders = self._ensure_list(borders)
        
        config = {"line_join": line_join}
        
        border_keys = ['border_1_inner', 'border_2_spacing', 'border_3_outer']
        
        for i, border in enumerate(borders[:3]):
            # 🆕 Garantir que cada border é um dict
            border = self._ensure_dict(border)
            if self._get_value(border, 'enabled', False):
                config[border_keys[i]] = {
                    "enabled": True,
                    "thickness_value": self._get_value(border, 'thickness', 0),
                    "thickness_unit": self._get_value(border, 'unit', 'px'),
                    "color_rgb": self._get_value(border, 'color_rgb', '0,0,0'),
                    "blur_radius": self._get_value(border, 'blur', 0),
                    "line_join": line_join
                }
        
        return config
    
    def _extract_text_styles(self, template_config: Dict) -> Dict:
        """
        Extrai estilos do template.
        
        Prioridade:
        1. template_config['_text_styles'] (carregado da tabela template_text_styles)
        2. template_config['multi-text-styling']['text_styles'] (do params JSONB)
        
        A segunda opção existe porque o orchestrator pode não ter carregado 
        '_text_styles' corretamente em todos os fluxos (ex: continue_pipeline).
        """
        # 1. Tentar fonte preferida: tabela template_text_styles
        # 🐛 FIX: Usar 'or {}' para tratar valores None explícitos
        text_styles = template_config.get('_text_styles') or {}
        
        if text_styles:
            logger.info(f"✅ [PNG_GENERATOR] Usando _text_styles da TABELA: {list(text_styles.keys())}")
            return text_styles
        
        # 2. Fallback: multi-text-styling do params JSONB
        mts = template_config.get('multi-text-styling') or {}
        text_styles = mts.get('text_styles') or {}
        
        if text_styles:
            logger.info(f"✅ [PNG_GENERATOR] Usando multi-text-styling.text_styles do PARAMS: {list(text_styles.keys())}")
            # Verificar se tem highlight configurado
            for style_name, style_cfg in text_styles.items():
                if isinstance(style_cfg, dict):
                    hl = style_cfg.get('highlight', {})
                    hl_enabled = self._get_value(hl, 'enabled', False)
                    logger.info(f"   • {style_name}: highlight.enabled = {hl_enabled}")
            return text_styles
        
        # ❌ Nenhuma fonte encontrada
        logger.error(f"❌ [PNG_GENERATOR] text_styles NÃO ENCONTRADO!")
        logger.error(f"❌ [PNG_GENERATOR] Keys disponíveis: {list(template_config.keys())}")
        logger.error(f"❌ [PNG_GENERATOR] multi-text-styling keys: {list(mts.keys()) if mts else 'N/A'}")
        
        return {}
    
    def _extract_enhanced_rules(self, template_config: Dict) -> Dict:
        """Extrai regras do enhanced-phrase-rules."""
        # 🐛 FIX: Usar 'or {}' para tratar valores None explícitos
        return template_config.get('enhanced-phrase-rules') or {}
    
    def _ensure_dict(self, value: Any) -> Dict:
        """
        🆕 Garante que o valor é um dict.
        
        Trata casos onde o PostgreSQL retorna JSON como string.
        """
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
                return {}
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"⚠️ [PNG_GENERATOR] Não conseguiu parsear string como JSON: {value[:100] if len(value) > 100 else value}")
                return {}
        return {}
    
    def _ensure_list(self, value: Any) -> List:
        """
        🆕 Garante que o valor é uma lista.
        
        Trata casos onde o PostgreSQL retorna JSON como string.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
                return []
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"⚠️ [PNG_GENERATOR] Não conseguiu parsear string como lista: {value[:100] if len(value) > 100 else value}")
                return []
        return []
    
    def _get_value(self, obj: Any, key: str, default: Any = None) -> Any:
        """
        Extrai valor de objeto, suportando formato {value: x}.
        
        Exemplo:
            {"font_family": {"value": "Poppins"}} → "Poppins"
            {"font_family": "Poppins"} → "Poppins"
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

