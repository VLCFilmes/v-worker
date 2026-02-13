"""
🎨 Template Loader Service - Nova Arquitetura

Carrega templates das novas tabelas modulares:
- template_configs (configs por schema)
- template_text_styles (estilos de texto)

Mantém retrocompatibilidade com video_editing_templates.params
"""

import logging
import json
from typing import Dict, Any, Optional
import uuid
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class TemplateLoaderService:
    """
    Serviço para carregar configurações de templates.
    
    Nova Arquitetura (COLUNAS POR SCHEMA):
    - enhanced_phrase_rules (coluna) - step 07
    - phrase_classification (coluna) - step 08
    - ... (mais colunas conforme steps são implementados)
    
    🚨 FAIL LOUD: SEM FALLBACK!
    - Se coluna estiver vazia, ERRO IMEDIATO
    - Força o Generator-V2 a salvar corretamente
    - Detecta bugs de persistência na criação
    """
    
    def __init__(self, db_connection_func=None):
        """
        Args:
            db_connection_func: Função para obter conexão com banco
        """
        self.db_connection_func = db_connection_func
    
    def load_phrase_rules(self, template_id: str) -> Dict[str, Any]:
        """
        🆕 Carrega APENAS enhanced-phrase-rules do template.
        
        🚨 FAIL LOUD - SEM FALLBACK:
        1. SELECT enhanced_phrase_rules FROM video_editing_templates
        2. Se NULL → ERRO IMEDIATO (não busca params!)
        3. Se OK → Retorna schema limpo (~2KB)
        
        Args:
            template_id: UUID do template
            
        Returns:
            Dict com enhanced-phrase-rules
            
        Raises:
            ValueError: Se template_id inválido, template não encontrado,
                       ou enhanced_phrase_rules não está na coluna dedicada
        """
        if not template_id:
            raise ValueError("template_id não pode ser vazio")
        
        # Validar UUID
        try:
            uuid.UUID(str(template_id))
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"template_id inválido: {template_id} - {e}")
        
        logger.info(f"🔍 [TemplateLoader] Carregando enhanced_phrase_rules para template {template_id}")
        
        db_conn = None
        db_cursor = None
        
        try:
            # Obter conexão
            if self.db_connection_func:
                db_conn = self.db_connection_func()
            else:
                from app.supabase_client import get_direct_db_connection
                db_conn = get_direct_db_connection()
            
            db_cursor = db_conn.cursor(cursor_factory=RealDictCursor)
            
            # Buscar coluna enhanced_phrase_rules (SEM FALLBACK!)
            db_cursor.execute("""
                SELECT enhanced_phrase_rules, name
                FROM video_editing_templates 
                WHERE id = %s
            """, (str(template_id),))
            
            row = db_cursor.fetchone()
            
            if not row:
                logger.error(f"❌ [TemplateLoader] Template {template_id} NÃO ENCONTRADO no banco!")
                raise ValueError(f"Template {template_id} não encontrado")
            
            # RealDictCursor retorna dict
            enhanced_phrase_rules = row['enhanced_phrase_rules'] if isinstance(row, dict) else row[0]
            template_name = row['name'] if isinstance(row, dict) else 'unknown'
            
            # 🆕 Fallback: Se coluna vazia, usar defaults
            if not enhanced_phrase_rules:
                logger.warning(f"⚠️ [TemplateLoader] Template '{template_name}' ({template_id}) SEM enhanced_phrase_rules!")
                logger.warning(f"⚠️ Usando valores padrão (template novo ou não migrado)")
                
                # Defaults mínimos para o pipeline funcionar
                enhanced_phrase_rules = {
                    "enabled": {"value": True},
                    "phrase_rules": {
                        "default_min_words": {"value": 2},
                        "default_max_words": {"value": 5},
                        "pause_threshold_ms": {"value": 400},
                        "duration_threshold_ms": {"value": 900},
                        "punctuation_rules": {
                            "enabled": {"value": True},
                            "max_words_before_break": {"value": 6}
                        }
                    },
                    "titles": {"enabled": {"value": False}},
                    "output_format": {
                        "include_style_tags": {"value": True},
                        "include_style_reasoning": {"value": True}
                    }
                }
                logger.info(f"✅ [TemplateLoader] Usando defaults para enhanced_phrase_rules")
            
            # ✅ Sucesso: Coluna tem dados
            logger.info(f"✅ [TemplateLoader] enhanced_phrase_rules carregado da COLUNA")
            logger.info(f"   • Template: {template_name}")
            logger.info(f"   • Tamanho: ~{len(str(enhanced_phrase_rules))} bytes")
            
            return enhanced_phrase_rules
            
        except Exception as e:
            logger.error(f"❌ [TemplateLoader] Erro ao carregar phrase_rules: {e}")
            raise
        finally:
            if db_cursor:
                db_cursor.close()
            if db_conn:
                db_conn.close()
    
    def load_phrase_classification(self, template_id: str) -> Dict[str, Any]:
        """
        🆕 Carrega classificação de frases do template (step 08).
        
        ⚠️ USADO APENAS para project_type = 'template_creation' (Generator-V2)!
        Para user_video, usar PhraseClassifierService (LLM/heurísticas).
        
        🚨 FAIL LOUD - SEM FALLBACK:
        1. SELECT phrase_classification FROM video_editing_templates
        2. Se NULL → ERRO IMEDIATO
        3. Se OK → Retorna schema (~2.6KB)
        
        Args:
            template_id: UUID do template
            
        Returns:
            Dict com phrase-classification:
            {
                "classification_mode": "manual",
                "auto_rules": {...},
                "classified_phrases": [
                    {"phrase_index": 0, "style_type": "emphasis", ...},
                    ...
                ]
            }
            
        Raises:
            ValueError: Se template_id inválido, template não encontrado,
                       ou phrase_classification não está na coluna dedicada
        """
        if not template_id:
            raise ValueError("template_id não pode ser vazio")
        
        # Validar UUID
        try:
            uuid.UUID(str(template_id))
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"template_id inválido: {template_id} - {e}")
        
        logger.info(f"🔍 [TemplateLoader] Carregando phrase_classification para template {template_id}")
        
        db_conn = None
        db_cursor = None
        
        try:
            # Obter conexão
            if self.db_connection_func:
                db_conn = self.db_connection_func()
            else:
                from app.supabase_client import get_direct_db_connection
                db_conn = get_direct_db_connection()
            
            db_cursor = db_conn.cursor(cursor_factory=RealDictCursor)
            
            # Buscar coluna phrase_classification (SEM FALLBACK!)
            db_cursor.execute("""
                SELECT phrase_classification, name
                FROM video_editing_templates 
                WHERE id = %s
            """, (str(template_id),))
            
            row = db_cursor.fetchone()
            
            if not row:
                logger.error(f"❌ [TemplateLoader] Template {template_id} NÃO ENCONTRADO no banco!")
                raise ValueError(f"Template {template_id} não encontrado")
            
            # RealDictCursor retorna dict
            phrase_classification = row['phrase_classification'] if isinstance(row, dict) else row[0]
            template_name = row['name'] if isinstance(row, dict) else 'unknown'
            
            # 🚨 FAIL LOUD: Se coluna vazia, ERRO IMEDIATO!
            if not phrase_classification:
                logger.error(f"❌ [TemplateLoader] Template '{template_name}' ({template_id}) SEM phrase_classification!")
                logger.error(f"❌ A coluna 'phrase_classification' está VAZIA!")
                logger.error(f"❌ Para template_creation: Generator-V2 deve classificar frases ANTES de processar!")
                raise ValueError(
                    f"Template '{template_name}' ({template_id}) não tem phrase_classification. "
                    f"Classifique as frases no Generator-V2 antes de processar."
                )
            
            # Validar se tem classified_phrases
            classified_phrases = phrase_classification.get('classified_phrases', {}).get('value', [])
            if not classified_phrases:
                logger.warning(f"⚠️ [TemplateLoader] Template '{template_name}' tem phrase_classification mas classified_phrases vazio!")
            
            # ✅ Sucesso: Coluna tem dados
            logger.info(f"✅ [TemplateLoader] phrase_classification carregado da COLUNA")
            logger.info(f"   • Template: {template_name}")
            logger.info(f"   • Modo: {phrase_classification.get('classification_mode', {}).get('value', 'N/A')}")
            logger.info(f"   • Frases classificadas: {len(classified_phrases)}")
            logger.info(f"   • Tamanho: ~{len(str(phrase_classification))} bytes")
            
            return phrase_classification
            
        except Exception as e:
            logger.error(f"❌ [TemplateLoader] Erro ao carregar phrase_classification: {e}")
            raise
        finally:
            if db_cursor:
                db_cursor.close()
            if db_conn:
                db_conn.close()
    
    # 🗑️ DEPRECATED: load_shadow() removido
    # Shadow agora é per-style via ts_*_shadow
    # Carregado automaticamente por load_multi_text_styling()
    
    def load_multi_text_styling(self, template_id: str) -> Dict[str, Dict[str, Any]]:
        """
        🆕 Carrega estilos de texto das COLUNAS DEDICADAS ts_* (step 09).
        
        Nova Arquitetura: Cada PROPRIEDADE em uma coluna separada!
        - ts_{estilo}_font JSONB (~300 bytes)
        - ts_{estilo}_borders JSONB (~400 bytes)
        - ts_{estilo}_highlight JSONB (~300 bytes)
        - ts_{estilo}_alignment JSONB (~150 bytes)
        - ts_{estilo}_positioning JSONB (~200 bytes)
        - ts_{estilo}_animation JSONB (~500 bytes)
        - ts_{estilo}_shadow JSONB (~200 bytes)
        - ts_{estilo}_background JSONB (~300 bytes)
        
        🚨 FAIL LOUD - SEM FALLBACK:
        1. SELECT ts_* FROM video_editing_templates
        2. Se NENHUM estilo tem dados → ERRO IMEDIATO
        3. Se OK → Monta dict com estilos a partir das colunas
        
        Args:
            template_id: UUID do template
            
        Returns:
            Dict com estrutura:
            {
                "default": {font_config, borders, highlight, alignment, positioning, animation_config, shadow, background},
                "emphasis": {...} ou None,
                "letter_effect": {...} ou None,
                "cartela": {...} ou None
            }
            
        Raises:
            ValueError: Se template_id inválido, template não encontrado,
                       ou NENHUM estilo está configurado
        """
        if not template_id:
            raise ValueError("template_id não pode ser vazio")
        
        # Validar UUID
        try:
            uuid.UUID(str(template_id))
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"template_id inválido: {template_id} - {e}")
        
        logger.info(f"🔍 [TemplateLoader] Carregando text_styles das COLUNAS ts_* para template {template_id}")
        
        db_conn = None
        db_cursor = None
        
        try:
            # Obter conexão
            if self.db_connection_func:
                db_conn = self.db_connection_func()
            else:
                from app.supabase_client import get_direct_db_connection
                db_conn = get_direct_db_connection()
            
            db_cursor = db_conn.cursor(cursor_factory=RealDictCursor)
            
            # Buscar TODAS as colunas ts_* de uma vez (32 colunas)
            db_cursor.execute("""
                SELECT 
                    name,
                    -- DEFAULT (9 colunas)
                    ts_default_font, ts_default_borders, ts_default_highlight,
                    ts_default_alignment, ts_default_positioning, ts_default_animation,
                    ts_default_shadow, ts_default_background, ts_default_cartela,
                    -- EMPHASIS (9 colunas)
                    ts_emphasis_font, ts_emphasis_borders, ts_emphasis_highlight,
                    ts_emphasis_alignment, ts_emphasis_positioning, ts_emphasis_animation,
                    ts_emphasis_shadow, ts_emphasis_background, ts_emphasis_cartela,
                    -- LETTER_EFFECT (9 colunas)
                    ts_letter_effect_font, ts_letter_effect_borders, ts_letter_effect_highlight,
                    ts_letter_effect_alignment, ts_letter_effect_positioning, ts_letter_effect_animation,
                    ts_letter_effect_shadow, ts_letter_effect_background, ts_letter_effect_cartela,
                    -- CARTELA (8 colunas)
                    ts_cartela_font, ts_cartela_borders, ts_cartela_highlight,
                    ts_cartela_alignment, ts_cartela_positioning, ts_cartela_animation,
                    ts_cartela_shadow, ts_cartela_background
                FROM video_editing_templates 
                WHERE id = %s
            """, (str(template_id),))
            
            row = db_cursor.fetchone()
            
            if not row:
                logger.error(f"❌ [TemplateLoader] Template {template_id} NÃO ENCONTRADO no banco!")
                raise ValueError(f"Template {template_id} não encontrado")
            
            # RealDictCursor retorna dict
            template_name = row['name'] if isinstance(row, dict) else row[0]
            
            # Helper para montar estilo a partir das colunas
            def build_style(prefix: str) -> Optional[Dict[str, Any]]:
                """Monta objeto de estilo a partir das colunas ts_{prefix}_*"""
                font = row.get(f'ts_{prefix}_font') if isinstance(row, dict) else None
                borders = row.get(f'ts_{prefix}_borders') if isinstance(row, dict) else None
                highlight = row.get(f'ts_{prefix}_highlight') if isinstance(row, dict) else None
                alignment = row.get(f'ts_{prefix}_alignment') if isinstance(row, dict) else None
                positioning = row.get(f'ts_{prefix}_positioning') if isinstance(row, dict) else None
                animation = row.get(f'ts_{prefix}_animation') if isinstance(row, dict) else None
                shadow = row.get(f'ts_{prefix}_shadow') if isinstance(row, dict) else None
                background = row.get(f'ts_{prefix}_background') if isinstance(row, dict) else None
                # 🆕 Cartela config (fundo visual para frases deste estilo)
                cartela = row.get(f'ts_{prefix}_cartela') if isinstance(row, dict) else None
                
                # Se TODAS as colunas estão vazias, retornar None
                if not any([font, borders, highlight, alignment, positioning, animation, shadow, background]):
                    return None
                
                # 🆕 Extrair padding do font_config (se existir)
                # O padding fica em ts_*_font.padding_x_percent e ts_*_font.padding_y_percent
                font_data = font or {}
                padding_config = {
                    'unit': 'percent',
                    'x_percent': font_data.get('padding_x_percent', 20),
                    'y_percent': font_data.get('padding_y_percent', 20)
                }
                
                # Montar objeto no formato esperado pelo PngGeneratorService
                style_obj = {
                    'font_config': font_data,
                    'borders': borders or [],
                    'highlight': highlight or {},
                    'alignment': alignment or {},
                    'positioning': positioning or {},
                    'animation_config': animation or {},
                    'shadow': shadow or {},
                    'background': background or {},
                    'padding': padding_config,  # 🆕 Padding extraído do font_config
                    # Determinar png_mode baseado no estilo
                    'png_mode': 'per_letter' if prefix == 'letter_effect' else 'per_word'
                }
                
                # 🆕 Adicionar cartela_config se configurada
                # 🔍 DEBUG v2.9.37: Verificar valor de cartela
                logger.info(f"   🔍 {prefix}: ts_{prefix}_cartela = {type(cartela).__name__}, enabled={cartela.get('enabled') if isinstance(cartela, dict) else 'N/A'}")
                if cartela and isinstance(cartela, dict) and cartela.get('enabled'):
                    style_obj['cartela_config'] = cartela
                    logger.info(f"   ✅ {prefix}: cartela_config ADICIONADA (type={cartela.get('type')})")
                elif cartela and isinstance(cartela, dict):
                    logger.info(f"   ⚠️ {prefix}: cartela existe mas enabled={cartela.get('enabled')} (NÃO adicionada)")
                else:
                    logger.info(f"   ❌ {prefix}: sem cartela configurada")
                
                return style_obj
            
            # Construir resultado
            result = {
                "default": build_style('default'),
                "emphasis": build_style('emphasis'),
                "letter_effect": build_style('letter_effect'),
                "cartela": build_style('cartela')
            }
            
            # Contar estilos configurados
            styles_count = sum(1 for v in result.values() if v is not None)
            
            # 🚨 FAIL LOUD: Se NENHUM estilo configurado, ERRO IMEDIATO!
            if styles_count == 0:
                logger.error(f"❌ [TemplateLoader] Template '{template_name}' ({template_id}) SEM text_styles!")
                logger.error(f"❌ TODAS as colunas ts_* estão VAZIAS!")
                logger.error(f"❌ BUG: Generator-V3 não salvou os estilos de texto nas colunas ts_*!")
                raise ValueError(
                    f"Template '{template_name}' ({template_id}) não tem NENHUM estilo de texto configurado. "
                    f"Verifique se o Generator-V3 está salvando nas colunas ts_*."
                )
            
            # ✅ Sucesso: Log detalhado
            logger.info(f"✅ [TemplateLoader] text_styles carregados das COLUNAS ts_*")
            logger.info(f"   • Template: {template_name}")
            logger.info(f"   • Estilos encontrados: {styles_count}/4")
            
            for style_tag, config in result.items():
                if config:
                    has_font = bool(config.get('font_config'))
                    has_borders = bool(config.get('borders'))
                    has_highlight = bool(config.get('highlight'))
                    has_animation = bool(config.get('animation_config'))
                    logger.info(f"   • {style_tag}: ✅ font={has_font} borders={has_borders} highlight={has_highlight} anim={has_animation}")
                else:
                    logger.info(f"   • {style_tag}: ❌ não configurado")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ [TemplateLoader] Erro ao carregar multi_text_styling: {e}")
            raise
        finally:
            if db_cursor:
                db_cursor.close()
            if db_conn:
                db_conn.close()
    
    def load_animation_config(self, template_id: str) -> Dict[str, Any]:
        """
        🆕 Carrega configurações de animação das COLUNAS DEDICADAS (step 11).
        
        Colunas:
        - stagger_and_opacity JSONB: Timing/delays de entrada e controle de opacidade
        - multi_animations JSONB: Animações visuais de texto (in/out/middle)
        - asset_animations JSONB: Animações de outros assets (overlays, camera, etc.)
        - animation_preset VARCHAR: Nome do preset selecionado
        
        Args:
            template_id: UUID do template
            
        Returns:
            Dict com estrutura:
            {
                "stagger_and_opacity": {...},
                "multi_animations": {...},
                "asset_animations": {...},
                "animation_preset": "typewriter" | None
            }
        """
        if not template_id:
            logger.warning("⚠️ [TemplateLoader] template_id vazio para animation_config")
            return self._get_default_animation_config()
        
        # Validar UUID
        try:
            uuid.UUID(str(template_id))
        except (ValueError, AttributeError, TypeError) as e:
            logger.warning(f"⚠️ [TemplateLoader] template_id inválido para animation_config: {template_id}")
            return self._get_default_animation_config()
        
        logger.info(f"🎬 [TemplateLoader] Carregando animation_config para template {template_id}")
        
        db_conn = None
        db_cursor = None
        
        try:
            # Obter conexão
            if self.db_connection_func:
                db_conn = self.db_connection_func()
            else:
                from app.supabase_client import get_direct_db_connection
                db_conn = get_direct_db_connection()
            
            db_cursor = db_conn.cursor(cursor_factory=RealDictCursor)
            
            # Buscar as 4 colunas de animação
            db_cursor.execute("""
                SELECT 
                    name,
                    stagger_and_opacity,
                    multi_animations,
                    asset_animations,
                    animation_preset
                FROM video_editing_templates 
                WHERE id = %s
            """, (str(template_id),))
            
            row = db_cursor.fetchone()
            
            if not row:
                logger.warning(f"⚠️ [TemplateLoader] Template {template_id} não encontrado para animation_config")
                return self._get_default_animation_config()
            
            # RealDictCursor retorna dict
            template_name = row['name'] if isinstance(row, dict) else row[0]
            stagger = row['stagger_and_opacity'] if isinstance(row, dict) else row[1]
            multi_anim = row['multi_animations'] if isinstance(row, dict) else row[2]
            asset_anim = row['asset_animations'] if isinstance(row, dict) else row[3]
            preset = row['animation_preset'] if isinstance(row, dict) else row[4]
            
            # Verificar se tem alguma config
            has_stagger = bool(stagger)
            has_multi = bool(multi_anim)
            has_asset = bool(asset_anim)
            has_preset = bool(preset)
            
            result = {
                "stagger_and_opacity": stagger or {},
                "multi_animations": multi_anim or {},
                "asset_animations": asset_anim or {},
                "animation_preset": preset
            }
            
            # Log detalhado
            logger.info(f"✅ [TemplateLoader] animation_config carregado das COLUNAS")
            logger.info(f"   • Template: {template_name}")
            logger.info(f"   • stagger_and_opacity: {'✅' if has_stagger else '❌'}")
            logger.info(f"   • multi_animations: {'✅' if has_multi else '❌'}")
            logger.info(f"   • asset_animations: {'✅' if has_asset else '❌'}")
            logger.info(f"   • animation_preset: {preset or 'N/A'}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ [TemplateLoader] Erro ao carregar animation_config: {e}")
            return self._get_default_animation_config()
        finally:
            if db_cursor:
                db_cursor.close()
            if db_conn:
                db_conn.close()
    
    def _get_default_animation_config(self) -> Dict[str, Any]:
        """Retorna config de animação padrão (instant, sem delays)."""
        return {
            "stagger_and_opacity": {
                "enabled": False,
                "stagger_config": {"delay_ms": 0}
            },
            "multi_animations": {},
            "asset_animations": {},
            "animation_preset": None
        }
    
    def load_template(self, template_id: str) -> Dict[str, Any]:
        """
        Carrega configuração completa de um template.
        
        Fluxo:
        1. Tentar carregar das novas tabelas (template_configs + template_text_styles)
        2. Se não encontrar, usar coluna params (retrocompatibilidade)
        3. Merge dos dados
        4. Retornar config completo
        
        Args:
            template_id: UUID do template
            
        Returns:
            Dict com configuração completa do template
        """
        logger.info(f"🚀 [TemplateLoader.load_template] INÍCIO - template_id={template_id}")
        
        if not template_id:
            logger.warning("⚠️ [TemplateLoader.load_template] template_id vazio")
            return {}
        
        # Validar UUID
        try:
            uuid.UUID(str(template_id))
        except (ValueError, AttributeError, TypeError) as e:
            logger.error(f"❌ [TemplateLoader.load_template] template_id inválido: {template_id} - {e}")
            return {}
        
        logger.info(f"🔍 [TemplateLoader.load_template] Carregando template {template_id}")
        
        db_conn = None
        db_cursor = None
        
        try:
            # Obter conexão
            if self.db_connection_func:
                db_conn = self.db_connection_func()
            else:
                from app.supabase_client import get_direct_db_connection
                db_conn = get_direct_db_connection()
            
            db_cursor = db_conn.cursor(cursor_factory=RealDictCursor)
            
            # ==================================================
            # PASSO 1: Buscar params antigo (retrocompatibilidade)
            # ==================================================
            legacy_params = self._load_legacy_params(db_cursor, template_id)
            
            # ==================================================
            # PASSO 2: Buscar das novas tabelas
            # ==================================================
            new_configs = self._load_modular_configs(db_cursor, template_id)
            text_styles = self._load_text_styles(db_cursor, template_id)
            
            # ==================================================
            # PASSO 3: Merge (novas tabelas têm prioridade)
            # ==================================================
            final_config = self._merge_configs(legacy_params, new_configs, text_styles)
            
            # Adicionar template_id ao config
            final_config['template_id'] = template_id
            
            logger.info(f"✅ [TemplateLoader] Template carregado: {len(final_config)} keys")
            logger.info(f"   - Legacy params: {len(legacy_params)} keys")
            logger.info(f"   - New configs: {len(new_configs)} schemas")
            logger.info(f"   - Text styles: {len(text_styles)} styles")
            
            return final_config
            
        except Exception as e:
            logger.error(f"❌ [TemplateLoader] Erro ao carregar template: {type(e).__name__}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
            
        finally:
            if db_cursor:
                try:
                    db_cursor.close()
                except:
                    pass
            if db_conn:
                try:
                    db_conn.close()
                except:
                    pass
    
    def _load_legacy_params(self, cursor, template_id: str) -> Dict[str, Any]:
        """
        ✅ Carrega colunas básicas dedicadas do template.
        
        Colunas carregadas:
        - base_layer (para text_video solid/gradient)
        - project_settings
        - template_mode
        - project_type
        - layout_spacing
        
        Args:
            cursor: Cursor do banco
            template_id: UUID do template
            
        Returns:
            Dict com configurações básicas ou {}
        """
        try:
            logger.info(f"✅ [TemplateLoader] Carregando colunas básicas dedicadas para template_id={template_id}")
            
            # Carregar colunas básicas dedicadas
            cursor.execute("""
                SELECT base_layer, project_settings, template_mode, project_type, layout_spacing, creative_layout, cartela_presets, phrase_classification, matting, multi_text_styling
                FROM video_editing_templates 
                WHERE id = %s
            """, (template_id,))
            
            row = cursor.fetchone()
            
            if not row:
                logger.warning(f"⚠️ [TemplateLoader] Template {template_id} NÃO encontrado")
                return {}
            
            # Inicializar resultado
            result = {}
            
            # ===== EXTRAIR COLUNAS BÁSICAS DEDICADAS =====
            if isinstance(row, dict):
                # base_layer (CRÍTICO para text_video mode!)
                if row.get('base_layer'):
                    result['base-layer'] = row['base_layer']
                    result['base_layer'] = row['base_layer']  # Ambas notações
                    logger.info(f"   • base_layer: ✅ Carregado da coluna dedicada")
                
                # project_settings
                if row.get('project_settings'):
                    result['project-settings'] = row['project_settings']
                    logger.info(f"   • project_settings: ✅ Carregado da coluna dedicada")
                
                # template_mode
                if row.get('template_mode'):
                    result['template-mode'] = row['template_mode']
                    logger.info(f"   • template_mode: ✅ Carregado da coluna dedicada")
                
                # project_type
                if row.get('project_type'):
                    result['project-type'] = row['project_type']
                    logger.info(f"   • project_type: ✅ Carregado da coluna dedicada")
                
                # layout_spacing (espaçamento entre palavras e linhas)
                if row.get('layout_spacing'):
                    result['layout_spacing'] = row['layout_spacing']
                    logger.info(f"   • layout_spacing: ✅ Carregado da coluna dedicada")
                
                # creative_layout (tamanhos dinâmicos e shifts de linha)
                if row.get('creative_layout'):
                    result['creative_layout'] = row['creative_layout']
                    logger.info(f"   • creative_layout: ✅ Carregado da coluna dedicada")
                
                # cartela_presets (presets de cartela por frase)
                if row.get('cartela_presets'):
                    result['cartela_presets'] = row['cartela_presets']
                    logger.info(f"   • cartela_presets: ✅ Carregado da coluna dedicada")
                
                # phrase_classification (classificação de frases)
                if row.get('phrase_classification'):
                    result['phrase_classification'] = row['phrase_classification']
                    logger.info(f"   • phrase_classification: ✅ Carregado da coluna dedicada")
                
                # 🆕 matting (recorte de pessoa / v-matting)
                if row.get('matting'):
                    result['matting'] = row['matting']
                    # Log do border_effect se presente
                    border_effect = row['matting'].get('border_effect', {})
                    if border_effect.get('enabled', {}).get('value'):
                        border_type = border_effect.get('type', {}).get('value', 'solid')
                        logger.info(f"   • matting: ✅ Carregado (border_effect: {border_type})")
                    else:
                        logger.info(f"   • matting: ✅ Carregado da coluna dedicada")
                
                # 🆕 multi_text_styling (toggles de enabled para cada estilo)
                if row.get('multi_text_styling'):
                    result['multi-text-styling'] = row['multi_text_styling']
                    # Log dos estilos habilitados
                    text_styles = row['multi_text_styling'].get('text_styles', {})
                    enabled_styles = []
                    for style in ['default', 'emphasis', 'letter_effect']:
                        style_config = text_styles.get(style, {})
                        enabled_obj = style_config.get('enabled', {})
                        is_enabled = enabled_obj.get('value', True) if isinstance(enabled_obj, dict) else enabled_obj
                        if is_enabled:
                            enabled_styles.append(style)
                    logger.info(f"   • multi_text_styling: ✅ Estilos habilitados: {enabled_styles}")
            
            if result:
                logger.info(f"   • Total colunas carregadas: {len(result)}")
            
            return result
                
        except Exception as e:
            logger.error(f"❌ [TemplateLoader] Erro ao carregar colunas básicas: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
    
    def _load_modular_configs(self, cursor, template_id: str) -> Dict[str, Dict[str, Any]]:
        """
        ⚠️ DEPRECATED: Tabela template_configs pode ser removida no futuro.
        
        Configs agora ficam em colunas dedicadas na tabela principal.
        Este método existe para retrocompatibilidade.
        
        Args:
            cursor: Cursor do banco
            template_id: UUID do template
            
        Returns:
            Dict com {schema_name: config_dict} ou {} se tabela não existir
        """
        try:
            cursor.execute("""
                SELECT schema_name, config
                FROM template_configs
                WHERE template_id = %s
                ORDER BY schema_name
            """, (template_id,))
            
            rows = cursor.fetchall()
            
            configs = {}
            for row in rows:
                schema_name = row['schema_name'] if isinstance(row, dict) else row[0]
                config = row['config'] if isinstance(row, dict) else row[1]
                
                if config:
                    configs[schema_name] = config
                    logger.debug(f"  ✅ Schema '{schema_name}' carregado de template_configs")
            
            if configs:
                logger.warning(f"⚠️ [TemplateLoader] template_configs ainda tem {len(configs)} schemas - MIGRAR para colunas dedicadas!")
            
            return configs
            
        except Exception as e:
            # Se tabela não existe, retornar vazio silenciosamente
            if "does not exist" in str(e) or "template_configs" in str(e):
                logger.info(f"   • template_configs: tabela não existe (OK, arquitetura nova)")
                return {}
            logger.error(f"❌ [TemplateLoader] Erro ao carregar modular configs: {e}")
            return {}
    
    def _load_text_styles(self, cursor, template_id: str) -> Dict[str, Dict[str, Any]]:
        """
        ⚠️ DEPRECATED: Tabela template_text_styles foi REMOVIDA!
        
        Agora usa colunas ts_* diretamente em video_editing_templates.
        Este método existe para manter compatibilidade com load_template().
        Use load_multi_text_styling() para a nova arquitetura.
        
        Args:
            cursor: Cursor do banco
            template_id: UUID do template
            
        Returns:
            Dict vazio (dados agora vêm das colunas ts_*)
        """
        logger.info(f"⚠️ [TemplateLoader._load_text_styles] DEPRECATED - Tabela template_text_styles foi removida!")
        logger.info(f"   • Use load_multi_text_styling() para carregar das colunas ts_*")
        
        # Retornar vazio - os estilos serão carregados via load_multi_text_styling()
        return {}
    
    def _merge_configs(
        self, 
        legacy: Dict[str, Any], 
        new_configs: Dict[str, Dict[str, Any]],
        text_styles: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Faz merge entre configs antigos e novos.
        
        Prioridade:
        1. Novas tabelas (se existirem)
        2. Legacy params (fallback)
        
        Args:
            legacy: Params antigos da coluna JSONB
            new_configs: Configs das novas tabelas por schema
            text_styles: Estilos de texto das novas tabelas
            
        Returns:
            Dict merged
        """
        # Começar com legacy como base
        result = legacy.copy()
        
        # Sobrescrever com configs modulares (prioridade)
        for schema_name, config in new_configs.items():
            result[schema_name] = config
            logger.debug(f"  🔄 Schema '{schema_name}' sobrescrito por nova config")
        
        # Montar multi-text-styling com text_styles (retrocompatibilidade)
        if text_styles:
            if 'multi-text-styling' not in result:
                result['multi-text-styling'] = {}
            if 'text_styles' not in result['multi-text-styling']:
                result['multi-text-styling']['text_styles'] = {}
            
            for style_tag, style_config in text_styles.items():
                # 🆕 MERGE: Preservar 'enabled' do multi_text_styling, adicionar resto das colunas ts_*
                existing_style = result['multi-text-styling']['text_styles'].get(style_tag, {})
                
                # Mesclar: style_config (ts_*) sobrescreve, mas preserva 'enabled' do existing
                merged_style = {**style_config}  # Começar com dados das colunas ts_*
                
                # Preservar 'enabled' se existir no multi_text_styling original
                if 'enabled' in existing_style:
                    merged_style['enabled'] = existing_style['enabled']
                    logger.debug(f"  ✅ Preservado 'enabled' do estilo '{style_tag}': {existing_style['enabled']}")
                
                result['multi-text-styling']['text_styles'][style_tag] = merged_style
                logger.debug(f"  🔄 Text style '{style_tag}' mesclado em multi-text-styling")
            
            # 🆕 IMPORTANTE: Adicionar também em '_text_styles' para acesso direto
            # O PngGeneratorService espera os estilos em '_text_styles' (fonte única da tabela)
            result['_text_styles'] = text_styles
            logger.info(f"  ✅ [_merge_configs] Text styles adicionados em '_text_styles': {list(text_styles.keys())}")
        else:
            logger.warning(f"  ⚠️ [_merge_configs] text_styles está VAZIO - _text_styles NÃO será adicionado!")
        
        logger.info(f"  📦 [_merge_configs] Resultado final tem '_text_styles': {'_text_styles' in result}")
        
        return result
    
    def load_animation_config(self, template_id: str) -> Dict[str, Any]:
        """
        🆕 Carrega configuração de animações do template (step 11).
        
        Carrega das 3 colunas dedicadas:
        - stagger_and_opacity: Timing/delays/opacity
        - multi_animations: Animações visuais de texto
        - asset_animations: Animações de outros assets
        
        🚨 FAIL LOUD - SEM FALLBACK:
        Se TODAS as colunas estiverem vazias → ERRO (template sem config de animação)
        Se PELO MENOS UMA tem dados → OK (animações parciais)
        
        Args:
            template_id: UUID do template
            
        Returns:
            Dict com estrutura:
            {
                "stagger_and_opacity": {...} ou None,
                "multi_animations": {...} ou None,
                "asset_animations": {...} ou None,
                "has_any_config": True/False
            }
            
        Raises:
            ValueError: Se template_id inválido ou template não encontrado
        """
        if not template_id:
            raise ValueError("template_id não pode ser vazio")
        
        # Validar UUID
        try:
            uuid.UUID(str(template_id))
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"template_id inválido: {template_id} - {e}")
        
        logger.info(f"🎬 [TemplateLoader] Carregando animation_config para template {template_id}")
        
        db_conn = None
        db_cursor = None
        
        try:
            # Obter conexão
            if self.db_connection_func:
                db_conn = self.db_connection_func()
            else:
                from app.supabase_client import get_direct_db_connection
                db_conn = get_direct_db_connection()
            
            db_cursor = db_conn.cursor(cursor_factory=RealDictCursor)
            
            # Buscar as 3 colunas de animação
            db_cursor.execute("""
                SELECT 
                    name,
                    stagger_and_opacity, 
                    multi_animations, 
                    asset_animations
                FROM video_editing_templates 
                WHERE id = %s
            """, (str(template_id),))
            
            row = db_cursor.fetchone()
            
            if not row:
                logger.error(f"❌ [TemplateLoader] Template {template_id} NÃO ENCONTRADO no banco!")
                raise ValueError(f"Template {template_id} não encontrado")
            
            # RealDictCursor retorna dict
            template_name = row['name'] if isinstance(row, dict) else 'unknown'
            stagger_and_opacity = row['stagger_and_opacity'] if isinstance(row, dict) else row[1]
            multi_animations = row['multi_animations'] if isinstance(row, dict) else row[2]
            asset_animations = row['asset_animations'] if isinstance(row, dict) else row[3]
            
            # Construir resultado
            result = {
                "stagger_and_opacity": stagger_and_opacity,
                "multi_animations": multi_animations,
                "asset_animations": asset_animations,
                "has_any_config": bool(stagger_and_opacity or multi_animations or asset_animations)
            }
            
            # Log detalhado
            logger.info(f"✅ [TemplateLoader] animation_config carregado para '{template_name}'")
            
            if stagger_and_opacity:
                enabled = stagger_and_opacity.get('enabled', {}).get('value', False) if isinstance(stagger_and_opacity.get('enabled'), dict) else stagger_and_opacity.get('enabled', False)
                logger.info(f"   • stagger_and_opacity: ✅ ~{len(str(stagger_and_opacity))} bytes (enabled={enabled})")
            else:
                logger.info(f"   • stagger_and_opacity: ❌ não configurado")
            
            if multi_animations:
                enabled = multi_animations.get('enabled', {}).get('value', False) if isinstance(multi_animations.get('enabled'), dict) else multi_animations.get('enabled', False)
                logger.info(f"   • multi_animations: ✅ ~{len(str(multi_animations))} bytes (enabled={enabled})")
            else:
                logger.info(f"   • multi_animations: ❌ não configurado")
            
            if asset_animations:
                enabled = asset_animations.get('enabled', {}).get('value', False) if isinstance(asset_animations.get('enabled'), dict) else asset_animations.get('enabled', False)
                logger.info(f"   • asset_animations: ✅ ~{len(str(asset_animations))} bytes (enabled={enabled})")
            else:
                logger.info(f"   • asset_animations: ❌ não configurado")
            
            if not result['has_any_config']:
                logger.warning(f"⚠️ [TemplateLoader] Template '{template_name}' não tem NENHUMA configuração de animação!")
                logger.warning(f"⚠️ Animações serão desabilitadas (aparição instantânea)")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ [TemplateLoader] Erro ao carregar animation_config: {e}")
            raise
        finally:
            if db_cursor:
                db_cursor.close()
            if db_conn:
                db_conn.close()

    # =========================================================================
    # 🔧 OVERRIDES: Frontend pode sobrescrever configurações do template
    # =========================================================================
    
    def load_template_with_overrides(
        self,
        template_id: str,
        overrides: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Carrega template e aplica overrides do frontend.
        
        Esta função permite que o chatbot ou Generator V3 sobrescrevam
        configurações específicas do template para uma renderização.
        
        Prioridade: DEFAULTS < TEMPLATE < OVERRIDES
        
        Args:
            template_id: UUID do template base
            overrides: Dict com configurações a sobrescrever
                Formato: {
                    "base-layer": { "storytelling_mode": "vlog", ... },
                    "project-settings": { "video_settings": { ... } },
                    ...
                }
        
        Returns:
            Dict com configuração merged (template + overrides)
        
        Example:
            # Chatbot coletou storytelling_mode=vlog, mas template tem solid
            overrides = {
                "base-layer": {
                    "storytelling_mode": "vlog",
                    "base_type": "vlog"
                }
            }
            config = loader.load_template_with_overrides(template_id, overrides)
            # config terá base_type=vlog, não solid
        """
        logger.info(f"🔧 [TemplateLoader] Carregando template com overrides")
        logger.info(f"   • template_id: {template_id}")
        logger.info(f"   • overrides keys: {list(overrides.keys()) if overrides else 'None'}")
        
        # 1. Carregar template base
        template_config = self.load_template(template_id)
        
        if not overrides:
            logger.info(f"   • Sem overrides - usando template como está")
            return template_config
        
        # 2. Aplicar overrides (deep merge)
        merged = self._deep_merge(template_config, overrides)
        
        # 3. Aplicar regras de implicação
        merged = self._apply_implication_rules(merged)
        
        logger.info(f"✅ [TemplateLoader] Template merged com overrides")
        
        return merged
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        Faz merge profundo de dois dicts.
        Override tem prioridade sobre base.
        
        Args:
            base: Dict base (template)
            override: Dict com valores a sobrescrever
            
        Returns:
            Dict merged
        """
        if not base:
            return override or {}
        if not override:
            return base
        
        result = dict(base)  # Cópia shallow
        
        for key, value in override.items():
            # Normalizar key (base-layer → base_layer)
            normalized_key = key.replace("-", "_")
            
            # Se valor é dict e base também tem dict, merge recursivo
            if isinstance(value, dict):
                if normalized_key in result and isinstance(result[normalized_key], dict):
                    result[normalized_key] = self._deep_merge(result[normalized_key], value)
                else:
                    result[normalized_key] = value
            else:
                result[normalized_key] = value
        
        return result
    
    def _apply_implication_rules(self, config: Dict) -> Dict:
        """
        Aplica regras de implicação após merge.
        
        Exemplo: Se storytelling_mode=vlog, então base_type deve ser vlog.
        
        Args:
            config: Dict de configuração após merge
            
        Returns:
            Dict com implicações aplicadas
        """
        # Regras de implicação (storytelling_mode → base_type)
        IMPLICATION_RULES = {
            "vlog": {"base_type": "vlog"},
            "text_video": {"base_type": "solid"},
            "voice_over": {"base_type": "solid"},
            "music": {"base_type": "solid"},
            "lyric_video": {"base_type": "solid"},
            "mixed": None  # Não implica nada específico
        }
        
        # Obter storytelling_mode
        base_layer = config.get("base_layer") or config.get("base-layer", {})
        storytelling_mode = None
        
        if isinstance(base_layer, dict):
            sm_value = base_layer.get("storytelling_mode")
            # Pode vir como {"value": "vlog"} ou "vlog"
            if isinstance(sm_value, dict) and "value" in sm_value:
                storytelling_mode = sm_value["value"]
            else:
                storytelling_mode = sm_value
        
        if not storytelling_mode:
            return config
        
        # Verificar se há implicação
        implication = IMPLICATION_RULES.get(storytelling_mode)
        if not implication:
            return config
        
        # Aplicar implicação (apenas se base_type não foi explicitamente setado)
        # Para saber se foi explicitamente setado, verificamos se está diferente do default
        current_base_type = None
        if isinstance(base_layer, dict):
            bt_value = base_layer.get("base_type")
            if isinstance(bt_value, dict) and "value" in bt_value:
                current_base_type = bt_value["value"]
            else:
                current_base_type = bt_value
        
        implied_base_type = implication.get("base_type")
        
        # Se storytelling_mode=vlog mas base_type está diferente de vlog, corrigir
        if implied_base_type and current_base_type != implied_base_type:
            logger.info(f"🔄 [IMPLICATION] storytelling_mode={storytelling_mode} → base_type={implied_base_type}")
            
            # Garantir que base_layer existe
            if "base_layer" not in config:
                config["base_layer"] = {}
            
            # Setar base_type
            config["base_layer"]["base_type"] = {"value": implied_base_type}
            
            # 🔧 FIX: Sincronizar "base-layer" (dash) com "base_layer" (underscore)
            # Após o _deep_merge, as duas chaves podem apontar para objetos diferentes.
            # Vários consumers (ex: subtitle_pipeline_service) leem "base-layer" primeiro.
            config["base-layer"] = config["base_layer"]
        
        return config

