"""
🎯 Phrase Classifier Service - Classificação de frases via LLM

Classifica cada frase do fraseamento em um dos tipos de estilo:
- default ⭐: Legenda padrão
- emphasis ⭐⭐: Destaque visual (fundo colorido, fonte maior)
- letter_effect ⭐⭐⭐: Animação letra por letra (máximo impacto)

Usa ai_config para gerenciamento centralizado de API keys.
Service key: 'phrase_classifier'
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Service key no ai_config (com fallbacks)
AI_CONFIG_SERVICE_KEYS = [
    'phrase_classifier',  # Específico (se configurado)
    'content_planner',    # Fallback comum
    'chat_assistant',     # Fallback genérico
    'chatbot',            # Outro fallback
]

# Tipos de estilo válidos
VALID_STYLE_TYPES = ['default', 'emphasis', 'letter_effect']


def _get_llm_config() -> Optional[Dict[str, Any]]:
    """
    Busca configuração da LLM via ai_config (centralizado).
    
    Tenta múltiplos service_keys em ordem de prioridade:
    1. phrase_classifier (específico)
    2. content_planner (comum)
    3. chat_assistant (genérico)
    4. chatbot (fallback)
    
    Qualquer serviço OpenAI serve - só precisamos da API key.
    """
    try:
        from app.ai_config import get_ai_config
        
        for service_key in AI_CONFIG_SERVICE_KEYS:
            config = get_ai_config(service_key)
            if config and config.get('api_key'):
                provider = config.get('provider', {}).get('name', 'unknown')
                if provider in ['openai', 'anthropic']:
                    logger.info(f"✅ Phrase classifier usando config de '{service_key}' ({provider})")
                    return config
        
        logger.debug(f"⚠️ Nenhum serviço OpenAI/Anthropic encontrado - usando heurísticas")
        return None
        
    except Exception as e:
        logger.debug(f"⚠️ Erro ao buscar ai_config: {e}")
        return None


# =============================================================================
# PROMPT DINÂMICO PARA CLASSIFICAÇÃO DE FRASES
# Gerado dinamicamente baseado nos tipos e cartelas habilitados
# =============================================================================

def _build_classification_prompt(
    enabled_types: List[str],
    cartela_enabled: Dict[str, bool],
    custom_instructions: str,
    phrases_text: str,
    with_regrouping: bool = True,
    total_phrases: int = 0,
    matting_enabled: bool = False,
    feature_blocks_enabled: bool = False
) -> str:
    """
    Constrói o prompt de classificação dinamicamente.
    
    Args:
        enabled_types: Lista de tipos habilitados ['default', 'emphasis', 'letter_effect']
        cartela_enabled: Dict indicando se cartela está habilitada por tipo
        custom_instructions: Instruções customizadas do usuário
        phrases_text: Texto das frases formatado
        with_regrouping: Se deve incluir seção de reagrupamento
        total_phrases: Total de frases para calcular porcentagens
        matting_enabled: Se recorte de pessoa (v-matting) está habilitado
        feature_blocks_enabled: Se deve gerar blocos de features (grupos de frases para cartela/matting)
    """
    # Descrição dos tipos
    type_descriptions = {
        'default': """**default** ⭐ - Frases normais, declarativas, informativas
   - Frases longas, explicativas
   - Transições suaves
   - Base do vídeo (50-60% do conteúdo)""",
        'emphasis': """**emphasis** ⭐⭐ - Frases importantes que merecem destaque
   - Perguntas retóricas ("né?", "E aí?", "curtiram?")
   - Afirmações enfáticas com "!"
   - Palavras-chave e conceitos importantes
   - Mudanças de assunto ou tom
   - Use em 25-35% das frases (MÍNIMO 20%)""",
        'letter_effect': """**letter_effect** ⭐⭐⭐ - Frases de MÁXIMO impacto visual
   - Frases curtas (1-4 palavras) com impacto
   - CTAs: "Fui!", "Valeu!", "Bora!", "E aí?"
   - Saudações: "Fala galera!", "Beleza?"
   - Palavras únicas de impacto: "INCRÍVEL", "Olha isso!"
   - Use em 10-20% das frases (MÍNIMO 10%)"""
    }
    
    # Montar lista de tipos disponíveis
    types_section = "TIPOS DISPONÍVEIS (você DEVE usar TODOS estes tipos):\n\n"
    for i, t in enumerate(enabled_types, 1):
        if t in type_descriptions:
            types_section += f"{i}. {type_descriptions[t]}\n\n"
    
    # Calcular mínimos absolutos
    min_emphasis = max(3, int(total_phrases * 0.20)) if 'emphasis' in enabled_types else 0
    min_letter_effect = max(2, int(total_phrases * 0.10)) if 'letter_effect' in enabled_types else 0
    
    # 🔧 v2.5.8: Regras obrigatórias CLARAS sobre tipos habilitados/desabilitados
    mandatory_rules = []
    
    # Determinar quais tipos NÃO estão habilitados
    all_types = ['default', 'emphasis', 'letter_effect']
    disabled_types = [t for t in all_types if t not in enabled_types]
    
    # REGRA CRÍTICA: Informar claramente quais tipos NÃO podem ser usados
    if disabled_types:
        mandatory_rules.append(f"⛔ PROIBIDO: NÃO use os tipos {disabled_types} - eles estão DESABILITADOS!")
        mandatory_rules.append(f"✅ SOMENTE USE os tipos: {enabled_types}")
    
    # Regras de quantidade para cada tipo habilitado
    if 'emphasis' in enabled_types:
        mandatory_rules.append(f"- OBRIGATÓRIO: Use 'emphasis' em PELO MENOS {min_emphasis} frases (você tem {total_phrases} frases)")
    if 'letter_effect' in enabled_types:
        mandatory_rules.append(f"- OBRIGATÓRIO: Use 'letter_effect' em PELO MENOS {min_letter_effect} frases")
    
    # Regra especial para quando só 1 tipo está habilitado
    if len(enabled_types) == 1:
        mandatory_rules.append(f"- Apenas '{enabled_types[0]}' está habilitado, classifique TODAS as frases como '{enabled_types[0]}'")
    else:
        # Só adicionar regra de distribuição de default se default estiver habilitado
        if 'default' in enabled_types:
            mandatory_rules.append("- IMPORTANTE: 'default' deve ser 50-60% do vídeo, NÃO MAIS!")
        mandatory_rules.append("- DIVERSIFIQUE: Distribua os tipos ao longo do vídeo, não apenas no final")
    
    # Seção de cartela MELHORADA
    cartela_section = ""
    types_with_cartela = [t for t in enabled_types if cartela_enabled.get(t, False)]
    if types_with_cartela:
        min_cartela = max(3, int(total_phrases * 0.15))  # 15% mínimo com cartela
        cartela_section = f"""
CARTELA (fundo visual colorido atrás do texto):
✅ TIPOS COM CARTELA HABILITADA: {', '.join(types_with_cartela)}

REGRAS DE CARTELA (MUITO IMPORTANTE):
1. Use "use_cartela": true em PELO MENOS {min_cartela} frases ({int(min_cartela/total_phrases*100) if total_phrases > 0 else 15}% do vídeo)
2. AGRUPE frases consecutivas com cartela - evite usar cartela em frases isoladas
   - BOM: frases 5,6,7 todas com use_cartela=true (formam um bloco de ~4+ segundos)
   - RUIM: frase 5 com cartela, frase 8 com cartela (isoladas)
3. Cada "bloco de cartela" deve cobrir PELO MENOS 3-4 frases consecutivas
4. A cartela deve cobrir aproximadamente 30-50% do tempo total do vídeo
5. Distribua os blocos de cartela ao longo do vídeo (início, meio, fim)

EXEMPLO DE BOM USO DE CARTELA:
- Frases 0-3: use_cartela=true (abertura com impacto)
- Frases 4-8: use_cartela=false (respiro)
- Frases 12-15: use_cartela=true (momento importante)
- Frases 16-20: use_cartela=false (respiro)
- Últimas 3-4 frases: use_cartela=true (fechamento)
"""
    
    # Seção de matting (recorte de pessoa)
    matting_section = ""
    if matting_enabled:
        min_matting = max(2, int(total_phrases * 0.10))  # 10% mínimo com matting
        
        # Detectar se só tem 1 tipo habilitado (ex: só 'default')
        # Nesse cenário, cartela + matting é a ÚNICA fonte de variedade visual
        only_one_type = len(enabled_types) == 1
        
        if only_one_type and types_with_cartela:
            # REGRA ESPECIAL: Quando só 1 tipo de texto está habilitado,
            # cartela e matting DEVEM andar juntos (são a única variedade visual)
            matting_section = f"""
MATTING (recorte de pessoa sobre cartela/background):
✅ RECORTE DE PESSOA HABILITADO

O matting remove o fundo do vídeo e mostra a pessoa recortada sobre as cartelas.
É um efeito visual de alto impacto.

⚠️ REGRA CRÍTICA — APENAS O ESTILO '{enabled_types[0]}' ESTÁ HABILITADO:
Como não há variedade de estilos de texto, a cartela + matting são a ÚNICA
fonte de variedade visual no vídeo. Por isso:

1. CARTELA E MATTING DEVEM SEMPRE ANDAR JUNTOS (OBRIGATÓRIO!)
   - Se uma frase tem use_cartela=true, DEVE ter use_matting=true também
   - Se uma frase tem use_matting=true, DEVE ter use_cartela=true também
   - NUNCA use cartela sem matting e vice-versa
2. Use "use_cartela": true + "use_matting": true em PELO MENOS {min_matting} frases
3. AGRUPE frases consecutivas com cartela+matting (blocos de 3-4 frases)
4. Alterne entre blocos COM cartela+matting e blocos SEM (ritmo visual)
5. Distribua ao longo do vídeo: abertura, meio e fechamento

EXEMPLO (OBRIGATÓRIO seguir este padrão):
- Frases 0-2: use_cartela=true, use_matting=true (abertura impactante)
- Frases 3-5: use_cartela=false, use_matting=false (respiro, texto normal)
- Frases 6-8: use_cartela=true, use_matting=true (momento de destaque)
- Últimas 2-3: use_cartela=true, use_matting=true (fechamento com impacto)
"""
        else:
            matting_section = f"""
MATTING (recorte de pessoa sobre cartela/background):
✅ RECORTE DE PESSOA HABILITADO

O matting remove o fundo do vídeo e mostra a pessoa recortada sobre as cartelas.
É um efeito visual de alto impacto, usado para momentos importantes.

REGRAS DE MATTING (MUITO IMPORTANTE):
1. Use "use_matting": true em PELO MENOS {min_matting} frases ({int(min_matting/total_phrases*100) if total_phrases > 0 else 10}% do vídeo)
2. O matting DEVE sempre acompanhar a cartela - onde tem matting, TEM que ter cartela também
3. Use matting nos mesmos blocos de cartela para criar um efeito visual coeso
4. AGRUPE: Se as frases 5-8 têm cartela, use matting nas frases 6-7 (dentro do bloco)
5. Matting é ideal para: momentos de destaque, CTAs, fechamento do vídeo

EXEMPLO DE BOM USO DE MATTING:
- Frases 0-2: use_cartela=true, use_matting=true (abertura impactante)
- Frases 3-5: use_cartela=true, use_matting=false (só cartela, pessoa normal)
- Frases 10-12: use_cartela=true, use_matting=true (momento de destaque)
- Últimas 2-3 frases: use_cartela=true, use_matting=true (fechamento com impacto)
"""
    
    # 🆕 v2.9.16: Seção de feature_blocks (grupos de frases para cartela/matting)
    feature_blocks_section = ""
    has_any_feature = any(cartela_enabled.values()) or matting_enabled
    
    if feature_blocks_enabled and has_any_feature:
        # Calcular tamanhos ideais de blocos
        block_size_min = 2
        block_size_max = min(8, max(4, total_phrases // 4))  # 2-8 frases por bloco
        num_blocks = max(3, total_phrases // block_size_max)  # No mínimo 3 blocos
        
        feature_blocks_section = f"""
FEATURE BLOCKS (AGRUPAMENTO DE FRASES PARA CARTELA/MATTING):
✅ BLOCOS DE FEATURES HABILITADOS

O vídeo deve ser dividido em BLOCOS de {block_size_min}-{block_size_max} frases consecutivas.
Cada bloco representa um "momento visual" onde cartela e matting são ligados/desligados juntos.

REGRAS DE BLOCOS (MUITO IMPORTANTE):
1. Divida as {total_phrases} frases em aproximadamente {num_blocks} blocos
2. Cada bloco deve ter entre {block_size_min} e {block_size_max} frases
3. TODOS os elementos dentro de um bloco devem ter o MESMO valor de use_cartela e use_matting
4. Alterne entre blocos COM features (cartela/matting=true) e SEM features (=false)
5. Blocos de abertura e fechamento geralmente têm features ligadas
6. Blocos de "respiro" no meio podem ter features desligadas

FORMATO DE FEATURE_BLOCKS:
Retorne um array "feature_blocks" onde cada item define:
- start_index: índice da primeira frase do bloco (0-based)
- end_index: índice da última frase do bloco (inclusive)
- use_cartela: true/false para todo o bloco
- use_matting: true/false para todo o bloco
- reason: justificativa curta (ex: "abertura impactante", "respiro", "conclusão")

EXEMPLO:
"feature_blocks": [
  {{"start_index": 0, "end_index": 3, "use_cartela": true, "use_matting": true, "reason": "abertura impactante"}},
  {{"start_index": 4, "end_index": 9, "use_cartela": false, "use_matting": false, "reason": "desenvolvimento/respiro"}},
  {{"start_index": 10, "end_index": 14, "use_cartela": true, "use_matting": false, "reason": "destaque intermediário"}},
  {{"start_index": 15, "end_index": 18, "use_cartela": true, "use_matting": true, "reason": "fechamento com impacto"}}
]

IMPORTANTE: Os blocos devem cobrir TODAS as frases (0 até {total_phrases - 1}) sem lacunas!
"""
    
    # Instruções customizadas
    custom_section = ""
    if custom_instructions:
        custom_section = f"""
INSTRUÇÕES ESPECIAIS DO USUÁRIO (PRIORIDADE MÁXIMA):
{custom_instructions}
"""
    
    # Formato de resposta - incluir use_matting se matting estiver habilitado
    matting_field = ', "use_matting": true' if matting_enabled else ''
    matting_field_false = ', "use_matting": false' if matting_enabled else ''
    
    # 🆕 v2.9.16: Incluir feature_blocks no formato de resposta
    feature_blocks_example = ""
    if feature_blocks_enabled and has_any_feature:
        feature_blocks_example = """,
  "feature_blocks": [
    {"start_index": 0, "end_index": 3, "use_cartela": true, "use_matting": true, "reason": "abertura"},
    {"start_index": 4, "end_index": 7, "use_cartela": false, "use_matting": false, "reason": "respiro"}
  ]"""
    
    if with_regrouping:
        response_format = f"""
RESPONDA APENAS COM JSON VÁLIDO (sem markdown, sem ```, apenas o JSON):
{{
  "classifications": [
    {{"index": 0, "type": "emphasis", "reason": "saudacao de abertura", "use_cartela": true{matting_field}}},
    {{"index": 1, "type": "default", "reason": "explicacao", "use_cartela": true{matting_field_false}}},
    {{"index": 2, "type": "letter_effect", "reason": "palavra de impacto", "use_cartela": true{matting_field}}}
  ],
  "regroupings": []{feature_blocks_example}
}}

IMPORTANTE: Responda APENAS o JSON, sem explicações antes ou depois.
"""
    else:
        response_format = f"""
RESPONDA APENAS COM JSON VÁLIDO (sem markdown, sem ```, apenas o JSON):
[
  {{"index": 0, "type": "emphasis", "reason": "saudacao", "use_cartela": true{matting_field}}},
  {{"index": 1, "type": "default", "reason": "explicacao", "use_cartela": true{matting_field_false}}}
]
"""
    
    # Montar prompt final
    prompt = f"""Você é um classificador de frases para legendas de vídeo.

{types_section}

REGRAS OBRIGATÓRIAS:
{chr(10).join(mandatory_rules) if mandatory_rules else '- Distribua as classificações de forma equilibrada'}
- Considere o contexto e fluxo narrativo
- Nunca use "letter_effect" em frases longas (mais de 4 palavras)
{cartela_section}
{matting_section}
{feature_blocks_section}
{custom_section}
FRASES PARA CLASSIFICAR:
{phrases_text}
{response_format}"""
    
    return prompt


# Prompt legado (mantido para compatibilidade)
CLASSIFICATION_PROMPT_SIMPLE = """Você é um classificador de frases para legendas de vídeo.

Analise as frases abaixo e classifique cada uma em um dos 3 tipos:

1. **default** ⭐ - Frases normais, declarativas, informativas
2. **emphasis** ⭐⭐ - Frases importantes que merecem destaque  
3. **letter_effect** ⭐⭐⭐ - Frases de MÁXIMO impacto visual (1-3 palavras)

REGRAS:
- Use TODOS os tipos disponíveis
- "emphasis" em pelo menos 15% das frases
- "letter_effect" em pelo menos 5% das frases curtas
{custom_instructions}
FRASES PARA CLASSIFICAR:
{phrases_text}

RESPONDA APENAS COM JSON VÁLIDO no formato:
[
  {{"index": 0, "type": "default", "reason": "frase declarativa normal"}},
  {{"index": 1, "type": "emphasis", "reason": "pergunta retórica importante"}}
]
"""


class PhraseClassifierService:
    """
    Serviço de classificação de frases usando LLM.
    
    Fluxo:
    1. Recebe lista de frases do fraseamento
    2. Monta prompt com as frases
    3. Chama LLM para classificar
    4. Adiciona style_type em cada frase
    
    Configuração:
    1. ai_config (banco) - service_key: 'phrase_classifier' (RECOMENDADO)
    2. Fallback para regras simples baseadas em tamanho/pontuação
    """
    
    def __init__(self):
        self.config = _get_llm_config()
        self.is_configured = bool(self.config and self.config.get('api_key'))
        
        if self.is_configured:
            logger.info(f"✅ Phrase classifier configurado (provider: {self.config.get('provider', 'openai')})")
        else:
            logger.warning("⚠️ Phrase classifier não configurado - usando regras heurísticas")
    
    def is_available(self) -> bool:
        """Verifica se o serviço está disponível"""
        return self.is_configured
    
    def classify_phrases(
        self,
        phrases: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Classifica uma lista de frases.
        
        Args:
            phrases: Lista de frases do fraseamento
                [{"text": "...", "start_time": 0.5, "end_time": 1.2, ...}, ...]
            context: Contexto opcional contendo:
                - enabled_types: Lista de tipos habilitados ['default', 'emphasis', 'letter_effect']
                - template_id: ID do template para buscar tipos habilitados
                - custom_phrase_instructions: Instruções customizadas do usuário
            
        Returns:
            Lista de frases com style_type adicionado:
            [{"text": "...", "style_type": "default", "style_confidence": 0.9, ...}, ...]
        """
        if not phrases:
            return []
        
        # 🆕 Determinar tipos habilitados do contexto ou template
        enabled_types = self._get_enabled_types(context)
        
        # 🆕 Buscar instruções customizadas
        custom_instructions = self._get_custom_instructions(context)
        if custom_instructions:
            logger.info(f"📝 Instruções customizadas: {custom_instructions[:100]}...")
        
        # 🆕 Buscar cartela habilitada por tipo
        cartela_enabled = self._get_cartela_enabled(context)
        
        # 🆕 Buscar matting (recorte de pessoa) habilitado
        matting_enabled = self._get_matting_enabled(context)
        
        logger.info(f"🎯 Classificando {len(phrases)} frases... (tipos: {enabled_types}, cartela: {cartela_enabled}, matting: {matting_enabled})")
        
        # Tentar LLM se disponível
        if self.is_configured:
            try:
                result = self._classify_with_llm(
                    phrases, context, enabled_types, custom_instructions, cartela_enabled, matting_enabled
                )
                if result:
                    return self._filter_by_enabled_types(result, enabled_types)
            except Exception as e:
                logger.error(f"❌ Erro na classificação LLM: {e}")
                logger.info("⚠️ Usando fallback heurístico...")
        
        # Fallback: regras heurísticas
        result = self._classify_with_heuristics(phrases, enabled_types, custom_instructions, cartela_enabled, matting_enabled)
        
        # 🔍 v2.9.122: Debug - verificar person_overlay_enabled ANTES do filter
        matting_before = sum(1 for p in result if p.get('person_overlay_enabled', False))
        logger.info(f"🔍 [DEBUG] ANTES de _filter_by_enabled_types: {matting_before} frases com person_overlay_enabled")
        
        filtered_result = self._filter_by_enabled_types(result, enabled_types)
        
        # 🔍 v2.9.122: Debug - verificar person_overlay_enabled DEPOIS do filter
        matting_after = sum(1 for p in filtered_result if p.get('person_overlay_enabled', False))
        logger.info(f"🔍 [DEBUG] DEPOIS de _filter_by_enabled_types: {matting_after} frases com person_overlay_enabled")
        
        return filtered_result
    
    def _get_enabled_types(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Determina quais tipos de estilo estão habilitados.
        
        Busca do contexto ou carrega do template (via multi-text-styling).
        Se nenhum tipo específico estiver habilitado, retorna todos.
        """
        # Se passou explicitamente no contexto, usar isso
        if context and 'enabled_types' in context:
            return context['enabled_types']
        
        # Tentar buscar do template
        if context and 'template_id' in context and context['template_id']:
            try:
                enabled = self._load_enabled_types_from_template(context['template_id'])
                if enabled:
                    return enabled
            except Exception as e:
                logger.warning(f"⚠️ [ENABLED_TYPES] Falha ao carregar tipos do template {context['template_id'][:8]}...: {e}")
        
        # Default: todos os tipos habilitados
        # ⚠️ ATENÇÃO: Se chegou aqui sem enabled_types do contexto, pode ser problema de conexão DB
        logger.warning(f"⚠️ [ENABLED_TYPES] Usando fallback: TODOS os tipos habilitados (sem dados do template)")
        return ['default', 'emphasis', 'letter_effect']
    
    def _get_custom_instructions(self, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        Busca instruções customizadas do usuário.
        
        Pode vir do contexto diretamente ou do template (enhanced-phrase-rules).
        Exemplos de instruções:
        - "Sempre agrupe 'câmera fotográfica' junto"
        - "Palavras como INCRÍVEL devem ser letter_effect"
        - "Frases com números devem ser emphasis"
        """
        # Se passou explicitamente no contexto, usar isso
        if context and 'custom_phrase_instructions' in context:
            instructions = context['custom_phrase_instructions']
            if instructions and isinstance(instructions, str) and instructions.strip():
                return instructions.strip()
        
        # Tentar buscar do template
        if context and 'template_id' in context and context['template_id']:
            try:
                instructions = self._load_custom_instructions_from_template(context['template_id'])
                if instructions:
                    return instructions
            except Exception as e:
                logger.debug(f"⚠️ Não conseguiu carregar instruções do template: {e}")
        
        return None
    
    def _load_custom_instructions_from_template(self, template_id: str) -> Optional[str]:
        """
        Carrega as instruções customizadas do template (enhanced-phrase-rules.phrase_rules.custom_phrase_instructions).
        """
        try:
            from app.video_orchestrator.services.template_loader import TemplateLoaderService
            loader = TemplateLoaderService()
            template_config = loader.load_template(template_id)
            
            if not template_config:
                return None
            
            # Buscar em enhanced-phrase-rules
            epr = template_config.get('enhanced-phrase-rules', {})
            phrase_rules = epr.get('phrase_rules', {})
            custom_instructions = phrase_rules.get('custom_phrase_instructions', {})
            
            # Suporta formato { value: "..." } ou valor direto
            if isinstance(custom_instructions, dict):
                value = custom_instructions.get('value', '')
            else:
                value = custom_instructions if custom_instructions else ''
            
            if value and isinstance(value, str) and value.strip():
                logger.info(f"📝 [CUSTOM_INSTRUCTIONS] Template {template_id[:8]}...: {value[:50]}...")
                return value.strip()
            
            return None
            
        except Exception as e:
            logger.debug(f"⚠️ Erro ao carregar instruções do template: {e}")
            return None
    
    def _load_enabled_types_from_template(self, template_id: str) -> Optional[List[str]]:
        """
        Carrega os tipos habilitados do template (multi-text-styling.text_styles.X.enabled).
        """
        try:
            from app.video_orchestrator.services.template_loader import TemplateLoaderService
            loader = TemplateLoaderService()
            template_config = loader.load_template(template_id)
            
            if not template_config:
                return None
            
            # Buscar em multi-text-styling
            mts = template_config.get('multi-text-styling', {})
            text_styles = mts.get('text_styles', {})
            
            enabled = []
            for style_type in VALID_STYLE_TYPES:
                style_config = text_styles.get(style_type, {})
                enabled_config = style_config.get('enabled', {})
                
                # Suporta formato { value: true } ou valor direto
                if isinstance(enabled_config, dict):
                    is_enabled = enabled_config.get('value', True)
                else:
                    is_enabled = enabled_config if enabled_config is not None else True
                
                if is_enabled:
                    enabled.append(style_type)
            
            if enabled:
                logger.info(f"🎨 [ENABLED_TYPES] Template {template_id[:8]}...: {enabled}")
                return enabled
            
            # Se nenhum está habilitado explicitamente, retornar None
            # para usar o fallback padrão (todos habilitados)
            logger.warning(f"⚠️ [ENABLED_TYPES] Nenhum estilo habilitado no template {template_id[:8]}...")
            return None
            
        except Exception as e:
            logger.debug(f"⚠️ Erro ao carregar tipos do template: {e}")
            return None
    
    def _load_cartela_enabled_from_template(self, template_id: str) -> Dict[str, bool]:
        """
        Carrega quais tipos têm cartela habilitada (ts_<style>_cartela.enabled).
        
        Returns:
            Dict com {'default': True/False, 'emphasis': True/False, 'letter_effect': True/False}
        """
        result = {'default': False, 'emphasis': False, 'letter_effect': False}
        
        try:
            import json
            import os
            import psycopg2
            
            # 🔍 v2.9.38: Usar DB_REMOTE_URL (que é a URL do banco remoto usado em todo o sistema)
            database_url = os.environ.get('DB_REMOTE_URL', '')
            if not database_url:
                # Fallback para DIRECT_DATABASE_URL
                database_url = os.environ.get('DIRECT_DATABASE_URL', '')
            if not database_url:
                # Fallback para variáveis individuais
                db_host = os.environ.get('POSTGRES_HOST', 'localhost')
                db_port = os.environ.get('POSTGRES_PORT', '5432')
                db_name = os.environ.get('POSTGRES_DB', 'postgres')
                db_user = os.environ.get('POSTGRES_USER', 'postgres')
                db_pass = os.environ.get('POSTGRES_PASSWORD', '')
                database_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            
            # 🔍 DEBUG: Log da conexão
            logger.info(f"🔍 [CARTELA_ENABLED] Conectando ao banco para template {template_id[:8]}...")
            logger.debug(f"   DB URL prefix: {database_url[:30]}...")
            
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            # Buscar as colunas ts_*_cartela
            # IMPORTANTE: A tabela correta é video_editing_templates (não templates)
            cursor.execute("""
                SELECT ts_default_cartela, ts_emphasis_cartela, ts_letter_effect_cartela
                FROM video_editing_templates
                WHERE id = %s
            """, (template_id,))
            
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not row:
                logger.warning(f"⚠️ [CARTELA_ENABLED] Template {template_id[:8]}... NÃO encontrado no banco!")
                return result
            
            # 🔍 DEBUG: Log dos dados brutos
            logger.info(f"🔍 [CARTELA_ENABLED] Dados brutos do banco:")
            logger.info(f"   default: {type(row[0]).__name__} = {str(row[0])[:100] if row[0] else 'None'}")
            logger.info(f"   emphasis: {type(row[1]).__name__} = {str(row[1])[:100] if row[1] else 'None'}")
            logger.info(f"   letter_effect: {type(row[2]).__name__} = {str(row[2])[:100] if row[2] else 'None'}")
            
            # Mapear colunas para tipos
            columns = ['default', 'emphasis', 'letter_effect']
            for i, style_type in enumerate(columns):
                cartela_data = row[i]
                
                if cartela_data:
                    # Pode ser string JSON ou dict
                    if isinstance(cartela_data, str):
                        try:
                            cartela_data = json.loads(cartela_data)
                        except Exception as parse_err:
                            logger.warning(f"   ⚠️ Erro ao parsear JSON de {style_type}: {parse_err}")
                            continue
                    
                    # 🔧 v2.9.32: Verificar enabled - suportar formato {value: true} ou direto
                    enabled_raw = cartela_data.get('enabled', False)
                    logger.info(f"   🔍 {style_type}.enabled_raw = {enabled_raw} (type: {type(enabled_raw).__name__})")
                    
                    if isinstance(enabled_raw, dict):
                        enabled = enabled_raw.get('value', False)
                    else:
                        enabled = bool(enabled_raw)
                    
                    if enabled:
                        result[style_type] = True
                        logger.info(f"   ✅ {style_type}: cartela habilitada!")
                    else:
                        logger.info(f"   ❌ {style_type}: cartela desabilitada (enabled={enabled})")
                else:
                    logger.info(f"   ❌ {style_type}: sem dados de cartela")
            
            if any(result.values()):
                logger.info(f"🎬 [CARTELA_ENABLED] Resultado final: {result}")
            else:
                logger.warning(f"⚠️ [CARTELA_ENABLED] Nenhuma cartela habilitada no template {template_id[:8]}...")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ [CARTELA_ENABLED] Erro ao carregar cartela do template: {e}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            return result
    
    def _get_cartela_enabled(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, bool]:
        """
        Determina quais tipos têm cartela habilitada.
        """
        # Se passou explicitamente no contexto, usar isso
        if context and 'cartela_enabled' in context:
            return context['cartela_enabled']
        
        # Tentar buscar do template
        if context and 'template_id' in context and context['template_id']:
            try:
                return self._load_cartela_enabled_from_template(context['template_id'])
            except Exception as e:
                logger.debug(f"⚠️ Não conseguiu carregar cartela do template: {e}")
        
        # Default: nenhuma cartela habilitada
        return {'default': False, 'emphasis': False, 'letter_effect': False}
    
    def _load_matting_enabled_from_template(self, template_id: str) -> bool:
        """
        Carrega se matting (recorte de pessoa) está habilitado no template.
        
        Busca a coluna 'matting' na tabela video_editing_templates.
        
        Returns:
            True se matting.enabled.value é True, False caso contrário
        """
        try:
            import json
            import os
            import psycopg2
            
            # Conectar diretamente ao banco PostgreSQL
            database_url = os.environ.get('DIRECT_DATABASE_URL', '')
            if not database_url:
                db_host = os.environ.get('POSTGRES_HOST', 'localhost')
                db_port = os.environ.get('POSTGRES_PORT', '5432')
                db_name = os.environ.get('POSTGRES_DB', 'postgres')
                db_user = os.environ.get('POSTGRES_USER', 'postgres')
                db_pass = os.environ.get('POSTGRES_PASSWORD', '')
                database_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            # Buscar a coluna matting
            cursor.execute("""
                SELECT matting
                FROM video_editing_templates
                WHERE id = %s
            """, (template_id,))
            
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not row or not row[0]:
                logger.debug(f"⚠️ [MATTING_ENABLED] Template {template_id[:8]}... não tem matting configurado")
                return False
            
            matting_data = row[0]
            
            # Pode ser string JSON ou dict
            if isinstance(matting_data, str):
                try:
                    matting_data = json.loads(matting_data)
                except:
                    return False
            
            # Verificar enabled.value
            enabled_obj = matting_data.get('enabled', {})
            if isinstance(enabled_obj, dict):
                enabled = enabled_obj.get('value', False)
            else:
                enabled = bool(enabled_obj)
            
            logger.info(f"🎭 [MATTING_ENABLED] Template {template_id[:8]}...: {enabled}")
            return enabled
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao carregar matting do template: {e}")
            return False
    
    def _get_matting_enabled(self, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Determina se matting (recorte de pessoa) está habilitado.
        """
        # Se passou explicitamente no contexto, usar isso
        if context and 'matting_enabled' in context:
            return context['matting_enabled']
        
        # Tentar buscar do template
        if context and 'template_id' in context and context['template_id']:
            try:
                return self._load_matting_enabled_from_template(context['template_id'])
            except Exception as e:
                logger.debug(f"⚠️ Não conseguiu carregar matting do template: {e}")
        
        # Default: matting desabilitado
        return False
    
    def _filter_by_enabled_types(
        self, 
        phrases: List[Dict[str, Any]], 
        enabled_types: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Filtra classificações para só usar tipos habilitados.
        Tipos desabilitados são convertidos para o PRIMEIRO tipo habilitado.
        
        🔧 v2.5.8: Não assume mais que 'default' sempre está disponível!
        """
        if not enabled_types:
            logger.warning("⚠️ [_filter_by_enabled_types] Nenhum tipo habilitado, retornando sem filtro")
            return phrases
        
        # O fallback é o PRIMEIRO tipo habilitado, não 'default'
        fallback_type = enabled_types[0]
        
        result = []
        converted_count = 0
        
        for phrase in phrases:
            phrase_copy = dict(phrase)
            style_type = phrase_copy.get('style_type', fallback_type)
            
            # Se o tipo não está habilitado, converter para o fallback (primeiro tipo habilitado)
            if style_type not in enabled_types:
                logger.debug(f"⚠️ Tipo '{style_type}' desabilitado, convertendo para '{fallback_type}'")
                phrase_copy['style_type'] = fallback_type
                phrase_copy['style_reason'] = f"convertido (tipo '{style_type}' desabilitado → {fallback_type})"
                converted_count += 1
            
            result.append(phrase_copy)
        
        if converted_count > 0:
            logger.info(f"🔄 [_filter_by_enabled_types] {converted_count} frases convertidas para '{fallback_type}'")
        
        return result
    
    def _normalize_classifications(
        self, 
        response: Any, 
        expected_count: int
    ) -> Optional[Dict[str, Any]]:
        """
        Normaliza a resposta da LLM para o formato esperado.
        
        Converte formatos alternativos como:
        - ["emphasis", "default", ...] → [{"index": 0, "type": "emphasis"}, ...]
        - {"0": "emphasis", "1": "default"} → [{"index": 0, "type": "emphasis"}, ...]
        
        Args:
            response: Resposta parseada (dict ou list)
            expected_count: Número esperado de classificações
            
        Returns:
            Dict normalizado com {"classifications": [...]} ou None se falhar
        """
        try:
            classifications = None
            regroupings = []
            feature_blocks = []  # 🆕 v2.9.16: Blocos de features
            
            # Extrair classificações dependendo do formato
            if isinstance(response, list):
                classifications = response
            elif isinstance(response, dict):
                classifications = response.get('classifications', [])
                regroupings = response.get('regroupings', [])
                feature_blocks = response.get('feature_blocks', [])  # 🆕 v2.9.16
            else:
                logger.warning(f"⚠️ Resposta não é dict nem list: {type(response)}")
                return None
            
            if not isinstance(classifications, list):
                logger.warning(f"⚠️ classifications não é lista: {type(classifications)}")
                return None
            
            if len(classifications) == 0:
                logger.warning("⚠️ Lista de classificações vazia")
                return None
            
            # 🆕 NORMALIZAR: Converter diferentes formatos para o padrão
            normalized = []
            
            for i, c in enumerate(classifications):
                # Item padrão inclui use_matting para suporte a recorte de pessoa
                item = {"index": i, "type": "default", "reason": "", "use_cartela": False, "use_matting": False}
                
                if isinstance(c, str):
                    # Formato: ["emphasis", "default", ...]
                    style_type = c.lower().strip()
                    if style_type in ['default', 'emphasis', 'letter_effect']:
                        item["type"] = style_type
                        item["reason"] = f"classificado como {style_type}"
                        logger.debug(f"🔄 Convertendo string '{c}' para objeto")
                    else:
                        logger.warning(f"⚠️ Tipo inválido '{c}', usando 'default'")
                        
                elif isinstance(c, dict):
                    # Formato já correto: {"index": 0, "type": "emphasis", ...}
                    item["index"] = c.get('index', i)
                    style_type = str(c.get('type', 'default')).lower().strip()
                    if style_type in ['default', 'emphasis', 'letter_effect']:
                        item["type"] = style_type
                    else:
                        logger.warning(f"⚠️ Tipo inválido '{c.get('type')}', usando 'default'")
                    item["reason"] = c.get('reason', '')
                    item["use_cartela"] = c.get('use_cartela', False)
                    # 🆕 Capturar use_matting da resposta da LLM
                    item["use_matting"] = c.get('use_matting', False)
                    
                elif isinstance(c, int):
                    # Formato numérico: [0, 1, 2] → default, emphasis, letter_effect
                    type_map = {0: 'default', 1: 'emphasis', 2: 'letter_effect'}
                    item["type"] = type_map.get(c, 'default')
                    
                else:
                    logger.warning(f"⚠️ Formato inesperado na posição {i}: {type(c).__name__}")
                
                normalized.append(item)
            
            # Verificar se tem classificações suficientes
            if len(normalized) < min(3, expected_count):
                logger.warning(f"⚠️ Poucas classificações após normalização: {len(normalized)}/{expected_count}")
                return None
            
            logger.info(f"✅ Classificações normalizadas: {len(normalized)} items")
            
            # 🆕 v2.9.16: Log de feature_blocks
            if feature_blocks:
                logger.info(f"📦 Feature blocks encontrados: {len(feature_blocks)} blocos")
            
            return {"classifications": normalized, "regroupings": regroupings, "feature_blocks": feature_blocks}
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao normalizar resposta: {e}")
            return None
    
    def _validate_classification_response(
        self, 
        response: Any, 
        expected_count: int
    ) -> bool:
        """
        Valida se a resposta da LLM tem a estrutura correta.
        
        NOTA: Use _normalize_classifications() antes para converter formatos alternativos.
        
        Args:
            response: Resposta parseada (dict ou list)
            expected_count: Número esperado de classificações
            
        Returns:
            True se válido, False caso contrário
        """
        try:
            # Aceitar formato de objeto ou array
            if isinstance(response, list):
                classifications = response
            elif isinstance(response, dict):
                classifications = response.get('classifications', [])
            else:
                logger.warning(f"⚠️ Resposta não é dict nem list: {type(response)}")
                return False
            
            # Verificar se é uma lista
            if not isinstance(classifications, list):
                logger.warning(f"⚠️ classifications não é lista: {type(classifications)}")
                return False
            
            # Verificar se tem pelo menos algumas classificações
            if len(classifications) < min(3, expected_count):
                logger.warning(f"⚠️ Poucas classificações: {len(classifications)}/{expected_count}")
                return False
            
            # Verificar estrutura de cada classificação
            for i, c in enumerate(classifications[:5]):  # Verificar apenas as 5 primeiras
                if not isinstance(c, dict):
                    # Formato alternativo (strings ou números) - precisa normalizar primeiro
                    logger.warning(f"⚠️ Classificação {i} não é dict: {type(c).__name__} = {str(c)[:100]}")
                    logger.info("💡 Use _normalize_classifications() para converter formatos alternativos")
                    return False
                if 'type' not in c and 'index' not in c:
                    logger.warning(f"⚠️ Classificação {i} sem 'type' ou 'index'")
                    return False
                if 'type' in c and c['type'] not in ['default', 'emphasis', 'letter_effect']:
                    logger.warning(f"⚠️ Tipo inválido na classificação {i}: {c['type']}")
                    # Não retornar False, apenas avisar
            
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao validar resposta: {e}")
            return False
    
    def _robust_json_parse(self, content: str) -> Optional[Dict[str, Any]]:
        """
        Tenta extrair e parsear JSON de forma robusta.
        
        Estratégias:
        1. Limpar markdown
        2. Encontrar início/fim do JSON
        3. Remover caracteres de controle
        4. Tentar corrigir erros comuns
        """
        import re
        
        if not content:
            return None
        
        original_content = content
        
        try:
            # Estratégia 1: Limpar markdown
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                parts = content.split("```")
                if len(parts) >= 2:
                    content = parts[1]
            
            content = content.strip()
            
            # Estratégia 2: Encontrar início do JSON
            if not content.startswith('{') and not content.startswith('['):
                start_brace = content.find('{')
                start_bracket = content.find('[')
                if start_brace >= 0 and (start_bracket < 0 or start_brace < start_bracket):
                    content = content[start_brace:]
                elif start_bracket >= 0:
                    content = content[start_bracket:]
            
            # Estratégia 3: Encontrar fim do JSON
            if content.startswith('{'):
                # Encontrar o último }
                last_brace = content.rfind('}')
                if last_brace > 0:
                    content = content[:last_brace + 1]
            elif content.startswith('['):
                # Encontrar o último ]
                last_bracket = content.rfind(']')
                if last_bracket > 0:
                    content = content[:last_bracket + 1]
            
            # Estratégia 4: Remover caracteres de controle
            content = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', content)
            
            # Estratégia 5: Tentar parsear
            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                logger.debug(f"⚠️ Parse falhou após limpeza: {e}")
                
                # Estratégia 6: Tentar corrigir vírgulas extras
                # Remove vírgulas antes de } ou ]
                content = re.sub(r',\s*}', '}', content)
                content = re.sub(r',\s*]', ']', content)
                
                # Estratégia 7: Tentar novamente
                try:
                    return json.loads(content)
                except json.JSONDecodeError as e2:
                    logger.debug(f"⚠️ Parse falhou após correção de vírgulas: {e2}")
                    
                    # Estratégia 8: Usar regex para extrair array de classificações
                    pattern = r'\[\s*\{[^]]+\}\s*\]'
                    match = re.search(pattern, original_content, re.DOTALL)
                    if match:
                        try:
                            return {"classifications": json.loads(match.group(0)), "regroupings": []}
                        except:
                            pass
                    
                    return None
                    
        except Exception as e:
            logger.warning(f"⚠️ Erro no parsing robusto: {e}")
            return None
    
    def _classify_with_llm(
        self,
        phrases: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
        enabled_types: Optional[List[str]] = None,
        custom_instructions: Optional[str] = None,
        cartela_enabled: Optional[Dict[str, bool]] = None,
        matting_enabled: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Classifica usando LLM (OpenAI/Claude).
        
        Args:
            phrases: Lista de frases para classificar
            context: Contexto com template_id, etc.
            enabled_types: Tipos habilitados ['default', 'emphasis', 'letter_effect']
            custom_instructions: Instruções customizadas do usuário
            cartela_enabled: Dict com tipos que têm cartela habilitada
            matting_enabled: Se recorte de pessoa (v-matting) está habilitado
        """
        enabled_types = enabled_types or VALID_STYLE_TYPES
        cartela_enabled = cartela_enabled or {'default': False, 'emphasis': False, 'letter_effect': False}
        
        try:
            from openai import OpenAI
            import re
            
            # Montar texto das frases - sanitizar COMPLETAMENTE para evitar quebra de JSON
            phrases_lines = []
            for i, p in enumerate(phrases):
                text = p.get('text', '')
                # Sanitização completa
                text = text.replace('"', "'")  # Aspas duplas → simples
                text = text.replace('\\', '')  # Remove backslashes
                text = text.replace('\n', ' ')  # Newlines → espaços
                text = text.replace('\r', ' ')  # Carriage returns
                text = text.replace('\t', ' ')  # Tabs
                text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)  # Remove control chars
                text = ' '.join(text.split())  # Normaliza espaços múltiplos
                text = text.strip()
                
                word_count = p.get('word_count', len(text.split()))
                phrases_lines.append(f"{i}. {text} ({word_count} palavras)")
            
            phrases_text = "\n".join(phrases_lines)
            
            logger.debug(f"📝 Frases para classificação (sanitizadas): {phrases_text[:500]}...")
            
            # Log dos tipos e cartelas habilitados
            logger.info(f"🎨 Tipos habilitados: {enabled_types}")
            logger.info(f"🎬 Cartela habilitada por tipo: {cartela_enabled}")
            logger.info(f"🎭 Matting (recorte de pessoa) habilitado: {matting_enabled}")
            if custom_instructions:
                logger.info(f"📝 Instruções customizadas: {custom_instructions[:100]}...")
            
            # Usar o novo prompt dinâmico
            with_regrouping = bool(custom_instructions)
            
            # 🆕 v2.9.16: Habilitar feature_blocks se cartela ou matting estiverem habilitados
            has_any_feature = any(cartela_enabled.values()) or matting_enabled
            feature_blocks_enabled = has_any_feature and context.get('feature_blocks_enabled', True) if context else has_any_feature
            
            prompt = _build_classification_prompt(
                enabled_types=enabled_types,
                cartela_enabled=cartela_enabled,
                custom_instructions=custom_instructions or "",
                phrases_text=phrases_text,
                with_regrouping=with_regrouping,
                total_phrases=len(phrases),
                matting_enabled=matting_enabled,
                feature_blocks_enabled=feature_blocks_enabled
            )
            
            if feature_blocks_enabled:
                logger.info("📦 Feature blocks habilitados (agrupamento de cartela/matting)")
            
            # Chamar OpenAI
            api_key = self.config.get('api_key', '')
            if not api_key:
                logger.error("❌ API key não encontrada")
                return None
            
            client = OpenAI(api_key=api_key)
            
            # ✅ Extrair nome do modelo corretamente (ai_config retorna dict)
            model_config = self.config.get('model', {})
            if isinstance(model_config, dict):
                model_name = model_config.get('name', 'gpt-4o-mini')
            else:
                model_name = model_config or 'gpt-4o-mini'
            
            # ✅ Extrair max_tokens de parameters (ai_config retorna dentro de parameters)
            params = self.config.get('parameters', {})
            if isinstance(params, dict):
                max_tokens = params.get('max_tokens', 2000)
            else:
                max_tokens = self.config.get('max_tokens', 2000)
            
            # Garantir que max_tokens é inteiro
            if isinstance(max_tokens, str):
                max_tokens = int(max_tokens)
            elif not isinstance(max_tokens, int):
                max_tokens = 2000
            
            logger.info(f"🤖 Chamando OpenAI: model={model_name}, max_tokens={max_tokens}")
            
            # =========================================================================
            # SOLUÇÃO ROBUSTA: Usar response_format + retry + validação
            # =========================================================================
            
            parsed_response = None
            last_error = None
            max_retries = 3
            
            for attempt in range(max_retries):
                try:
                    # 🆕 Usar response_format para GARANTIR JSON válido
                    # Isso força a OpenAI a retornar apenas JSON estruturado
                    response = client.chat.completions.create(
                        model=model_name,
                        messages=[
                            {
                                "role": "system", 
                                "content": """Você é um classificador de frases para legendas de vídeo.
REGRA ABSOLUTA: Retorne APENAS um objeto JSON válido, sem texto antes ou depois.
O JSON deve ter a estrutura exata: {"classifications": [...], "regroupings": [...]}"""
                            },
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.3,
                        max_tokens=max_tokens,
                        response_format={"type": "json_object"}  # 🆕 FORÇA JSON VÁLIDO
                    )
                    
                    content = response.choices[0].message.content
                    logger.info(f"📝 [Attempt {attempt+1}/{max_retries}] Resposta LLM: {len(content)} chars")
                    
                    # Com response_format=json_object, o content JÁ É JSON válido
                    raw_response = json.loads(content)
                    
                    # 🆕 NORMALIZAR antes de validar - converte ["emphasis", ...] para [{"index": 0, "type": "emphasis"}, ...]
                    parsed_response = self._normalize_classifications(raw_response, len(phrases))
                    if not parsed_response:
                        logger.warning(f"⚠️ [Attempt {attempt+1}] Falha na normalização")
                        raise ValueError("Não foi possível normalizar a resposta da LLM")
                    
                    # 🆕 Validar estrutura do JSON já normalizado
                    if not self._validate_classification_response(parsed_response, len(phrases)):
                        raise ValueError("JSON válido mas estrutura incorreta")
                    
                    logger.info(f"✅ JSON parseado com sucesso na tentativa {attempt+1}")
                    break  # Sucesso, sair do loop
                    
                except json.JSONDecodeError as e:
                    last_error = e
                    logger.warning(f"⚠️ [Attempt {attempt+1}] JSON inválido: {e}")
                    logger.debug(f"📝 Conteúdo problemático: {content[:500] if 'content' in locals() else 'N/A'}...")
                    
                    # Tentar recuperar o JSON com parsing robusto
                    if 'content' in locals():
                        parsed_response = self._robust_json_parse(content)
                        if parsed_response:
                            logger.info(f"✅ JSON recuperado com parsing robusto")
                            break
                    
                    # Aguardar antes de retry (exponential backoff)
                    if attempt < max_retries - 1:
                        import time
                        wait_time = (attempt + 1) * 2  # 2s, 4s, 6s
                        logger.info(f"⏳ Aguardando {wait_time}s antes de retry...")
                        time.sleep(wait_time)
                        
                except Exception as e:
                    last_error = e
                    logger.warning(f"⚠️ [Attempt {attempt+1}] Erro: {e}")
                    if attempt < max_retries - 1:
                        import time
                        time.sleep((attempt + 1) * 2)
            
            # Se ainda não conseguiu, lançar erro
            if parsed_response is None:
                if last_error:
                    raise last_error
                raise ValueError("Não foi possível obter resposta válida da LLM após 3 tentativas")
            
            # Suportar ambos formatos: array simples ou objeto com classifications/regroupings
            if isinstance(parsed_response, list):
                classifications = parsed_response
                regroupings = []
                feature_blocks = []
            else:
                classifications = parsed_response.get('classifications', [])
                regroupings = parsed_response.get('regroupings', [])
                feature_blocks = parsed_response.get('feature_blocks', [])  # 🆕 v2.9.16
            
            # 🆕 Aplicar reagrupamentos se houver
            working_phrases = list(phrases)
            if regroupings:
                working_phrases = self._apply_regroupings(working_phrases, regroupings)
                logger.info(f"🔄 Aplicados {len(regroupings)} reagrupamentos")
            
            # Aplicar classificações às frases
            result = []
            for i, phrase in enumerate(working_phrases):
                phrase_copy = dict(phrase)
                
                # Encontrar classificação correspondente
                classification = next(
                    (c for c in classifications if c.get('index') == i),
                    None
                )
                
                if classification:
                    style_type = classification.get('type', 'default')
                    if style_type not in VALID_STYLE_TYPES:
                        style_type = 'default'
                    
                    phrase_copy['style_type'] = style_type
                    phrase_copy['style_reason'] = classification.get('reason', '')
                    phrase_copy['style_confidence'] = 0.9
                    
                    # 🆕 Capturar use_cartela da resposta da LLM
                    use_cartela = classification.get('use_cartela', False)
                    # Só permitir cartela se o tipo tiver cartela habilitada
                    if use_cartela and cartela_enabled.get(style_type, False):
                        phrase_copy['use_cartela'] = True
                    else:
                        phrase_copy['use_cartela'] = False
                else:
                    phrase_copy['style_type'] = 'default'
                    phrase_copy['style_confidence'] = 0.5
                    phrase_copy['use_cartela'] = False
                
                result.append(phrase_copy)
            
            # 🆕 v2.9.16: Aplicar feature_blocks se houver
            if feature_blocks:
                result = self._apply_feature_blocks(result, feature_blocks)
                logger.info(f"📦 Aplicados {len(feature_blocks)} feature blocks")
            
            # 🆕 Pós-processamento: garantir cobertura mínima e agrupar cartelas/matting
            # 🔧 v3.5.0: Passar enabled_types para pareamento correto cartela↔matting
            result = self._post_process_classifications(result, cartela_enabled, matting_enabled, enabled_types)
            
            # 🆕 v2.9.16: Salvar feature_blocks nas frases para o frontend
            if feature_blocks:
                for phrase in result:
                    phrase['_feature_blocks'] = feature_blocks  # Salvar referência para o frontend
            
            # Estatísticas
            stats = {t: sum(1 for p in result if p.get('style_type') == t) for t in VALID_STYLE_TYPES}
            cartela_count = sum(1 for p in result if p.get('use_cartela', False))
            matting_count = sum(1 for p in result if p.get('use_matting', False))
            logger.info(f"📊 Classificação LLM: {stats}, com cartela: {cartela_count}, com matting: {matting_count}")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Erro ao parsear JSON da LLM: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Erro na chamada LLM: {e}")
            return None
    
    def _apply_feature_blocks(
        self,
        phrases: List[Dict[str, Any]],
        feature_blocks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        🆕 v2.9.16: Aplica feature_blocks às frases.
        
        Feature blocks definem grupos de frases que compartilham
        as mesmas configurações de cartela e matting.
        
        Args:
            phrases: Lista de frases já classificadas
            feature_blocks: Lista de blocos com start_index, end_index, use_cartela, use_matting
            
        Returns:
            Frases com use_cartela e use_matting atualizados baseado nos blocos
        """
        if not feature_blocks or not phrases:
            return phrases
        
        result = [dict(p) for p in phrases]
        
        # Indexar blocos por frase para acesso rápido
        phrase_to_block = {}
        for block in feature_blocks:
            start = block.get('start_index', 0)
            end = block.get('end_index', start)
            use_cartela = block.get('use_cartela', False)
            use_matting = block.get('use_matting', False)
            block_id = f"{start}-{end}"
            
            for i in range(start, min(end + 1, len(result))):
                phrase_to_block[i] = {
                    'block_id': block_id,
                    'use_cartela': use_cartela,
                    'use_matting': use_matting,
                    'reason': block.get('reason', '')
                }
        
        # Aplicar blocos às frases
        applied_count = 0
        for i, phrase in enumerate(result):
            if i in phrase_to_block:
                block_info = phrase_to_block[i]
                phrase['use_cartela'] = block_info['use_cartela']
                phrase['use_matting'] = block_info['use_matting']
                phrase['feature_block_id'] = block_info['block_id']
                phrase['feature_block_reason'] = block_info['reason']
                applied_count += 1
        
        logger.info(f"📦 [FEATURE_BLOCKS] Aplicado a {applied_count}/{len(result)} frases")
        return result
    
    def _post_process_classifications(
        self,
        phrases: List[Dict[str, Any]],
        cartela_enabled: Dict[str, bool],
        matting_enabled: bool = False,
        enabled_types: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Pós-processa classificações para garantir:
        1. Cobertura mínima de tipos não-default (40-50%)
        2. Cobertura mínima de cartela (30-50%)
        3. Agrupamento de cartelas (mínimo 4 segundos / 3-4 frases consecutivas)
        4. Distribuição ao longo do vídeo (início, meio, fim)
        5. Matting acompanhando cartelas (se habilitado)
        
        🔧 v3.5.0: enabled_types agora é passado para garantir pareamento
        correto cartela↔matting quando apenas 1 tipo de texto está habilitado.
        """
        if not phrases:
            return phrases
        
        result = [dict(p) for p in phrases]
        total = len(result)
        
        # =========================================================================
        # 1. GARANTIR COBERTURA MÍNIMA DE TIPOS NÃO-DEFAULT
        # =========================================================================
        default_count = sum(1 for p in result if p.get('style_type') == 'default')
        non_default_count = total - default_count
        
        # Se menos de 40% é não-default, promover algumas frases
        min_non_default = int(total * 0.40)
        if non_default_count < min_non_default:
            need_to_promote = min_non_default - non_default_count
            logger.info(f"🔄 [POST_PROCESS] Promovendo {need_to_promote} frases para não-default")
            
            # Encontrar frases default que podem ser promovidas
            # Priorizar: perguntas, exclamações, frases curtas
            for i, p in enumerate(result):
                if need_to_promote <= 0:
                    break
                if p.get('style_type') != 'default':
                    continue
                
                text = p.get('text', '')
                word_count = p.get('word_count', len(text.split()))
                
                # Promover para letter_effect se for curta
                if word_count <= 3:
                    result[i]['style_type'] = 'letter_effect'
                    result[i]['style_reason'] = 'promovido: frase curta'
                    need_to_promote -= 1
                # Promover para emphasis se tiver pontuação especial
                elif '?' in text or '!' in text or text.endswith(','):
                    result[i]['style_type'] = 'emphasis'
                    result[i]['style_reason'] = 'promovido: pontuação enfática'
                    need_to_promote -= 1
        
        # =========================================================================
        # 2. GARANTIR COBERTURA MÍNIMA DE CARTELA (se habilitada)
        # =========================================================================
        types_with_cartela = [t for t in ['default', 'emphasis', 'letter_effect'] 
                             if cartela_enabled.get(t, False)]
        
        if types_with_cartela:
            cartela_count = sum(1 for p in result if p.get('use_cartela', False))
            min_cartela = max(4, int(total * 0.30))  # 30% mínimo
            
            if cartela_count < min_cartela:
                need_cartela = min_cartela - cartela_count
                logger.info(f"🎬 [POST_PROCESS] Adicionando cartela em {need_cartela} frases")
                
                # Estratégia: criar blocos de cartela no início, meio e fim
                # Cada bloco deve ter pelo menos 3-4 frases consecutivas
                
                # Bloco 1: Primeiras 4-6 frases (abertura)
                block_1_end = min(6, total // 3)
                for i in range(block_1_end):
                    if result[i].get('style_type') in types_with_cartela:
                        if not result[i].get('use_cartela', False):
                            result[i]['use_cartela'] = True
                            need_cartela -= 1
                            if need_cartela <= 0:
                                break
                
                # Bloco 2: Meio (1/3 até 2/3 do vídeo)
                if need_cartela > 0:
                    mid_start = total // 3
                    mid_end = min(mid_start + 5, 2 * total // 3)
                    for i in range(mid_start, mid_end):
                        if result[i].get('style_type') in types_with_cartela:
                            if not result[i].get('use_cartela', False):
                                result[i]['use_cartela'] = True
                                need_cartela -= 1
                                if need_cartela <= 0:
                                    break
                
                # Bloco 3: Últimas 4-6 frases (fechamento)
                if need_cartela > 0:
                    end_start = max(0, total - 6)
                    for i in range(end_start, total):
                        if result[i].get('style_type') in types_with_cartela:
                            if not result[i].get('use_cartela', False):
                                result[i]['use_cartela'] = True
                                need_cartela -= 1
                                if need_cartela <= 0:
                                    break
        
        # =========================================================================
        # 3. AGRUPAR CARTELAS (evitar cartelas isoladas)
        # =========================================================================
        # Se uma frase tem cartela mas as vizinhas não, expandir o bloco
        if types_with_cartela:
            for i in range(1, len(result) - 1):
                if result[i].get('use_cartela', False):
                    # Verificar se está isolada
                    prev_has = result[i-1].get('use_cartela', False)
                    next_has = result[i+1].get('use_cartela', False)
                    
                    if not prev_has and not next_has:
                        # Está isolada, tentar expandir
                        if result[i-1].get('style_type') in types_with_cartela:
                            result[i-1]['use_cartela'] = True
                        if result[i+1].get('style_type') in types_with_cartela:
                            result[i+1]['use_cartela'] = True
        
        # =========================================================================
        # 5. PROCESSAR MATTING (recorte de pessoa)
        # =========================================================================
        if matting_enabled:
            # 🔧 v3.5.0: Usar enabled_types (da config do template) ao invés de
            # inferir dos style_types atribuídos pela LLM. A LLM pode atribuir
            # tipos variados, mas _filter_by_enabled_types vai convergir tudo depois.
            # O que importa é quantos tipos o TEMPLATE permite.
            if enabled_types is not None:
                only_one_enabled_type = len(enabled_types) == 1
                logger.info(f"🎭 [POST_PROCESS] enabled_types={enabled_types}, only_one={only_one_enabled_type}")
            else:
                # Fallback: inferir dos dados (comportamento antigo)
                enabled_types_set = set(p.get('style_type', 'default') for p in result)
                only_one_enabled_type = len(enabled_types_set) <= 1
            
            # Garantir que matting acompanhe as cartelas
            matting_count = sum(1 for p in result if p.get('use_matting', False))
            min_matting = max(2, int(total * 0.10))  # 10% mínimo
            
            if matting_count < min_matting:
                # Adicionar matting em frases que já têm cartela
                # Priorizar: emphasis, letter_effect, frases curtas
                candidates = []
                for i, p in enumerate(result):
                    if p.get('use_cartela', False) and not p.get('use_matting', False):
                        score = 0
                        if p.get('style_type') == 'letter_effect':
                            score += 3
                        elif p.get('style_type') == 'emphasis':
                            score += 2
                        if p.get('word_count', 5) <= 4:
                            score += 1
                        # Quando só tem 1 tipo habilitado, todas com cartela são candidatas fortes
                        if only_one_enabled_type:
                            score += 5
                        candidates.append((i, score))
                
                # Ordenar por score e adicionar matting
                candidates.sort(key=lambda x: -x[1])
                need_matting = min_matting - matting_count
                
                for i, _ in candidates[:need_matting]:
                    result[i]['use_matting'] = True
                
                logger.info(f"🎭 [POST_PROCESS] Adicionado matting em {min(need_matting, len(candidates))} frases")
            
            # =========================================================================
            # 5b. PAREAMENTO CARTELA + MATTING (quando só 1 tipo de texto)
            # Quando não há variedade de estilos de texto, cartela e matting
            # DEVEM andar juntos — são a única fonte de variedade visual.
            # =========================================================================
            if only_one_enabled_type and types_with_cartela:
                paired_count = 0
                for p in result:
                    if p.get('use_cartela', False) and not p.get('use_matting', False):
                        # Cartela sem matting quando só tem 1 tipo → adicionar matting
                        p['use_matting'] = True
                        paired_count += 1
                    elif p.get('use_matting', False) and not p.get('use_cartela', False):
                        # Matting sem cartela → adicionar cartela (se tipo aceita)
                        style_type = p.get('style_type', 'default')
                        if cartela_enabled.get(style_type, False):
                            p['use_cartela'] = True
                            paired_count += 1
                        else:
                            p['use_matting'] = False
                            paired_count += 1
                
                effective_type = enabled_types[0] if enabled_types else 'default'
                if paired_count > 0:
                    logger.info(f"🎭 [POST_PROCESS] Pareamento forçado (só tipo '{effective_type}'): "
                                f"{paired_count} frases sincronizadas cartela↔matting")
            else:
                # Modo normal: garantir que matting SEMPRE tenha cartela
                for p in result:
                    if p.get('use_matting', False) and not p.get('use_cartela', False):
                        style_type = p.get('style_type', 'default')
                        if cartela_enabled.get(style_type, False):
                            p['use_cartela'] = True
                            logger.debug(f"🎭 Adicionando cartela para frase com matting: {p.get('text', '')[:30]}...")
                        else:
                            p['use_matting'] = False
            
            # Propagar use_matting para person_overlay_enabled (campo usado pelo orchestrator)
            for p in result:
                if p.get('use_matting', False):
                    p['person_overlay_enabled'] = True
        
        # Log final
        final_cartela = sum(1 for p in result if p.get('use_cartela', False))
        final_matting = sum(1 for p in result if p.get('use_matting', False)) if matting_enabled else 0
        final_stats = {t: sum(1 for p in result if p.get('style_type') == t) for t in VALID_STYLE_TYPES}
        
        log_msg = f"✅ [POST_PROCESS] Final: {final_stats}, cartela: {final_cartela}/{total}"
        if matting_enabled:
            log_msg += f", matting: {final_matting}/{total}"
        logger.info(log_msg)
        
        return result
    
    def _apply_regroupings(
        self,
        phrases: List[Dict[str, Any]],
        regroupings: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Aplica reagrupamentos sugeridos pela LLM.
        
        Suporta:
        - merge: junta duas ou mais frases em uma
        - split: divide uma frase em duas
        """
        if not regroupings:
            return phrases
        
        result = list(phrases)
        
        # Processar merges primeiro (de trás para frente para manter índices válidos)
        merges = [r for r in regroupings if r.get('action') == 'merge']
        merges.sort(key=lambda x: max(x.get('indices', [0])), reverse=True)
        
        for merge in merges:
            indices = merge.get('indices', [])
            if len(indices) < 2:
                continue
            
            indices = sorted(indices)
            if any(i >= len(result) or i < 0 for i in indices):
                logger.warning(f"⚠️ Merge inválido: índices {indices} fora do range")
                continue
            
            # Juntar as frases
            merged_text_parts = []
            merged_words = []
            first_phrase = result[indices[0]]
            
            for idx in indices:
                phrase = result[idx]
                merged_text_parts.append(phrase.get('text', ''))
                merged_words.extend(phrase.get('words', []))
            
            merged_phrase = dict(first_phrase)
            merged_phrase['text'] = ' '.join(merged_text_parts)
            merged_phrase['words'] = merged_words
            merged_phrase['word_count'] = len(merged_words)
            merged_phrase['regrouped'] = True
            merged_phrase['regroup_reason'] = merge.get('reason', 'merge sugerido pela IA')
            
            # Atualizar tempos
            if merged_words:
                merged_phrase['start_time'] = min(w.get('start', 0) for w in merged_words)
                merged_phrase['end_time'] = max(w.get('end', 0) for w in merged_words)
            
            # Remover frases originais e inserir a merged
            for idx in reversed(indices):
                result.pop(idx)
            result.insert(indices[0], merged_phrase)
            
            logger.debug(f"🔄 Merge aplicado: {indices} → '{merged_phrase['text'][:50]}...'")
        
        # Processar splits (de trás para frente)
        splits = [r for r in regroupings if r.get('action') == 'split']
        splits.sort(key=lambda x: x.get('index', 0), reverse=True)
        
        for split in splits:
            idx = split.get('index', -1)
            split_at = split.get('split_at_word', -1)
            
            if idx < 0 or idx >= len(result) or split_at < 1:
                logger.warning(f"⚠️ Split inválido: índice {idx}, split_at {split_at}")
                continue
            
            phrase = result[idx]
            words = phrase.get('words', [])
            
            if split_at >= len(words):
                continue
            
            # Criar duas novas frases
            words_1 = words[:split_at]
            words_2 = words[split_at:]
            
            phrase_1 = dict(phrase)
            phrase_1['text'] = ' '.join(w.get('text', '') for w in words_1)
            phrase_1['words'] = words_1
            phrase_1['word_count'] = len(words_1)
            phrase_1['regrouped'] = True
            phrase_1['regroup_reason'] = split.get('reason', 'split sugerido pela IA')
            if words_1:
                phrase_1['start_time'] = min(w.get('start', 0) for w in words_1)
                phrase_1['end_time'] = max(w.get('end', 0) for w in words_1)
            
            phrase_2 = dict(phrase)
            phrase_2['text'] = ' '.join(w.get('text', '') for w in words_2)
            phrase_2['words'] = words_2
            phrase_2['word_count'] = len(words_2)
            phrase_2['regrouped'] = True
            phrase_2['regroup_reason'] = split.get('reason', 'split sugerido pela IA')
            if words_2:
                phrase_2['start_time'] = min(w.get('start', 0) for w in words_2)
                phrase_2['end_time'] = max(w.get('end', 0) for w in words_2)
            
            # Substituir frase original por duas novas
            result.pop(idx)
            result.insert(idx, phrase_2)
            result.insert(idx, phrase_1)
            
            logger.debug(f"✂️ Split aplicado: {idx} → '{phrase_1['text'][:30]}...' + '{phrase_2['text'][:30]}...'")
        
        # Reindexar
        for i, phrase in enumerate(result):
            phrase['phrase_index'] = i
        
        return result
    
    def _classify_with_heuristics(
        self,
        phrases: List[Dict[str, Any]],
        enabled_types: Optional[List[str]] = None,
        custom_instructions: Optional[str] = None,
        cartela_enabled: Optional[Dict[str, bool]] = None,
        matting_enabled: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Classificação baseada em regras heurísticas (fallback).
        
        Regras:
        - 1-2 palavras + sem pontuação complexa → letter_effect
        - Contém ? ou ! → emphasis
        - Resto → default
        
        Só classifica para tipos habilitados.
        Se custom_instructions for fornecido, tenta aplicar regras simples baseadas em palavras-chave.
        Também aplica matting heuristicamente se habilitado.
        """
        enabled_types = enabled_types or VALID_STYLE_TYPES
        cartela_enabled = cartela_enabled or {'default': False, 'emphasis': False, 'letter_effect': False}
        
        # 🔧 v2.5.8: O tipo padrão é o PRIMEIRO tipo habilitado, não 'default'
        fallback_type = enabled_types[0] if enabled_types else 'default'
        logger.info(f"🎯 [HEURISTICS] Tipo fallback: {fallback_type}, habilitados: {enabled_types}")
        
        # 🆕 Extrair palavras-chave das instruções customizadas para heurística
        emphasis_keywords = []
        letter_effect_keywords = []
        if custom_instructions:
            # Tentar extrair padrões simples das instruções
            instructions_lower = custom_instructions.lower()
            if 'emphasis' in instructions_lower or 'destaque' in instructions_lower:
                # Buscar palavras entre aspas
                import re
                quoted = re.findall(r"['\"]([^'\"]+)['\"]", custom_instructions)
                emphasis_keywords.extend([q.lower() for q in quoted])
            if 'letter_effect' in instructions_lower or 'impacto' in instructions_lower:
                import re
                quoted = re.findall(r"['\"]([^'\"]+)['\"]", custom_instructions)
                letter_effect_keywords.extend([q.lower() for q in quoted])
        
        if emphasis_keywords:
            logger.info(f"📝 Heurística: palavras para emphasis: {emphasis_keywords}")
        if letter_effect_keywords:
            logger.info(f"📝 Heurística: palavras para letter_effect: {letter_effect_keywords}")
        
        result = []
        letter_effect_count = 0
        max_letter_effect = max(1, len(phrases) // 10)  # Máx 10%
        
        for phrase in phrases:
            phrase_copy = dict(phrase)
            text = phrase.get('text', '').strip()
            text_lower = text.lower()
            word_count = phrase.get('word_count', len(text.split()))
            
            # 🔧 v2.5.8: Usar o primeiro tipo habilitado como padrão
            style_type = fallback_type
            reason = f'heurística: frase padrão ({fallback_type})'
            
            # 🆕 Regra 0: Palavras-chave customizadas para letter_effect (PRIORIDADE MÁXIMA)
            if 'letter_effect' in enabled_types and letter_effect_keywords:
                if any(kw in text_lower for kw in letter_effect_keywords):
                    style_type = 'letter_effect'
                    matched = [kw for kw in letter_effect_keywords if kw in text_lower]
                    reason = f'instrução customizada: contém "{matched[0]}"'
                    letter_effect_count += 1
            
            # 🆕 Regra 0.5: Palavras-chave customizadas para emphasis (PRIORIDADE ALTA)
            elif 'emphasis' in enabled_types and emphasis_keywords:
                if any(kw in text_lower for kw in emphasis_keywords):
                    style_type = 'emphasis'
                    matched = [kw for kw in emphasis_keywords if kw in text_lower]
                    reason = f'instrução customizada: contém "{matched[0]}"'
            
            # Regra 1: Frases muito curtas → letter_effect (com limite e se habilitado)
            elif 'letter_effect' in enabled_types and word_count <= 2 and letter_effect_count < max_letter_effect:
                if not any(p in text for p in ['?', '...', ',']):
                    style_type = 'letter_effect'
                    reason = 'heurística: frase curta de impacto'
                    letter_effect_count += 1
            
            # Regra 2: Perguntas e exclamações → emphasis (se habilitado)
            elif 'emphasis' in enabled_types and ('?' in text or '!' in text):
                style_type = 'emphasis'
                reason = 'heurística: pontuação enfática'
            
            # Regra 3: Palavras-chave de CTA → emphasis (se habilitado)
            elif 'emphasis' in enabled_types and any(kw in text_lower for kw in ['clique', 'acesse', 'link', 'agora', 'já']):
                style_type = 'emphasis'
                reason = 'heurística: palavra-chave CTA'
            
            phrase_copy['style_type'] = style_type
            phrase_copy['style_reason'] = reason
            phrase_copy['style_confidence'] = 0.7
            phrase_copy['style_source'] = 'heuristic'
            phrase_copy['use_cartela'] = False  # Será definido no pós-processamento
            
            result.append(phrase_copy)
        
        # Estatísticas antes do pós-processamento
        stats = {t: sum(1 for p in result if p.get('style_type') == t) for t in VALID_STYLE_TYPES}
        logger.info(f"📊 Classificação heurística (antes pós-proc): {stats}")
        
        # 🆕 Aplicar pós-processamento para garantir cobertura
        result = self._post_process_classifications(result, cartela_enabled, matting_enabled)
        
        # Estatísticas finais
        stats = {t: sum(1 for p in result if p.get('style_type') == t) for t in VALID_STYLE_TYPES}
        cartela_count = sum(1 for p in result if p.get('use_cartela', False))
        logger.info(f"📊 Classificação heurística (final): {stats}, com cartela: {cartela_count}")
        
        return result
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica status do serviço"""
        return {
            "available": self.is_available(),
            "mode": "llm" if self.is_configured else "heuristic",
            "provider": self.config.get('provider') if self.config else None
        }

