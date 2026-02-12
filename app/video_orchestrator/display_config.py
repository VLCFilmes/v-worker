"""
🎭 Display Config - Configuração do Pipeline Visualizer (WOW Feature)

Este arquivo controla O QUE o usuário vê durante o processamento.
- Bloqueia dados sensíveis
- Traduz termos técnicos para linguagem amigável
- Define mensagens customizadas por etapa

IMPORTANTE: Nunca mostrar ao usuário:
- IPs, senhas, tokens, API keys
- UUIDs de usuários
- Caminhos internos de arquivos
- Erros com stack traces
"""

import re
from typing import Dict, Any, Optional

# =============================================================================
# 🚫 PADRÕES BLOQUEADOS (nunca mostrar)
# =============================================================================

BLOCKED_PATTERNS = [
    r'password=\S+',
    r'api[_-]?key[=:]\S+',
    r'bearer\s+\S+',
    r'postgresql://\S+',
    r'redis://\S+',
    r'sk-proj-\S+',
    r'sk-\S{20,}',
    r'/users/[a-f0-9-]{36}/',
    r'Authorization:\s*\S+',
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',  # IPs
    r'[a-f0-9-]{36}',  # UUIDs (serão substituídos por versão curta)
]

# =============================================================================
# 🔄 TRADUÇÕES (termos técnicos → amigáveis)
# =============================================================================

FRIENDLY_NAMES = {
    # Serviços
    'v-services': '🎨 Gerador de Legendas',
    'v-matting': '✂️ Recorte Inteligente',
    'v-editor': '🎬 Editor de Vídeo',
    'orchestrator': '🎯 Coordenador',
    'video-worker': '⚙️ Processador',
    
    # Tecnologias
    'whisper': '🎤 Transcrição',
    'openai': '🧠 IA',
    'modal': '☁️ GPU Cloud',
    'remotion': '🎬 Renderizador',
    'ffmpeg': '🎬 Processador de Vídeo',
    'psycopg2': '📡 Banco de Dados',
    'redis': '📨 Mensageria',
    'backblaze': '☁️ Storage',
    'b2': '☁️ Storage',
    
    # Ações
    'transcribe': 'transcrever',
    'classify': 'classificar',
    'render': 'renderizar',
    'upload': 'enviar',
    'download': 'baixar',
}

# =============================================================================
# 📝 ETAPAS DO PIPELINE
# =============================================================================

PIPELINE_STEPS = {
    'NORMALIZE': {
        'label': '🎵 Normalizando Áudio',
        'start': '🎵 Preparando áudio do vídeo...',
        'progress': '🎵 Normalizando: {percent}%',
        'complete': '✅ Áudio preparado!',
    },
    'CONCAT': {
        'label': '🔗 Concatenando Vídeos',
        'start': '🔗 Juntando seus vídeos...',
        'progress': '🔗 Processando: {current}/{total}',
        'complete': '✅ Vídeos unidos!',
    },
    'SILENCE_CUT': {
        'label': '✂️ Removendo Silêncios',
        'start': '✂️ Identificando silêncios...',
        'progress': '✂️ Cortando: {percent}%',
        'complete': '✅ Silêncios removidos!',
    },
    'TRANSCRIBE': {
        'label': '🎤 Transcrição',
        'start': '🎤 Ouvindo seu vídeo...',
        'progress': '🎤 Identificando palavras: {percent}%',
        'complete': '✅ {count} frases encontradas!',
    },
    'PHRASE_GROUP': {
        'label': '📝 Agrupamento',
        'start': '📝 Organizando frases...',
        'progress': '📝 Agrupando: {current}/{total}',
        'complete': '✅ Frases organizadas!',
    },
    'CLASSIFY': {
        'label': '🏷️ Classificação',
        'start': '🧠 Analisando contexto com IA...',
        'progress': '🏷️ Classificando: {current}/{total}',
        'complete': '✅ Estilos definidos: {stats}',
    },
    'PNG_GEN': {
        'label': '🖼️ Gerando Legendas',
        'start': '🎨 Criando legendas personalizadas...',
        'progress': '🖼️ Gerando: "{word}" ({current}/{total})',
        'complete': '✅ {count} legendas criadas!',
    },
    'SHADOW': {
        'label': '🌓 Sombras',
        'start': '🌓 Aplicando sombras...',
        'progress': '🌓 Processando: {current}/{total}',
        'complete': '✅ Sombras aplicadas!',
    },
    'POSITION': {
        'label': '📐 Posicionamento',
        'start': '📐 Calculando posições...',
        'progress': '📐 Posicionando: {current}/{total}',
        'complete': '✅ Layout definido!',
    },
    'MATTING': {
        'label': '✂️ Recorte de Fundo',
        'start': '✂️ Preparando recorte de pessoa...',
        'progress': '✂️ Processando: {current}/{total} segmentos',
        'complete': '✅ Recorte concluído!',
    },
    'RENDER': {
        'label': '🎬 Renderização',
        'start': '🎬 Montando seu vídeo...',
        'progress': '🎬 Renderizando: {percent}%',
        'complete': '🎉 Vídeo pronto!',
    },
}

# =============================================================================
# 🎨 MENSAGENS DE LOG ESTILIZADAS (Code Theatre)
# =============================================================================

LOG_TEMPLATES = {
    'PNG_GEN': [
        '🎨 Carregando fonte: {font_family} ({weight})',
        '📐 Dimensões: {width}x{height} @ {size_percent}%',
        '🖼️ Renderizando "{word}"...',
        '   └─ Cor: {color}',
        '   └─ Bordas: {border_count} camadas',
        '   └─ Estilo: {style_type}',
        '✅ Legenda gerada!',
    ],
    'MATTING': [
        '☁️ Conectando GPU Cloud...',
        '🧠 Modelo BiRefNet carregado',
        '✂️ Processando frame {current}/{total}',
        '📊 Qualidade: {quality}%',
        '✅ Segmento {segment_id} concluído!',
    ],
    'RENDER': [
        '🎬 Preparando composição...',
        '📦 {subtitle_count} legendas carregadas',
        '🎥 Renderizando frame {current}/{total}',
        '📊 Progresso: {percent}%',
        '☁️ Enviando para storage...',
        '✅ Vídeo salvo!',
    ],
}

# =============================================================================
# 🛡️ FUNÇÕES DE SANITIZAÇÃO
# =============================================================================

def sanitize_text(text: str) -> str:
    """Remove dados sensíveis de um texto."""
    if not text:
        return text
    
    result = text
    
    # Substituir UUIDs por versão curta
    result = re.sub(
        r'([a-f0-9]{8})-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}',
        r'\1...',
        result
    )
    
    # Remover padrões bloqueados
    for pattern in BLOCKED_PATTERNS:
        result = re.sub(pattern, '[REDACTED]', result, flags=re.IGNORECASE)
    
    # Aplicar traduções amigáveis
    for tech_term, friendly_term in FRIENDLY_NAMES.items():
        result = re.sub(
            rf'\b{re.escape(tech_term)}\b',
            friendly_term,
            result,
            flags=re.IGNORECASE
        )
    
    return result


def get_step_message(step: str, event_type: str, data: Dict[str, Any] = None) -> str:
    """
    Retorna mensagem amigável para uma etapa.
    
    Args:
        step: Nome da etapa (ex: 'PNG_GEN')
        event_type: Tipo do evento ('start', 'progress', 'complete')
        data: Dados para interpolação
    
    Returns:
        Mensagem formatada
    """
    step_config = PIPELINE_STEPS.get(step, {})
    template = step_config.get(event_type, f'{step}: {event_type}')
    
    if data:
        try:
            return template.format(**data)
        except KeyError:
            return template
    
    return template


def create_display_event(
    step: str,
    event_type: str,
    current: int = None,
    total: int = None,
    percent: int = None,
    metadata: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Cria evento formatado para o frontend.
    
    Args:
        step: Nome da etapa
        event_type: 'start', 'progress', 'complete', 'error'
        current: Contador atual
        total: Total de itens
        percent: Porcentagem (0-100)
        metadata: Dados extras (word, style_type, etc.)
    
    Returns:
        Evento formatado e sanitizado
    """
    step_config = PIPELINE_STEPS.get(step, {})
    
    # Calcular porcentagem se não fornecida
    if percent is None and current is not None and total is not None and total > 0:
        percent = round((current / total) * 100)
    
    # Preparar dados para interpolação
    format_data = {
        'current': current or 0,
        'total': total or 0,
        'percent': percent or 0,
        'count': total or current or 0,
        **(metadata or {})
    }
    
    # Gerar mensagem
    message = get_step_message(step, event_type, format_data)
    
    event = {
        'step': step,
        'step_label': step_config.get('label', step),
        'event_type': event_type,
        'message': sanitize_text(message),
    }
    
    if current is not None:
        event['current'] = current
    if total is not None:
        event['total'] = total
    if percent is not None:
        event['percent'] = percent
    
    # Adicionar metadata sanitizada
    if metadata:
        safe_metadata = {}
        for key, value in metadata.items():
            if isinstance(value, str):
                safe_metadata[key] = sanitize_text(value)
            elif isinstance(value, (int, float, bool)):
                safe_metadata[key] = value
        event['metadata'] = safe_metadata
    
    return event


def get_log_lines(step: str, phase: str = None, data: Dict[str, Any] = None) -> list:
    """
    Retorna linhas de log estilizadas para Code Theatre.
    
    Args:
        step: Nome da etapa
        phase: Fase específica (opcional)
        data: Dados para interpolação
    
    Returns:
        Lista de linhas formatadas
    """
    templates = LOG_TEMPLATES.get(step, [])
    lines = []
    
    for template in templates:
        try:
            line = template.format(**(data or {}))
            lines.append(sanitize_text(line))
        except KeyError:
            lines.append(sanitize_text(template))
    
    return lines
