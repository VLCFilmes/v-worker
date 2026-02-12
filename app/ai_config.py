"""
AI Config Module

Módulo helper para interagir com o AI Control Center.
Fornece funções para:
1. Buscar configurações de serviços de IA
2. Registrar uso de tokens
3. Descriptografar API keys

Uso:
    from app.ai_config import get_ai_config, log_token_usage
    
    # Buscar configuração
    config = get_ai_config('content_planner')
    
    # Usar configuração
    response = openai.ChatCompletion.create(
        model=config['model']['name'],
        api_key=config['api_key'],
        ...
    )
    
    # Registrar uso
    log_token_usage(
        service_key='content_planner',
        provider_name='openai',
        model_name='gpt-4-turbo',
        input_tokens=response.usage.prompt_tokens,
        output_tokens=response.usage.completion_tokens
    )
"""

import requests
import logging
import os
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# ============================================================================
# Configurações
# ============================================================================

API_BASE = os.getenv('KONG_INTERNAL_URL', 'http://localhost:18000')
SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
DB_URL = os.getenv('DATABASE_URL')  # Para acesso direto ao banco

# Se True, vai direto ao banco sem tentar API (evita logs de erro desnecessários)
USE_DIRECT_DB = os.getenv('AI_CONFIG_USE_DIRECT_DB', 'false').lower() in ('true', '1', 'yes')

# ============================================================================
# Função Principal: Buscar Configuração de Serviço
# ============================================================================

def get_ai_config(service_key: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
    """
    Busca configuração de um serviço no AI Control Center.
    
    Pode buscar via API (recomendado) ou diretamente no banco (fallback).
    
    Args:
        service_key: Identificador do serviço (ex: 'content_planner')
        use_cache: Se True, usa cache em memória (TODO: implementar)
    
    Returns:
        dict com estrutura:
        {
            'service_key': str,
            'provider': {
                'name': str,
                'display_name': str,
                'api_base_url': str
            },
            'model': {
                'name': str,
                'display_name': str,
                'context_window': int
            },
            'api_key': str (descriptografada),
            'parameters': {
                'temperature': float,
                'max_tokens': int,
                'top_p': float
            }
        }
        
        Retorna None se serviço não encontrado ou não configurado.
    
    Example:
        >>> config = get_ai_config('content_planner')
        >>> if config:
        >>>     print(f"Using {config['model']['name']} from {config['provider']['name']}")
    """
    try:
        # Se configurado para ir direto ao banco, pula a API
        if USE_DIRECT_DB:
            config = _get_config_via_db(service_key)
            if config:
                logger.info(f"✅ Configuração carregada via DB: {service_key} -> {config.get('model', {}).get('name', 'N/A')}")
                return config
            logger.error(f"❌ Configuração não encontrada para serviço: {service_key}")
            return None
        
        # Método 1: Buscar via API (RECOMENDADO)
        config = _get_config_via_api(service_key)
        
        if config:
            logger.info(f"✅ Configuração carregada via API: {service_key} -> {config.get('model', {}).get('name', 'N/A')}")
            return config
        
        # Método 2: Buscar diretamente no banco (FALLBACK)
        logger.warning(f"⚠️ API falhou, tentando acesso direto ao banco...")
        config = _get_config_via_db(service_key)
        
        if config:
            logger.info(f"✅ Configuração carregada via DB: {service_key} -> {config.get('model', {}).get('name', 'N/A')}")
            return config
        
        logger.error(f"❌ Configuração não encontrada para serviço: {service_key}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar configuração: {str(e)}", exc_info=True)
        return None


def _get_config_via_api(service_key: str) -> Optional[Dict[str, Any]]:
    """Busca configuração via API endpoint (método preferido)."""
    try:
        url = f"{API_BASE}/api/ai-config/service/{service_key}"
        
        response = requests.get(
            url,
            headers={
                'apikey': SERVICE_ROLE_KEY,
                'Authorization': f'Bearer {SERVICE_ROLE_KEY}'
            },
            timeout=5
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            logger.warning(f"⚠️ Serviço não encontrado: {service_key}")
            return None
        else:
            logger.error(f"❌ Erro API: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout ao chamar API de configuração")
        return None
    except requests.exceptions.ConnectionError:
        logger.error("❌ Erro de conexão com API de configuração")
        return None
    except Exception as e:
        logger.error(f"❌ Erro ao chamar API: {str(e)}")
        return None


def _get_config_via_db(service_key: str) -> Optional[Dict[str, Any]]:
    """Busca configuração diretamente no banco (fallback)."""
    if not DB_URL:
        logger.error("❌ DATABASE_URL não configurada")
        return None
    
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query para buscar configuração completa
        cursor.execute("""
            SELECT 
                s.service_key,
                p.name AS provider_name,
                p.display_name AS provider_display_name,
                p.api_base_url,
                m.model_name,
                m.display_name AS model_display_name,
                m.context_window,
                c.api_key_encrypted,
                c.temperature,
                c.max_tokens,
                c.top_p
            FROM ai_services s
            JOIN ai_service_configs c ON s.id = c.service_id AND c.is_active = true
            JOIN ai_providers p ON c.provider_id = p.id
            JOIN ai_models m ON c.model_id = m.id
            WHERE s.service_key = %s AND s.managed_by = 'site_admin'
        """, (service_key,))
        
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            logger.warning(f"⚠️ Configuração não encontrada no banco: {service_key}")
            return None
        
        # Descriptografar API key
        api_key = _decrypt_api_key(row['api_key_encrypted'])
        
        if not api_key:
            logger.error(f"❌ Falha ao descriptografar API key para: {service_key}")
            return None
        
        # Montar estrutura de resposta
        return {
            'service_key': row['service_key'],
            'provider': {
                'name': row['provider_name'],
                'display_name': row['provider_display_name'],
                'api_base_url': row['api_base_url']
            },
            'model': {
                'name': row['model_name'],
                'display_name': row['model_display_name'],
                'context_window': row['context_window']
            },
            'api_key': api_key,
            'parameters': {
                'temperature': float(row['temperature']),
                'max_tokens': row['max_tokens'],
                'top_p': float(row['top_p'])
            }
        }
        
    except psycopg2.Error as e:
        logger.error(f"❌ Erro de banco de dados: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"❌ Erro ao buscar config via DB: {str(e)}")
        return None


def _decrypt_api_key(encrypted_key: str) -> Optional[str]:
    """
    Descriptografa API key usando pgcrypto.
    
    TODO: Implementar descriptografia real.
    Por enquanto, retorna a key como está (assumindo que não está criptografada em dev).
    """
    # TODO: Implementar descriptografia com pgcrypto
    # Por enquanto, retornar como está
    return encrypted_key


# ============================================================================
# Função Principal: Registrar Uso de Tokens
# ============================================================================

def log_token_usage(
    service_key: str,
    provider_name: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    user_id: Optional[str] = None,
    project_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Registra uso de tokens no AI Control Center.
    
    O custo é calculado automaticamente no banco de dados baseado nos preços
    do modelo configurado.
    
    Args:
        service_key: Identificador do serviço (ex: 'content_planner')
        provider_name: Nome do provedor (ex: 'openai', 'anthropic')
        model_name: Nome do modelo (ex: 'gpt-4-turbo', 'claude-3-5-sonnet')
        input_tokens: Número de tokens de input
        output_tokens: Número de tokens de output
        user_id: ID do usuário (opcional)
        project_id: ID do projeto (opcional)
        conversation_id: ID da conversa (opcional)
        metadata: Dados adicionais (opcional) - dict serializado como JSON
    
    Returns:
        bool: True se registrado com sucesso, False caso contrário
    
    Example:
        >>> log_token_usage(
        >>>     service_key='content_planner',
        >>>     provider_name='openai',
        >>>     model_name='gpt-4-turbo',
        >>>     input_tokens=500,
        >>>     output_tokens=300,
        >>>     metadata={'endpoint': '/api/ai/generate-text'}
        >>> )
        True
    """
    try:
        # Método 1: Registrar via API (RECOMENDADO)
        success = _log_usage_via_api(
            service_key=service_key,
            provider_name=provider_name,
            model_name=model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            user_id=user_id,
            project_id=project_id,
            conversation_id=conversation_id,
            metadata=metadata
        )
        
        if success:
            logger.info(
                f"✅ Uso registrado: {service_key} -> "
                f"{input_tokens + output_tokens} tokens "
                f"({provider_name}/{model_name})"
            )
            return True
        
        # Método 2: Registrar diretamente no banco (FALLBACK)
        logger.warning("⚠️ API falhou, tentando acesso direto ao banco...")
        success = _log_usage_via_db(
            service_key=service_key,
            provider_name=provider_name,
            model_name=model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            user_id=user_id,
            project_id=project_id,
            conversation_id=conversation_id,
            metadata=metadata
        )
        
        if success:
            logger.info(f"✅ Uso registrado via DB: {service_key}")
            return True
        
        logger.error(f"❌ Falha ao registrar uso: {service_key}")
        return False
        
    except Exception as e:
        logger.error(f"❌ Erro ao registrar uso: {str(e)}", exc_info=True)
        return False


def _log_usage_via_api(
    service_key: str,
    provider_name: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    user_id: Optional[str] = None,
    project_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """Registra uso via API endpoint (método preferido)."""
    try:
        url = f"{API_BASE}/api/ai-config/token-usage"
        
        payload = {
            'service_key': service_key,
            'provider_name': provider_name,
            'model_name': model_name,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'user_id': user_id,
            'project_id': project_id,
            'conversation_id': conversation_id,
            'metadata': metadata or {}
        }
        
        response = requests.post(
            url,
            json=payload,
            headers={
                'apikey': SERVICE_ROLE_KEY,
                'Authorization': f'Bearer {SERVICE_ROLE_KEY}',
                'Content-Type': 'application/json'
            },
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            cost = data.get('estimated_cost', 0)
            logger.debug(f"💰 Custo estimado: ${cost:.4f}")
            return True
        else:
            logger.warning(f"⚠️ Falha ao registrar uso via API: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        logger.warning("⚠️ Timeout ao registrar uso via API")
        return False
    except requests.exceptions.ConnectionError:
        logger.warning("⚠️ Erro de conexão ao registrar uso via API")
        return False
    except Exception as e:
        logger.warning(f"⚠️ Erro ao registrar uso via API: {str(e)}")
        return False


def _log_usage_via_db(
    service_key: str,
    provider_name: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    user_id: Optional[str] = None,
    project_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """Registra uso diretamente no banco (fallback)."""
    if not DB_URL:
        logger.error("❌ DATABASE_URL não configurada")
        return False
    
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        # Buscar service_id
        cursor.execute(
            "SELECT id FROM ai_services WHERE service_key = %s",
            (service_key,)
        )
        row = cursor.fetchone()
        
        if not row:
            logger.error(f"❌ Serviço não encontrado: {service_key}")
            cursor.close()
            conn.close()
            return False
        
        service_id = row[0]
        
        # Inserir registro de uso
        import json
        cursor.execute("""
            INSERT INTO ai_token_usage (
                service_id,
                user_id,
                project_id,
                conversation_id,
                provider_name,
                model_name,
                input_tokens,
                output_tokens,
                request_metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            service_id,
            user_id,
            project_id,
            conversation_id,
            provider_name,
            model_name,
            input_tokens,
            output_tokens,
            json.dumps(metadata) if metadata else None
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.Error as e:
        logger.error(f"❌ Erro de banco de dados ao registrar uso: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"❌ Erro ao registrar uso via DB: {str(e)}")
        return False


# ============================================================================
# Funções Auxiliares
# ============================================================================

def get_available_models(provider_name: Optional[str] = None) -> list:
    """
    Lista modelos disponíveis (opcionalmente filtrado por provedor).
    
    Args:
        provider_name: Nome do provedor (opcional)
    
    Returns:
        Lista de modelos com informações completas
    """
    # TODO: Implementar
    pass


def validate_service_config(service_key: str) -> Dict[str, Any]:
    """
    Valida se um serviço está corretamente configurado.
    
    Args:
        service_key: Identificador do serviço
    
    Returns:
        dict com status da validação:
        {
            'is_valid': bool,
            'errors': list,
            'warnings': list
        }
    """
    # TODO: Implementar
    pass


# ============================================================================
# Cache (TODO: Implementar)
# ============================================================================

# TODO: Implementar cache em memória para configurações
# Pode usar Redis ou simples dict com TTL
# Isso reduz chamadas ao banco/API

_config_cache = {}
CACHE_TTL = 300  # 5 minutos

def _get_from_cache(key: str) -> Optional[Dict[str, Any]]:
    """Busca configuração do cache."""
    # TODO: Implementar
    pass

def _set_in_cache(key: str, value: Dict[str, Any], ttl: int = CACHE_TTL):
    """Salva configuração no cache."""
    # TODO: Implementar
    pass

