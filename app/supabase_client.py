"""
Cliente Supabase compartilhado para evitar imports circulares

🔧 Atualizado: Agora usa Connection Pool para melhor performance
"""
from supabase import create_client, Client
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# 🆕 Importar Connection Pool
from .db import get_db_connection, return_db_connection, get_db_cursor

def get_supabase_client() -> Client:
    """Retorna uma instância do cliente Supabase para operações no banco"""
    # ✅ CORREÇÃO: Custom-API deve conectar diretamente ao PostgREST, não via Kong
    url = os.getenv('SUPABASE_URL', 'http://rest:3000')
    # Use service role key for admin operations  
    key = os.getenv('SERVICE_ROLE_KEY')  # ✅ CORREÇÃO: Usar mesmo nome que projects.py
    if not key:
        raise ValueError("SERVICE_ROLE_KEY environment variable is required")
    
    print(f"🔍 [SUPABASE_CLIENT] Conectando com URL: {url}")
    print(f"🔍 [SUPABASE_CLIENT] Service key presente: {bool(key)}")
    
    return create_client(url, key)

def get_direct_db_connection():
    """
    Retorna conexão do pool ao PostgreSQL.
    
    ⚠️ IMPORTANTE: Sempre devolva a conexão com return_db_connection()!
    
    Ou melhor, use get_db_cursor() context manager.
    """
    # 🆕 Usar Connection Pool em vez de nova conexão
    conn = get_db_connection()
    
    # Definir search_path para incluir schema 'auth'
    cursor = conn.cursor()
    cursor.execute("SET search_path TO public, auth;")
    cursor.close()
    
    return conn


def close_direct_db_connection(conn):
    """
    Devolve conexão ao pool.
    """
    return_db_connection(conn)


def save_message_direct(
    conversation_id: str, 
    user_id: str, 
    sender: str, 
    content: str, 
    component_type: str = None,
    component_props: dict = None,
    project_id: str = None,
    prompt_version: str = None
):
    """
    Salva uma mensagem diretamente no banco de dados com TODOS os campos
    
    Args:
        conversation_id: ID da conversa
        user_id: ID do usuário
        sender: 'user' ou 'assistant'
        content: Conteúdo da mensagem
        component_type: Tipo de componente (opcional)
        component_props: Props do componente incluindo buttons (opcional)
        project_id: ID do projeto (opcional)
        prompt_version: Versão do prompt usado (opcional)
    
    Returns:
        dict: Mensagem salva com message_id
    """
    # 🆕 FIX: Converter strings vazias em None para campos UUID
    if project_id == '':
        project_id = None
    
    try:
        # 🆕 Usar Connection Pool com context manager
        with get_db_cursor() as cursor:
            # Definir search_path
            cursor.execute("SET search_path TO public, auth;")
            
            # Converter component_props para JSON string se não for None
            component_props_json = json.dumps(component_props) if component_props is not None else None
            
            # 🔧 FIX 30/Jan/2026: Usar chatbot_messages (V4) ao invés de conversations_messages (V2)
            import uuid
            msg_id = str(uuid.uuid4())
            query = """
            INSERT INTO chatbot_messages (
                id,
                message_id,
                conversation_id, 
                sender, 
                content, 
                component_type, 
                component_props,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING message_id, conversation_id, sender, content, component_type, component_props, created_at
            """
            
            cursor.execute(query, (
                msg_id,
                msg_id,
                conversation_id, 
                sender, 
                content, 
                component_type,
                component_props_json
            ))
            result = cursor.fetchone()
            
            # Log mais detalhado
            buttons_count = len(component_props.get('buttons', [])) if component_props and 'buttons' in component_props else 0
            print(f"✅ [POOL_DB] Mensagem salva: {sender} -> {conversation_id} (botões: {buttons_count})")
            
            # 🆕 Emitir evento SSE para mensagens do assistant
            if sender == 'assistant' and result:
                try:
                    from app.routes.chat_sse import emit_new_message
                    buttons = component_props.get('buttons', []) if component_props else []
                    emit_new_message(
                        conversation_id=conversation_id,
                        message_id=str(result['message_id']),
                        sender=sender,
                        content=content,
                        component_type=component_type,
                        component_props=component_props,
                        buttons=buttons
                    )
                    print(f"📡 [SSE] Evento new_message emitido para {conversation_id[:8]}...")
                except Exception as sse_err:
                    print(f"⚠️ [SSE] Erro ao emitir evento: {sse_err}")
            
            return dict(result) if result else None
        
    except Exception as e:
        print(f"❌ [POOL_DB] Erro ao salvar mensagem: {e}")
        raise

def query_projects_direct(user_id: str, limit: int = 20, offset: int = 0):
    """
    Consulta projetos do usuário usando Connection Pool
    """
    try:
        # 🆕 Usar Connection Pool com context manager
        with get_db_cursor() as cursor:
            cursor.execute("SET search_path TO public, auth;")
            
            query = """
            SELECT project_id, name, status, project_type, created_at, conversation_id
            FROM projects 
            WHERE user_id = %s 
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
            """
            
            cursor.execute(query, (user_id, limit, offset))
            results = cursor.fetchall()
            
            # Converter para formato compatível com Supabase
            items = [dict(row) for row in results]
            
            print(f"🔍 [POOL_DB] Encontrados {len(items)} projetos para usuário {user_id}")
            return {'data': items, 'error': None}
        
    except Exception as e:
        print(f"❌ [POOL_DB] Erro na consulta direta: {str(e)}")
        return {'data': None, 'error': str(e)}
