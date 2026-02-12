"""
Auth Decorators - Proteção de rotas admin
Decorator @admin_required para proteger endpoints sensíveis

Suporta dois métodos de autenticação:
1. JWT Token do admin no header Authorization
2. SERVICE_ROLE_KEY no header apikey (para comunicação backend-to-backend)
"""

from functools import wraps
from flask import request, jsonify
import jwt
import os

# Configuração de Admin - Lê do ambiente com fallback para valores de desenvolvimento
JWT_SECRET = os.environ.get('JWT_SECRET', os.environ.get('GOTRUE_JWT_SECRET', 'a5bee558c41f3b0c242dceadeb8abe1612f9f18b'))
ADMIN_UUID = os.environ.get('ADMIN_UUID', '8d04a8bf-0b80-48fa-9fb1-0b42dcb36a11')  # fotovinicius2@gmail.com
SERVICE_ROLE_KEY = os.environ.get('SERVICE_ROLE_KEY', '')


def is_service_role(apikey: str) -> bool:
    """
    Verifica se a apikey é a SERVICE_ROLE_KEY válida.
    Usado para autenticação backend-to-backend (ex: site_admin -> custom-api).
    
    Args:
        apikey: Valor do header 'apikey'
    
    Returns:
        True se é a SERVICE_ROLE_KEY válida, False caso contrário
    """
    if not apikey or not SERVICE_ROLE_KEY:
        return False
    return apikey == SERVICE_ROLE_KEY


def is_admin(auth_header: str) -> bool:
    """
    Verifica se o token JWT pertence ao admin.
    
    Args:
        auth_header: Header Authorization completo (ex: "Bearer eyJ...")
    
    Returns:
        True se o usuário é admin, False caso contrário
    """
    if not auth_header:
        return False
    
    try:
        # Remove "Bearer " se presente
        token = auth_header.replace('Bearer ', '').strip()
        if not token:
            return False
        
        # Decodifica o JWT
        decoded = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        user_id = decoded.get('sub')
        
        # Verifica se é o admin
        return user_id == ADMIN_UUID
    
    except jwt.ExpiredSignatureError:
        print('🔒 Admin check: Token expirado')
        return False
    except jwt.InvalidTokenError as e:
        print(f'🔒 Admin check: Token inválido - {e}')
        return False
    except Exception as e:
        print(f'🔒 Admin check: Erro inesperado - {e}')
        return False


def admin_required(f):
    """
    Decorator que protege uma rota, exigindo autenticação de admin.
    
    Suporta dois métodos:
    1. Header 'Authorization: Bearer <jwt_token>' com token do admin
    2. Header 'apikey: <service_role_key>' para comunicação backend-to-backend
    
    Uso:
        @admin_bp.route('/admin/users', methods=['GET'])
        @admin_required
        def get_users():
            ...
    
    Retorna 401 se não for admin, caso contrário executa a função.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        apikey = request.headers.get('apikey', '')
        
        # Método 1: SERVICE_ROLE_KEY (backend-to-backend)
        if is_service_role(apikey):
            print(f'✅ Service Role autenticado - acessando {request.path}')
            return f(*args, **kwargs)
        
        # Método 2: JWT do admin
        if is_admin(auth_header):
            print(f'✅ Admin JWT autenticado - acessando {request.path}')
            return f(*args, **kwargs)
        
        # Nenhum método funcionou
        print(f'🚫 Acesso negado a {request.path} - Admin requerido')
        return jsonify({
            'error': 'Acesso negado: Admin requerido',
            'path': request.path,
            'hint': 'Use um token JWT válido de admin no header Authorization, ou apikey com SERVICE_ROLE_KEY'
        }), 401
    
    return decorated_function


def dev_only(f):
    """
    Decorator que protege rotas de desenvolvimento.
    Exige admin + ambiente de dev (opcional, por enquanto só admin).
    
    Suporta os mesmos métodos de autenticação que admin_required.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        apikey = request.headers.get('apikey', '')
        
        # Método 1: SERVICE_ROLE_KEY (backend-to-backend)
        if is_service_role(apikey):
            print(f'⚠️ DEV ROUTE: Service Role acessando {request.path}')
            return f(*args, **kwargs)
        
        # Método 2: JWT do admin
        if is_admin(auth_header):
            print(f'⚠️ DEV ROUTE: Admin JWT acessando {request.path}')
            return f(*args, **kwargs)
        
        # Nenhum método funcionou
        print(f'🚫 Acesso negado a {request.path} - Dev/Admin requerido')
        return jsonify({
            'error': 'Acesso negado: Rota de desenvolvimento requer admin',
            'path': request.path
        }), 401
    
    return decorated_function

