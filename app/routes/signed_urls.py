"""
🔐 SIGNED URLS API - vinicius.ai
Geração de URLs assinadas temporárias para assets privados no Backblaze B2
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import os
import requests

# Criar blueprint
signed_urls_bp = Blueprint('signed_urls', __name__)

# Configuração B2
B2_KEY_ID = os.environ.get('B2_APPLICATION_KEY_ID')
B2_KEY = os.environ.get('B2_APPLICATION_KEY')


@signed_urls_bp.route('/api/assets/signed-url', methods=['POST'])
def generate_signed_url():
    """
    POST /api/assets/signed-url
    Gera URL assinada temporária para um asset privado
    
    Body: {
        bucket_name: string,
        file_path: string,
        duration_seconds: number (opcional, padrão: 3600 = 1 hora)
    }
    
    Response: {
        url: string,
        authorization_token: string,
        expires_at: string,
        duration_seconds: number
    }
    """
    try:
        # ⚠️ SEGURANÇA: Em produção, Kong valida SERVICE_ROLE_KEY
        # Em desenvolvimento local (porta 5021), validação opcional
        
        data = request.get_json()
        
        # Validar campos obrigatórios
        if 'bucket_name' not in data or 'file_path' not in data:
            return jsonify({'error': 'Missing required fields: bucket_name, file_path'}), 400
        
        bucket_name = data['bucket_name']
        file_path = data['file_path']
        duration_seconds = data.get('duration_seconds', 3600)  # Padrão: 1 hora
        
        # Usar b2sdk para autorizar e obter bucket info
        from b2sdk.v2 import InMemoryAccountInfo, B2Api
        
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        
        # Autorizar
        b2_api.authorize_account('production', B2_KEY_ID, B2_KEY)
        
        # Buscar bucket
        bucket = b2_api.get_bucket_by_name(bucket_name)
        
        # Obter dados necessários
        account_auth_token = b2_api.account_info.get_account_auth_token()
        api_url = b2_api.account_info.get_api_url()
        download_url = b2_api.account_info.get_download_url()
        
        # Gerar token de download temporário via API REST do B2
        response = requests.post(
            f'{api_url}/b2api/v2/b2_get_download_authorization',
            headers={'Authorization': account_auth_token},
            json={
                'bucketId': bucket.id_,
                'fileNamePrefix': 'asset-library/',  # Limitar ao prefixo asset-library
                'validDurationInSeconds': duration_seconds
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"B2 API error: {response.text}")
        
        b2_response = response.json()
        download_token = b2_response['authorizationToken']
        
        # Construir URL completa
        full_url = f"{download_url}/file/{bucket_name}/{file_path}"
        
        # Calcular data de expiração
        expires_at = (datetime.utcnow() + timedelta(seconds=duration_seconds)).isoformat() + 'Z'
        
        print(f"✅ Signed URL generated for: {file_path}")
        print(f"   Token: {download_token[:50]}...")
        print(f"   Expires: {expires_at}")
        
        return jsonify({
            'url': full_url,
            'authorization_token': download_token,
            'expires_at': expires_at,
            'duration_seconds': duration_seconds
        }), 200
        
    except Exception as e:
        print(f"❌ Error generating signed URL: {str(e)}")
        return jsonify({'error': str(e)}), 500


@signed_urls_bp.route('/api/assets/batch-signed-urls', methods=['POST'])
def generate_batch_signed_urls():
    """
    POST /api/assets/batch-signed-urls
    Gera múltiplas URLs assinadas de uma vez (otimizado)
    
    Body: {
        bucket_name: string,
        file_paths: string[],
        duration_seconds: number (opcional)
    }
    
    Response: {
        urls: [{file_path, url, authorization_token}],
        expires_at: string,
        duration_seconds: number
    }
    """
    try:
        data = request.get_json()
        
        if 'bucket_name' not in data or 'file_paths' not in data:
            return jsonify({'error': 'Missing required fields: bucket_name, file_paths'}), 400
        
        bucket_name = data['bucket_name']
        file_paths = data['file_paths']
        duration_seconds = data.get('duration_seconds', 3600)
        
        if not isinstance(file_paths, list):
            return jsonify({'error': 'file_paths must be an array'}), 400
        
        # Usar b2sdk
        from b2sdk.v2 import InMemoryAccountInfo, B2Api
        
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        b2_api.authorize_account('production', B2_KEY_ID, B2_KEY)
        
        # Buscar bucket
        bucket = b2_api.get_bucket_by_name(bucket_name)
        
        # Obter dados necessários
        account_auth_token = b2_api.account_info.get_account_auth_token()
        api_url = b2_api.account_info.get_api_url()
        download_url = b2_api.account_info.get_download_url()
        
        # Gerar um único token de download que funciona para todos os arquivos
        response = requests.post(
            f'{api_url}/b2api/v2/b2_get_download_authorization',
            headers={'Authorization': account_auth_token},
            json={
                'bucketId': bucket.id_,
                'fileNamePrefix': 'asset-library/',
                'validDurationInSeconds': duration_seconds
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"B2 API error: {response.text}")
        
        b2_response = response.json()
        download_token = b2_response['authorizationToken']
        
        expires_at = (datetime.utcnow() + timedelta(seconds=duration_seconds)).isoformat() + 'Z'
        
        # Gerar URLs para todos os arquivos
        signed_urls = []
        for file_path in file_paths:
            full_url = f"{download_url}/file/{bucket_name}/{file_path}"
            signed_urls.append({
                'file_path': file_path,
                'url': full_url,
                'authorization_token': download_token  # Mesmo token para todos
            })
        
        print(f"✅ Batch signed URLs generated for {len(file_paths)} files")
        
        return jsonify({
            'urls': signed_urls,
            'expires_at': expires_at,
            'duration_seconds': duration_seconds
        }), 200
        
    except Exception as e:
        print(f"❌ Error generating batch signed URLs: {str(e)}")
        return jsonify({'error': str(e)}), 500
