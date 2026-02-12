"""
📦 B2 Client - Cliente Backblaze B2 para Upload/Download

Usado pelo Video Orchestrator e outros serviços para salvar
arquivos diretamente no B2 (vídeos mid-production, exports, metadata).
"""

import os
import json
import logging
import requests
from typing import Optional, Dict, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Configurações B2
B2_KEY_ID = os.environ.get('B2_APPLICATION_KEY_ID')
B2_KEY = os.environ.get('B2_APPLICATION_KEY')
B2_BUCKET_NAME = os.environ.get('B2_BUCKET_NAME', 'vinicius-ai-cdn-global')


class B2Client:
    """
    Cliente para operações no Backblaze B2.
    
    Encapsula autenticação e operações de upload/download.
    """
    
    def __init__(self):
        self._api = None
        self._bucket = None
        self._auth_token = None
        self._api_url = None
        
    def _get_api(self):
        """Inicializa a API B2 com lazy loading."""
        if self._api is None:
            try:
                from b2sdk.v2 import B2Api, InMemoryAccountInfo
                
                info = InMemoryAccountInfo()
                self._api = B2Api(info)
                self._api.authorize_account('production', B2_KEY_ID, B2_KEY)
                logger.info("[B2Client] ✅ API B2 autorizada")
                
            except Exception as e:
                logger.error(f"[B2Client] ❌ Erro ao autorizar B2: {e}")
                raise
        return self._api
    
    def _get_bucket(self, bucket_name: str = None):
        """Obtém referência ao bucket."""
        bucket_name = bucket_name or B2_BUCKET_NAME
        
        if self._bucket is None or self._bucket.name != bucket_name:
            api = self._get_api()
            self._bucket = api.get_bucket_by_name(bucket_name)
            logger.info(f"[B2Client] 📦 Bucket obtido: {bucket_name}")
            
        return self._bucket
    
    def _generate_signed_url_internal(self, bucket, file_path: str, duration_seconds: int = 86400) -> str:
        """
        Gera URL assinada para bucket privado.
        
        Args:
            bucket: Referência ao bucket B2
            file_path: Caminho do arquivo no bucket
            duration_seconds: Validade em segundos (default: 24 horas)
            
        Returns:
            URL com token de autorização
        """
        try:
            api = self._get_api()
            
            # Obter informações de autenticação
            account_auth_token = api.account_info.get_account_auth_token()
            api_url = api.account_info.get_api_url()
            download_url_base = api.account_info.get_download_url()
            
            # Extrair prefixo do diretório
            path_parts = file_path.split('/')
            prefix = '/'.join(path_parts[:-1]) + '/' if len(path_parts) > 1 else ''
            
            # Gerar token de download via API B2
            response = requests.post(
                f"{api_url}/b2api/v2/b2_get_download_authorization",
                headers={'Authorization': account_auth_token},
                json={
                    'bucketId': bucket.id_,
                    'fileNamePrefix': prefix,
                    'validDurationInSeconds': duration_seconds
                }
            )
            
            download_token = response.json()['authorizationToken']
            
            # Construir URL completa com token
            signed_url = f"{download_url_base}/file/{bucket.name}/{file_path}?Authorization={download_token}"
            
            logger.info(f"[B2Client] 🔐 URL assinada gerada (válida por {duration_seconds}s)")
            return signed_url
            
        except Exception as e:
            logger.error(f"[B2Client] ❌ Erro ao gerar URL assinada: {e}")
            # Fallback: URL sem token (não vai funcionar para bucket privado)
            return f"https://f002.backblazeb2.com/file/{bucket.name}/{file_path}"
    
    def upload_file_from_url(
        self,
        source_url: str,
        destination_path: str,
        bucket_name: str = None,
        content_type: str = "video/mp4"
    ) -> Dict[str, Any]:
        """
        Faz upload de um arquivo a partir de uma URL.
        
        Útil para salvar vídeos do v-services no B2.
        
        Args:
            source_url: URL do arquivo para download
            destination_path: Path de destino no B2
            bucket_name: Nome do bucket (default: B2_BUCKET_NAME)
            content_type: MIME type do arquivo
            
        Returns:
            Dict com file_id, file_name, public_url
        """
        bucket = self._get_bucket(bucket_name)
        
        logger.info(f"[B2Client] 📥 Baixando de: {source_url[:60]}...")
        
        # Baixar arquivo
        response = requests.get(source_url, stream=True, timeout=300)
        response.raise_for_status()
        
        file_data = response.content
        file_size = len(file_data)
        
        logger.info(f"[B2Client] 📤 Fazendo upload para B2: {destination_path} ({file_size/1024/1024:.1f} MB)")
        
        # Upload para B2
        file_info = bucket.upload_bytes(
            data_bytes=file_data,
            file_name=destination_path,
            content_type=content_type
        )
        
        # Gerar URL assinada (bucket é privado)
        signed_url = self._generate_signed_url_internal(bucket, destination_path)
        
        logger.info(f"[B2Client] ✅ Upload concluído: {destination_path}")
        
        return {
            'file_id': file_info.id_,
            'file_name': file_info.file_name,
            'file_size': file_size,
            'public_url': signed_url,  # URL assinada com token
            'bucket_name': bucket.name
        }
    
    def upload_bytes(
        self,
        data: bytes,
        destination_path: str,
        bucket_name: str = None,
        content_type: str = "application/octet-stream"
    ) -> Dict[str, Any]:
        """
        Faz upload de bytes diretamente para o B2.
        
        Args:
            data: Bytes para upload
            destination_path: Path de destino no B2
            bucket_name: Nome do bucket
            content_type: MIME type
            
        Returns:
            Dict com file_id, file_name, public_url
        """
        bucket = self._get_bucket(bucket_name)
        
        logger.info(f"[B2Client] 📤 Fazendo upload direto: {destination_path} ({len(data)/1024:.1f} KB)")
        
        file_info = bucket.upload_bytes(
            data_bytes=data,
            file_name=destination_path,
            content_type=content_type
        )
        
        # Gerar URL assinada (bucket é privado)
        signed_url = self._generate_signed_url_internal(bucket, destination_path)
        
        return {
            'file_id': file_info.id_,
            'file_name': file_info.file_name,
            'file_size': len(data),
            'public_url': signed_url,  # URL assinada com token
            'bucket_name': bucket.name
        }
    
    def upload_json(
        self,
        data: dict,
        destination_path: str,
        bucket_name: str = None
    ) -> Dict[str, Any]:
        """
        Faz upload de um dict como JSON.
        
        Útil para salvar metadados de projeto.
        
        Args:
            data: Dict para serializar como JSON
            destination_path: Path de destino no B2
            bucket_name: Nome do bucket
            
        Returns:
            Dict com file_id, file_name, public_url
        """
        # Adicionar metadata
        data['_saved_at'] = datetime.now(timezone.utc).isoformat()
        
        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        
        return self.upload_bytes(
            data=json_bytes,
            destination_path=destination_path,
            bucket_name=bucket_name,
            content_type='application/json'
        )
    
    def delete_file(
        self,
        file_path: str,
        bucket_name: str = None
    ) -> bool:
        """
        Deleta um arquivo do B2.
        
        Args:
            file_path: Path do arquivo no B2
            bucket_name: Nome do bucket
            
        Returns:
            True se deletado com sucesso
        """
        try:
            bucket = self._get_bucket(bucket_name)
            
            # Buscar versão do arquivo
            file_versions = list(bucket.ls(file_path, latest_only=True))
            
            if not file_versions:
                logger.warning(f"[B2Client] ⚠️ Arquivo não encontrado: {file_path}")
                return False
            
            file_version = file_versions[0]
            bucket.delete_file_version(file_version[0].id_, file_version[0].file_name)
            
            logger.info(f"[B2Client] 🗑️ Arquivo deletado: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"[B2Client] ❌ Erro ao deletar: {e}")
            return False
    
    def generate_signed_url(
        self,
        file_path: str,
        bucket_name: str = None,
        valid_duration_seconds: int = 3600
    ) -> Optional[str]:
        """
        Gera URL assinada temporária para download.
        
        Args:
            file_path: Path do arquivo no B2
            bucket_name: Nome do bucket
            valid_duration_seconds: Validade em segundos (default: 1 hora)
            
        Returns:
            URL assinada ou None se falhar
        """
        try:
            bucket = self._get_bucket(bucket_name)
            
            logger.info(f"[B2Client] 🔍 Buscando arquivo: {file_path}")
            
            # Buscar arquivo
            file_versions = list(bucket.ls(file_path, latest_only=True))
            
            if not file_versions:
                logger.warning(f"[B2Client] ⚠️ Arquivo não encontrado: {file_path}")
                # Tentar gerar URL mesmo assim (o arquivo pode existir mas ls não encontrou)
                logger.info(f"[B2Client] 🔄 Tentando gerar URL sem verificar existência...")
            else:
                logger.info(f"[B2Client] ✅ Arquivo encontrado: {file_versions[0]}")
            
            # Gerar URL assinada (usando prefixo do diretório para maior compatibilidade)
            # Usar só o prefixo do diretório (sem o nome do arquivo) para evitar problemas
            path_parts = file_path.split('/')
            prefix = '/'.join(path_parts[:-1]) + '/' if len(path_parts) > 1 else ''
            
            download_auth = bucket.get_download_authorization(
                file_name_prefix=prefix,  # Prefixo do diretório
                valid_duration_in_seconds=valid_duration_seconds
            )
            
            # Usar f001 (padrão do Backblaze) em vez de f002
            signed_url = f"https://f001.backblazeb2.com/file/{bucket.name}/{file_path}?Authorization={download_auth}"
            
            logger.info(f"[B2Client] ✅ URL assinada gerada: {signed_url[:80]}...")
            
            return signed_url
            
        except Exception as e:
            logger.error(f"[B2Client] ❌ Erro ao gerar URL assinada: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def file_exists(
        self,
        file_path: str,
        bucket_name: str = None
    ) -> bool:
        """
        Verifica se um arquivo existe no B2.
        
        Args:
            file_path: Path do arquivo
            bucket_name: Nome do bucket
            
        Returns:
            True se existe
        """
        try:
            bucket = self._get_bucket(bucket_name)
            file_versions = list(bucket.ls(file_path, latest_only=True))
            return len(file_versions) > 0
        except:
            return False


# Singleton
_b2_client = None

def get_b2_client() -> B2Client:
    """Obtém instância singleton do cliente B2."""
    global _b2_client
    if _b2_client is None:
        _b2_client = B2Client()
    return _b2_client

