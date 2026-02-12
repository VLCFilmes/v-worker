"""
🎬 Render Service - Envia payload para renderização no v-editor

Este serviço é responsável por:
1. Receber o payload final do PayloadBuilderService
2. Chamar a API do v-editor para renderização
3. Acompanhar status e receber resultado via webhook

🆕 v2.9.180 (23/Jan/2026): Suporte a nova estrutura de paths no B2
- Envia configuração de upload estruturado para o v-editor
- Feature flag USE_NEW_B2_PATHS controla comportamento
"""

import os
import json
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime

# 🆕 v2.9.180: Import das funções de path
from ...utils import b2_paths

logger = logging.getLogger(__name__)

# URL do V-Editor
V_EDITOR_URL = os.environ.get('V_EDITOR_URL', 'https://editor.vinicius.ai')

# 🆕 v2.9.97: Suporte a v-editor-ffmpeg como alternativa ao Remotion
# USE_FFMPEG_EDITOR=true para usar FFmpeg (mais rápido, sem animações)
USE_FFMPEG_EDITOR = os.environ.get('USE_FFMPEG_EDITOR', 'false').lower() == 'true'
V_EDITOR_FFMPEG_URL = os.environ.get('V_EDITOR_FFMPEG_URL', 'http://v-editor-ffmpeg:5000')

# 🆕 v2.9.200: Suporte a múltiplos workers de render (igual v-matting)
# Configuração de URLs para cada worker
V_EDITOR_WORKERS = {
    'hetzner-main': {
        'url': os.environ.get('V_EDITOR_HETZNER_URL', 'http://v-editor:5018'),
        'endpoint': '/render-video',
        'type': 'remotion',
        'description': 'Hetzner AX41 (CPU)'
    },
    'linux-home': {
        'url': os.environ.get('V_EDITOR_HOME_URL', 'http://linux-home:5018'),
        'endpoint': '/render-video',
        'type': 'remotion',
        'description': 'Linux Home (GPU)'
    },
    'modal': {
        'url': os.environ.get('V_EDITOR_MODAL_URL', 'https://fotovinicius2--v-editor-render-sync.modal.run'),
        'endpoint': '',  # Modal usa endpoint direto
        'type': 'modal',
        'description': 'Modal Cloud (CPU 8-core + 32GB)'
    },
    # 🆕 v2.9.210: Modal Light - 4x mais barato!
    'modal-light': {
        'url': 'https://fotovinicius2--v-editor-render-light-sync.modal.run',
        'endpoint': '',  # Modal usa endpoint direto
        'type': 'modal',
        'description': 'Modal Cloud LIGHT (CPU 2-core + 8GB) - 4x mais barato!'
    },
    # 🆕 v2.9.250: v-editor-python - Editor Python/MoviePy (alternativa ao Remotion)
    # Nota: Container escuta em 5018 (interno), exposto externamente em 5019
    # Na rede Docker, usa porta interna (5018). Externamente usa 5019.
    'python': {
        'url': os.environ.get('V_EDITOR_PYTHON_URL', 'http://v-editor-python:5018'),
        'endpoint': '/render',
        'type': 'python',
        'description': 'Python/MoviePy Editor (CPU) - Alternativa ao Remotion'
    },
    'python-local': {
        'url': os.environ.get('V_EDITOR_PYTHON_LOCAL_URL', 'http://localhost:5019'),
        'endpoint': '/render',
        'type': 'python',
        'description': 'Python/MoviePy Editor Local (Dev)'
    }
}

# URL do webhook de callback
# 🆕 v2.9.60: URL interna na rede Docker para webhook
# O v-editor está na mesma rede Docker, então pode chamar diretamente via nome do container
# 🆕 v2.9.99: Corrigido fallback para usar api.vinicius.ai
WEBHOOK_BASE_URL = os.environ.get('WEBHOOK_INTERNAL_URL') or \
                   os.environ.get('CUSTOM_API_INTERNAL_URL') or \
                   os.environ.get('CALLBACK_BASE_URL') or \
                   'https://api.vinicius.ai'  # Cloudflare Tunnel


class RenderService:
    """
    Serviço de renderização de vídeos.
    
    Envia payload para o v-editor e acompanha o resultado.
    
    🆕 v2.9.97: Suporte a v-editor-ffmpeg (USE_FFMPEG_EDITOR=true)
    🆕 v2.9.200: Suporte a múltiplos workers (hetzner-main, linux-home, modal)
    """
    
    def __init__(
        self, 
        v_editor_url: str = None, 
        webhook_base_url: str = None, 
        use_ffmpeg: bool = None,
        editor_worker_id: str = None  # 🆕 v2.9.200: Worker específico
    ):
        # 🆕 v2.9.200: Se worker_id especificado, usar configuração do worker
        self.editor_worker_id = editor_worker_id
        self.worker_config = None
        
        if editor_worker_id and editor_worker_id in V_EDITOR_WORKERS:
            self.worker_config = V_EDITOR_WORKERS[editor_worker_id]
            self.v_editor_url = self.worker_config['url']
            self.endpoint = f"{self.v_editor_url}{self.worker_config['endpoint']}"
            self.is_modal = self.worker_config['type'] == 'modal'
            logger.info(f"🎬 [WORKER:{editor_worker_id}] Render Service: {self.endpoint}")
        else:
            # Fallback: comportamento legado
            self.is_modal = False
            
            # 🆕 v2.9.97: Escolher editor baseado em ENV ou parâmetro
            self.use_ffmpeg = use_ffmpeg if use_ffmpeg is not None else USE_FFMPEG_EDITOR
            
            if self.use_ffmpeg:
                self.v_editor_url = V_EDITOR_FFMPEG_URL
                self.endpoint = f"{self.v_editor_url}/render"  # FFmpeg usa /render
                logger.info(f"🎬 [FFMPEG] Render Service inicializado: {self.endpoint}")
            else:
                self.v_editor_url = v_editor_url or V_EDITOR_URL
                self.endpoint = f"{self.v_editor_url}/render-video"  # Remotion usa /render-video
                logger.info(f"🎬 [REMOTION] Render Service inicializado: {self.endpoint}")
        
        self.webhook_url = webhook_base_url or WEBHOOK_BASE_URL
    
    def _convert_to_internal_url(self, url: str) -> str:
        """
        🆕 v2.9.58: Converte URL externa para interna (Docker network).
        
        O v-editor está na mesma rede Docker que o v-services, então pode acessar
        diretamente via http://v-services:5000 ao invés de https://services.vinicius.ai
        """
        if not url:
            return url
        
        if 'https://services.vinicius.ai' in url:
            internal_url = url.replace('https://services.vinicius.ai', 'http://v-services:5000')
            logger.debug(f"🔄 [v2.9.58] URL convertida para interna: {internal_url[:80]}...")
            return internal_url
        
        if 'http://services.vinicius.ai' in url:
            internal_url = url.replace('http://services.vinicius.ai', 'http://v-services:5000')
            logger.debug(f"🔄 [v2.9.58] URL convertida para interna: {internal_url[:80]}...")
            return internal_url
        
        return url
    
    def _build_base_layer(self, payload: Dict[str, Any], video_url: str) -> Dict[str, Any]:
        """
        Constrói o base_layer preservando configurações existentes no payload
        (como zoom_keyframes) e adicionando/atualizando URLs se necessário.
        
        🆕 v2.10.9: Suporte a zoom_keyframes e outras configurações customizadas
        """
        # Obter base_layer existente no payload (se houver)
        existing_base_layer = payload.get("base_layer", {})
        
        if video_url:
            # Modo vídeo: Preservar video_base existente e adicionar URLs
            video_base = existing_base_layer.get("video_base", {})
            video_base["urls"] = [video_url]  # Atualizar/adicionar URLs
            
            return {
                "video_base": video_base
            }
        else:
            # Modo solid: Retornar configuração padrão ou existente
            return existing_base_layer if existing_base_layer else {
                "solid_base": {
                    "color": "#000000",
                    "opacity": 1
                }
            }
    
    def _build_b2_upload_config(
        self,
        user_id: str,
        project_id: str,
        job_id: str,
        phase: int = 2,
        version: int = None
    ) -> Dict[str, Any]:
        """
        🆕 v2.9.180: Monta configuração de upload para o v-editor.
        
        O v-editor usará esses dados para determinar o path de upload no B2.
        
        Args:
            user_id: UUID do usuário
            project_id: UUID do projeto
            job_id: UUID do job
            phase: Fase do pipeline (1 ou 2)
            version: Versão do render (auto-detectado se None)
        
        Returns:
            Dict com configuração de upload
        """
        # Obter próxima versão do banco se não especificada
        if version is None:
            version = self._get_next_render_version(job_id, phase, project_id)
        
        # Gerar path usando função centralizada
        upload_path = b2_paths.generate_render_path(
            user_id=user_id,
            project_id=project_id,
            job_id=job_id,
            version=version,
            phase=phase,
            extension="mp4"
        )
        
        config = {
            "enabled": b2_paths.USE_NEW_B2_PATHS,
            "use_structured_path": b2_paths.USE_NEW_B2_PATHS,
            "path": upload_path,
            "user_id": user_id,
            "project_id": project_id,
            "job_id": job_id,
            "phase": phase,
            "version": version,
            "bucket": b2_paths.DEFAULT_BUCKET,
            # Fallback para path legacy se feature flag desativada
            "legacy_path": b2_paths.generate_render_path_legacy(job_id, "mp4")
        }
        
        logger.info(f"📁 [B2 Config] Feature flag: {b2_paths.USE_NEW_B2_PATHS}")
        logger.info(f"   Path: {upload_path if b2_paths.USE_NEW_B2_PATHS else config['legacy_path']}")
        logger.info(f"   Version: {version}")
        
        return config
    
    def _get_next_render_version(self, job_id: str, phase: int, project_id: str = None) -> int:
        """
        🆕 v3.2.15: Obtém próxima versão de render do banco de dados.
        
        Consulta a tabela render_versions para determinar o próximo número de versão.
        
        🔧 v3.2.15: AGORA BUSCA POR project_id (não job_id) para suportar
        arquitetura 1 JOB_ID = 1 RENDER onde cada re-render tem novo job_id.
        
        Args:
            job_id: UUID do job
            phase: Fase do pipeline (1 ou 2)
            project_id: UUID do projeto (OBRIGATÓRIO para versionamento correto)
        
        Returns:
            Próximo número de versão (1 se primeiro render)
        """
        try:
            # Import aqui para evitar circular import
            from ...db import get_db_connection
            
            # 🆕 v3.2.15: Se não tiver project_id, buscar do job
            if not project_id:
                from ..jobs import JobManager
                job_manager = JobManager(db_connection_func=get_db_connection)
                job = job_manager.get_job(job_id)
                if job and job.project_id:
                    project_id = job.project_id
                    logger.debug(f"📊 [Version] project_id obtido do job: {project_id[:8]}")
            
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # 🆕 v3.2.15: BUSCAR POR project_id (não job_id!)
                    # Assim, versões continuam crescendo mesmo com job_ids diferentes
                    cur.execute("""
                        SELECT COALESCE(MAX(version_number), 0) + 1 as next_version
                        FROM render_versions
                        WHERE project_id = %s AND phase = %s
                    """, (project_id, phase))
                    
                    result = cur.fetchone()
                    next_version = result[0] if result else 1
                    
                    logger.info(f"📊 [Version] Project {project_id[:8] if project_id else 'N/A'} Phase {phase}: próxima versão = v{next_version}")
                    return next_version
                    
        except Exception as e:
            logger.warning(f"⚠️ [Version] Erro ao consultar versão: {e}. Usando v1")
            return 1
    
    def submit_render_job(
        self,
        job_id: str,
        payload: Dict[str, Any],
        user_id: str,
        project_id: str,
        template_id: str = None,
        callback_endpoint: str = "/api/webhook/render-complete"
    ) -> Dict[str, Any]:
        """
        Envia job de renderização para o v-editor.
        
        Args:
            job_id: ID do job do Video Orchestrator
            payload: Payload completo para o Remotion (saída do PayloadBuilderService)
            user_id: ID do usuário
            project_id: ID do projeto
            template_id: ID do template (opcional)
            callback_endpoint: Endpoint para receber callback
            
        Returns:
            {
                "status": "queued" | "rendering_started" | "error",
                "job_id": "...",
                "message": "...",
                ...
            }
        """
        logger.info(f"🎬 Enviando job {job_id} para renderização...")
        
        # Montar webhook URL
        webhook_url = f"{self.webhook_url}{callback_endpoint}"
        
        # Montar payload do v-editor
        render_payload = self._build_render_payload(
            job_id=job_id,
            payload=payload,
            user_id=user_id,
            project_id=project_id,
            template_id=template_id,
            webhook_url=webhook_url
        )
        
        # 🆕 v3.8.0: Salvar payload no pipeline_debug_logs para o LLM Sandbox Director
        # O Director precisa acessar o payload via GET /api/video/payload/tracks/{job_id}
        try:
            from app.supabase_client import get_direct_db_connection
            import json as _json
            from datetime import datetime as _dt
            _conn = get_direct_db_connection()
            _cur = _conn.cursor()
            _cur.execute(
                "INSERT INTO pipeline_debug_logs (job_id, step_name, direction, payload, created_at) VALUES (%s, %s, %s, %s, %s)",
                (job_id, 'render_service', 'input', _json.dumps(render_payload), _dt.utcnow())
            )
            _conn.commit()
            _cur.close()
            _conn.close()
            logger.info(f"💾 [DEBUG_LOG] Payload do render salvo para Director (job={job_id})")
        except Exception as e:
            logger.warning(f"⚠️ [DEBUG_LOG] Falha ao salvar payload do render: {e}")
        
        # 🆕 v2.10.10: Determinar se editor é síncrono ou assíncrono
        is_async_editor = not getattr(self, 'use_ffmpeg', False)
        
        try:
            # 🆕 v2.9.200: Modal tem comportamento diferente (síncrono, payload adaptado)
            if self.is_modal:
                return self._submit_to_modal(job_id, render_payload, webhook_url)
            
            # 🆕 v2.10.10: Arquitetura 100% assíncrona via webhook para v-editor-python
            #   - FFmpeg: SÍNCRONO (timeout 600s) - resposta contém vídeo pronto
            #   - v-editor-python: ASSÍNCRONO (timeout 5s) - apenas confirma job aceito
            #                      Resultado final vem via webhook /api/webhook/render-complete
            #   - Outros: ASSÍNCRONO (timeout 5s)
            if is_async_editor:
                timeout = 5  # Apenas para confirmar que job foi aceito (ACK)
            else:
                timeout = 600  # FFmpeg é síncrono - aguarda render completo
            
            response = requests.post(
                self.endpoint,
                json=render_payload,
                timeout=timeout
            )
            
            if response.status_code in [200, 202]:
                result = response.json()
                
                # 🆕 v2.9.97: FFmpeg retorna resposta síncrona com output_path e b2_url
                if getattr(self, 'use_ffmpeg', False) and result.get('success'):
                    b2_url = result.get('b2_url') or result.get('video_url')
                    logger.info(f"✅ [FFMPEG] Job {job_id} renderizado em {result.get('total_time_seconds', 0):.1f}s")
                    logger.info(f"   📦 Output: {result.get('output_path')}")
                    logger.info(f"   📎 B2 URL: {b2_url[:80] if b2_url else 'N/A'}...")
                    return {
                        "status": "success",
                        "render_status": "completed",  # FFmpeg é síncrono
                        "job_id": job_id,
                        "output_path": result.get("output_path"),
                        "output_url": b2_url,  # Para compatibilidade com orchestrator
                        "b2_url": b2_url,
                        "render_time_seconds": result.get("render_time_seconds"),
                        "file_size_bytes": result.get("file_size_bytes"),
                        "message": f"Rendered in {result.get('total_time_seconds', 0):.1f}s"
                    }
                elif getattr(self, 'use_ffmpeg', False) and not result.get('success'):
                    error_msg = result.get('error', 'Unknown FFmpeg error')
                    logger.error(f"❌ [FFMPEG] Erro: {error_msg}")
                    return {"status": "error", "error": error_msg}
                else:
                    # 🆕 v2.10.10: Editores assíncronos (v-editor-python, Remotion)
                    # Apenas confirmam que job foi aceito. Resultado vem via webhook.
                    logger.info(f"✅ [ASYNC] Job {job_id} aceito pelo v-editor")
                    logger.info(f"   💡 Resultado virá via webhook: {webhook_url}")
                    return {
                        "status": "success",
                        "render_status": "processing",  # 🆕 v2.10.10: Sempre 'processing' (webhook atualiza)
                        "job_id": job_id,
                        "message": "Job aceito - aguardando webhook com resultado final"
                    }
            else:
                error_msg = f"V-Editor retornou {response.status_code}: {response.text[:200]}"
                logger.error(f"❌ {error_msg}")
                return {
                    "status": "error",
                    "error": error_msg
                }
                
        except requests.Timeout:
            # 🆕 v2.10.10: Timeout em editor assíncrono é apenas no ACK (conexão)
            if is_async_editor:
                error_msg = "Timeout ao conectar com v-editor (falha ao aceitar job)"
            else:
                error_msg = "Timeout ao processar vídeo (FFmpeg)"
            logger.error(f"❌ {error_msg}")
            return {"status": "error", "error": error_msg}
            
        except requests.RequestException as e:
            error_msg = f"Erro de conexão: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {"status": "error", "error": error_msg}
    
    def _transform_urls_for_modal(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        🆕 v2.9.203: Transforma URLs locais para URLs públicas.
        
        No Hetzner, os PNGs são servidos localmente via localhost:3000.
        Para o Modal, precisamos usar URLs públicas via services.vinicius.ai.
        
        Transformações:
        - localhost:3000/app/shared/... → https://services.vinicius.ai/shared/...
        - localhost:3000/shared-assets/... → https://services.vinicius.ai/shared/...
        - /app/shared/... → https://services.vinicius.ai/shared/...
        """
        import json
        import re
        
        # Converter para string JSON para fazer replace em massa
        payload_str = json.dumps(payload)
        
        # Substituições de URLs locais para públicas
        # A rota no v-services é /shared-files/<path>
        replacements = [
            # localhost com porta
            (r'http://localhost:3000/app/shared/', 'https://services.vinicius.ai/shared-files/'),
            (r'http://localhost:3000/shared-assets/', 'https://services.vinicius.ai/shared-files/'),
            (r'http://localhost:3000/shared/', 'https://services.vinicius.ai/shared-files/'),
            # Caminhos relativos (sem host)
            (r'"/app/shared/', '"https://services.vinicius.ai/shared-files/'),
            (r'"/shared-assets/', '"https://services.vinicius.ai/shared-files/'),
        ]
        
        for pattern, replacement in replacements:
            payload_str = re.sub(pattern, replacement, payload_str)
        
        transformed = json.loads(payload_str)
        
        # Log de transformação
        original_count = json.dumps(payload).count('localhost:3000')
        final_count = payload_str.count('localhost:3000')
        if original_count > 0:
            logger.info(f"🔄 [MODAL] URLs transformadas: {original_count - final_count} URLs localhost → services.vinicius.ai")
        
        return transformed
    
    def _submit_to_modal(
        self,
        job_id: str,
        render_payload: Dict[str, Any],
        webhook_url: str
    ) -> Dict[str, Any]:
        """
        🆕 v2.9.200: Submete render para Modal Cloud.
        
        O Modal v-editor é síncrono - aguarda o render completo e retorna a URL.
        
        Args:
            job_id: ID do job
            render_payload: Payload de render (mesmo formato do v-editor local)
            webhook_url: URL para callback (não usado no Modal síncrono)
        
        Returns:
            Dict com status e URL do vídeo
        """
        logger.info(f"🎬 [MODAL] Submetendo render para Modal Cloud...")
        logger.info(f"   Endpoint: {self.endpoint}")
        
        # 🆕 v2.9.203: Transformar URLs locais para públicas
        transformed_payload = self._transform_urls_for_modal(render_payload)
        
        # 🆕 v2.9.204: Modal precisa de URL pública para webhook (não pode resolver 'v-api')
        public_webhook_url = webhook_url
        if 'v-api' in webhook_url or 'localhost' in webhook_url or '127.0.0.1' in webhook_url:
            public_webhook_url = f"https://api.vinicius.ai/api/webhook/render-complete"
            logger.info(f"🔄 [MODAL] Webhook URL convertida para pública: {public_webhook_url}")
        
        # Adaptar payload para formato Modal
        modal_payload = {
            "job_id": job_id,
            "render_payload": transformed_payload,
            "webhook_url": public_webhook_url,
            "upload_to_b2": True
        }
        
        try:
            # Modal é síncrono - timeout longo (30 min)
            response = requests.post(
                self.endpoint,
                json=modal_payload,
                timeout=1800  # 30 minutos
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if result.get("status") == "success":
                    video_url = result.get("video_url")
                    metrics = result.get("metrics", {})
                    
                    logger.info(f"✅ [MODAL] Render concluído em {metrics.get('total_time', 0):.1f}s")
                    logger.info(f"   📎 URL: {video_url[:80] if video_url else 'N/A'}...")
                    
                    return {
                        "status": "success",
                        "render_status": "completed",
                        "job_id": job_id,
                        "output_url": video_url,
                        "b2_url": video_url,
                        "render_time_seconds": metrics.get("render_time"),
                        "total_time_seconds": metrics.get("total_time"),
                        "file_size_bytes": int(metrics.get("output_size_mb", 0) * 1024 * 1024),
                        "message": f"Modal render completed in {metrics.get('total_time', 0):.1f}s",
                        "worker": "modal"
                    }
                else:
                    error_msg = result.get("error", "Unknown Modal error")
                    logger.error(f"❌ [MODAL] Erro: {error_msg}")
                    return {"status": "error", "error": error_msg}
            else:
                error_msg = f"Modal retornou {response.status_code}: {response.text[:200]}"
                logger.error(f"❌ [MODAL] {error_msg}")
                return {"status": "error", "error": error_msg}
                
        except requests.Timeout:
            error_msg = "Timeout ao chamar Modal (30 min)"
            logger.error(f"❌ [MODAL] {error_msg}")
            return {"status": "error", "error": error_msg}
            
        except requests.RequestException as e:
            error_msg = f"Erro de conexão com Modal: {str(e)}"
            logger.error(f"❌ [MODAL] {error_msg}")
            return {"status": "error", "error": error_msg}
    
    def _build_render_payload(
        self,
        job_id: str,
        payload: Dict[str, Any],
        user_id: str,
        project_id: str,
        template_id: str,
        webhook_url: str
    ) -> Dict[str, Any]:
        """
        Monta o payload no formato esperado pelo v-editor.
        
        O v-editor espera:
        - project_settings: Configurações de projeto (canvas, fps, etc)
        - tracks: Objeto com subtitles, highlights, word_bgs, etc
        - webhook_url: URL para callback
        """
        # Extrair dados do payload do PayloadBuilderService
        tracks = payload.get("tracks", {})
        canvas = payload.get("canvas", {"width": 720, "height": 1280})
        fps = payload.get("fps", 30)
        duration_in_frames = payload.get("duration_in_frames", 0)
        video_url = payload.get("video_url", "")
        
        # Se a URL é do B2 privado, SEMPRE gerar nova URL assinada
        # (URLs assinadas expiram, então regeneramos para garantir acesso)
        if video_url and "vinicius-ai-cdn-global" in video_url:
            try:
                from app.utils.b2_client import get_b2_client
                b2_client = get_b2_client()
                
                # Extrair o path do arquivo da URL (removendo Authorization se existir)
                # URL pode ser: 
                # - https://f001.backblazeb2.com/file/vinicius-ai-cdn-global/users/...
                # - https://f001.backblazeb2.com/file/vinicius-ai-cdn-global/users/...?Authorization=...
                base_url = video_url.split('?')[0]  # Remover query params
                path_start = base_url.find("/file/vinicius-ai-cdn-global/") + len("/file/vinicius-ai-cdn-global/")
                file_path = base_url[path_start:]
                
                logger.info(f"🔐 Gerando URL assinada para: {file_path[:60]}...")
                
                # Gerar URL assinada (válida por 24h)
                signed_url = b2_client.generate_signed_url(file_path, valid_duration_seconds=86400)
                if signed_url:
                    logger.info(f"✅ URL assinada gerada para v-editor: {signed_url[:80]}...")
                    video_url = signed_url
                else:
                    logger.warning(f"⚠️ generate_signed_url retornou None")
            except Exception as e:
                logger.error(f"❌ Erro ao gerar URL assinada: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        # 🎬 Assinar URLs de vídeo nas cartelas (bg_full_screen)
        bg_full_screen = tracks.get("bg_full_screen", [])
        if bg_full_screen:
            logger.info(f"🔐 Processando {len(bg_full_screen)} cartelas bg_full_screen...")
            from app.utils.b2_client import get_b2_client
            
            for i, bg in enumerate(bg_full_screen):
                # Verificar se é cartela de vídeo com URL do B2 privado
                bg_src = bg.get("src", "")
                is_video = bg.get("is_video", False)
                
                if is_video and bg_src and "vinicius-ai-cdn-global" in bg_src:
                    try:
                        b2_client = get_b2_client()
                        
                        # Extrair path do arquivo
                        base_url = bg_src.split('?')[0]  # Remover query params existentes
                        path_start = base_url.find("/file/vinicius-ai-cdn-global/") + len("/file/vinicius-ai-cdn-global/")
                        file_path = base_url[path_start:]
                        
                        logger.info(f"   🔐 Cartela #{i+1}: Gerando URL assinada para: {file_path[:50]}...")
                        
                        # Gerar URL assinada (válida por 24h)
                        signed_url = b2_client.generate_signed_url(file_path, valid_duration_seconds=86400)
                        if signed_url:
                            bg["src"] = signed_url
                            logger.info(f"   ✅ Cartela #{i+1}: URL assinada OK")
                        else:
                            logger.warning(f"   ⚠️ Cartela #{i+1}: generate_signed_url retornou None")
                    except Exception as e:
                        logger.error(f"   ❌ Cartela #{i+1}: Erro ao gerar URL assinada: {e}")
        
        # 🆕 Assinar URLs de vídeo nas person_overlay (WebM com alpha OU luma matte)
        person_overlay = tracks.get("person_overlay", [])
        if person_overlay:
            logger.info(f"🔐 Processando {len(person_overlay)} person_overlay...")
            from app.utils.b2_client import get_b2_client
            
            for i, overlay in enumerate(person_overlay):
                b2_client = None  # Lazy init
                
                # 1. Assinar src (URL principal - máscara ou WebM)
                overlay_src = overlay.get("src", "")
                if overlay_src and "vinicius-ai-cdn-global" in overlay_src:
                    try:
                        b2_client = get_b2_client()
                        
                        # Extrair path do arquivo
                        base_url = overlay_src.split('?')[0]
                        path_start = base_url.find("/file/vinicius-ai-cdn-global/") + len("/file/vinicius-ai-cdn-global/")
                        file_path = base_url[path_start:]
                        
                        logger.info(f"   🔐 Person Overlay #{i+1}: Gerando URL assinada para src: {file_path[:50]}...")
                        
                        # Gerar URL assinada (válida por 24h)
                        signed_url = b2_client.generate_signed_url(file_path, valid_duration_seconds=86400)
                        if signed_url:
                            overlay["src"] = signed_url
                            logger.info(f"   ✅ Person Overlay #{i+1}: src assinada OK")
                        else:
                            logger.warning(f"   ⚠️ Person Overlay #{i+1}: generate_signed_url retornou None")
                    except Exception as e:
                        logger.error(f"   ❌ Person Overlay #{i+1}: Erro ao gerar URL assinada: {e}")
                
                # 🆕 v2.9.282: Assinar mask_url (para luma matte no v-editor-python)
                mask_url = overlay.get("mask_url", "")
                if mask_url and "vinicius-ai-cdn-global" in mask_url:
                    try:
                        if not b2_client:
                            b2_client = get_b2_client()
                        
                        base_url = mask_url.split('?')[0]
                        path_start = base_url.find("/file/vinicius-ai-cdn-global/") + len("/file/vinicius-ai-cdn-global/")
                        file_path = base_url[path_start:]
                        
                        logger.info(f"   🔐 Person Overlay #{i+1}: Gerando URL assinada para mask_url: {file_path[:50]}...")
                        
                        signed_url = b2_client.generate_signed_url(file_path, valid_duration_seconds=86400)
                        if signed_url:
                            overlay["mask_url"] = signed_url
                            logger.info(f"   ✅ Person Overlay #{i+1}: mask_url assinada OK")
                        else:
                            logger.warning(f"   ⚠️ Person Overlay #{i+1}: mask_url - generate_signed_url retornou None")
                    except Exception as e:
                        logger.error(f"   ❌ Person Overlay #{i+1}: Erro ao assinar mask_url: {e}")
                
                # 🆕 v2.9.282: Assinar original_video_url (para luma matte no v-editor-python)
                original_video_url = overlay.get("original_video_url", "")
                if original_video_url and "vinicius-ai-cdn-global" in original_video_url:
                    try:
                        if not b2_client:
                            b2_client = get_b2_client()
                        
                        base_url = original_video_url.split('?')[0]
                        path_start = base_url.find("/file/vinicius-ai-cdn-global/") + len("/file/vinicius-ai-cdn-global/")
                        file_path = base_url[path_start:]
                        
                        logger.info(f"   🔐 Person Overlay #{i+1}: Gerando URL assinada para original_video_url: {file_path[:50]}...")
                        
                        signed_url = b2_client.generate_signed_url(file_path, valid_duration_seconds=86400)
                        if signed_url:
                            overlay["original_video_url"] = signed_url
                            logger.info(f"   ✅ Person Overlay #{i+1}: original_video_url assinada OK")
                        else:
                            logger.warning(f"   ⚠️ Person Overlay #{i+1}: original_video_url - generate_signed_url retornou None")
                    except Exception as e:
                        logger.error(f"   ❌ Person Overlay #{i+1}: Erro ao assinar original_video_url: {e}")
        
        # 🆕 v2.9.0: Processar URLs de vídeo nos video_segments (clips do hybrid cut)
        video_segments = tracks.get("video_segments", [])
        if video_segments:
            logger.info(f"🔐 Processando {len(video_segments)} video_segments (hybrid cut)...")
            from app.utils.b2_client import get_b2_client
            
            for i, segment in enumerate(video_segments):
                segment_src = segment.get("src", "")
                
                # 🆕 v2.9.58: Converter URL externa para interna (Docker network)
                if segment_src and "services.vinicius.ai" in segment_src:
                    segment["src"] = self._convert_to_internal_url(segment_src)
                    logger.info(f"   🔄 Video Segment #{i+1}: URL convertida para interna")
                elif segment_src and "vinicius-ai-cdn-global" in segment_src:
                    try:
                        b2_client = get_b2_client()
                        
                        # Extrair path do arquivo
                        base_url = segment_src.split('?')[0]
                        path_start = base_url.find("/file/vinicius-ai-cdn-global/") + len("/file/vinicius-ai-cdn-global/")
                        file_path = base_url[path_start:]
                        
                        logger.info(f"   🔐 Video Segment #{i+1}: Gerando URL assinada para: {file_path[:50]}...")
                        
                        # Gerar URL assinada (válida por 24h)
                        signed_url = b2_client.generate_signed_url(file_path, valid_duration_seconds=86400)
                        if signed_url:
                            segment["src"] = signed_url
                            logger.info(f"   ✅ Video Segment #{i+1}: URL assinada OK")
                        else:
                            logger.warning(f"   ⚠️ Video Segment #{i+1}: generate_signed_url retornou None")
                    except Exception as e:
                        logger.error(f"   ❌ Video Segment #{i+1}: Erro ao gerar URL assinada: {e}")
        
        # Montar project_settings
        project_settings = {
            "video_settings": {
                "width": canvas.get("width", 720),
                "height": canvas.get("height", 1280),
                "fps": fps,
                "duration_in_frames": duration_in_frames
            }
        }
        
        # 🎬 Montar quality_settings baseado no template
        # Converter preset/quality para CRF (menor = melhor qualidade)
        # CRF: 18 = alta qualidade, 23 = média, 28 = baixa
        template_quality = payload.get("quality", "high")
        template_preset = payload.get("preset", "medium")
        
        # Mapear quality para CRF base
        quality_to_crf = {
            "ultra": 15,
            "high": 18,
            "medium": 23,
            "low": 28,
            "draft": 32
        }
        base_crf = quality_to_crf.get(template_quality, 23)
        
        # Ajustar CRF pelo preset (presets mais lentos = melhor compressão)
        preset_adjustment = {
            "ultrafast": 4,
            "superfast": 3,
            "veryfast": 2,
            "faster": 1,
            "fast": 0,
            "medium": 0,
            "slow": -1,
            "slower": -2,
            "veryslow": -3,
            "placebo": -4
        }
        final_crf = max(10, min(35, base_crf + preset_adjustment.get(template_preset, 0)))
        
        quality_settings = {
            "crf": final_crf,
            "codec": "h264",
            "pixel_format": "yuv420p",
            "audio_bitrate": "192k" if template_quality in ["ultra", "high"] else "128k",
            "preset": template_preset
        }
        
        logger.info(f"🎬 Quality settings: quality={template_quality}, preset={template_preset} → CRF={final_crf}")
        
        # Payload final para v-editor
        render_payload = {
            "jobId": job_id,  # IMPORTANTE: Usar o mesmo job_id do orchestrator
            "job_id": job_id,  # Duplicado para compatibilidade
            
            # Metadados
            "user_id": user_id,
            "project_id": project_id,
            "template_id": template_id,
            
            # Webhook
            "webhook_url": webhook_url,
            "webhook_metadata": {
                "job_id": job_id,
                "user_id": user_id,
                "project_id": project_id,
                "template_id": template_id,
                "source": "video_orchestrator"
            },
            
            # Configurações do projeto
            "project_settings": project_settings,
            
            # Tracks (legendas, highlights, backgrounds)
            "tracks": tracks,
            
            # Base layer (formato esperado pelo v-editor)
            # 🆕 v2.10.9: Preservar base_layer existente no payload (para zoom_keyframes, etc)
            "base_type": payload.get("base_type", "video" if video_url else "solid"),
            "base_layer": self._build_base_layer(payload, video_url),
            
            # Configurações de renderização
            "render_settings": payload.get("render_settings", {
                "solid_background": not video_url,
                "background_color": "#000000"
            }),
            
            # 🎬 Configurações de qualidade
            "quality_settings": quality_settings,
            
            # 🆕 v2.9.180: Configurações de upload para B2
            # O v-editor usará esses dados para gerar o path correto
            "b2_upload_config": self._build_b2_upload_config(
                user_id=user_id,
                project_id=project_id,
                job_id=job_id,
                phase=2  # Render sempre é fase 2
            )
        }
        
        # 🎬 Copiar subtitle_animation_config se presente no payload original
        if "subtitle_animation_config" in payload:
            render_payload["subtitle_animation_config"] = payload["subtitle_animation_config"]
            logger.info(f"   - subtitle_animation_config: {payload['subtitle_animation_config']}")
        
        # Logs detalhados para debug
        logger.info(f"📦 Payload montado para v-editor:")
        logger.info(f"   - Subtitles: {len(tracks.get('subtitles', []))}")
        logger.info(f"   - Base type: {render_payload.get('base_type')}")
        logger.info(f"   - Video URL do payload original: {payload.get('video_url', 'AUSENTE')[:80] if payload.get('video_url') else 'AUSENTE'}")
        logger.info(f"   - Video URL final: {video_url[:80] + '...' if video_url and len(video_url) > 80 else video_url}")
        logger.info(f"   - base_layer.video_base.urls: {render_payload.get('base_layer', {}).get('video_base', {}).get('urls', [])[:1] if render_payload.get('base_layer', {}).get('video_base') else 'N/A'}")
        
        return render_payload
    
    def check_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Verifica status de um job no v-editor.
        
        Args:
            job_id: ID do job
            
        Returns:
            {
                "status": "queued" | "rendering" | "completed" | "failed",
                "progress": 0-100,
                ...
            }
        """
        try:
            response = requests.get(
                f"{self.v_editor_url}/job/{job_id}",
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"status": "not_found", "job_id": job_id}
            else:
                return {"status": "error", "error": f"Status {response.status_code}"}
                
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica status do v-editor."""
        try:
            response = requests.get(
                f"{self.v_editor_url}/health",
                timeout=5
            )
            return {
                "available": response.status_code == 200,
                "endpoint": self.endpoint,
                "v_editor_status": response.json() if response.ok else None
            }
        except Exception as e:
            return {
                "available": False,
                "endpoint": self.endpoint,
                "error": str(e)
            }


# Singleton instance
_render_service = None


def get_render_service() -> RenderService:
    """Retorna instância singleton do RenderService."""
    global _render_service
    if _render_service is None:
        _render_service = RenderService()
    return _render_service

