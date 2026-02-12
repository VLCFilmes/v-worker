"""
🔔 Video Orchestrator Callbacks - Webhooks de Retorno

Endpoints para receber callbacks de serviços e entregar resultados ao frontend.
"""

import json
import logging
from flask import Blueprint, request, jsonify
from datetime import datetime, timezone
from psycopg2.extras import RealDictCursor
from .pipeline_events import emit_job_complete, emit_step_complete, emit_job_error

logger = logging.getLogger(__name__)

# Blueprint
video_callbacks_bp = Blueprint('video_callbacks', __name__)


# ═══════════════════════════════════════════════════════════════════════
# BACKGROUND: Enriquecer e entregar vídeo no chat
# HLS transcoding + thumbnail + dimensões → depois criar mensagem
# ═══════════════════════════════════════════════════════════════════════

def _extract_video_metadata(video_url: str) -> dict:
    """
    Extrai thumbnail JPEG + ThumbHash + dimensões do vídeo via ffmpeg/ffprobe.
    Retorna { thumbnail_b64, thumb_hash, width, height } ou parcial em caso de erro.
    """
    import subprocess, tempfile, os, base64
    result = {}
    tmp_video = None
    
    try:
        # Download do vídeo (apenas primeiros MB para thumbnail)
        import requests
        tmp_video = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False)
        resp = requests.get(video_url, stream=True, timeout=30)
        resp.raise_for_status()
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=65536):
            tmp_video.write(chunk)
            downloaded += len(chunk)
            if downloaded > 5 * 1024 * 1024:  # 5MB suficiente para thumbnail
                break
        tmp_video.close()
        
        # ─── Dimensões via ffprobe ───
        try:
            probe = subprocess.run(
                ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                 '-show_streams', tmp_video.name],
                capture_output=True, text=True, timeout=15
            )
            import json as _json
            probe_data = _json.loads(probe.stdout)
            for stream in probe_data.get('streams', []):
                if stream.get('codec_type') == 'video':
                    result['width'] = int(stream.get('width', 0))
                    result['height'] = int(stream.get('height', 0))
                    break
            logger.info(f"📐 [METADATA] Dimensões: {result.get('width')}x{result.get('height')}")
        except Exception as e:
            logger.warning(f"⚠️ [METADATA] Erro ao extrair dimensões: {e}")
        
        # ─── Thumbnail JPEG via ffmpeg ───
        try:
            thumb_path = tmp_video.name + '_thumb.jpg'
            subprocess.run(
                ['ffmpeg', '-y', '-i', tmp_video.name,
                 '-ss', '1', '-vframes', '1',
                 '-vf', 'scale=480:-2',
                 '-q:v', '4', thumb_path],
                capture_output=True, timeout=15
            )
            
            if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
                with open(thumb_path, 'rb') as f:
                    thumb_bytes = f.read()
                result['thumbnail_b64'] = 'data:image/jpeg;base64,' + base64.b64encode(thumb_bytes).decode()
                logger.info(f"📸 [METADATA] Thumbnail gerado: {len(thumb_bytes)}B")
                
                # ─── ThumbHash ───
                try:
                    from PIL import Image
                    from io import BytesIO
                    from thumbhash.encode import rgba_to_thumbhash
                    
                    img = Image.open(BytesIO(thumb_bytes))
                    img = img.convert('RGBA')
                    img.thumbnail((100, 100))
                    w, h = img.size
                    pixels = list(img.getdata())
                    rgba = []
                    for r, g, b, a in pixels:
                        rgba.extend([r, g, b, a])
                    
                    th = rgba_to_thumbhash(w, h, rgba)
                    result['thumb_hash'] = base64.b64encode(bytes(th)).decode()
                    logger.info(f"🔑 [METADATA] ThumbHash gerado")
                except Exception as e:
                    logger.warning(f"⚠️ [METADATA] ThumbHash falhou: {e}")
                
                os.unlink(thumb_path)
        except Exception as e:
            logger.warning(f"⚠️ [METADATA] Thumbnail falhou: {e}")
    
    except Exception as e:
        logger.warning(f"⚠️ [METADATA] Erro geral: {e}")
    finally:
        if tmp_video and os.path.exists(tmp_video.name):
            os.unlink(tmp_video.name)
    
    return result


def _deliver_video_with_enrichment(data: dict):
    """
    Background thread: enriquece o vídeo (HLS + thumbnail + dimensões)
    e SÓ DEPOIS cria a mensagem no chat.
    
    Isso garante que quando o vídeo aparece no chat, já tem:
    - HLS URL para streaming instantâneo
    - Thumbnail JPEG + ThumbHash (sem tela preta)
    - Dimensões corretas (vertical/horizontal)
    """
    import uuid as uuid_module
    
    job_id = data['job_id']
    video_url = data['video_url']
    project_id = data['project_id']
    conversation_id = data['conversation_id']
    duration = data['duration']
    file_size = data['file_size']
    rendering_time = data['rendering_time']
    file_size_mb = round(file_size / (1024 * 1024), 2) if file_size else 0
    
    hls_url = None
    thumbnail_b64 = None
    thumb_hash = None
    video_width = None
    video_height = None
    
    logger.info(f"🚀 [DELIVERY] Iniciando enriquecimento para job {job_id[:8]}...")
    
    # ─── 1. Thumbnail + ThumbHash + Dimensões (rápido, ~5s) ───
    try:
        logger.info(f"📸 [DELIVERY] Extraindo metadata do vídeo...")
        metadata = _extract_video_metadata(video_url)
        thumbnail_b64 = metadata.get('thumbnail_b64')
        thumb_hash = metadata.get('thumb_hash')
        video_width = metadata.get('width')
        video_height = metadata.get('height')
        logger.info(f"📸 [DELIVERY] Metadata extraído: {video_width}x{video_height}, thumb={'sim' if thumbnail_b64 else 'não'}")
    except Exception as e:
        logger.warning(f"⚠️ [DELIVERY] Metadata falhou: {e}")
    
    # ─── 2. HLS Transcoding (lento, ~60s, se R2 configurado) ───
    try:
        from app.services.hls_transcoding_service import is_r2_configured, process_video_for_hls
        if is_r2_configured():
            logger.info(f"🎬 [DELIVERY] HLS transcoding...")
            hls_result = process_video_for_hls(
                input_url=video_url,
                project_id=project_id,
                video_id=job_id,
            )
            hls_url = hls_result.get('hls_url')
            logger.info(f"🎬 [DELIVERY] HLS pronto: {hls_url}")
        else:
            logger.info("[DELIVERY] R2 não configurado, pulando HLS")
    except Exception as e:
        logger.warning(f"⚠️ [DELIVERY] HLS falhou (vídeo será entregue sem HLS): {e}")
    
    # ─── 3. Criar mensagem no chat COM TUDO pronto ───
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        content = f"🎬 Seu vídeo está pronto! ({duration:.1f}s, {file_size_mb}MB)"
        
        props = {
            "type": "video_delivery_phase_2",
            "phase": 2,
            "video_url": video_url,
            "phase1_video_url": data.get('phase1_video_url'),
            "duration": duration,
            "file_size": file_size,
            "file_size_mb": file_size_mb,
            "rendering_time_seconds": rendering_time,
            "job_id": job_id,
            "project_id": project_id,
            "conversation_id": conversation_id,
            "phrase_groups": data.get('phrase_groups', []),
            "speech_segments": data.get('speech_segments', []),
            "buttons": [
                {"text": "📥 Download", "action": "download", "url": video_url},
                {"text": "🔄 Renderizar novamente", "action": "re_render"}
            ],
            "source": "render_complete_webhook",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # Adicionar dados enriquecidos (se disponíveis)
        if hls_url:
            props["hls_url"] = hls_url
        if thumbnail_b64:
            props["thumbnail_url"] = thumbnail_b64
        if thumb_hash:
            props["thumb_hash"] = thumb_hash
        if video_width and video_height:
            props["video_width"] = video_width
            props["video_height"] = video_height
        
        component_props = json.dumps(props)
        
        bot_message_id = str(uuid_module.uuid4())
        cursor.execute("""
            INSERT INTO chatbot_messages (
                id, message_id, conversation_id, sender, content,
                component_type, component_props, created_at
            ) VALUES (%s, %s, %s, 'bot', %s, 'video_delivery_phase_2', %s, NOW())
            RETURNING message_id
        """, (bot_message_id, bot_message_id, conversation_id, content, component_props))
        
        message_row = cursor.fetchone()
        message_id = message_row['message_id'] if message_row else bot_message_id
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ [DELIVERY] Mensagem criada: {message_id[:8]}... (HLS={'sim' if hls_url else 'não'}, thumb={'sim' if thumbnail_b64 else 'não'})")
        
        # ─── 4. SSE: Notificar frontend ───
        try:
            from app.routes.chat_sse import emit_new_message, emit_processing_complete
            
            emit_new_message(
                conversation_id=conversation_id,
                message_id=str(message_id),
                sender='bot',
                content=content,
                component_type='video_delivery_phase_2',
                component_props=props,
            )
            emit_processing_complete(
                conversation_id=conversation_id,
                job_id=job_id,
                video_url=video_url,
            )
            logger.info(f"📡 [DELIVERY] SSE emitido para {conversation_id[:8]}...")
        except Exception as sse_err:
            logger.warning(f"⚠️ [DELIVERY] SSE falhou: {sse_err}")
        
        # ─── 5. Email ───
        try:
            from app.routes.email import notify_video_ready
            notify_video_ready(
                user_id=data.get('render_user_id'),
                video_url=video_url,
                project_id=project_id,
                job_id=job_id,
                duration=duration,
                file_size=file_size,
            )
        except Exception as email_err:
            logger.warning(f"⚠️ [DELIVERY] Email falhou: {email_err}")
        
    except Exception as e:
        logger.error(f"❌ [DELIVERY] Erro ao criar mensagem: {e}")
        import traceback
        logger.error(traceback.format_exc())


@video_callbacks_bp.route('/webhook/render-complete', methods=['POST'])
def render_complete_webhook():
    """
    POST /api/webhook/render-complete
    
    Recebe callback do v-editor quando um render é concluído.
    Salva uma mensagem no chat com o vídeo renderizado.
    
    Request Body (do v-editor):
    {
        "jobId": "uuid",
        "user_id": "uuid",
        "status": "completed",
        "video_url": "https://...",
        "rendering_time_seconds": 98,
        "b2Url": "https://...",
        "duration": 33.3,
        "fileSize": 6082560,
        ...
    }
    
    Response:
    {
        "status": "success",
        "message_id": "uuid",
        "conversation_id": "uuid"
    }
    """
    try:
        from app.supabase_client import get_direct_db_connection
        
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        job_id = data.get('jobId')
        user_id = data.get('user_id')
        status = data.get('status')
        video_url = data.get('video_url') or data.get('b2Url')
        rendering_time = data.get('rendering_time_seconds', 0)
        duration = data.get('duration', 0)
        file_size = data.get('fileSize', 0)
        
        logger.info(f"📹 Render complete webhook recebido: job={job_id}, status={status}")
        
        if status != 'completed' or not video_url:
            logger.warning(f"⚠️ Render não completado ou sem URL: status={status}")
            return jsonify({"status": "ignored", "reason": "Not completed or no video_url"}), 200
        
        # Buscar conversation_id e project_id do job
        conn = get_direct_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Primeiro tentar na tabela video_processing_jobs
        # 🆕 v2.9.181: Buscar também phase1_video_url para exibição lado a lado
        cursor.execute("""
            SELECT conversation_id, project_id, phase1_video_url, original_video_url
            FROM video_processing_jobs 
            WHERE job_id = %s
        """, (job_id,))
        
        job_row = cursor.fetchone()
        
        # 🗑️ REMOVIDO: Fallback para project_renders (tabela legado renomeada para project_renders_legado em 04/Fev/2026)
        # Agora todos os jobs estão APENAS em video_processing_jobs
        
        if not job_row:
            logger.warning(f"⚠️ Job {job_id} não encontrado em nenhuma tabela")
            cursor.close()
            conn.close()
            return jsonify({"status": "ignored", "reason": "Job not found"}), 200
        
        conversation_id = job_row['conversation_id']
        project_id = job_row['project_id']
        # 🆕 v2.9.181: Extrair phase1_video_url para exibição lado a lado no frontend
        phase1_video_url = job_row.get('phase1_video_url') or job_row.get('original_video_url')
        
        # 🗑️ REMOVIDO: UPDATE em project_renders (tabela legado renomeada para project_renders_legado em 04/Fev/2026)
        # A tabela ativa agora é render_versions (atualizada via create_render_version abaixo)
        
        # ✅ Atualizar video_processing_jobs com a URL do vídeo renderizado
        # Isso é CRÍTICO para o polling do frontend funcionar corretamente
        # O frontend faz GET /api/video/job/{id} que lê output_video_url desta tabela
        # 🆕 Salvar tanto em output_video_url quanto em phase2_video_url
        cursor.execute("""
            UPDATE video_processing_jobs
            SET output_video_url = %s,
                phase2_video_url = %s,
                status = 'completed',
                completed_at = NOW()
            WHERE job_id = %s
        """, (video_url, video_url, job_id))
        
        logger.info(f"✅ video_processing_jobs atualizado: job_id={job_id}")
        logger.info(f"   • output_video_url = {video_url[:60]}...")
        logger.info(f"   • phase2_video_url = {video_url[:60]}...")
        
        # 🆕 v2.9.172: Criar versão no histórico de renderizações
        # 🐛 v2.10.8: Logging detalhado para debug de falhas
        try:
            # Buscar dados adicionais do job + template_id do projeto
            cursor.execute("""
                SELECT vpj.phrase_groups, vpj.user_id, vpj.options, vpj.project_id,
                       p.template_id
                FROM video_processing_jobs vpj
                LEFT JOIN projects p ON vpj.project_id = p.project_id
                WHERE vpj.job_id = %s
            """, (job_id,))
            job_data = cursor.fetchone()
            
            template_id = job_data.get('template_id') if job_data else None
            logger.info(f"📋 [RENDER_VERSION] Dados do job: user_id={job_data.get('user_id') if job_data else 'N/A'}, template_id={template_id[:8] if template_id else 'N/A'}..., has_phrase_groups={bool(job_data and job_data.get('phrase_groups'))}")
            
            # Verificar se tabela existe (pode não existir em ambientes antigos)
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'render_versions'
                )
            """)
            result = cursor.fetchone()
            table_exists = result['exists'] if result else False
            
            logger.info(f"📋 [RENDER_VERSION] Tabela existe: {table_exists}")
            
            if table_exists:
                # Preparar parâmetros com logging
                # 🆕 v3.2.15: Adicionado project_id para suportar arquitetura 1 JOB_ID = 1 RENDER
                params = {
                    'job_id': job_id,
                    'project_id': str(project_id) if project_id else None,  # 🆕 v3.2.15
                    'phase': 2,
                    'video_url': video_url,
                    'created_by': job_data['user_id'] if job_data else user_id,
                    'template_id': template_id,  # 🆕 Buscar de projects.template_id
                    'phrase_groups': json.dumps(job_data['phrase_groups']) if job_data and job_data['phrase_groups'] else None,
                    'render_settings': json.dumps(job_data['options']) if job_data and job_data['options'] else None,
                    'worker_id': data.get('worker_id', 'unknown'),
                    'render_duration_ms': int(rendering_time * 1000) if rendering_time else None,
                    'duration_ms': int(duration * 1000) if duration else None,
                    'file_size_bytes': file_size,
                    'version_note': 'Render automático via webhook'
                }
                
                logger.info(f"📋 [RENDER_VERSION] Chamando create_render_version com params: job_id={params['job_id'][:8]}..., project_id={params['project_id'][:8] if params['project_id'] else 'N/A'}..., phase={params['phase']}, template={template_id[:8] if template_id else 'N/A'}..., worker={params['worker_id']}")
                
                # 🆕 v3.2.15: Atualizado para passar p_project_id (2º parâmetro)
                cursor.execute("""
                    SELECT save_render_version(
                        p_job_id := %s,
                        p_project_id := %s,
                        p_phase := %s,
                        p_video_url := %s,
                        p_created_by := %s,
                        p_template_id := %s,
                        p_template_config := NULL,
                        p_phrase_groups := %s,
                        p_render_settings := %s,
                        p_worker_id := %s,
                        p_render_duration_ms := %s,
                        p_duration_ms := %s,
                        p_file_size_bytes := %s,
                        p_version_note := %s
                    ) as version_id
                """, (
                    params['job_id'],
                    params['project_id'],  # 🆕 v3.2.15
                    params['phase'],
                    params['video_url'],
                    params['created_by'],
                    params['template_id'],
                    params['phrase_groups'],
                    params['render_settings'],
                    params['worker_id'],
                    params['render_duration_ms'],
                    params['duration_ms'],
                    params['file_size_bytes'],
                    params['version_note']
                ))
                
                version_result = cursor.fetchone()
                logger.info(f"📼 [RENDER_VERSION] Versão criada: {version_result['version_id']}")
                
                # 🆕 v3.3.0 → v4.4.2: Atualizar script_data no template APENAS para renders de demo
                # ⚠️ FIX v4.4.2 (10/Fev/2026): Projetos regulares de usuários estavam
                # sobrescrevendo script_data do template original (lastJobId, phase2VideoUrl,
                # processedPhrases), contaminando o template com dados de projetos individuais.
                # Agora, apenas renders de demo/template (criados pelo system user no generator-v3)
                # atualizam o template. Renders de projetos regulares são ignorados.
                SYSTEM_USER_IDS = {
                    '00000000-0000-0000-0000-000000000000',  # Nil UUID (generator-v3 frontend)
                }
                render_user_id = job_data.get('user_id') if job_data else None
                is_template_demo_render = render_user_id in SYSTEM_USER_IDS

                if template_id and is_template_demo_render:
                    try:
                        logger.info(f"📝 [SCRIPT_DATA] Atualizando script_data do template (demo render, user={render_user_id})")
                        
                        cursor.execute("""
                            UPDATE video_editing_templates
                            SET script_data = jsonb_set(
                                jsonb_set(
                                    jsonb_set(
                                        COALESCE(script_data, '{}'),
                                        '{lastJobId}',
                                        to_jsonb(%s::text)
                                    ),
                                    '{phase2VideoUrl}',
                                    to_jsonb(%s::text)
                                ),
                                '{processedPhrases}',
                                COALESCE((
                                    SELECT phrase_groups 
                                    FROM video_processing_jobs 
                                    WHERE job_id = %s
                                ), '[]'::jsonb)
                            )
                            WHERE id = %s
                        """, (job_id, video_url, job_id, template_id))
                        
                        updated_rows = cursor.rowcount
                        logger.info(f"✅ [SCRIPT_DATA] script_data atualizado! Rows affected: {updated_rows}")
                    except Exception as script_err:
                        logger.error(f"❌ [SCRIPT_DATA] Erro ao atualizar script_data: {script_err}")
                elif template_id:
                    logger.info(
                        f"⏭️ [SCRIPT_DATA] Pulando atualização do template — render de projeto regular "
                        f"(user={render_user_id}, template={template_id[:8]}). "
                        f"Apenas demo renders (system user) atualizam o template."
                    )
                
            else:
                logger.info(f"📼 [RENDER_VERSION] Tabela render_versions não existe, pulando versionamento")
        except Exception as ver_err:
            logger.error(f"❌ [RENDER_VERSION] Erro ao criar versão: {type(ver_err).__name__}: {ver_err}")
            import traceback
            logger.error(f"❌ [RENDER_VERSION] Traceback: {traceback.format_exc()}")
        
        # 🆕 30/Jan/2026: Completar Pipeline Visualizer (mesma arquitetura do upload_visualizer)
        try:
            from app.routes.visualizer_persistence import complete_pipeline_visualizer
            complete_pipeline_visualizer(
                conversation_id=conversation_id,
                job_id=job_id,
                video_url=video_url,
                duration_ms=int(duration * 1000) if duration else None
            )
            logger.info(f"[RENDER_CALLBACK] ✅ Pipeline visualizer completado: job={job_id}")
        except Exception as viz_err:
            logger.warning(f"[RENDER_CALLBACK] ⚠️ Erro ao completar pipeline visualizer: {viz_err}")
            try:
                cursor.execute("""
                    UPDATE chatbot_messages 
                    SET component_props = component_props || '{"status": "complete", "is_complete": true}'::jsonb
                    WHERE conversation_id = %s 
                    AND component_type IN ('video_processing', 'pipeline_visualizer')
                    AND component_props->>'job_id' = %s
                """, (conversation_id, job_id))
            except Exception:
                pass
        
        # 🆕 v2.9.185: Pipeline Observability
        try:
            cursor.execute("""
                SELECT id FROM pipeline_runs 
                WHERE job_id = %s AND phase = 2 
                ORDER BY created_at DESC LIMIT 1
            """, (job_id,))
            run_row = cursor.fetchone()
            
            if run_row:
                run_id = run_row['id']
                cursor.execute("""
                    UPDATE pipeline_runs 
                    SET status = 'completed', completed_at = NOW(),
                        output_video_url = %s, duration_ms = %s,
                        file_size_bytes = %s, error_message = NULL
                    WHERE id = %s
                """, (video_url, int(duration * 1000) if duration else None, file_size, run_id))
                cursor.execute("""
                    UPDATE pipeline_steps 
                    SET status = 'completed', completed_at = NOW(), duration_ms = %s
                    WHERE run_id = %s AND step_name = 'render'
                """, (int(rendering_time * 1000) if rendering_time else None, run_id))
                from uuid import uuid4
                cursor.execute("""
                    INSERT INTO pipeline_artifacts (id, step_id, artifact_name, artifact_type, url, file_size_bytes, duration_ms, created_at)
                    SELECT %s, ps.id, 'final_video', 'output', %s, %s, %s, NOW()
                    FROM pipeline_steps ps
                    WHERE ps.run_id = %s AND ps.step_name = 'render'
                    LIMIT 1
                """, (str(uuid4()), video_url, file_size, int(duration * 1000) if duration else None, run_id))
                conn.commit()
                logger.info(f"📊 [OBSERVABILITY] Run {run_id[:8]}... marcado como completed")
        except Exception as obs_err:
            logger.warning(f"⚠️ [OBSERVABILITY] Erro (não crítico): {obs_err}")
        
        # 📺 SSE: Emitir eventos de pipeline IMEDIATAMENTE (antes do HLS)
        try:
            emit_step_complete(job_id, 'RENDER', duration_ms=int(rendering_time * 1000) if rendering_time else None)
            emit_job_complete(job_id, video_url, duration_ms=int(rendering_time * 1000) if rendering_time else None)
            logger.info(f"📡 [SSE] Job complete emitido para {job_id[:8]}...")
        except Exception as sse_error:
            logger.warning(f"⚠️ Erro ao emitir SSE pipeline: {sse_error}")
        
        cursor.close()
        conn.close()
        
        # ═══════════════════════════════════════════════════════════════════
        # 🆕 12/Fev/2026 v2: ENTREGA COMPLETA EM BACKGROUND
        # HLS + Thumbnail + Dimensões → depois cria mensagem no chat
        # O webhook retorna 200 imediatamente; o vídeo só aparece no chat
        # quando tudo estiver pronto (HLS streaming, thumbnail, aspect ratio).
        # ═══════════════════════════════════════════════════════════════════
        import threading
        _bg_data = {
            'job_id': job_id,
            'user_id': user_id,
            'video_url': video_url,
            'project_id': str(project_id) if project_id else None,
            'conversation_id': str(conversation_id),
            'phase1_video_url': phase1_video_url,
            'duration': duration,
            'file_size': file_size,
            'rendering_time': rendering_time,
            'phrase_groups': job_data.get('phrase_groups', []) if job_data else [],
            'speech_segments': job_data.get('speech_segments', []) if job_data else [],
            'render_user_id': job_data.get('user_id') if job_data else user_id,
        }
        threading.Thread(
            target=_deliver_video_with_enrichment,
            args=(_bg_data,),
            daemon=True,
        ).start()
        
        return jsonify({
            "status": "success",
            "conversation_id": str(conversation_id),
            "video_url": video_url,
            "note": "Video delivery em background (HLS + thumbnail)"
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erro no webhook render-complete: {e}")
        import traceback
        traceback.print_exc()
        
        # 📺 WOW Feature: Emitir evento de erro
        try:
            emit_job_error(job_id, str(e), step='RENDER')
        except:
            pass
        
        # 🆕 v4.4.2: Notificar admin sobre falha no render
        try:
            from app.services.admin_notification_service import notify_render_failure
            notify_render_failure(
                job_id=job_id,
                error_message=str(e),
                project_id=job_data.get('project_id') if job_data else None,
            )
        except:
            pass
        
        return jsonify({"error": str(e)}), 500


@video_callbacks_bp.route('/webhook/process-complete', methods=['POST'])
def process_complete_webhook():
    """
    POST /api/webhook/process-complete
    
    Callback interno quando o processamento da Fase 1 é concluído.
    Pode ser usado para notificar o frontend ou disparar próximos passos.
    
    Request Body:
    {
        "job_id": "uuid",
        "status": "completed",
        "conversation_id": "uuid",
        "project_id": "uuid",
        "output_video_url": "https://...",
        "transcription_text": "...",
        "total_duration_ms": 45000,
        "completed_at": "2025-11-30T..."
    }
    """
    try:
        from app.supabase_client import get_direct_db_connection
        
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        job_id = data.get('job_id')
        status = data.get('status')
        conversation_id = data.get('conversation_id')
        project_id = data.get('project_id')
        output_video_url = data.get('output_video_url')
        transcription_text = data.get('transcription_text')
        
        logger.info(f"🎬 Process complete webhook: job={job_id}, status={status}")
        
        if status != 'completed':
            # Notificar erro no chat
            if conversation_id:
                conn = get_direct_db_connection()
                cursor = conn.cursor()
                
                error_message = data.get('error_message', 'Erro desconhecido no processamento')
                content = f"❌ Erro no processamento: {error_message}"
                
                # 🔧 FIX 29/Jan/2026: Usar chatbot_messages (tabela principal do V4)
                import uuid as uuid_module
                error_msg_id = str(uuid_module.uuid4())
                cursor.execute("""
                    INSERT INTO chatbot_messages (
                        id, message_id, conversation_id, sender, content, created_at
                    ) VALUES (%s, %s, %s, 'bot', %s, NOW())
                """, (error_msg_id, error_msg_id, conversation_id, content))
                
                conn.commit()
                cursor.close()
                conn.close()
            
            return jsonify({"status": "error_notified"}), 200
        
        # Processamento concluído com sucesso
        if conversation_id:
            conn = get_direct_db_connection()
            cursor = conn.cursor()
            
            # Criar mensagem de sucesso
            content = "✅ Processamento concluído! Seu vídeo foi normalizado, silêncios cortados e transcrito."
            
            component_props = json.dumps({
                "type": "process_complete",
                "job_id": job_id,
                "project_id": project_id,
                "output_video_url": output_video_url,
                "has_transcription": bool(transcription_text),
                "buttons": [
                    {"text": "📝 Ver transcrição", "action": "show_transcription"},
                    {"text": "▶️ Próximo passo", "action": "next_step"}
                ],
                "source": "process_complete_webhook",
                "completed_at": data.get('completed_at')
            })
            
            # 🔧 FIX 29/Jan/2026: Usar chatbot_messages (tabela principal do V4)
            import uuid as uuid_module
            proc_msg_id = str(uuid_module.uuid4())
            cursor.execute("""
                INSERT INTO chatbot_messages (
                    id, message_id, conversation_id, sender, content, component_type, component_props, created_at
                ) VALUES (%s, %s, %s, 'bot', %s, 'process_complete', %s, NOW())
            """, (proc_msg_id, proc_msg_id, conversation_id, content, component_props))
            
            conn.commit()
            cursor.close()
            conn.close()
        
        logger.info(f"✅ Process complete notificado para conversation {conversation_id}")
        
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        logger.error(f"❌ Erro no webhook process-complete: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

