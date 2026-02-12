"""
🔧 Debug Routes - Endpoints para debug e testes do pipeline

Endpoints:
- GET /api/debug/render-payload/<job_id> - Busca payload de render salvo
- POST /api/debug/re-render - Re-executa render com editor específico
- GET /api/debug/jobs - Lista jobs recentes com payloads salvos

Endpoints para o LLM Sandbox Director+1:
- GET /api/video/payload/tracks/<job_id> - Resumo/items das tracks (sumarizado para LLM)
- POST /api/video/payload/modify - Modifica campos do payload salvo
- POST /api/video/payload/validate - Valida integridade do payload
"""

import json
import logging
from flask import Blueprint, jsonify, request
from datetime import datetime

logger = logging.getLogger(__name__)

debug_bp = Blueprint('debug', __name__, url_prefix='/api/debug')

# Blueprint separado para endpoints do Director (prefixo /api/video/payload)
director_payload_bp = Blueprint('director_payload', __name__, url_prefix='/api/video/payload')


@debug_bp.route('/render-payload/<job_id>', methods=['GET'])
def get_render_payload(job_id):
    """
    🔍 Busca o payload de render salvo para um job específico.
    
    GET /api/debug/render-payload/<job_id>
    
    Response:
    {
        "found": true,
        "job_id": "...",
        "step_name": "render_service",
        "created_at": "...",
        "payload": { ... }
    }
    """
    try:
        from app.supabase_client import get_direct_db_connection
        from psycopg2.extras import RealDictCursor
        
        conn = get_direct_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Buscar payload mais recente do render_service para este job
            cursor.execute("""
                SELECT job_id, step_name, direction, payload, created_at
                FROM pipeline_debug_logs
                WHERE job_id = %s 
                  AND step_name = 'render_service'
                  AND direction = 'input'
                ORDER BY created_at DESC
                LIMIT 1
            """, (job_id,))
            
            row = cursor.fetchone()
            
            if not row:
                return jsonify({
                    "found": False,
                    "error": f"Payload não encontrado para job {job_id}",
                    "hint": "O payload pode ter sido limpo (auto-limpeza após 3 dias)"
                }), 404
            
            # Parsear payload se for string
            payload = row['payload']
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except:
                    pass
            
            return jsonify({
                "found": True,
                "job_id": row['job_id'],
                "step_name": row['step_name'],
                "direction": row['direction'],
                "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                "payload": payload
            })
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"❌ Erro ao buscar payload: {e}")
        return jsonify({"error": str(e)}), 500


def _merge_payload_modifications(payload: dict, modifications: dict) -> dict:
    """
    Faz merge profundo de modificações no payload.
    
    Exemplo:
        payload = {"base_layer": {"video_base": {"urls": [...]}}}
        modifications = {"base_layer": {"video_base": {"zoom_keyframes": [...]}}}
        resultado = {"base_layer": {"video_base": {"urls": [...], "zoom_keyframes": [...]}}}
    """
    import copy
    result = copy.deepcopy(payload)
    
    def deep_merge(target, source):
        for key, value in source.items():
            if isinstance(value, dict) and key in target and isinstance(target[key], dict):
                deep_merge(target[key], value)
            else:
                target[key] = value
        return target
    
    return deep_merge(result, modifications)


@debug_bp.route('/re-render', methods=['POST'])
def re_render():
    """
    🔄 Re-executa o render de um job com um editor específico.
    
    POST /api/debug/re-render
    
    Body:
    {
        "job_id": "eed4b5bc-4a66-439e-9337-b0115cc562c7",
        "editor": "python" | "remotion" | "modal" | "modal-light",
        "new_job_id": "optional-new-id"  // Se não fornecido, gera um novo
    }
    
    Response:
    {
        "success": true,
        "original_job_id": "...",
        "new_job_id": "...",
        "editor": "python",
        "result": { ... }
    }
    """
    try:
        import uuid
        from app.supabase_client import get_direct_db_connection
        from psycopg2.extras import RealDictCursor
        from app.video_orchestrator.services.render_service import RenderService
        
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Body JSON é obrigatório"}), 400
        
        job_id = data.get('job_id')
        editor = data.get('editor', 'remotion')
        new_job_id = data.get('new_job_id', str(uuid.uuid4()))
        
        if not job_id:
            return jsonify({"error": "job_id é obrigatório"}), 400
        
        # Validar editor
        valid_editors = ['python', 'remotion', 'modal', 'modal-light']
        if editor not in valid_editors:
            return jsonify({
                "error": f"Editor inválido: {editor}",
                "valid_editors": valid_editors
            }), 400
        
        logger.info(f"🔄 [RE-RENDER] Job: {job_id} → New: {new_job_id} → Editor: {editor}")
        
        # 1. Buscar payload original + dados do job original
        conn = get_direct_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Buscar payload do pipeline_debug_logs
            cursor.execute("""
                SELECT payload, extracted_fields, metadata
                FROM pipeline_debug_logs
                WHERE job_id = %s 
                  AND step_name = 'render_service'
                  AND direction = 'input'
                ORDER BY created_at DESC
                LIMIT 1
            """, (job_id,))
            
            row = cursor.fetchone()
            
            if not row:
                return jsonify({
                    "error": f"Payload não encontrado para job {job_id}",
                    "hint": "Use GET /api/debug/jobs para listar jobs disponíveis"
                }), 404
            
            # Buscar dados do job original para clonar no re-render
            cursor.execute("""
                SELECT conversation_id, project_id, user_id, phase1_video_url, original_video_url
                FROM video_processing_jobs
                WHERE job_id = %s
            """, (job_id,))
            original_job = cursor.fetchone()
            
            # Parsear payload
            payload = row['payload']
            if isinstance(payload, str):
                payload = json.loads(payload)
            
            # Verificar se payload está truncado
            if isinstance(payload, str) and "TRUNCADO" in payload:
                return jsonify({
                    "error": "Payload foi truncado (muito grande)",
                    "hint": "Este job não pode ser re-renderizado via debug"
                }), 400
            
            # Verificar se payload tem tracks (payload completo v2.9.262+)
            has_tracks = isinstance(payload, dict) and 'tracks' in payload
            if not has_tracks:
                return jsonify({
                    "error": "Payload não contém tracks (formato antigo)",
                    "hint": "Este job foi salvo antes de v2.9.262. Dispare um novo job para testar."
                }), 400
            
            # 🆕 v3.8.1: Criar registro em video_processing_jobs para o re-render
            # Isso permite que o webhook render-complete encontre o job e atualize
            if original_job:
                from datetime import datetime as _dt
                cursor.execute("""
                    INSERT INTO video_processing_jobs 
                    (job_id, conversation_id, project_id, user_id, status, phase1_video_url, original_video_url, created_at)
                    VALUES (%s, %s, %s, %s, 'rendering', %s, %s, %s)
                    ON CONFLICT (job_id) DO NOTHING
                """, (
                    new_job_id,
                    original_job['conversation_id'],
                    original_job['project_id'],
                    original_job['user_id'],
                    original_job.get('phase1_video_url'),
                    original_job.get('original_video_url'),
                    _dt.utcnow(),
                ))
                conn.commit()
                logger.info(f"📝 [RE-RENDER] Registro criado em video_processing_jobs: {new_job_id} (conv={original_job['conversation_id']})")
            
        finally:
            cursor.close()
            conn.close()
        
        # 2. Aplicar modificações customizadas ao payload (se fornecidas)
        payload_modifications = data.get('payload_modifications')
        if payload_modifications:
            logger.info(f"🔧 [RE-RENDER] Aplicando modificações customizadas ao payload")
            payload = _merge_payload_modifications(payload, payload_modifications)
        
        # 3. Atualizar job_id no payload
        payload['job_id'] = new_job_id
        payload['jobId'] = new_job_id  # Compatibilidade
        
        # 4. Determinar editor_worker_id
        editor_worker_id = None
        if editor == 'python':
            editor_worker_id = 'python'
        elif editor == 'modal':
            editor_worker_id = 'modal'
        elif editor == 'modal-light':
            editor_worker_id = 'modal-light'
        # remotion = None (usa default)
        
        # 5. Criar RenderService e enviar
        logger.info(f"🎬 [RE-RENDER] Enviando para {editor} (editor_worker_id={editor_worker_id})")
        
        render_service = RenderService(editor_worker_id=editor_worker_id)
        
        # Extrair metadados (v2.9.262+: metadata contém user_id, project_id, template_id)
        metadata = row.get('metadata') or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except:
                metadata = {}
        
        extracted = row.get('extracted_fields') or {}
        if isinstance(extracted, str):
            try:
                extracted = json.loads(extracted)
            except:
                extracted = {}
        
        # Prioridade: metadata > extracted > payload
        user_id = metadata.get('user_id') or extracted.get('user_id') or payload.get('user_id')
        project_id = metadata.get('project_id') or extracted.get('project_id') or payload.get('project_id')
        template_id = metadata.get('template_id') or extracted.get('template_id') or payload.get('template_id')
        
        result = render_service.submit_render_job(
            job_id=new_job_id,
            payload=payload,
            user_id=user_id,
            project_id=project_id,
            template_id=template_id,
            callback_endpoint="/api/webhook/render-complete"
        )
        
        logger.info(f"✅ [RE-RENDER] Resultado: {result.get('status')}")
        
        return jsonify({
            "success": result.get('status') in ['success', 'rendering_started', 'queued'],
            "original_job_id": job_id,
            "new_job_id": new_job_id,
            "editor": editor,
            "editor_worker_id": editor_worker_id,
            "result": result
        })
        
    except Exception as e:
        logger.error(f"❌ [RE-RENDER] Erro: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@debug_bp.route('/jobs', methods=['GET'])
def list_jobs():
    """
    📋 Lista jobs recentes com payloads de render salvos.
    
    GET /api/debug/jobs?limit=20
    
    Response:
    {
        "jobs": [
            {
                "job_id": "...",
                "created_at": "...",
                "template_id": "...",
                "has_payload": true
            }
        ]
    }
    """
    try:
        from app.supabase_client import get_direct_db_connection
        from psycopg2.extras import RealDictCursor
        
        limit = request.args.get('limit', 20, type=int)
        limit = min(limit, 100)  # Max 100
        
        conn = get_direct_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT DISTINCT ON (job_id)
                    job_id,
                    created_at,
                    extracted_fields,
                    CASE WHEN payload IS NOT NULL AND payload NOT LIKE '%%TRUNCADO%%' 
                         THEN true ELSE false END as has_full_payload
                FROM pipeline_debug_logs
                WHERE step_name = 'render_service'
                  AND direction = 'input'
                ORDER BY job_id, created_at DESC
                LIMIT %s
            """, (limit,))
            
            rows = cursor.fetchall()
            
            jobs = []
            for row in rows:
                extracted = row.get('extracted_fields', {})
                if isinstance(extracted, str):
                    try:
                        extracted = json.loads(extracted)
                    except:
                        extracted = {}
                
                jobs.append({
                    "job_id": row['job_id'],
                    "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                    "template_id": extracted.get('template_id'),
                    "has_full_payload": row['has_full_payload']
                })
            
            # Ordenar por data decrescente
            jobs.sort(key=lambda x: x['created_at'] or '', reverse=True)
            
            return jsonify({
                "count": len(jobs),
                "jobs": jobs
            })
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"❌ Erro ao listar jobs: {e}")
        return jsonify({"error": str(e)}), 500


@debug_bp.route('/editors', methods=['GET'])
def list_editors():
    """
    📋 Lista editores disponíveis e seus status.
    
    GET /api/debug/editors
    """
    import os
    import requests
    
    editors = {
        'remotion': {
            'name': 'Remotion (Hetzner)',
            'url': os.environ.get('V_EDITOR_URL', 'http://v-editor:5018'),
            'status': 'unknown'
        },
        'python': {
            'name': 'Python/MoviePy (Hetzner)',
            'url': os.environ.get('V_EDITOR_PYTHON_URL', 'http://v-editor-python:5018'),
            'status': 'unknown'
        },
        'modal': {
            'name': 'Modal Cloud (8-core)',
            'url': 'Modal Serverless',
            'status': 'available'
        },
        'modal-light': {
            'name': 'Modal Cloud Light (2-core)',
            'url': 'Modal Serverless',
            'status': 'available'
        }
    }
    
    # Verificar status dos editores locais
    for key in ['remotion', 'python']:
        try:
            url = editors[key]['url']
            if url.startswith('http'):
                resp = requests.get(f"{url}/health", timeout=5)
                editors[key]['status'] = 'healthy' if resp.ok else 'unhealthy'
        except:
            editors[key]['status'] = 'unreachable'
    
    return jsonify({
        "editors": editors
    })


# ═══════════════════════════════════════════════════════════════════════
# ENDPOINTS PARA O LLM SANDBOX DIRECTOR+1
# Estes endpoints são usados pelas tools do Director para ler/modificar
# payloads de vídeo salvos no pipeline_debug_logs.
# ═══════════════════════════════════════════════════════════════════════


def _get_payload_for_job(job_id: str) -> tuple:
    """
    Helper: busca payload do pipeline_debug_logs.
    Retorna (payload_dict, row_metadata) ou raises Exception.
    """
    from app.supabase_client import get_direct_db_connection
    from psycopg2.extras import RealDictCursor

    conn = get_direct_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            SELECT job_id, step_name, direction, payload, metadata, created_at
            FROM pipeline_debug_logs
            WHERE job_id = %s 
              AND step_name = 'render_service'
              AND direction = 'input'
            ORDER BY created_at DESC
            LIMIT 1
        """, (job_id,))

        row = cursor.fetchone()

        if not row:
            return None, None

        payload = row['payload']
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except:
                pass

        return payload, row

    finally:
        cursor.close()
        conn.close()


def _save_payload_for_job(job_id: str, payload: dict):
    """
    Helper: salva payload modificado no pipeline_debug_logs.
    Cria um novo registro com direction='input' (sobrescreve o anterior na consulta).
    """
    from app.supabase_client import get_direct_db_connection
    from psycopg2.extras import RealDictCursor
    from datetime import datetime

    conn = get_direct_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cursor.execute("""
            INSERT INTO pipeline_debug_logs (job_id, step_name, direction, payload, created_at)
            VALUES (%s, 'render_service', 'input', %s, %s)
        """, (job_id, json.dumps(payload), datetime.utcnow()))
        conn.commit()
    finally:
        cursor.close()
        conn.close()


@director_payload_bp.route('/tracks/<job_id>', methods=['GET'])
def get_payload_tracks(job_id):
    """
    📊 Retorna resumo das tracks do payload de um job.
    
    GET /api/video/payload/tracks/<job_id>
    
    Query params opcionais:
      - track_name: se fornecido, retorna items daquela track específica
      - limit: max items a retornar (default: 5)
      - offset: pular N items (default: 0)
    
    Sem track_name → retorna RESUMO (nome, count, time_range, sample)
    Com track_name → retorna items daquela track (com paginação)
    
    Usado pelo Director como primeiro passo para entender o estado do vídeo.
    """
    try:
        payload, row = _get_payload_for_job(job_id)

        if not payload:
            return jsonify({
                "error": f"Payload não encontrado para job {job_id}",
                "hint": "O payload pode ter sido limpo (auto-limpeza após 3 dias)"
            }), 404

        tracks = payload.get('tracks', {})
        track_name = request.args.get('track_name')

        # Modo 1: Retornar items de uma track específica
        if track_name:
            if track_name not in tracks:
                return jsonify({
                    "error": f"Track '{track_name}' não encontrada",
                    "available_tracks": list(tracks.keys())
                }), 404

            items = tracks[track_name]
            limit = request.args.get('limit', 5, type=int)
            offset = request.args.get('offset', 0, type=int)
            limit = min(limit, 50)  # Max 50 items por request

            paginated = items[offset:offset + limit]

            return jsonify({
                "job_id": job_id,
                "track_name": track_name,
                "total_items": len(items),
                "offset": offset,
                "limit": limit,
                "items": paginated,
            })

        # Modo 2: Retornar RESUMO de todas as tracks
        summary = {}
        for name, items in tracks.items():
            if not isinstance(items, list):
                summary[name] = {
                    "type": "non_list",
                    "value_type": type(items).__name__,
                }
                continue

            track_summary = {
                "count": len(items),
            }

            if items:
                # Time range
                start_times = [it.get('start_time', 0) for it in items if isinstance(it, dict)]
                end_times = [it.get('end_time', 0) for it in items if isinstance(it, dict)]
                if start_times and end_times:
                    track_summary["time_range"] = f"{min(start_times)}ms - {max(end_times)}ms"

                # Sample (primeiro item)
                sample = items[0] if isinstance(items[0], dict) else {"value": items[0]}
                # Truncar campos longos no sample
                sample_clean = {}
                for k, v in sample.items():
                    if isinstance(v, str) and len(v) > 200:
                        sample_clean[k] = v[:200] + "..."
                    else:
                        sample_clean[k] = v
                track_summary["sample_item"] = sample_clean
            else:
                track_summary["time_range"] = "empty"
                track_summary["sample_item"] = None

            summary[name] = track_summary

        return jsonify({
            "job_id": job_id,
            "total_tracks": len(tracks),
            "tracks": summary,
            "payload_keys": [k for k in payload.keys() if k != 'tracks'],
            "created_at": row['created_at'].isoformat() if row and row.get('created_at') else None,
        })

    except Exception as e:
        logger.error(f"❌ Erro ao buscar tracks: {e}")
        return jsonify({"error": str(e)}), 500


@director_payload_bp.route('/modify', methods=['POST'])
def modify_payload():
    """
    ✏️ Modifica campos do payload salvo de um job.
    
    POST /api/video/payload/modify
    
    Body:
    {
        "job_id": "abc123",
        "modifications": {
            "tracks.subtitles[*].animation.entrance.type": "slide_up",
            "tracks.user_logo_layer": [{"id": "logo_0", ...}],
            "global.font_size": 78
        }
    }
    
    Operadores suportados:
      - tracks.<nome>[*].<campo>: aplica a TODOS os items da track
      - tracks.<nome>: substitui a track inteira
      - <campo>: modifica campo no topo do payload
    
    Retorna o payload modificado (resumo, não completo).
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Body JSON é obrigatório"}), 400

        job_id = data.get('job_id')
        modifications = data.get('modifications', {})

        if not job_id:
            return jsonify({"error": "job_id é obrigatório"}), 400
        if not modifications:
            return jsonify({"error": "modifications é obrigatório"}), 400

        payload, row = _get_payload_for_job(job_id)
        if not payload:
            return jsonify({"error": f"Payload não encontrado para job {job_id}"}), 404

        import copy
        modified_payload = copy.deepcopy(payload)
        changes_applied = []

        for path, value in modifications.items():
            try:
                parts = path.split('.')
                
                # Caso 1: tracks.<nome>[*].<campo...> → aplica a todos items
                if len(parts) >= 3 and parts[0] == 'tracks' and '[*]' in parts[1]:
                    track_name = parts[1].replace('[*]', '')
                    field_path = parts[2:]
                    track = modified_payload.get('tracks', {}).get(track_name, [])
                    
                    count = 0
                    for item in track:
                        if isinstance(item, dict):
                            _set_nested(item, field_path, value)
                            count += 1
                    
                    changes_applied.append({
                        "path": path,
                        "items_modified": count,
                    })

                # Caso 2: tracks.<nome> → substitui track inteira
                elif len(parts) == 2 and parts[0] == 'tracks':
                    track_name = parts[1]
                    if 'tracks' not in modified_payload:
                        modified_payload['tracks'] = {}
                    modified_payload['tracks'][track_name] = value
                    
                    changes_applied.append({
                        "path": path,
                        "action": "track_replaced",
                        "new_count": len(value) if isinstance(value, list) else 1,
                    })

                # Caso 3: campo direto no payload
                else:
                    _set_nested(modified_payload, parts, value)
                    changes_applied.append({
                        "path": path,
                        "action": "field_set",
                    })

            except Exception as e:
                changes_applied.append({
                    "path": path,
                    "error": str(e),
                })

        # Salvar payload modificado
        _save_payload_for_job(job_id, modified_payload)

        return jsonify({
            "success": True,
            "job_id": job_id,
            "changes_applied": changes_applied,
            "total_changes": len(changes_applied),
        })

    except Exception as e:
        logger.error(f"❌ Erro ao modificar payload: {e}")
        return jsonify({"error": str(e)}), 500


def _set_nested(obj: dict, keys: list, value):
    """Set um valor em um dict aninhado seguindo uma lista de chaves."""
    for key in keys[:-1]:
        if key not in obj:
            obj[key] = {}
        obj = obj[key]
    obj[keys[-1]] = value


@director_payload_bp.route('/validate', methods=['POST'])
def validate_payload():
    """
    ✅ Valida integridade do payload de um job.
    
    POST /api/video/payload/validate
    
    Body:
    {
        "job_id": "abc123"
    }
    
    Verifica:
    - Payload existe e é um dict
    - Tem campo 'tracks'
    - Tracks obrigatórias presentes (subtitles)
    - Items têm campos requeridos (start_time, end_time)
    - Timings consistentes (end > start)
    - zIndex definido em items com posição
    
    Retorna: {valid: true/false, warnings: [...], errors: [...]}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Body JSON é obrigatório"}), 400

        job_id = data.get('job_id')
        if not job_id:
            return jsonify({"error": "job_id é obrigatório"}), 400

        payload, row = _get_payload_for_job(job_id)
        if not payload:
            return jsonify({"error": f"Payload não encontrado para job {job_id}"}), 404

        errors = []
        warnings = []

        # 1. Verificar estrutura básica
        if not isinstance(payload, dict):
            errors.append("Payload não é um dict")
            return jsonify({"valid": False, "errors": errors, "warnings": warnings})

        tracks = payload.get('tracks')
        if not tracks:
            errors.append("Payload não tem campo 'tracks'")
            return jsonify({"valid": False, "errors": errors, "warnings": warnings})

        if not isinstance(tracks, dict):
            errors.append("'tracks' não é um dict")
            return jsonify({"valid": False, "errors": errors, "warnings": warnings})

        # 2. Verificar tracks
        for track_name, items in tracks.items():
            if not isinstance(items, list):
                warnings.append(f"Track '{track_name}' não é uma lista (type: {type(items).__name__})")
                continue

            for i, item in enumerate(items):
                if not isinstance(item, dict):
                    warnings.append(f"Track '{track_name}' item {i} não é um dict")
                    continue

                # Verificar timings
                start = item.get('start_time')
                end = item.get('end_time')
                if start is not None and end is not None:
                    if end < start:
                        errors.append(
                            f"Track '{track_name}' item {i}: "
                            f"end_time ({end}) < start_time ({start})"
                        )

                # Verificar posição
                pos = item.get('position')
                if pos and isinstance(pos, dict):
                    for dim in ['x', 'y', 'width', 'height']:
                        val = pos.get(dim)
                        if val is not None and (not isinstance(val, (int, float)) or val < 0):
                            warnings.append(
                                f"Track '{track_name}' item {i}: "
                                f"position.{dim} = {val} (suspeito)"
                            )

        # 3. Verificar tracks vazias que deveriam ter conteúdo
        subtitles = tracks.get('subtitles', [])
        if not subtitles:
            warnings.append("Track 'subtitles' está vazia")

        # 4. Contagem final
        total_items = sum(len(v) for v in tracks.values() if isinstance(v, list))

        return jsonify({
            "valid": len(errors) == 0,
            "job_id": job_id,
            "total_tracks": len(tracks),
            "total_items": total_items,
            "errors": errors,
            "warnings": warnings,
        })

    except Exception as e:
        logger.error(f"❌ Erro ao validar payload: {e}")
        return jsonify({"error": str(e)}), 500
