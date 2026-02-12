"""
Render Versions API - Histórico de Renderizações

Endpoints para gerenciar versões de vídeos renderizados.

Endpoints:
    GET  /api/render-versions/<job_id>          - Lista versões de um job
    GET  /api/render-versions/<job_id>/current  - Retorna versão atual
    POST /api/render-versions/<job_id>/restore  - Restaura versão anterior
    POST /api/render-versions                   - Cria nova versão (interno)

Versão: 1.0.0
Data: 23/Jan/2026
"""

import os
import logging
from typing import Optional

from flask import Blueprint, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor, Json

logger = logging.getLogger(__name__)

render_versions_bp = Blueprint('render_versions', __name__, url_prefix='/api/render-versions')


def get_db_connection():
    """Obtém conexão com o banco de dados."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL não configurada")
    return psycopg2.connect(db_url)


# =============================================================================
# LISTAR VERSÕES
# =============================================================================

@render_versions_bp.route('/<job_id>', methods=['GET'])
def list_versions(job_id: str):
    """
    Lista todas as versões de um job.
    
    Query params:
        - phase: Filtrar por fase (1 ou 2)
        
    Returns:
        {
            "job_id": "xxx",
            "versions": [
                {
                    "id": "...",
                    "version_number": 2,
                    "phase": 2,
                    "video_url": "https://...",
                    "is_current": true,
                    "created_at": "2026-01-23T14:30:00Z",
                    "version_note": "Ajustei fonte"
                },
                ...
            ],
            "total": 2
        }
    """
    try:
        phase = request.args.get('phase', type=int)
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if phase:
                cur.execute("""
                    SELECT 
                        id, version_number, phase, video_url, thumbnail_url,
                        is_current, created_at, version_note,
                        duration_ms, file_size_bytes, resolution,
                        worker_id, render_duration_ms
                    FROM render_versions
                    WHERE job_id = %s AND phase = %s AND is_deleted = FALSE
                    ORDER BY version_number DESC
                """, (job_id, phase))
            else:
                cur.execute("""
                    SELECT 
                        id, version_number, phase, video_url, thumbnail_url,
                        is_current, created_at, version_note,
                        duration_ms, file_size_bytes, resolution,
                        worker_id, render_duration_ms
                    FROM render_versions
                    WHERE job_id = %s AND is_deleted = FALSE
                    ORDER BY phase, version_number DESC
                """, (job_id,))
            
            versions = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        
        return jsonify({
            "job_id": job_id,
            "versions": versions,
            "total": len(versions)
        })
        
    except Exception as e:
        logger.error(f"Erro ao listar versões: {e}")
        return jsonify({"error": str(e)}), 500


@render_versions_bp.route('/<job_id>/current', methods=['GET'])
def get_current_version(job_id: str):
    """
    Retorna a versão atual de um job.
    
    Query params:
        - phase: Fase (1 ou 2, default: 2)
        
    Returns:
        {
            "id": "...",
            "version_number": 2,
            "video_url": "https://...",
            ...
        }
    """
    try:
        phase = request.args.get('phase', 2, type=int)
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM render_versions
                WHERE job_id = %s AND phase = %s AND is_current = TRUE AND is_deleted = FALSE
            """, (job_id, phase))
            
            version = cur.fetchone()
        
        conn.close()
        
        if not version:
            return jsonify({"error": "Nenhuma versão encontrada"}), 404
        
        return jsonify(dict(version))
        
    except Exception as e:
        logger.error(f"Erro ao buscar versão atual: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# RESTAURAR VERSÃO
# =============================================================================

@render_versions_bp.route('/<job_id>/restore', methods=['POST'])
def restore_version(job_id: str):
    """
    Restaura uma versão anterior.
    
    Body:
        {
            "version_id": "uuid da versão a restaurar"
        }
        
    Returns:
        {
            "success": true,
            "new_version_id": "...",
            "message": "Versão 2 restaurada como versão 4"
        }
    """
    try:
        data = request.get_json()
        version_id = data.get('version_id')
        
        if not version_id:
            return jsonify({"error": "version_id é obrigatório"}), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Verificar se a versão existe e pertence ao job
            cur.execute("""
                SELECT id, version_number, phase, video_url 
                FROM render_versions 
                WHERE id = %s AND job_id = %s AND is_deleted = FALSE
            """, (version_id, job_id))
            
            old_version = cur.fetchone()
            if not old_version:
                conn.close()
                return jsonify({"error": "Versão não encontrada"}), 404
            
            # Usar função SQL para restaurar
            cur.execute("SELECT restore_render_version(%s) as new_version_id", (version_id,))
            result = cur.fetchone()
            new_version_id = result['new_version_id']
            
            # Buscar número da nova versão
            cur.execute("SELECT version_number FROM render_versions WHERE id = %s", (new_version_id,))
            new_version = cur.fetchone()
            
            conn.commit()
        
        conn.close()
        
        return jsonify({
            "success": True,
            "new_version_id": str(new_version_id),
            "restored_from_version": old_version['version_number'],
            "new_version_number": new_version['version_number'],
            "message": f"Versão {old_version['version_number']} restaurada como versão {new_version['version_number']}"
        })
        
    except Exception as e:
        logger.error(f"Erro ao restaurar versão: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# CRIAR VERSÃO (uso interno pelo sistema)
# =============================================================================

@render_versions_bp.route('', methods=['POST'])
def create_version():
    """
    Cria uma nova versão de render.
    
    Normalmente chamado pelo callback de render-complete.
    
    Body:
        {
            "job_id": "...",
            "phase": 2,
            "video_url": "https://...",
            "user_id": "...",
            "template_id": "...",
            "template_config": {...},
            "phrase_groups": [...],
            "render_settings": {...},
            "worker_id": "hetzner",
            "render_duration_ms": 45000,
            "duration_ms": 30000,
            "file_size_bytes": 5000000,
            "resolution": "1080x1920",
            "version_note": "Render automático"
        }
    """
    try:
        data = request.get_json()
        
        job_id = data.get('job_id')
        phase = data.get('phase', 2)
        video_url = data.get('video_url')
        
        if not job_id or not video_url:
            return jsonify({"error": "job_id e video_url são obrigatórios"}), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Usar função SQL para criar versão
            cur.execute("""
                SELECT create_render_version(
                    p_job_id := %s,
                    p_phase := %s,
                    p_video_url := %s,
                    p_created_by := %s,
                    p_template_id := %s,
                    p_template_config := %s,
                    p_phrase_groups := %s,
                    p_render_settings := %s,
                    p_worker_id := %s,
                    p_render_duration_ms := %s,
                    p_duration_ms := %s,
                    p_file_size_bytes := %s,
                    p_resolution := %s,
                    p_version_note := %s
                ) as version_id
            """, (
                job_id,
                phase,
                video_url,
                data.get('user_id'),
                data.get('template_id'),
                Json(data.get('template_config')) if data.get('template_config') else None,
                Json(data.get('phrase_groups')) if data.get('phrase_groups') else None,
                Json(data.get('render_settings')) if data.get('render_settings') else None,
                data.get('worker_id'),
                data.get('render_duration_ms'),
                data.get('duration_ms'),
                data.get('file_size_bytes'),
                data.get('resolution'),
                data.get('version_note', 'Render automático')
            ))
            
            result = cur.fetchone()
            version_id = result['version_id']
            
            # Buscar versão criada
            cur.execute("""
                SELECT id, version_number, phase, video_url, is_current, created_at
                FROM render_versions WHERE id = %s
            """, (version_id,))
            
            version = dict(cur.fetchone())
            
            conn.commit()
        
        conn.close()
        
        logger.info(f"📼 [RENDER_VERSION] Criada versão {version['version_number']} para job {job_id} (fase {phase})")
        
        return jsonify({
            "success": True,
            "version": version
        }), 201
        
    except Exception as e:
        logger.error(f"Erro ao criar versão: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# ADICIONAR NOTA À VERSÃO
# =============================================================================

@render_versions_bp.route('/<version_id>/note', methods=['PUT'])
def update_version_note(version_id: str):
    """
    Adiciona ou atualiza nota em uma versão.
    
    Body:
        {
            "note": "Versão aprovada pelo cliente"
        }
    """
    try:
        data = request.get_json()
        note = data.get('note', '')
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE render_versions 
                SET version_note = %s 
                WHERE id = %s
                RETURNING id
            """, (note, version_id))
            
            if cur.rowcount == 0:
                conn.close()
                return jsonify({"error": "Versão não encontrada"}), 404
            
            conn.commit()
        
        conn.close()
        
        return jsonify({
            "success": True,
            "message": "Nota atualizada"
        })
        
    except Exception as e:
        logger.error(f"Erro ao atualizar nota: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# SOFT DELETE VERSÃO
# =============================================================================

@render_versions_bp.route('/<version_id>', methods=['DELETE'])
def delete_version(version_id: str):
    """
    Soft delete de uma versão (marca como is_deleted = TRUE).
    
    Não permite deletar a versão atual.
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Verificar se é a versão atual
            cur.execute("""
                SELECT is_current FROM render_versions WHERE id = %s
            """, (version_id,))
            
            version = cur.fetchone()
            if not version:
                conn.close()
                return jsonify({"error": "Versão não encontrada"}), 404
            
            if version['is_current']:
                conn.close()
                return jsonify({"error": "Não é possível deletar a versão atual"}), 400
            
            # Soft delete
            cur.execute("""
                UPDATE render_versions SET is_deleted = TRUE WHERE id = %s
            """, (version_id,))
            
            conn.commit()
        
        conn.close()
        
        return jsonify({
            "success": True,
            "message": "Versão removida"
        })
        
    except Exception as e:
        logger.error(f"Erro ao deletar versão: {e}")
        return jsonify({"error": str(e)}), 500
