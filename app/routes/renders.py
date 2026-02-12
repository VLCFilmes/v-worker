"""
Renders Blueprint - Endpoints para polling de status de renders
Usado pelo frontend para verificar status de jobs do v-editor
"""

from flask import Blueprint, jsonify, request
import logging
from ..supabase_client import get_direct_db_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

renders_bp = Blueprint('renders', __name__)

@renders_bp.route('/renders/<job_id>', methods=['GET'])
def get_render_status(job_id):
    """
    GET /api/renders/{jobId}
    
    Retorna o status atual de um render job.
    Usado pelo frontend para polling enquanto o v-editor processa.
    
    Response:
    {
        "job_id": "uuid",
        "status": "pending|rendering|completed|failed",
        "progress_percent": 0-100,
        "video_url": "https://...",
        "thumbnail_url": "https://...",
        "error_message": "...",
        "created_at": "2025-10-08T...",
        "started_at": "2025-10-08T...",
        "completed_at": "2025-10-08T..."
    }
    """
    
    try:
        # Buscar job no banco de dados
        conn = get_direct_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                pr.id as job_id,
                pr.status,
                pr.rendered_video_url as video_url,
                pr.rendered_thumb_url as thumbnail_url,
                pr.created_at,
                pr.version,
                pr.timeline_json,
                p.project_id,
                p.name as project_name,
                p.project_type
            FROM project_renders pr
            LEFT JOIN projects p ON p.project_id = pr.project_id
            WHERE pr.id = %s
        """
        
        cursor.execute(query, (job_id,))
        job = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not job:
            logger.warning(f"Job {job_id} not found")
            return jsonify({
                "error": "Job not found",
                "job_id": job_id
            }), 404
        
        # Montar response
        response = {
            "job_id": str(job['job_id']),
            "status": job['status'],
            "video_url": job['video_url'],
            "thumbnail_url": job['thumbnail_url'],
            "created_at": job['created_at'].isoformat() if job['created_at'] else None,
            "version": job['version'],
            "project": {
                "project_id": str(job['project_id']) if job['project_id'] else None,
                "name": job['project_name'],
                "type": job['project_type']
            }
        }
        
        logger.info(f"Job {job_id} status: {job['status']}")
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Error fetching job {job_id}: {str(e)}", exc_info=True)
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500


@renders_bp.route('/renders/<job_id>/update', methods=['POST'])
def update_render_status(job_id):
    """
    POST /api/renders/{jobId}/update
    
    Atualiza o status de um render job.
    Chamado pelo v-editor durante o processamento.
    
    Body:
    {
        "status": "rendering|completed|failed",
        "progress_percent": 0-100,
        "video_url": "https://...",
        "thumbnail_url": "https://...",
        "error_message": "..."
    }
    """
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Validar status
        valid_statuses = ['pending', 'rendering', 'completed', 'failed', 'cancelled']
        status = data.get('status')
        
        if status and status not in valid_statuses:
            return jsonify({
                "error": "Invalid status",
                "valid_statuses": valid_statuses
            }), 400
        
        # Montar UPDATE query dinamicamente
        update_fields = []
        update_values = []
        
        if status:
            update_fields.append("status = %s")
            update_values.append(status)
        
        if 'video_url' in data:
            update_fields.append("rendered_video_url = %s")
            update_values.append(data['video_url'])
        
        if 'thumbnail_url' in data:
            update_fields.append("rendered_thumb_url = %s")
            update_values.append(data['thumbnail_url'])
        
        if not update_fields:
            return jsonify({"error": "No fields to update"}), 400
        
        # Adicionar job_id ao final
        update_values.append(job_id)
        
        # Executar UPDATE
        conn = get_direct_db_connection()
        cursor = conn.cursor()
        
        query = f"""
            UPDATE project_renders
            SET {', '.join(update_fields)}
            WHERE id = %s
            RETURNING id, status
        """
        
        cursor.execute(query, update_values)
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({"error": "Job not found"}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Job {job_id} updated to status: {status}")
        
        return jsonify({
            "success": True,
            "job_id": str(result[0]),
            "status": result[1]
        }), 200
        
    except Exception as e:
        logger.error(f"Error updating job {job_id}: {str(e)}", exc_info=True)
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

