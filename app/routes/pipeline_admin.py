"""
Pipeline Admin API - Endpoints para Dashboard de Observabilidade

Permite visualizar e gerenciar execuções do pipeline de vídeo.

Endpoints:
    GET /api/admin/pipeline/runs - Lista runs com filtros
    GET /api/admin/pipeline/runs/<run_id> - Detalhes de um run
    GET /api/admin/pipeline/runs/<run_id>/steps - Steps de um run
    GET /api/admin/pipeline/steps/<step_id> - Detalhes de um step
    GET /api/admin/pipeline/steps/<step_id>/payloads - Payloads de um step
    GET /api/admin/pipeline/steps/<step_id>/artifacts - Artifacts de um step
    GET /api/admin/pipeline/stats - Estatísticas gerais

Versão: 1.0.0
Data: 23/Jan/2026
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from flask import Blueprint, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

pipeline_admin_bp = Blueprint('pipeline_admin', __name__, url_prefix='/api/admin/pipeline')


def get_db_connection():
    """Obtém conexão com o banco de dados."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL não configurada")
    return psycopg2.connect(db_url)


# =============================================================================
# RUNS
# =============================================================================

@pipeline_admin_bp.route('/runs', methods=['GET'])
def list_runs():
    """
    Lista execuções do pipeline com filtros.
    
    Query params:
        - user_id: Filtrar por usuário
        - job_id: Filtrar por job
        - project_id: Filtrar por projeto
        - status: Filtrar por status (pending, running, completed, failed)
        - phase: Filtrar por fase (1, 2)
        - worker_id: Filtrar por worker
        - from_date: Data inicial (ISO format)
        - to_date: Data final (ISO format)
        - limit: Limite de resultados (default: 50, max: 200)
        - offset: Offset para paginação
        - order_by: Campo para ordenação (created_at, completed_at, total_duration_ms)
        - order_dir: Direção (asc, desc)
    
    Returns:
        {
            "runs": [...],
            "total": 123,
            "limit": 50,
            "offset": 0
        }
    """
    try:
        # Parâmetros de filtro
        user_id = request.args.get('user_id')
        job_id = request.args.get('job_id')
        project_id = request.args.get('project_id')
        status = request.args.get('status')
        phase = request.args.get('phase', type=int)
        worker_id = request.args.get('worker_id')
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        
        # Parâmetros de paginação
        limit = min(request.args.get('limit', 50, type=int), 200)
        offset = request.args.get('offset', 0, type=int)
        order_by = request.args.get('order_by', 'created_at')
        order_dir = request.args.get('order_dir', 'desc').upper()
        
        # Validar order_by
        valid_order_fields = ['created_at', 'completed_at', 'total_duration_ms', 'status']
        if order_by not in valid_order_fields:
            order_by = 'created_at'
        if order_dir not in ['ASC', 'DESC']:
            order_dir = 'DESC'
        
        # Construir query
        conditions = []
        params = []
        
        if user_id:
            conditions.append("r.user_id = %s")
            params.append(user_id)
        if job_id:
            conditions.append("r.job_id = %s")
            params.append(job_id)
        if project_id:
            conditions.append("r.project_id = %s")
            params.append(project_id)
        if status:
            conditions.append("r.status = %s")
            params.append(status)
        if phase:
            conditions.append("r.phase = %s")
            params.append(phase)
        if worker_id:
            conditions.append("r.worker_id = %s")
            params.append(worker_id)
        if from_date:
            conditions.append("r.created_at >= %s")
            params.append(from_date)
        if to_date:
            conditions.append("r.created_at <= %s")
            params.append(to_date)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Count total
            cur.execute(f"""
                SELECT COUNT(*) as total
                FROM pipeline_runs r
                WHERE {where_clause}
            """, params)
            total = cur.fetchone()['total']
            
            # Fetch runs with step counts
            cur.execute(f"""
                SELECT 
                    r.id,
                    r.job_id,
                    r.user_id,
                    r.project_id,
                    r.template_id,
                    r.phase,
                    r.run_number,
                    r.status,
                    r.worker_id,
                    r.backend_version,
                    r.created_at,
                    r.started_at,
                    r.completed_at,
                    r.total_duration_ms,
                    r.input_video_url,
                    r.output_video_url,
                    r.error_message,
                    COUNT(s.id) as total_steps,
                    COUNT(s.id) FILTER (WHERE s.status = 'completed') as completed_steps,
                    COUNT(s.id) FILTER (WHERE s.status = 'failed') as failed_steps
                FROM pipeline_runs r
                LEFT JOIN pipeline_steps s ON s.run_id = r.id
                WHERE {where_clause}
                GROUP BY r.id
                ORDER BY r.{order_by} {order_dir}
                LIMIT %s OFFSET %s
            """, params + [limit, offset])
            
            runs = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        
        return jsonify({
            "runs": runs,
            "total": total,
            "limit": limit,
            "offset": offset
        })
        
    except Exception as e:
        logger.error(f"Erro ao listar runs: {e}")
        return jsonify({"error": str(e)}), 500


@pipeline_admin_bp.route('/runs/<run_id>', methods=['GET'])
def get_run(run_id: str):
    """
    Obtém detalhes completos de um run.
    
    Inclui:
        - Informações do run
        - Lista de steps com status
        - Contagem de artifacts e payloads
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Run info
            cur.execute("""
                SELECT * FROM pipeline_runs WHERE id = %s
            """, (run_id,))
            run = cur.fetchone()
            
            if not run:
                return jsonify({"error": "Run not found"}), 404
            
            run = dict(run)
            
            # Steps with artifact/payload counts
            cur.execute("""
                SELECT 
                    s.*,
                    COUNT(DISTINCT a.id) as artifact_count,
                    COUNT(DISTINCT p.id) as payload_count,
                    COALESCE(
                        jsonb_object_agg(m.metric_name, m.metric_value) 
                        FILTER (WHERE m.metric_name IS NOT NULL), 
                        '{}'::jsonb
                    ) as metrics
                FROM pipeline_steps s
                LEFT JOIN pipeline_artifacts a ON a.step_id = s.id
                LEFT JOIN pipeline_payloads p ON p.step_id = s.id
                LEFT JOIN pipeline_metrics m ON m.step_id = s.id
                WHERE s.run_id = %s
                GROUP BY s.id
                ORDER BY s.step_order
            """, (run_id,))
            
            steps = [dict(row) for row in cur.fetchall()]
            run['steps'] = steps
        
        conn.close()
        
        return jsonify(run)
        
    except Exception as e:
        logger.error(f"Erro ao buscar run {run_id}: {e}")
        return jsonify({"error": str(e)}), 500


@pipeline_admin_bp.route('/runs/<run_id>/steps', methods=['GET'])
def get_run_steps(run_id: str):
    """Lista steps de um run."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM pipeline_steps
                WHERE run_id = %s
                ORDER BY step_order
            """, (run_id,))
            steps = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        return jsonify({"steps": steps})
        
    except Exception as e:
        logger.error(f"Erro ao buscar steps do run {run_id}: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# STEPS
# =============================================================================

@pipeline_admin_bp.route('/steps/<step_id>', methods=['GET'])
def get_step(step_id: str):
    """
    Obtém detalhes completos de um step.
    
    Inclui artifacts, payloads e métricas.
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Step info
            cur.execute("SELECT * FROM pipeline_steps WHERE id = %s", (step_id,))
            step = cur.fetchone()
            
            if not step:
                return jsonify({"error": "Step not found"}), 404
            
            step = dict(step)
            
            # Artifacts
            cur.execute("""
                SELECT * FROM pipeline_artifacts
                WHERE step_id = %s
                ORDER BY artifact_type, artifact_name
            """, (step_id,))
            step['artifacts'] = [dict(row) for row in cur.fetchall()]
            
            # Payloads
            cur.execute("""
                SELECT * FROM pipeline_payloads
                WHERE step_id = %s
                ORDER BY direction, created_at
            """, (step_id,))
            step['payloads'] = [dict(row) for row in cur.fetchall()]
            
            # Metrics
            cur.execute("""
                SELECT * FROM pipeline_metrics
                WHERE step_id = %s
            """, (step_id,))
            step['metrics'] = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        return jsonify(step)
        
    except Exception as e:
        logger.error(f"Erro ao buscar step {step_id}: {e}")
        return jsonify({"error": str(e)}), 500


@pipeline_admin_bp.route('/steps/<step_id>/payloads', methods=['GET'])
def get_step_payloads(step_id: str):
    """
    Obtém payloads (request/response) de um step.
    
    Query params:
        - direction: Filtrar por direção (request, response)
    """
    try:
        direction = request.args.get('direction')
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if direction:
                cur.execute("""
                    SELECT * FROM pipeline_payloads
                    WHERE step_id = %s AND direction = %s
                    ORDER BY created_at
                """, (step_id, direction))
            else:
                cur.execute("""
                    SELECT * FROM pipeline_payloads
                    WHERE step_id = %s
                    ORDER BY direction, created_at
                """, (step_id,))
            
            payloads = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        return jsonify({"payloads": payloads})
        
    except Exception as e:
        logger.error(f"Erro ao buscar payloads do step {step_id}: {e}")
        return jsonify({"error": str(e)}), 500


@pipeline_admin_bp.route('/steps/<step_id>/artifacts', methods=['GET'])
def get_step_artifacts(step_id: str):
    """
    Obtém artifacts de um step.
    
    Query params:
        - type: Filtrar por tipo (input, output, intermediate)
    """
    try:
        artifact_type = request.args.get('type')
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if artifact_type:
                cur.execute("""
                    SELECT * FROM pipeline_artifacts
                    WHERE step_id = %s AND artifact_type = %s
                    ORDER BY artifact_name
                """, (step_id, artifact_type))
            else:
                cur.execute("""
                    SELECT * FROM pipeline_artifacts
                    WHERE step_id = %s
                    ORDER BY artifact_type, artifact_name
                """, (step_id,))
            
            artifacts = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        return jsonify({"artifacts": artifacts})
        
    except Exception as e:
        logger.error(f"Erro ao buscar artifacts do step {step_id}: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# STATS
# =============================================================================

@pipeline_admin_bp.route('/stats', methods=['GET'])
def get_stats():
    """
    Obtém estatísticas gerais do pipeline.
    
    Query params:
        - period: Período (today, week, month, all) - default: today
    
    Returns:
        {
            "total_runs": 100,
            "completed_runs": 80,
            "failed_runs": 10,
            "running_runs": 5,
            "avg_duration_ms": 45000,
            "total_videos_processed": 75,
            "by_worker": {...},
            "by_step": {...}
        }
    """
    try:
        period = request.args.get('period', 'today')
        
        # Calcular data de início baseado no período
        now = datetime.utcnow()
        if period == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == 'week':
            start_date = now - timedelta(days=7)
        elif period == 'month':
            start_date = now - timedelta(days=30)
        else:
            start_date = None
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Filtro de data
            date_filter = "AND r.created_at >= %s" if start_date else ""
            date_params = [start_date] if start_date else []
            
            # Stats gerais
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total_runs,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed_runs,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_runs,
                    COUNT(*) FILTER (WHERE status = 'running') as running_runs,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_runs,
                    AVG(total_duration_ms) FILTER (WHERE status = 'completed') as avg_duration_ms,
                    COUNT(DISTINCT output_video_url) FILTER (WHERE output_video_url IS NOT NULL) as total_videos
                FROM pipeline_runs r
                WHERE 1=1 {date_filter}
            """, date_params)
            stats = dict(cur.fetchone())
            
            # Por worker
            cur.execute(f"""
                SELECT 
                    worker_id,
                    COUNT(*) as count,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    AVG(total_duration_ms) FILTER (WHERE status = 'completed') as avg_duration
                FROM pipeline_runs r
                WHERE 1=1 {date_filter}
                GROUP BY worker_id
            """, date_params)
            stats['by_worker'] = {row['worker_id']: dict(row) for row in cur.fetchall()}
            
            # Por step (falhas)
            cur.execute(f"""
                SELECT 
                    s.step_name,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE s.status = 'failed') as failed,
                    AVG(s.duration_ms) as avg_duration
                FROM pipeline_steps s
                JOIN pipeline_runs r ON r.id = s.run_id
                WHERE 1=1 {date_filter}
                GROUP BY s.step_name
                ORDER BY failed DESC
            """, date_params)
            stats['by_step'] = {row['step_name']: dict(row) for row in cur.fetchall()}
        
        conn.close()
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Erro ao buscar stats: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# SEARCH
# =============================================================================

@pipeline_admin_bp.route('/search', methods=['GET'])
def search():
    """
    Busca em runs, steps, artifacts e payloads.
    
    Query params:
        - q: Termo de busca (busca em error_message, artifact_name, etc.)
        - type: Tipo de entidade (run, step, artifact, payload)
        - limit: Limite de resultados
    """
    try:
        query = request.args.get('q', '')
        entity_type = request.args.get('type', 'run')
        limit = min(request.args.get('limit', 20, type=int), 100)
        
        if not query or len(query) < 3:
            return jsonify({"error": "Query must be at least 3 characters"}), 400
        
        search_term = f"%{query}%"
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if entity_type == 'run':
                cur.execute("""
                    SELECT id, job_id, status, error_message, created_at
                    FROM pipeline_runs
                    WHERE job_id::text ILIKE %s OR error_message ILIKE %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (search_term, search_term, limit))
            elif entity_type == 'step':
                cur.execute("""
                    SELECT s.id, s.run_id, s.step_name, s.status, s.error_message
                    FROM pipeline_steps s
                    WHERE s.step_name ILIKE %s OR s.error_message ILIKE %s
                    ORDER BY s.run_id DESC
                    LIMIT %s
                """, (search_term, search_term, limit))
            elif entity_type == 'artifact':
                cur.execute("""
                    SELECT a.id, a.step_id, a.artifact_name, a.url
                    FROM pipeline_artifacts a
                    WHERE a.artifact_name ILIKE %s OR a.url ILIKE %s
                    LIMIT %s
                """, (search_term, search_term, limit))
            else:
                return jsonify({"error": f"Unknown type: {entity_type}"}), 400
            
            results = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        return jsonify({"results": results, "count": len(results)})
        
    except Exception as e:
        logger.error(f"Erro na busca: {e}")
        return jsonify({"error": str(e)}), 500
