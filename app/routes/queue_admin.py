"""
Queue Admin API - Endpoints para Dashboard de Filas e Workers

Gerencia workers, regras de roteamento e estatísticas de filas.

Endpoints:
    GET /api/admin/queues/workers - Lista workers
    GET /api/admin/queues/workers/<id> - Detalhes de um worker
    PUT /api/admin/queues/workers/<id> - Atualiza worker
    POST /api/admin/queues/workers/<id>/toggle - Liga/desliga worker
    
    GET /api/admin/queues/rules - Lista regras de roteamento
    POST /api/admin/queues/rules - Cria nova regra
    PUT /api/admin/queues/rules/<id> - Atualiza regra
    DELETE /api/admin/queues/rules/<id> - Remove regra
    POST /api/admin/queues/rules/reorder - Reordena prioridades
    
    POST /api/admin/queues/simulate - Simula roteamento
    GET /api/admin/queues/stats - Estatísticas gerais

Versão: 1.0.0
Data: 23/Jan/2026
"""

import os
import json
import logging
import redis
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from flask import Blueprint, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor, Json

logger = logging.getLogger(__name__)

queue_admin_bp = Blueprint('queue_admin', __name__, url_prefix='/api/admin/queues')


def get_db_connection():
    """Obtém conexão com o banco de dados."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL não configurada")
    return psycopg2.connect(db_url)


def get_redis_connection():
    """Obtém conexão com Redis."""
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    return redis.from_url(redis_url)


# =============================================================================
# WORKERS
# =============================================================================

@queue_admin_bp.route('/workers', methods=['GET'])
def list_workers():
    """
    Lista todos os workers.
    
    Query params:
        - type: Filtrar por tipo (fixed, dynamic, serverless)
        - status: Filtrar por status (online, offline, etc.)
        - enabled: Filtrar por habilitado (true/false)
    """
    try:
        worker_type = request.args.get('type')
        status = request.args.get('status')
        enabled = request.args.get('enabled')
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT * FROM workers WHERE 1=1"
            params = []
            
            if worker_type:
                query += " AND type = %s"
                params.append(worker_type)
            
            if status:
                query += " AND status = %s"
                params.append(status)
            
            if enabled is not None:
                query += " AND enabled = %s"
                params.append(enabled.lower() == 'true')
            
            query += " ORDER BY type, name"
            
            cur.execute(query, params)
            workers = cur.fetchall()
            
            # Adicionar info de fila Redis para cada worker
            try:
                r = get_redis_connection()
                for worker in workers:
                    queue_key = f"video:queue:{worker['worker_id']}"
                    worker['queue_size'] = r.llen(queue_key)
            except Exception as redis_err:
                logger.warning(f"Erro ao buscar filas Redis: {redis_err}")
        
        conn.close()
        
        return jsonify({
            "workers": [dict(w) for w in workers],
            "total": len(workers)
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao listar workers: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/workers/<worker_id>', methods=['GET'])
def get_worker(worker_id: str):
    """Obtém detalhes de um worker."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM workers WHERE worker_id = %s", (worker_id,))
            worker = cur.fetchone()
            
            if not worker:
                return jsonify({"error": "Worker não encontrado"}), 404
            
            # Buscar estatísticas recentes
            cur.execute("""
                SELECT * FROM worker_stats 
                WHERE worker_id = %s 
                ORDER BY recorded_at DESC 
                LIMIT 10
            """, (worker_id,))
            stats = cur.fetchall()
            
            # Buscar logs de roteamento recentes
            cur.execute("""
                SELECT * FROM routing_logs 
                WHERE selected_worker_id = %s 
                ORDER BY created_at DESC 
                LIMIT 20
            """, (worker_id,))
            routing_logs = cur.fetchall()
        
        conn.close()
        
        # Info de fila Redis
        try:
            r = get_redis_connection()
            queue_key = f"video:queue:{worker_id}"
            worker['queue_size'] = r.llen(queue_key)
            worker['queue_items'] = [json.loads(item) for item in r.lrange(queue_key, 0, 9)]
        except Exception as redis_err:
            logger.warning(f"Erro ao buscar fila Redis: {redis_err}")
            worker['queue_size'] = 0
            worker['queue_items'] = []
        
        return jsonify({
            "worker": dict(worker),
            "stats": [dict(s) for s in stats],
            "routing_logs": [dict(l) for l in routing_logs]
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao buscar worker: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/workers/<worker_id>', methods=['PUT'])
def update_worker(worker_id: str):
    """Atualiza configurações de um worker."""
    try:
        data = request.get_json()
        
        allowed_fields = [
            'name', 'description', 'host', 'port', 'health_endpoint',
            'max_concurrent_jobs', 'enabled', 'supports_matting',
            'supports_render', 'supports_phase1'
        ]
        
        updates = {k: v for k, v in data.items() if k in allowed_fields}
        
        if not updates:
            return jsonify({"error": "Nenhum campo válido para atualizar"}), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
            values = list(updates.values()) + [worker_id]
            
            cur.execute(f"""
                UPDATE workers 
                SET {set_clause}
                WHERE worker_id = %s
                RETURNING *
            """, values)
            
            worker = cur.fetchone()
            conn.commit()
            
            if not worker:
                return jsonify({"error": "Worker não encontrado"}), 404
        
        conn.close()
        
        logger.info(f"📝 Worker {worker_id} atualizado: {list(updates.keys())}")
        
        return jsonify({
            "status": "success",
            "worker": dict(worker)
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao atualizar worker: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/workers/<worker_id>/toggle', methods=['POST'])
def toggle_worker(worker_id: str):
    """Liga/desliga um worker."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                UPDATE workers 
                SET enabled = NOT enabled
                WHERE worker_id = %s
                RETURNING worker_id, enabled
            """, (worker_id,))
            
            result = cur.fetchone()
            conn.commit()
            
            if not result:
                return jsonify({"error": "Worker não encontrado"}), 404
        
        conn.close()
        
        status = "habilitado" if result['enabled'] else "desabilitado"
        logger.info(f"🔄 Worker {worker_id} {status}")
        
        return jsonify({
            "status": "success",
            "worker_id": worker_id,
            "enabled": result['enabled']
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao toggle worker: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# ROUTING RULES
# =============================================================================

@queue_admin_bp.route('/rules', methods=['GET'])
def list_rules():
    """Lista todas as regras de roteamento."""
    try:
        enabled_only = request.args.get('enabled', 'false').lower() == 'true'
        applies_to = request.args.get('applies_to')
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT * FROM routing_rules WHERE 1=1"
            params = []
            
            if enabled_only:
                query += " AND enabled = true"
            
            if applies_to:
                query += " AND (applies_to = %s OR applies_to = 'all')"
                params.append(applies_to)
            
            query += " ORDER BY priority DESC, created_at"
            
            cur.execute(query, params)
            rules = cur.fetchall()
        
        conn.close()
        
        return jsonify({
            "rules": [dict(r) for r in rules],
            "total": len(rules)
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao listar regras: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/rules', methods=['POST'])
def create_rule():
    """Cria uma nova regra de roteamento."""
    try:
        data = request.get_json()
        
        required = ['name', 'conditions', 'target_worker_id']
        for field in required:
            if field not in data:
                return jsonify({"error": f"Campo obrigatório: {field}"}), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO routing_rules (
                    name, description, enabled, priority, conditions,
                    target_worker_id, fallback_worker_id, applies_to
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (
                data['name'],
                data.get('description'),
                data.get('enabled', True),
                data.get('priority', 0),
                Json(data['conditions']),
                data['target_worker_id'],
                data.get('fallback_worker_id'),
                data.get('applies_to', 'all')
            ))
            
            rule = cur.fetchone()
            conn.commit()
        
        conn.close()
        
        logger.info(f"✨ Nova regra criada: {data['name']} (priority={data.get('priority', 0)})")
        
        return jsonify({
            "status": "success",
            "rule": dict(rule)
        }), 201
        
    except Exception as e:
        logger.error(f"Erro ao criar regra: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/rules/<rule_id>', methods=['PUT'])
def update_rule(rule_id: str):
    """Atualiza uma regra de roteamento."""
    try:
        data = request.get_json()
        
        allowed_fields = [
            'name', 'description', 'enabled', 'priority', 'conditions',
            'target_worker_id', 'fallback_worker_id', 'applies_to'
        ]
        
        updates = {}
        for k, v in data.items():
            if k in allowed_fields:
                if k == 'conditions':
                    updates[k] = Json(v)
                else:
                    updates[k] = v
        
        if not updates:
            return jsonify({"error": "Nenhum campo válido para atualizar"}), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
            values = list(updates.values()) + [rule_id]
            
            cur.execute(f"""
                UPDATE routing_rules 
                SET {set_clause}
                WHERE id = %s
                RETURNING *
            """, values)
            
            rule = cur.fetchone()
            conn.commit()
            
            if not rule:
                return jsonify({"error": "Regra não encontrada"}), 404
        
        conn.close()
        
        logger.info(f"📝 Regra {rule_id[:8]}... atualizada")
        
        return jsonify({
            "status": "success",
            "rule": dict(rule)
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao atualizar regra: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/rules/<rule_id>', methods=['DELETE'])
def delete_rule(rule_id: str):
    """Remove uma regra de roteamento."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM routing_rules WHERE id = %s RETURNING id", (rule_id,))
            deleted = cur.fetchone()
            conn.commit()
            
            if not deleted:
                return jsonify({"error": "Regra não encontrada"}), 404
        
        conn.close()
        
        logger.info(f"🗑️ Regra {rule_id[:8]}... removida")
        
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        logger.error(f"Erro ao remover regra: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/rules/<rule_id>/toggle', methods=['POST'])
def toggle_rule(rule_id: str):
    """Habilita/desabilita uma regra."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                UPDATE routing_rules 
                SET enabled = NOT enabled
                WHERE id = %s
                RETURNING id, name, enabled
            """, (rule_id,))
            
            result = cur.fetchone()
            conn.commit()
            
            if not result:
                return jsonify({"error": "Regra não encontrada"}), 404
        
        conn.close()
        
        status = "habilitada" if result['enabled'] else "desabilitada"
        logger.info(f"🔄 Regra '{result['name']}' {status}")
        
        return jsonify({
            "status": "success",
            "rule_id": rule_id,
            "enabled": result['enabled']
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao toggle regra: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# SIMULADOR DE ROTEAMENTO
# =============================================================================

@queue_admin_bp.route('/simulate', methods=['POST'])
def simulate_routing():
    """
    Simula qual worker seria selecionado para um job.
    
    Request Body:
    {
        "video_duration": 10.5,
        "segments": 3,
        "template_id": "xxx-yyy-zzz",
        "applies_to": "matting"
    }
    """
    try:
        data = request.get_json()
        
        video_duration = data.get('video_duration', 0)
        segments = data.get('segments', 1)
        template_id = data.get('template_id')
        applies_to = data.get('applies_to', 'matting')
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Buscar regras ativas ordenadas por prioridade
            cur.execute("""
                SELECT * FROM routing_rules 
                WHERE enabled = true 
                AND (applies_to = %s OR applies_to = 'all')
                ORDER BY priority DESC
            """, (applies_to,))
            rules = cur.fetchall()
            
            # Buscar workers para verificar status
            cur.execute("SELECT worker_id, status, enabled FROM workers")
            workers = {w['worker_id']: w for w in cur.fetchall()}
        
        conn.close()
        
        # Obter tamanhos de fila
        queue_sizes = {}
        try:
            r = get_redis_connection()
            for worker_id in workers.keys():
                queue_key = f"video:queue:{worker_id}"
                queue_sizes[worker_id] = r.llen(queue_key)
        except Exception:
            pass
        
        # Avaliar cada regra
        evaluated_rules = []
        selected_rule = None
        selected_worker = None
        
        for rule in rules:
            conditions = rule['conditions']
            match = True
            match_reasons = []
            
            # Avaliar cada condição
            if 'video_duration_lt' in conditions:
                if video_duration >= conditions['video_duration_lt']:
                    match = False
                else:
                    match_reasons.append(f"duration {video_duration}s < {conditions['video_duration_lt']}s")
            
            if 'video_duration_gt' in conditions:
                if video_duration <= conditions['video_duration_gt']:
                    match = False
                else:
                    match_reasons.append(f"duration {video_duration}s > {conditions['video_duration_gt']}s")
            
            if 'video_duration_between' in conditions:
                min_d, max_d = conditions['video_duration_between']
                if not (min_d <= video_duration <= max_d):
                    match = False
                else:
                    match_reasons.append(f"duration {video_duration}s between {min_d}-{max_d}s")
            
            if 'segments_lt' in conditions:
                if segments >= conditions['segments_lt']:
                    match = False
                else:
                    match_reasons.append(f"segments {segments} < {conditions['segments_lt']}")
            
            if 'segments_lte' in conditions:
                if segments > conditions['segments_lte']:
                    match = False
                else:
                    match_reasons.append(f"segments {segments} <= {conditions['segments_lte']}")
            
            if 'segments_gt' in conditions:
                if segments <= conditions['segments_gt']:
                    match = False
                else:
                    match_reasons.append(f"segments {segments} > {conditions['segments_gt']}")
            
            if 'queue_size_gt' in conditions:
                worker_to_check = conditions.get('worker', rule['target_worker_id'])
                queue_size = queue_sizes.get(worker_to_check, 0)
                if queue_size <= conditions['queue_size_gt']:
                    match = False
                else:
                    match_reasons.append(f"queue({worker_to_check}) {queue_size} > {conditions['queue_size_gt']}")
            
            if 'queue_size_lt' in conditions:
                worker_to_check = conditions.get('worker', rule['target_worker_id'])
                queue_size = queue_sizes.get(worker_to_check, 0)
                if queue_size >= conditions['queue_size_lt']:
                    match = False
                else:
                    match_reasons.append(f"queue({worker_to_check}) {queue_size} < {conditions['queue_size_lt']}")
            
            if 'template_id_eq' in conditions:
                if template_id != conditions['template_id_eq']:
                    match = False
                else:
                    match_reasons.append(f"template matches")
            
            if 'hour_between' in conditions:
                current_hour = datetime.now().hour
                min_h, max_h = conditions['hour_between']
                if not (min_h <= current_hour <= max_h):
                    match = False
                else:
                    match_reasons.append(f"hour {current_hour} between {min_h}-{max_h}")
            
            # Se não tem condições, é fallback (sempre match)
            if not conditions:
                match_reasons.append("fallback (no conditions)")
            
            evaluated_rules.append({
                "rule_id": str(rule['id']),
                "name": rule['name'],
                "priority": rule['priority'],
                "conditions": conditions,
                "matched": match,
                "match_reasons": match_reasons,
                "target_worker": rule['target_worker_id']
            })
            
            if match and not selected_rule:
                selected_rule = rule
                
                # Verificar se target está disponível
                target = rule['target_worker_id']
                target_worker = workers.get(target)
                
                if target_worker and target_worker['enabled'] and target_worker['status'] == 'online':
                    selected_worker = target
                elif rule['fallback_worker_id']:
                    fallback = workers.get(rule['fallback_worker_id'])
                    if fallback and fallback['enabled']:
                        selected_worker = rule['fallback_worker_id']
                else:
                    selected_worker = target  # Usar mesmo assim
        
        return jsonify({
            "status": "success",
            "input": {
                "video_duration": video_duration,
                "segments": segments,
                "template_id": template_id,
                "applies_to": applies_to
            },
            "result": {
                "selected_worker": selected_worker,
                "selected_rule": {
                    "id": str(selected_rule['id']) if selected_rule else None,
                    "name": selected_rule['name'] if selected_rule else None,
                    "priority": selected_rule['priority'] if selected_rule else None
                } if selected_rule else None,
                "was_fallback": selected_worker != selected_rule['target_worker_id'] if selected_rule else False
            },
            "evaluated_rules": evaluated_rules,
            "queue_sizes": queue_sizes
        }), 200
        
    except Exception as e:
        logger.error(f"Erro na simulação: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# =============================================================================
# ESTATÍSTICAS
# =============================================================================

@queue_admin_bp.route('/stats', methods=['GET'])
def get_stats():
    """Obtém estatísticas gerais de filas e workers."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Workers ativos
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'online') as online,
                    COUNT(*) FILTER (WHERE status = 'offline') as offline,
                    COUNT(*) FILTER (WHERE type = 'fixed') as fixed,
                    COUNT(*) FILTER (WHERE type = 'dynamic') as dynamic,
                    COUNT(*) FILTER (WHERE type = 'serverless') as serverless
                FROM workers WHERE enabled = true
            """)
            worker_stats = cur.fetchone()
            
            # Regras ativas
            cur.execute("SELECT COUNT(*) as total FROM routing_rules WHERE enabled = true")
            rules_count = cur.fetchone()['total']
            
            # Jobs roteados hoje
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT selected_worker_id) as workers_used
                FROM routing_logs 
                WHERE created_at > CURRENT_DATE
            """)
            routing_today = cur.fetchone()
            
            # Jobs por worker hoje
            cur.execute("""
                SELECT selected_worker_id, COUNT(*) as count
                FROM routing_logs 
                WHERE created_at > CURRENT_DATE
                GROUP BY selected_worker_id
                ORDER BY count DESC
            """)
            jobs_by_worker = cur.fetchall()
        
        conn.close()
        
        # Filas Redis
        queue_stats = {}
        total_queued = 0
        try:
            r = get_redis_connection()
            for queue_name in ['video:matting:queue', 'video:render:queue', 'video:phase1:queue']:
                size = r.llen(queue_name)
                queue_stats[queue_name] = size
                total_queued += size
        except Exception:
            pass
        
        return jsonify({
            "workers": dict(worker_stats),
            "rules_active": rules_count,
            "routing_today": dict(routing_today),
            "jobs_by_worker": [dict(j) for j in jobs_by_worker],
            "queues": queue_stats,
            "total_queued": total_queued
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao buscar stats: {e}")
        return jsonify({"error": str(e)}), 500


@queue_admin_bp.route('/redis', methods=['GET'])
def get_redis_info():
    """Obtém informações das filas Redis."""
    try:
        r = get_redis_connection()
        
        # Buscar todas as filas conhecidas
        queues = {}
        for pattern in ['video:*:queue', 'matting:*', 'render:*']:
            for key in r.scan_iter(pattern):
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                key_type = r.type(key_str).decode('utf-8')
                
                if key_type == 'list':
                    queues[key_str] = {
                        'type': 'list',
                        'size': r.llen(key_str),
                        'first_item': r.lindex(key_str, 0)
                    }
                elif key_type == 'set':
                    queues[key_str] = {
                        'type': 'set',
                        'size': r.scard(key_str)
                    }
                elif key_type == 'hash':
                    queues[key_str] = {
                        'type': 'hash',
                        'size': r.hlen(key_str)
                    }
        
        # Info do Redis
        info = r.info()
        
        return jsonify({
            "queues": queues,
            "redis_info": {
                "connected_clients": info.get('connected_clients'),
                "used_memory_human": info.get('used_memory_human'),
                "uptime_in_days": info.get('uptime_in_days'),
                "redis_version": info.get('redis_version')
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao buscar Redis info: {e}")
        return jsonify({"error": str(e)}), 500
