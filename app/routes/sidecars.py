"""
Rotas para gerenciamento de Sidecars (Template Master V3)
"""
from flask import Blueprint, request, jsonify
import psycopg2
import json
import logging
from ..supabase_client import get_direct_db_connection

sidecars_bp = Blueprint('sidecars', __name__)
logger = logging.getLogger(__name__)

@sidecars_bp.route('/api/sidecars/<sidecar_id>', methods=['GET'])
def get_sidecar(sidecar_id):
    """
    Busca um sidecar específico por sidecar_id
    """
    try:
        logger.info(f"📤 Buscando sidecar: {sidecar_id}")
        conn = get_direct_db_connection()
        
        if not conn:
            logger.error("❌ Conexão com DB falhou (conn is None)")
            return jsonify({'error': 'Database connection failed'}), 500
        
        logger.info("✅ Conexão DB estabelecida")
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                id,
                sidecar_id,
                field_path,
                field_name,
                field_type,
                field_constraint,
                item_constraint,
                created_at,
                updated_at
            FROM public.template_sidecars
            WHERE sidecar_id = %s
        """, (sidecar_id,))

        row = cursor.fetchone()
        logger.info(f"📊 Query result: {row is not None}")
        
        cursor.close()
        conn.close()

        if not row:
            logger.warning(f"⚠️ Sidecar {sidecar_id} não encontrado")
            return jsonify({'error': 'Sidecar not found'}), 404

        # RealDictCursor retorna dict, não tupla
        sidecar = {
            'id': str(row['id']),
            'sidecar_id': row['sidecar_id'],
            'field_path': row['field_path'],
            'field_name': row['field_name'],
            'field_type': row['field_type'],
            'field_constraint': row['field_constraint'],
            'item_constraint': row['item_constraint'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None,
            'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None
        }

        logger.info(f"✅ Sidecar {sidecar_id} encontrado")
        return jsonify(sidecar), 200

    except Exception as e:
        logger.error(f"❌ Erro ao buscar sidecar {sidecar_id}: {str(e)}")
        logger.error(f"❌ Tipo do erro: {type(e)}")
        logger.error(f"❌ Args do erro: {e.args}")
        return jsonify({'error': str(e)}), 500


@sidecars_bp.route('/api/sidecars/<sidecar_id>', methods=['PATCH'])
def update_sidecar(sidecar_id):
    """
    Atualiza um sidecar específico
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        # Campos permitidos para atualização
        allowed_fields = ['field_constraint', 'item_constraint']
        update_fields = []
        update_values = []

        for field in allowed_fields:
            if field in data:
                update_fields.append(f"{field} = %s")
                # Converter para JSON se for dict
                if isinstance(data[field], dict):
                    update_values.append(json.dumps(data[field]))
                else:
                    update_values.append(data[field])

        if not update_fields:
            return jsonify({'error': 'No valid fields to update'}), 400

        # Adicionar updated_at
        update_fields.append("updated_at = now()")
        update_values.append(sidecar_id)

        conn = get_direct_db_connection()
        cursor = conn.cursor()

        query = f"""
            UPDATE public.template_sidecars
            SET {', '.join(update_fields)}
            WHERE sidecar_id = %s
            RETURNING 
                id,
                sidecar_id,
                field_path,
                field_name,
                field_type,
                field_constraint,
                item_constraint,
                created_at,
                updated_at
        """

        cursor.execute(query, update_values)
        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        if not row:
            return jsonify({'error': 'Sidecar not found'}), 404

        # RealDictCursor retorna dict, não tupla
        updated_sidecar = {
            'id': str(row['id']),
            'sidecar_id': row['sidecar_id'],
            'field_path': row['field_path'],
            'field_name': row['field_name'],
            'field_type': row['field_type'],
            'field_constraint': row['field_constraint'],
            'item_constraint': row['item_constraint'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None,
            'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None
        }

        logger.info(f"Sidecar {sidecar_id} atualizado com sucesso")
        return jsonify(updated_sidecar), 200

    except Exception as e:
        logger.error(f"Erro ao atualizar sidecar {sidecar_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@sidecars_bp.route('/api/sidecars', methods=['GET'])
def get_all_sidecars():
    """
    Busca todos os sidecars (paginado)
    """
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        offset = (page - 1) * per_page

        conn = get_direct_db_connection()
        cursor = conn.cursor()

        # Contar total
        cursor.execute("SELECT COUNT(*) FROM public.template_sidecars")
        total = cursor.fetchone()[0]

        # Buscar dados
        cursor.execute("""
            SELECT 
                id,
                sidecar_id,
                field_path,
                field_name,
                field_type,
                field_constraint,
                item_constraint
            FROM public.template_sidecars
            ORDER BY field_path
            LIMIT %s OFFSET %s
        """, (per_page, offset))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # RealDictCursor retorna dicts, não tuplas
        sidecars = [
            {
                'id': str(row['id']),
                'sidecar_id': row['sidecar_id'],
                'field_path': row['field_path'],
                'field_name': row['field_name'],
                'field_type': row['field_type'],
                'field_constraint': row['field_constraint'],
                'item_constraint': row['item_constraint']
            }
            for row in rows
        ]

        return jsonify({
            'data': sidecars,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page
            }
        }), 200

    except Exception as e:
        logger.error(f"Erro ao buscar sidecars: {str(e)}")
        return jsonify({'error': str(e)}), 500

