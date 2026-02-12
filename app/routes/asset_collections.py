"""
🎨 ASSET COLLECTIONS API - vinicius.ai
Gerenciamento de collections de assets (vídeos, imagens) para uso em templates
"""

from flask import Blueprint, request, jsonify
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json

# Criar blueprint
asset_collections_bp = Blueprint('asset_collections', __name__)

# Database connection from environment (sslmode=prefer para compatibilidade Docker local)
DB_URL = (os.environ.get('DB_REMOTE_URL') or '').replace('sslmode=require', 'sslmode=prefer')

def get_db_connection():
    """Cria conexão com PostgreSQL"""
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


@asset_collections_bp.route('/asset-collections', methods=['GET'])
def list_asset_collections():
    """
    GET /asset-collections
    Lista asset collections com filtros opcionais
    Query params: type, is_active, is_public, workspace_id
    """
    try:
        # Parâmetros de filtro
        collection_type = request.args.get('type')  # ex: 'fullscreen_bg'
        is_active = request.args.get('is_active')  # 'true' ou 'false'
        is_public = request.args.get('is_public')
        workspace_id = request.args.get('workspace_id')
        
        # Construir query SQL
        query = """
            SELECT 
                id, name, slug, type, description, tags, category,
                assets, metadata, thumbnail_url, thumbnail_file_path, preview_url,
                ai_base_prompt, ai_user_prompt,
                created_by, workspace_id, is_public, is_active,
                created_at, updated_at, usage_count, last_used_at
            FROM asset_collections
            WHERE 1=1
        """
        params = []
        
        if collection_type:
            query += " AND type = %s"
            params.append(collection_type)
        
        if is_active:
            query += " AND is_active = %s"
            params.append(is_active.lower() == 'true')
        
        if is_public:
            query += " AND is_public = %s"
            params.append(is_public.lower() == 'true')
        
        if workspace_id:
            query += " AND workspace_id = %s"
            params.append(workspace_id)
        
        query += " ORDER BY created_at DESC"
        
        # Executar query
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        collections = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Converter para lista de dicts
        result = []
        for col in collections:
            collection_dict = dict(col)
            # Converter datetime para string ISO
            if collection_dict.get('created_at'):
                collection_dict['created_at'] = collection_dict['created_at'].isoformat()
            if collection_dict.get('updated_at'):
                collection_dict['updated_at'] = collection_dict['updated_at'].isoformat()
            if collection_dict.get('last_used_at'):
                collection_dict['last_used_at'] = collection_dict['last_used_at'].isoformat()
            result.append(collection_dict)
        
        return jsonify(result), 200
        
    except Exception as e:
        print(f"❌ Error listing asset collections: {str(e)}")
        return jsonify({'error': str(e)}), 500


@asset_collections_bp.route('/asset-collections/<collection_id>', methods=['GET'])
def get_asset_collection(collection_id):
    """
    GET /asset-collections/:id
    Busca uma collection específica por ID
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id, name, slug, type, description, tags, category,
                assets, metadata, thumbnail_url, thumbnail_file_path, preview_url,
                ai_base_prompt, ai_user_prompt,
                created_by, workspace_id, is_public, is_active,
                created_at, updated_at, usage_count, last_used_at
            FROM asset_collections
            WHERE id = %s
        """, (collection_id,))
        
        collection = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not collection:
            return jsonify({'error': 'Collection not found'}), 404
        
        # Converter para dict e serializar datetimes
        collection_dict = dict(collection)
        if collection_dict.get('created_at'):
            collection_dict['created_at'] = collection_dict['created_at'].isoformat()
        if collection_dict.get('updated_at'):
            collection_dict['updated_at'] = collection_dict['updated_at'].isoformat()
        if collection_dict.get('last_used_at'):
            collection_dict['last_used_at'] = collection_dict['last_used_at'].isoformat()
        
        return jsonify(collection_dict), 200
        
    except Exception as e:
        print(f"❌ Error fetching asset collection: {str(e)}")
        return jsonify({'error': str(e)}), 500


@asset_collections_bp.route('/asset-collections', methods=['POST'])
def create_asset_collection():
    """
    POST /asset-collections
    Cria uma nova asset collection
    Body: {name, slug, type, description, tags, category, assets, metadata, ...}
    """
    try:
        data = request.get_json()
        
        # Validar campos obrigatórios
        required_fields = ['name', 'slug', 'type', 'assets']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Inserir nova collection
        cursor.execute("""
            INSERT INTO asset_collections (
                name, slug, type, description, tags, category,
                assets, metadata, thumbnail_url, preview_url,
                created_by, workspace_id, is_public, is_active
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s
            )
            RETURNING id, created_at
        """, (
            data['name'],
            data['slug'],
            data['type'],
            data.get('description'),
            data.get('tags', []),
            data.get('category'),
            json.dumps(data['assets']),
            json.dumps(data.get('metadata', {})),
            data.get('thumbnail_url'),
            data.get('preview_url'),
            data.get('created_by'),
            data.get('workspace_id'),
            data.get('is_public', False),
            data.get('is_active', True)
        ))
        
        result = cursor.fetchone()
        new_id = result['id']
        created_at = result['created_at']
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'id': new_id,
            'message': 'Asset collection created successfully',
            'created_at': created_at.isoformat()
        }), 201
        
    except psycopg2.IntegrityError as e:
        print(f"❌ Integrity error: {str(e)}")
        return jsonify({'error': 'Collection with this slug already exists'}), 409
    except Exception as e:
        print(f"❌ Error creating asset collection: {str(e)}")
        return jsonify({'error': str(e)}), 500


@asset_collections_bp.route('/asset-collections/<collection_id>', methods=['PUT'])
def update_asset_collection(collection_id):
    """
    PUT /asset-collections/:id
    Atualiza uma asset collection
    """
    try:
        data = request.get_json()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Construir query de update dinâmica
        update_fields = []
        params = []
        
        allowed_fields = [
            'name', 'slug', 'type', 'description', 'tags', 'category',
            'assets', 'metadata', 'thumbnail_url', 'thumbnail_file_path', 'preview_url',
            'ai_base_prompt', 'ai_user_prompt',
            'is_public', 'is_active'
        ]
        
        for field in allowed_fields:
            if field in data:
                if field in ['assets', 'metadata']:
                    update_fields.append(f"{field} = %s::jsonb")
                    params.append(json.dumps(data[field]))
                else:
                    update_fields.append(f"{field} = %s")
                    params.append(data[field])
        
        if not update_fields:
            return jsonify({'error': 'No fields to update'}), 400
        
        # Adicionar updated_at
        update_fields.append("updated_at = NOW()")
        params.append(collection_id)
        
        query = f"""
            UPDATE asset_collections
            SET {', '.join(update_fields)}
            WHERE id = %s
            RETURNING id, updated_at
        """
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Collection not found'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'id': result['id'],
            'message': 'Asset collection updated successfully',
            'updated_at': result['updated_at'].isoformat()
        }), 200
        
    except psycopg2.IntegrityError as e:
        print(f"❌ Integrity error: {str(e)}")
        return jsonify({'error': 'Collection with this slug already exists'}), 409
    except Exception as e:
        print(f"❌ Error updating asset collection: {str(e)}")
        return jsonify({'error': str(e)}), 500


@asset_collections_bp.route('/asset-collections/<collection_id>', methods=['DELETE'])
def delete_asset_collection(collection_id):
    """
    DELETE /asset-collections/:id
    Deleta uma asset collection (soft delete - marca como inativa)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Soft delete - apenas marca como inativa
        cursor.execute("""
            UPDATE asset_collections
            SET is_active = false, updated_at = NOW()
            WHERE id = %s
            RETURNING id
        """, (collection_id,))
        
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Collection not found'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'message': 'Asset collection deleted successfully (soft delete)',
            'id': result['id']
        }), 200
        
    except Exception as e:
        print(f"❌ Error deleting asset collection: {str(e)}")
        return jsonify({'error': str(e)}), 500


@asset_collections_bp.route('/asset-collections/<collection_id>/increment-usage', methods=['POST'])
def increment_usage(collection_id):
    """
    POST /asset-collections/:id/increment-usage
    Incrementa o contador de uso de uma collection
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE asset_collections
            SET 
                usage_count = usage_count + 1,
                last_used_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            RETURNING id, usage_count
        """, (collection_id,))
        
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Collection not found'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'id': result['id'],
            'usage_count': result['usage_count'],
            'message': 'Usage count incremented'
        }), 200
        
    except Exception as e:
        print(f"❌ Error incrementing usage: {str(e)}")
        return jsonify({'error': str(e)}), 500

