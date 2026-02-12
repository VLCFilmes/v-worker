"""
Template Sidecars Modular Routes - Sistema Modular
Endpoints para gerenciar sidecars modulares (29 colunas JSONB)
"""

from flask import Blueprint, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os

sidecars_bp = Blueprint('template_sidecars_modular', __name__)

# Lista de IDs válidos (26 items - após remoção de llm-sidecar e llm-assistant)
# Sincronizado com template_master_items.py
VALID_ITEM_IDS = [
    # LLMs (topo) - Interface especializada
    'llm-design',
    'llm-color',
    
    # Config extraídos
    'template-mode',
    'project-type',
    'template-theme',
    
    # Items principais
    'project-settings',
    'z-index-hierarchy',
    'base-layer',
    'enhanced-phrase-rules',
    'text-alignment',
    'fullscreen-overlay',
    'multi-text-styling',
    'shadow',
    'stagger-and-opacity',
    'multi-backgrounds',
    'multi-animations',
    'asset-animations',
    'positioning',
    'pre-rendered-assets',
    'future-assets',
    'video-base-zoom',
    'template-import',
    'script-data',
    
    # Preview (final)
    'preview-settings',
    'preview-static',
    'preview-n8n'
]

def get_db_connection():
    """Cria conexão com o banco de dados"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT', 5432),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD'),
        database=os.getenv('POSTGRES_DB', 'postgres')
    )

def get_sidecar_column_name(item_id):
    """
    Converte item-id para nome de coluna de sidecar
    Exemplo: project-settings -> item_project_settings_sidecars
    """
    if item_id not in VALID_ITEM_IDS:
        raise ValueError(f"Invalid item_id: {item_id}")
    return f"item_{item_id.replace('-', '_')}_sidecars"


# ============================================
# GET /api/sidecars/items/{item_id}
# Retorna sidecars de um item específico
# ============================================

@sidecars_bp.route('/api/sidecars/items/<item_id>', methods=['GET'])
def get_item_sidecars(item_id):
    """
    Retorna sidecars de um item específico
    
    Exemplo: GET /api/sidecars/items/multi-text-styling
    Retorna: {
        "item_id": "multi-text-styling",
        "sidecars": {
            "text_color_001": {...},
            "font_size_002": {...}
        }
    }
    """
    try:
        # Validar item_id
        if item_id not in VALID_ITEM_IDS:
            return jsonify({
                'error': f'Invalid item_id: {item_id}',
                'valid_ids': VALID_ITEM_IDS
            }), 400
        
        column_name = get_sidecar_column_name(item_id)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Buscar template_master_id (v3.0)
        cursor.execute("""
            SELECT id FROM template_master WHERE version = '3.0' LIMIT 1
        """)
        master_result = cursor.fetchone()
        
        if not master_result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Template master v3.0 not found'}), 404
        
        template_master_id = master_result['id']
        
        # Buscar sidecars do item
        query = f"""
            SELECT 
                {column_name} as sidecars,
                template_master_id,
                updated_at
            FROM template_sidecars
            WHERE template_master_id = %s
        """
        
        cursor.execute(query, (template_master_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({
                'error': 'Sidecars not found for this template_master',
                'template_master_id': str(template_master_id)
            }), 404
        
        return jsonify({
            'item_id': item_id,
            'sidecars': result['sidecars'] or {},
            'template_master_id': str(result['template_master_id']),
            'updated_at': result['updated_at'].isoformat() if result['updated_at'] else None
        }), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Internal error: {str(e)}'}), 500


# ============================================
# GET /api/sidecars/items/batch
# Retorna sidecars de múltiplos items
# ============================================

@sidecars_bp.route('/api/sidecars/items/batch', methods=['POST'])
def get_batch_item_sidecars():
    """
    Retorna sidecars de múltiplos items de uma vez
    
    Body: {
        "item_ids": ["multi-text-styling", "multi-animations", "shadow"]
    }
    
    Retorna: {
        "multi-text-styling": {...},
        "multi-animations": {...},
        "shadow": {...}
    }
    """
    try:
        data = request.get_json()
        item_ids = data.get('item_ids', [])
        
        if not item_ids:
            return jsonify({'error': 'item_ids is required and must be non-empty'}), 400
        
        # Validar todos os IDs
        invalid_ids = [id for id in item_ids if id not in VALID_ITEM_IDS]
        if invalid_ids:
            return jsonify({
                'error': f'Invalid item_ids: {invalid_ids}',
                'valid_ids': VALID_ITEM_IDS
            }), 400
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Buscar template_master_id
        cursor.execute("""
            SELECT id FROM template_master WHERE version = '3.0' LIMIT 1
        """)
        master_result = cursor.fetchone()
        
        if not master_result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Template master v3.0 not found'}), 404
        
        template_master_id = master_result['id']
        
        # Construir query com múltiplas colunas
        columns = ', '.join([get_sidecar_column_name(id) for id in item_ids])
        query = f"""
            SELECT 
                {columns},
                template_master_id,
                updated_at
            FROM template_sidecars
            WHERE template_master_id = %s
        """
        
        cursor.execute(query, (template_master_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({
                'error': 'Sidecars not found',
                'template_master_id': str(template_master_id)
            }), 404
        
        # Montar resposta
        response = {
            'template_master_id': str(result['template_master_id']),
            'updated_at': result['updated_at'].isoformat() if result['updated_at'] else None,
            'items': {}
        }
        
        for item_id in item_ids:
            column_name = get_sidecar_column_name(item_id)
            response['items'][item_id] = result[column_name] or {}
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({'error': f'Internal error: {str(e)}'}), 500


# ============================================
# GET /api/sidecars/field/{sidecar_id}
# Busca um sidecar específico por ID
# ============================================

@sidecars_bp.route('/api/sidecars/field/<sidecar_id>', methods=['GET'])
def get_field_sidecar(sidecar_id):
    """
    Busca um sidecar específico por sidecar_id
    Procura em todas as colunas modulares
    
    Exemplo: GET /api/sidecars/field/text_color_001
    Retorna: {
        "sidecar_id": "text_color_001",
        "item_id": "multi-text-styling",
        "sidecar_data": {...}
    }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Buscar template_master_id
        cursor.execute("""
            SELECT id FROM template_master WHERE version = '3.0' LIMIT 1
        """)
        master_result = cursor.fetchone()
        
        if not master_result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Template master v3.0 not found'}), 404
        
        template_master_id = master_result['id']
        
        # Buscar em todas as colunas de sidecars
        # Construir query dinâmica para procurar o sidecar_id
        found = False
        found_item_id = None
        found_sidecar_data = None
        
        for item_id in VALID_ITEM_IDS:
            column_name = get_sidecar_column_name(item_id)
            
            query = f"""
                SELECT 
                    {column_name} -> %s as sidecar_data
                FROM template_sidecars
                WHERE template_master_id = %s
                AND {column_name} ? %s
            """
            
            cursor.execute(query, (sidecar_id, template_master_id, sidecar_id))
            result = cursor.fetchone()
            
            if result and result['sidecar_data']:
                found = True
                found_item_id = item_id
                found_sidecar_data = result['sidecar_data']
                break
        
        cursor.close()
        conn.close()
        
        if not found:
            return jsonify({
                'error': f'Sidecar not found: {sidecar_id}',
                'hint': 'Check if sidecar_id is correct or if it exists in template_sidecars'
            }), 404
        
        return jsonify({
            'sidecar_id': sidecar_id,
            'item_id': found_item_id,
            'sidecar_data': found_sidecar_data
        }), 200
        
    except Exception as e:
        return jsonify({'error': f'Internal error: {str(e)}'}), 500


# ============================================
# GET /api/sidecars/all
# Retorna TODOS os sidecars (todas as colunas)
# ============================================

@sidecars_bp.route('/api/sidecars/all', methods=['GET'])
def get_all_sidecars():
    """
    Retorna todos os sidecars de todos os items
    
    ATENÇÃO: Este endpoint pode retornar muito dado (~760 sidecars)
    Use os endpoints específicos para melhor performance
    
    Retorna: {
        "template_master_id": "...",
        "items": {
            "llm-design": {...},
            "llm-color": {...},
            ...
        }
    }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Buscar template_master_id
        cursor.execute("""
            SELECT id FROM template_master WHERE version = '3.0' LIMIT 1
        """)
        master_result = cursor.fetchone()
        
        if not master_result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Template master v3.0 not found'}), 404
        
        template_master_id = master_result['id']
        
        # Buscar todas as colunas de sidecars
        columns = ', '.join([get_sidecar_column_name(id) for id in VALID_ITEM_IDS])
        query = f"""
            SELECT 
                {columns},
                template_master_id,
                updated_at
            FROM template_sidecars
            WHERE template_master_id = %s
        """
        
        cursor.execute(query, (template_master_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({
                'error': 'Sidecars not found',
                'template_master_id': str(template_master_id)
            }), 404
        
        # Montar resposta com todos os items
        response = {
            'template_master_id': str(result['template_master_id']),
            'updated_at': result['updated_at'].isoformat() if result['updated_at'] else None,
            'items': {}
        }
        
        for item_id in VALID_ITEM_IDS:
            column_name = get_sidecar_column_name(item_id)
            sidecars = result[column_name] or {}
            
            # Só incluir items que têm sidecars
            if sidecars and sidecars != {}:
                response['items'][item_id] = sidecars
        
        # Estatísticas
        total_sidecars = sum(
            len(sidecars) if isinstance(sidecars, dict) else 0 
            for sidecars in response['items'].values()
        )
        
        response['stats'] = {
            'total_items_with_sidecars': len(response['items']),
            'total_sidecars': total_sidecars
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({'error': f'Internal error: {str(e)}'}), 500


# ============================================
# GET /api/sidecars/stats
# Estatísticas sobre os sidecars
# ============================================

@sidecars_bp.route('/api/sidecars/stats', methods=['GET'])
def get_sidecars_stats():
    """
    Retorna estatísticas sobre os sidecars modulares
    
    Retorna: {
        "total_items": 29,
        "items_with_sidecars": 20,
        "total_sidecars": 761,
        "breakdown": {
            "multi-text-styling": 45,
            "multi-animations": 38,
            ...
        }
    }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Buscar template_master_id
        cursor.execute("""
            SELECT id FROM template_master WHERE version = '3.0' LIMIT 1
        """)
        master_result = cursor.fetchone()
        
        if not master_result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Template master v3.0 not found'}), 404
        
        template_master_id = master_result['id']
        
        # Construir query para contar sidecars em cada coluna
        stats = {
            'total_items': len(VALID_ITEM_IDS),
            'items_with_sidecars': 0,
            'total_sidecars': 0,
            'breakdown': {}
        }
        
        for item_id in VALID_ITEM_IDS:
            column_name = get_sidecar_column_name(item_id)
            
            # Contar quantos sidecars existem nesta coluna
            query = f"""
                SELECT 
                    CASE 
                        WHEN {column_name} IS NULL OR {column_name} = '{{}}'::jsonb 
                        THEN 0 
                        ELSE (SELECT COUNT(*) FROM jsonb_object_keys({column_name}))
                    END as count
                FROM template_sidecars
                WHERE template_master_id = %s
            """
            
            cursor.execute(query, (template_master_id,))
            result = cursor.fetchone()
            
            if result and result['count'] > 0:
                count = result['count']
                stats['breakdown'][item_id] = count
                stats['items_with_sidecars'] += 1
                stats['total_sidecars'] += count
        
        cursor.close()
        conn.close()
        
        return jsonify(stats), 200
        
    except Exception as e:
        return jsonify({'error': f'Internal error: {str(e)}'}), 500


# ============================================
# PATCH /api/sidecars/items/{item_id}
# Atualiza sidecars de um item
# ============================================

@sidecars_bp.route('/api/sidecars/items/<item_id>', methods=['PATCH'])
def update_item_sidecars(item_id):
    """
    Atualiza sidecars de um item específico
    
    Body: {
        "sidecars": {
            "text_color_001": {...},
            "font_size_002": {...}
        }
    }
    
    ATENÇÃO: Substitui TODOS os sidecars do item!
    Para atualizar apenas um sidecar, use PATCH /api/sidecars/field/{sidecar_id}
    """
    try:
        if item_id not in VALID_ITEM_IDS:
            return jsonify({
                'error': f'Invalid item_id: {item_id}',
                'valid_ids': VALID_ITEM_IDS
            }), 400
        
        data = request.get_json()
        sidecars = data.get('sidecars')
        
        if sidecars is None:
            return jsonify({'error': 'sidecars field is required'}), 400
        
        if not isinstance(sidecars, dict):
            return jsonify({'error': 'sidecars must be a JSON object'}), 400
        
        column_name = get_sidecar_column_name(item_id)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Buscar template_master_id
        cursor.execute("""
            SELECT id FROM template_master WHERE version = '3.0' LIMIT 1
        """)
        master_result = cursor.fetchone()
        
        if not master_result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Template master v3.0 not found'}), 404
        
        template_master_id = master_result['id']
        
        # Atualizar sidecars do item
        query = f"""
            UPDATE template_sidecars
            SET 
                {column_name} = %s,
                updated_at = NOW()
            WHERE template_master_id = %s
            RETURNING id, updated_at
        """
        
        cursor.execute(query, (json.dumps(sidecars), template_master_id))
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({
                'error': 'Failed to update sidecars',
                'template_master_id': str(template_master_id)
            }), 500
        
        return jsonify({
            'success': True,
            'item_id': item_id,
            'sidecars_count': len(sidecars),
            'updated_at': result['updated_at'].isoformat() if result['updated_at'] else None,
            'message': f'Sidecars for {item_id} updated successfully'
        }), 200
        
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'error': f'Internal error: {str(e)}'}), 500

