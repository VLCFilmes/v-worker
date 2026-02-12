"""
Template Master Items Routes - Sistema Modular
Endpoints para gerenciar items individuais do template_master
"""

from flask import Blueprint, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os

items_bp = Blueprint('template_master_items', __name__)

# Lista de IDs válidos (26 items - após remoção de llm-sidecar e llm-assistant)
VALID_ITEM_IDS = [
    # LLMs (topo) - Interface especializada
    'llm-design',
    'llm-color',
    
    # Config extraídos de project-settings
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

def get_column_name(item_id):
    """
    Converte item-id para nome de coluna
    Exemplo: project-settings -> item_project_settings
    """
    if item_id not in VALID_ITEM_IDS:
        raise ValueError(f"Invalid item_id: {item_id}")
    return f"item_{item_id.replace('-', '_')}"


# ============================================
# GET /api/template-master/items
# Lista todos os items
# ============================================

@items_bp.route('/api/template-master/items', methods=['GET'])
def get_all_items():
    """
    Retorna todos os items do template master
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Buscar todos os items
        columns = ', '.join([get_column_name(item_id) for item_id in VALID_ITEM_IDS])
        query = f"""
            SELECT 
                {columns},
                items_metadata
            FROM template_master
            WHERE version = '3.0'
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({'error': 'Template master not found'}), 404
        
        # Formatar resposta
        items = {}
        for item_id in VALID_ITEM_IDS:
            column = get_column_name(item_id)
            items[item_id] = result[column]
        
        return jsonify({
            'version': '3.0',
            'items': items,
            'metadata': result['items_metadata']
        }), 200
        
    except Exception as e:
        print(f"[ERROR] get_all_items: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================
# GET /api/template-master/items/{item_id}
# Buscar item específico
# ============================================

@items_bp.route('/api/template-master/items/<item_id>', methods=['GET'])
def get_item(item_id):
    """
    Retorna dados de um item específico
    """
    try:
        column = get_column_name(item_id)
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(f"""
            SELECT {column} as item_data
            FROM template_master
            WHERE version = '3.0'
        """)
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result or not result['item_data']:
            return jsonify({'error': 'Item not found'}), 404
        
        return jsonify({
            'item_id': item_id,
            'data': result['item_data']
        }), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        print(f"[ERROR] get_item({item_id}): {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================
# PATCH /api/template-master/items/{item_id}
# Atualizar item específico
# ============================================

@items_bp.route('/api/template-master/items/<item_id>', methods=['PATCH'])
def update_item(item_id):
    """
    Atualiza um item específico do template master
    """
    try:
        column = get_column_name(item_id)
        data = request.json.get('data')
        
        if not data:
            return jsonify({'error': 'Missing data field'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"""
            UPDATE template_master
            SET 
                {column} = %s,
                updated_at = NOW()
            WHERE version = '3.0'
            RETURNING id
        """, (json.dumps(data),))
        
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({'error': 'Template master not found'}), 404
        
        print(f"[SUCCESS] Item {item_id} atualizado com sucesso")
        
        return jsonify({
            'success': True,
            'item_id': item_id,
            'message': f'Item {item_id} updated successfully'
        }), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        print(f"[ERROR] update_item({item_id}): {str(e)}")
        conn.rollback()
        return jsonify({'error': str(e)}), 500


# ============================================
# GET /api/template-master/items/order
# Buscar ordem dos items
# ============================================

@items_bp.route('/api/template-master/items/order', methods=['GET'])
def get_items_order():
    """
    Retorna a ordem atual dos items
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT items_metadata->'order' as order_data
            FROM template_master
            WHERE version = '3.0'
        """)
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            'order': result['order_data'] if result else []
        }), 200
        
    except Exception as e:
        print(f"[ERROR] get_items_order: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================
# PUT /api/template-master/items/order
# Atualizar ordem dos items
# ============================================

@items_bp.route('/api/template-master/items/order', methods=['PUT'])
def update_items_order():
    """
    Atualiza a ordem dos items
    """
    try:
        new_order = request.json.get('order', [])
        
        # Validar que todos os IDs são válidos
        invalid_ids = [id for id in new_order if id not in VALID_ITEM_IDS]
        if invalid_ids:
            return jsonify({'error': f'Invalid item IDs: {invalid_ids}'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE template_master
            SET 
                items_metadata = jsonb_set(
                    items_metadata,
                    '{order}',
                    %s::jsonb
                ),
                updated_at = NOW()
            WHERE version = '3.0'
            RETURNING id
        """, (json.dumps(new_order),))
        
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({'error': 'Template master not found'}), 404
        
        print(f"[SUCCESS] Items order atualizada: {len(new_order)} items")
        
        return jsonify({
            'success': True,
            'order': new_order,
            'message': 'Items order updated successfully'
        }), 200
        
    except Exception as e:
        print(f"[ERROR] update_items_order: {str(e)}")
        conn.rollback()
        return jsonify({'error': str(e)}), 500


# ============================================
# PATCH /api/template-master/items/metadata
# Atualizar metadata de items
# ============================================

@items_bp.route('/api/template-master/items/metadata', methods=['PATCH'])
def update_items_metadata():
    """
    Atualiza metadata completo dos items
    """
    try:
        new_metadata = request.json.get('metadata')
        
        if not new_metadata:
            return jsonify({'error': 'Missing metadata field'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE template_master
            SET 
                items_metadata = %s,
                updated_at = NOW()
            WHERE version = '3.0'
            RETURNING id
        """, (json.dumps(new_metadata),))
        
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({'error': 'Template master not found'}), 404
        
        print(f"[SUCCESS] Items metadata atualizado")
        
        return jsonify({
            'success': True,
            'message': 'Items metadata updated successfully'
        }), 200
        
    except Exception as e:
        print(f"[ERROR] update_items_metadata: {str(e)}")
        conn.rollback()
        return jsonify({'error': str(e)}), 500


# ============================================
# GET /api/template-master/items/registry
# Buscar registry de items (para UI)
# ============================================

@items_bp.route('/api/template-master/items/registry', methods=['GET'])
def get_items_registry():
    """
    Retorna o registry de items para a UI
    (nomes, ícones, categorias, etc.)
    """
    try:
        # Por enquanto, retornar dados estáticos
        # Futuramente, isso pode vir do banco
        registry = {
            'version': '3.0',
            'items': {
                'template-mode': {
                    'id': 'template-mode',
                    'name': 'Template Mode',
                    'icon': '🎯',
                    'category': 'core',
                    'description': 'Modo de operação do template',
                    'version': '1.0.0',
                    'default_enabled': True,
                    'dependencies': []
                },
                'project-type': {
                    'id': 'project-type',
                    'name': 'Project Type',
                    'icon': '📁',
                    'category': 'core',
                    'description': 'Tipo de projeto sendo criado',
                    'version': '1.0.0',
                    'default_enabled': True,
                    'dependencies': []
                },
                'template-theme': {
                    'id': 'template-theme',
                    'name': 'Template Theme',
                    'icon': '🎨',
                    'category': 'core',
                    'description': 'Tema visual do template',
                    'version': '1.0.0',
                    'default_enabled': True,
                    'dependencies': []
                },
                'project-settings': {
                    'id': 'project-settings',
                    'name': 'Project Settings',
                    'icon': '⚙️',
                    'category': 'core',
                    'description': 'Video settings and project configuration'
                },
                'z-index-hierarchy': {
                    'id': 'z-index-hierarchy',
                    'name': 'Z-Index Hierarchy',
                    'icon': '📚',
                    'category': 'layout',
                    'description': 'Layer ordering configuration'
                },
                'base-layer': {
                    'id': 'base-layer',
                    'name': 'Base Layer',
                    'icon': '🎨',
                    'category': 'visual',
                    'description': 'Background and base layer settings'
                },
                'enhanced-phrase-rules': {
                    'id': 'enhanced-phrase-rules',
                    'name': 'Enhanced Phrase Rules',
                    'icon': '📝',
                    'category': 'text',
                    'description': 'Advanced phrase detection rules'
                },
                'text-alignment': {
                    'id': 'text-alignment',
                    'name': 'Text Alignment',
                    'icon': '📐',
                    'category': 'text',
                    'description': 'Text positioning and alignment'
                },
                'fullscreen-overlay': {
                    'id': 'fullscreen-overlay',
                    'name': 'Fullscreen Overlay',
                    'icon': '🖥️',
                    'category': 'visual',
                    'description': 'Fullscreen background effects'
                },
                'multi-text-styling': {
                    'id': 'multi-text-styling',
                    'name': 'Text Styling',
                    'icon': '✍️',
                    'category': 'text',
                    'description': 'Font, size, colors, and borders'
                },
                'shadow': {
                    'id': 'shadow',
                    'name': 'Shadow',
                    'icon': '🌑',
                    'category': 'effects',
                    'description': 'Shadow and glow effects'
                },
                'stagger-and-opacity': {
                    'id': 'stagger-and-opacity',
                    'name': 'Stagger & Opacity',
                    'icon': '💫',
                    'category': 'animation',
                    'description': 'Timing and opacity controls'
                },
                'multi-backgrounds': {
                    'id': 'multi-backgrounds',
                    'name': 'Backgrounds',
                    'icon': '🎭',
                    'category': 'visual',
                    'description': 'Word and phrase backgrounds'
                },
                'multi-animations': {
                    'id': 'multi-animations',
                    'name': 'Animations',
                    'icon': '🎬',
                    'category': 'animation',
                    'description': 'Text animation effects'
                },
                'asset-animations': {
                    'id': 'asset-animations',
                    'name': 'Asset Animations',
                    'icon': '🎪',
                    'category': 'animation',
                    'description': 'Asset-specific animations'
                },
                'positioning': {
                    'id': 'positioning',
                    'name': 'Positioning',
                    'icon': '🎯',
                    'category': 'layout',
                    'description': 'Global positioning and margins'
                },
                'pre-rendered-assets': {
                    'id': 'pre-rendered-assets',
                    'name': 'Pre-rendered Assets',
                    'icon': '🖼️',
                    'category': 'assets',
                    'description': 'Static asset configuration'
                },
                'future-assets': {
                    'id': 'future-assets',
                    'name': 'Future Assets',
                    'icon': '🔮',
                    'category': 'assets',
                    'description': 'Placeholder for future assets'
                },
                'video-base-zoom': {
                    'id': 'video-base-zoom',
                    'name': 'Video Base Zoom',
                    'icon': '🔍',
                    'category': 'video',
                    'description': 'Background video zoom effects'
                },
                'llm-sidecar': {
                    'id': 'llm-sidecar',
                    'name': 'LLM Sidecar',
                    'icon': '🤖',
                    'category': 'ai',
                    'description': 'AI assistant configuration'
                },
                'llm-assistant': {
                    'id': 'llm-assistant',
                    'name': 'LLM Assistant',
                    'icon': '🧠',
                    'category': 'ai',
                    'description': 'Advanced AI settings'
                },
                'template-import': {
                    'id': 'template-import',
                    'name': 'Template Import',
                    'icon': '📥',
                    'category': 'tools',
                    'description': 'Import/export configuration'
                },
                'script-data': {
                    'id': 'script-data',
                    'name': 'Script Data',
                    'icon': '📜',
                    'category': 'core',
                    'description': 'Script processing configuration'
                }
            }
        }
        
        return jsonify(registry), 200
        
    except Exception as e:
        print(f"[ERROR] get_items_registry: {str(e)}")
        return jsonify({'error': str(e)}), 500

