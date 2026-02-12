"""
Template Master Routes - Leitura de schemas e grupos

Rotas para acessar dados do template-master-v3:
- GET /api/template-master/groups: Lista grupos definidos em items-order.json
- GET /api/template-master/items/{item_id}: Retorna JSON de um schema específico
"""

from flask import Blueprint, jsonify
from pathlib import Path
import json

template_master_bp = Blueprint('template_master', __name__)


@template_master_bp.route('/groups', methods=['GET'])
def get_groups():
    """
    Retorna os grupos definidos em items-order.json
    
    Response:
    {
      "success": true,
      "data": {
        "basics": ["template-mode", "project-type", ...],
        "visual": ["multi-text-styling", "shadow", ...],
        "animation": [...],
        "preview": [...]
      },
      "version": "3.0.1"
    }
    """
    try:
        items_order_path = Path(__file__).parent.parent / 'data' / 'template-master-v3' / 'metadata' / 'items-order.json'
        
        if not items_order_path.exists():
            return jsonify({
                'success': False,
                'error': 'items-order.json não encontrado'
            }), 404
        
        with open(items_order_path, 'r', encoding='utf-8') as f:
            items_order = json.load(f)
        
        # Extrair grupos
        groups = {}
        for group in items_order.get('groups', []):
            groups[group['id']] = {
                'title': group.get('title', group['id']),
                'items': group.get('items', []),
                'collapsible': group.get('collapsible', True),
                'collapsed_by_default': group.get('collapsed_by_default', False)
            }
        
        print(f"[template-master] GET /groups: {len(groups)} grupos retornados")
        
        return jsonify({
            'success': True,
            'data': groups,
            'version': items_order.get('version'),
            'default_order': items_order.get('default_order', []),
            'ui': items_order.get('ui', {})
        })
        
    except Exception as e:
        print(f"[template-master] Erro ao ler groups: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@template_master_bp.route('/items/<item_id>', methods=['GET'])
def get_item(item_id):
    """
    Retorna o JSON completo de um item específico
    
    Response:
    {
      "success": true,
      "data": { ...conteúdo do base-layer.json... },
      "item_id": "base-layer"
    }
    """
    try:
        item_path = Path(__file__).parent.parent / 'data' / 'template-master-v3' / 'items' / f'{item_id}.json'
        
        if not item_path.exists():
            return jsonify({
                'success': False,
                'error': f'Item {item_id} não encontrado'
            }), 404
        
        with open(item_path, 'r', encoding='utf-8') as f:
            item_data = json.load(f)
        
        print(f"[template-master] GET /items/{item_id}: sucesso")
        
        return jsonify({
            'success': True,
            'data': item_data,
            'item_id': item_id
        })
        
    except json.JSONDecodeError as e:
        print(f"[template-master] Erro ao parsear JSON de {item_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Erro ao parsear JSON: {str(e)}'
        }), 500
    except Exception as e:
        print(f"[template-master] Erro ao ler item {item_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@template_master_bp.route('/items', methods=['GET'])
def list_items():
    """
    Lista todos os items disponíveis
    
    Response:
    {
      "success": true,
      "data": ["base-layer", "multi-animations", ...],
      "count": 33
    }
    """
    try:
        items_dir = Path(__file__).parent.parent / 'data' / 'template-master-v3' / 'items'
        
        if not items_dir.exists():
            return jsonify({
                'success': False,
                'error': 'Diretório items/ não encontrado'
            }), 404
        
        # Listar apenas arquivos .json
        items = [f.stem for f in items_dir.glob('*.json')]
        items.sort()
        
        print(f"[template-master] GET /items: {len(items)} items listados")
        
        return jsonify({
            'success': True,
            'data': items,
            'count': len(items)
        })
        
    except Exception as e:
        print(f"[template-master] Erro ao listar items: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
