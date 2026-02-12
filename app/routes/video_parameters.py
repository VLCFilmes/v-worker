# 🎬 Video Parameters API Routes
# Routes para buscar parâmetros de edição de vídeo do banco PostgreSQL

import logging
import os
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime
from flask import Blueprint, jsonify, request

# Configuração do blueprint
video_parameters_bp = Blueprint('video_parameters', __name__)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Retorna conexão direta ao PostgreSQL"""
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', 5432)
        )
        return connection
    except Exception as e:
        logger.error(f"❌ Erro ao conectar no PostgreSQL: {str(e)}")
        raise

def _get_available_fonts():
    """
    Busca fontes disponíveis do v-services API dinamicamente
    Retorna lista de nomes de famílias de fontes
    """
    try:
        services_url = os.getenv('SERVICES_API_URL', 'https://services.vinicius.ai')
        response = requests.get(
            f"{services_url}/png-subtitles/fonts/list",
            timeout=5
        )
        
        if response.ok:
            fonts_data = response.json()
            fonts = fonts_data.get('fonts', [])
            
            # Extrair nomes EXATOS de fontes (ex: "Montserrat-bold", "Poppins-regular")
            # IMPORTANTE: Usar 'name' ao invés de 'family' para ter o nome completo com variante
            font_names = list(set([font['name'] for font in fonts if 'name' in font]))
            font_names.sort()
            
            logger.info(f"✅ Loaded {len(font_names)} exact font names from v-services: {font_names}")
            return font_names
        else:
            logger.warning(f"⚠️ v-services fonts API returned {response.status_code}")
            return []
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout ao buscar fontes do v-services (5s)")
        return []
    except Exception as e:
        logger.error(f"❌ Erro ao buscar fontes do v-services: {str(e)}")
        return []

@video_parameters_bp.route('/api/video-parameters', methods=['GET', 'POST'])
def get_all_parameters():
    """
    Retorna todos os parâmetros de edição de vídeo
    GET /api/video-parameters (simples)
    POST /api/video-parameters (com contexto do Style Selector)
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Se for POST, extrair dados do body
        context_data = {}
        if request.method == 'POST':
            context_data = request.get_json() or {}
            logger.info(f"📦 Contexto recebido: session_id={context_data.get('session_id')}")
        
        # Query SQL direta ao PostgreSQL
        query = """
            SELECT 
                parameter_group,
                parameter_key,
                parameter_type,
                default_value,
                creative_description,
                technical_notes,
                value_references,
                constraints,
                decision_hints,
                tags,
                depends_on,
                incompatible_with,
                is_active
            FROM video_editing_parameters 
            WHERE is_active = true 
            ORDER BY parameter_group, parameter_key
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Converter para lista de dicionários
        data = [dict(row) for row in results]
        
        logger.info(f"✅ Retornados {len(data)} parâmetros de vídeo")
        
        # 🆕 BUSCAR FONTES DISPONÍVEIS E INJETAR DINAMICAMENTE
        available_fonts = _get_available_fonts()
        
        # Fallback se v-services estiver offline
        if not available_fonts:
            logger.warning("⚠️ Using fallback fonts (v-services unavailable)")
            # IMPORTANTE: Usar nomes EXATOS como retorna o v-services
            available_fonts = ['Montserrat-bold', 'Montserrat-regular', 'Poppins-bold', 'Poppins-regular', 'Roboto-regular', 'BebasNeue-Regular']
        
        # Injetar fontes no parâmetro text_style.fontFamily
        for param in data:
            if (param.get('parameter_key') == 'fontFamily' and 
                param.get('parameter_group') == 'text_style'):
                
                # Sobrescrever constraints com fontes dinâmicas
                if not param.get('constraints'):
                    param['constraints'] = {}
                
                param['constraints']['type'] = 'enum'
                param['constraints']['allowed_values'] = available_fonts
                param['constraints']['dynamic'] = True
                param['constraints']['last_updated'] = datetime.utcnow().isoformat()
                param['constraints']['source'] = 'v-services_api'
                
                # Adicionar dica de decisão
                if not param.get('decision_hints'):
                    param['decision_hints'] = {}
                
                param['decision_hints']['fonts_source'] = 'v-services_api_dynamic'
                param['decision_hints']['rule'] = f"MUST choose from {len(available_fonts)} available fonts. DO NOT invent font names."
                param['decision_hints']['fallback'] = available_fonts[0] if available_fonts else 'Montserrat Bold'
                
                logger.info(f"✅ Injected {len(available_fonts)} dynamic fonts into fontFamily parameter")
        
        # Response inclui contexto se foi POST
        response_data = {
            'success': True,
            'data': data,
            'total': len(data),
            'metadata': {
                'available_fonts_count': len(available_fonts),
                'fonts_dynamically_loaded': len(available_fonts) > 0,
                'fonts_source': 'v-services_api' if available_fonts else 'fallback'
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Se foi POST, incluir contexto na resposta
        if request.method == 'POST':
            response_data.update({
                'context_received': {
                    'style_definition': context_data.get('style_definition', {}),
                    'content_analysis': context_data.get('content_analysis', {}),
                    'session_id': context_data.get('session_id'),
                    'request_type': context_data.get('request_type')
                }
            })
        
        return jsonify(response_data)
            
    except Exception as e:
        logger.error(f"❌ Erro ao buscar parâmetros: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Erro interno do servidor'
        }), 500
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

@video_parameters_bp.route('/api/video-parameters/search', methods=['POST'])
def search_parameters():
    """
    Busca parâmetros baseado em critérios criativos
    POST /api/video-parameters/search
    """
    connection = None
    cursor = None
    
    try:
        data = request.get_json() or {}
        
        # Extrair critérios de busca
        creative_concept = data.get('creative_concept', '')
        platform = data.get('platform', 'linkedin')
        content_type = data.get('content_type', 'educational')
        tone_preference = data.get('tone_preference', 'casual')
        limit = min(int(data.get('limit', 50)), 100)  # Máximo 100
        
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Query base com filtro criativo
        base_query = """
            SELECT 
                parameter_group,
                parameter_key,
                parameter_type,
                default_value,
                creative_description,
                technical_notes,
                value_references,
                constraints,
                decision_hints,
                tags,
                depends_on,
                incompatible_with,
                is_active
            FROM video_editing_parameters 
            WHERE is_active = true
        """
        
        params = []
        
        # Filtros baseados no conteúdo criativo
        if creative_concept:
            base_query += """ AND (
                creative_description ILIKE %s OR 
                technical_notes ILIKE %s
            )"""
            concept_param = f'%{creative_concept}%'
            params.extend([concept_param, concept_param])
        
        # Adicionar ORDER BY e LIMIT
        base_query += " ORDER BY parameter_group, parameter_key LIMIT %s"
        params.append(limit)
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        # Se não encontrou resultados específicos, buscar parâmetros essenciais
        if not results and creative_concept:
            logger.info("🔄 Busca específica não retornou resultados, buscando parâmetros essenciais")
            essential_groups = ['video_settings', 'text_style', 'phrase_rules', 'global_position']
            
            fallback_query = """
                SELECT 
                    parameter_group,
                    parameter_key,
                    parameter_type,
                    default_value,
                    creative_description,
                    technical_notes,
                    value_references,
                    constraints,
                    decision_hints,
                    tags,
                    depends_on,
                    incompatible_with,
                    is_active
                FROM video_editing_parameters 
                WHERE is_active = true 
                AND parameter_group = ANY(%s)
                ORDER BY parameter_group, parameter_key
            """
            
            cursor.execute(fallback_query, [essential_groups])
            results = cursor.fetchall()
        
        # Converter para lista de dicionários
        data_results = [dict(row) for row in results]
        
        if data_results:
            logger.info(f"✅ Busca retornou {len(data_results)} parâmetros para '{creative_concept}'")
            return jsonify({
                'success': True,
                'data': data_results,
                'total': len(data_results),
                'search_criteria': {
                    'creative_concept': creative_concept,
                    'platform': platform,
                    'content_type': content_type,
                    'tone_preference': tone_preference
                },
                'timestamp': '2025-09-24T22:30:00Z'
            })
        else:
            logger.warning(f"⚠️ Nenhum parâmetro encontrado para busca: {creative_concept}")
            return jsonify({
                'success': True,
                'data': [],
                'total': 0,
                'message': f'Nenhum parâmetro encontrado para "{creative_concept}"'
            })
        
    except Exception as e:
        logger.error(f"❌ Erro na busca de parâmetros: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Erro interno do servidor'
        }), 500
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

@video_parameters_bp.route('/api/video-parameters/by-group/<group_name>', methods=['GET'])
def get_parameters_by_group(group_name):
    """
    Retorna parâmetros de um grupo específico
    GET /api/video-parameters/by-group/text_style
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                parameter_group,
                parameter_key,
                parameter_type,
                default_value,
                creative_description,
                technical_notes,
                value_references,
                constraints,
                decision_hints,
                tags,
                depends_on,
                incompatible_with,
                is_active
            FROM video_editing_parameters 
            WHERE is_active = true 
            AND parameter_group = %s
            ORDER BY parameter_key
        """
        
        cursor.execute(query, [group_name])
        results = cursor.fetchall()
        
        # Converter para lista de dicionários
        data = [dict(row) for row in results]
        
        if data:
            logger.info(f"✅ Retornados {len(data)} parâmetros do grupo '{group_name}'")
            return jsonify({
                'success': True,
                'data': data,
                'group': group_name,
                'total': len(data)
            })
        else:
            return jsonify({
                'success': True,
                'data': [],
                'group': group_name,
                'total': 0,
                'message': f'Nenhum parâmetro encontrado no grupo "{group_name}"'
            })
            
    except Exception as e:
        logger.error(f"❌ Erro ao buscar grupo {group_name}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Erro interno do servidor'
        }), 500
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

@video_parameters_bp.route('/api/video-parameters/update-hierarchy', methods=['POST'])
def update_parameter_hierarchy():
    """
    Atualiza a hierarquia de um parâmetro
    POST /api/video-parameters/update-hierarchy
    
    Body: {
        "parameter_id": "uuid",
        "parameter_category_path": "ITEM_X > subcategory",
        "parent_category": "subcategory",
        "hierarchy_level": 3,
        "display_order": 1
    }
    """
    connection = None
    cursor = None
    
    try:
        data = request.get_json() or {}
        
        # Validação
        parameter_id = data.get('parameter_id')
        parameter_category_path = data.get('parameter_category_path')
        
        if not parameter_id or not parameter_category_path:
            return jsonify({
                'success': False,
                'error': 'parameter_id e parameter_category_path são obrigatórios'
            }), 400
        
        parent_category = data.get('parent_category')
        hierarchy_level = data.get('hierarchy_level', 3)
        display_order = data.get('display_order', 0)
        
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Query SQL para atualizar
        query = """
            UPDATE video_editing_parameters
            SET 
                parameter_category_path = %s,
                parent_category = %s,
                hierarchy_level = %s,
                display_order = %s,
                updated_at = NOW()
            WHERE id = %s
            RETURNING id, parameter_key, parameter_category_path, parent_category, hierarchy_level, display_order
        """
        
        cursor.execute(query, [
            parameter_category_path,
            parent_category,
            hierarchy_level,
            display_order,
            parameter_id
        ])
        
        result = cursor.fetchone()
        connection.commit()
        
        if result:
            result_dict = dict(result)
            logger.info(f"✅ Hierarquia atualizada: {result_dict['parameter_key']} → {result_dict['parameter_category_path']}")
            
            return jsonify({
                'success': True,
                'message': 'Hierarquia atualizada com sucesso',
                'parameter': result_dict
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Parâmetro com ID {parameter_id} não encontrado'
            }), 404
            
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"❌ Erro ao atualizar hierarquia: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Erro interno do servidor'
        }), 500
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

@video_parameters_bp.route('/api/video-parameters/hierarchy', methods=['GET'])
def get_parameters_with_hierarchy():
    """
    Retorna todos os parâmetros com informações de hierarquia
    GET /api/video-parameters/hierarchy
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                id,
                parameter_group,
                parameter_key,
                parameter_type,
                default_value,
                creative_description,
                technical_notes,
                constraints,
                decision_hints,
                tags,
                applies_to,
                parameter_category_path,
                hierarchy_level,
                parent_category,
                display_order,
                is_active,
                created_at,
                updated_at
            FROM video_editing_parameters 
            WHERE is_active = true 
            ORDER BY 
                parameter_category_path NULLS FIRST,
                display_order,
                parameter_key
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Converter para lista de dicionários
        data = [dict(row) for row in results]
        
        logger.info(f"✅ Retornados {len(data)} parâmetros com hierarquia")
        
        return jsonify({
            'success': True,
            'data': data,
            'total': len(data)
        })
            
    except Exception as e:
        logger.error(f"❌ Erro ao buscar parâmetros com hierarquia: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Erro interno do servidor'
        }), 500
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()