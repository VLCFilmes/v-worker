"""
Rotas da API para gerenciamento de templates
Data: 2025-11-06
"""

from flask import Blueprint, request, jsonify
from datetime import datetime
import psycopg2
import psycopg2.extras
import os
import json

templates_bp = Blueprint('templates', __name__)

# Configuração do banco
# 🔧 v3.3.0: sslmode=require falha no PostgreSQL Docker local (sem SSL)
# Usar sslmode=prefer para compatibilidade (tenta SSL, fallback sem SSL)
_raw_db_url = os.getenv('DB_REMOTE_URL') or os.getenv('DATABASE_URL', '')
DATABASE_URL = _raw_db_url.replace('sslmode=require', 'sslmode=prefer')

@templates_bp.route('/api/templates/<template_id>/render', methods=['POST'])
def save_template_render(template_id):
    """
    Salva o resultado da renderização de um template.
    Chamado pelo N8N após renderizar o vídeo.
    
    Payload esperado:
    {
        "job_id": "template_47dcd417-e8c2-4dc8-bbf3-bf2ca67a09b0_1762217306_5fd9o45j3",
        "video_url": "https://b2.../video.mp4",
        "thumbnail_url": "https://b2.../thumb.jpg",
        "duration": 30,
        "resolution": "1080x1920",
        "file_size": 5242880,
        "status": "completed",
        "error_message": null,
        "metadata": {...}
    }
    """
    try:
        data = request.get_json()
        print(f"[save_template_render] Recebendo renderização para template {template_id}")
        print(f"[save_template_render] Payload: {json.dumps(data, indent=2)}")
        
        # Validações
        if not data.get('job_id'):
            return jsonify({'error': 'job_id é obrigatório'}), 400
        
        if not data.get('video_url'):
            return jsonify({'error': 'video_url é obrigatório'}), 400
        
        # Conectar ao banco
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Verificar se template existe (buscar em video_editing_templates)
        cursor.execute("""
            SELECT id, name FROM video_editing_templates WHERE id = %s
            UNION ALL
            SELECT id, name FROM templates WHERE id = %s
            LIMIT 1
        """, (template_id, template_id))
        template = cursor.fetchone()
        
        if not template:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Template {template_id} não encontrado'}), 404
        
        # Verificar se job_id já existe (idempotência)
        cursor.execute("""
            SELECT id, job_id FROM template_renders WHERE job_id = %s
        """, (data.get('job_id'),))
        
        existing_render = cursor.fetchone()
        
        if existing_render:
            print(f"[save_template_render] ⚠️ job_id já existe: {existing_render['job_id']}")
            print(f"[save_template_render] Atualizando renderização existente...")
            
            # Atualizar renderização existente
            cursor.execute("""
                UPDATE template_renders
                SET 
                    video_url = %s,
                    thumbnail_url = %s,
                    duration = %s,
                    resolution = %s,
                    file_size = %s,
                    status = %s,
                    error_message = %s,
                    metadata = %s,
                    rendered_at = %s,
                    updated_at = %s
                WHERE job_id = %s
                RETURNING id
            """, (
                data.get('video_url'),
                data.get('thumbnail_url'),
                data.get('duration'),
                data.get('resolution'),
                data.get('file_size'),
                data.get('status', 'completed'),
                data.get('error_message'),
                json.dumps(data.get('metadata', {})),
                data.get('rendered_at', datetime.utcnow().isoformat()),
                datetime.utcnow(),
                data.get('job_id')
            ))
            
            render_id = cursor.fetchone()['id']
            print(f"[save_template_render] ✅ Renderização atualizada: {render_id}")
        else:
            # Inserir nova renderização
            cursor.execute("""
                INSERT INTO template_renders (
                    job_id,
                    template_id,
                    video_url,
                    thumbnail_url,
                    duration,
                    resolution,
                    file_size,
                    status,
                    error_message,
                    metadata,
                    rendered_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.get('job_id'),
                template_id,
                data.get('video_url'),
                data.get('thumbnail_url'),
                data.get('duration'),
                data.get('resolution'),
                data.get('file_size'),
                data.get('status', 'completed'),
                data.get('error_message'),
                json.dumps(data.get('metadata', {})),
                data.get('rendered_at', datetime.utcnow().isoformat())
            ))
            
            render_id = cursor.fetchone()['id']
            print(f"[save_template_render] ✅ Nova renderização salva com ID: {render_id}")
        
        # Atualizar template (última renderização + contador)
        # Tentar atualizar em video_editing_templates primeiro, depois em templates
        cursor.execute("""
            UPDATE video_editing_templates
            SET 
                last_render_id = %s,
                last_rendered_at = %s,
                render_count = COALESCE(render_count, 0) + 1,
                updated_at = %s
            WHERE id = %s
        """, (
            render_id,
            data.get('rendered_at', datetime.utcnow().isoformat()),
            datetime.utcnow(),
            template_id
        ))
        
        # Se não atualizou nenhuma linha, tentar em templates
        if cursor.rowcount == 0:
            cursor.execute("""
                UPDATE templates
                SET 
                    last_render_id = %s,
                    last_rendered_at = %s,
                    render_count = COALESCE(render_count, 0) + 1,
                    updated_at = %s
                WHERE id = %s
            """, (
                render_id,
                data.get('rendered_at', datetime.utcnow().isoformat()),
                datetime.utcnow(),
                template_id
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"[save_template_render] ✅ Sucesso! Template {template['name']} renderizado")
        
        return jsonify({
            'success': True,
            'render_id': str(render_id),
            'job_id': data.get('job_id'),
            'template_id': str(template_id),
            'template_name': template['name'],
            'video_url': data.get('video_url'),
            'message': 'Vídeo renderizado salvo com sucesso'
        }), 201
        
    except Exception as e:
        print(f"[save_template_render] ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/<template_id>/render/latest', methods=['GET'])
def get_latest_render(template_id):
    """
    Busca a última renderização de um template.
    Usado pelo frontend para exibir o vídeo renderizado.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("""
            SELECT 
                id,
                template_id,
                video_url,
                thumbnail_url,
                duration,
                resolution,
                file_size,
                status,
                error_message,
                metadata,
                rendered_at,
                created_at
            FROM template_renders
            WHERE template_id = %s
            ORDER BY rendered_at DESC
            LIMIT 1
        """, (template_id,))
        
        render = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not render:
            # Retorna 200 com null ao invés de 404 para templates sem render
            return jsonify({
                'render': None,
                'message': 'No renders found for this template',
                'template_id': template_id
            }), 200
        
        # Converter para dict e formatar datas
        result = dict(render)
        result['id'] = str(result['id'])
        result['template_id'] = str(result['template_id'])
        result['rendered_at'] = result['rendered_at'].isoformat() if result['rendered_at'] else None
        result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
        
        return jsonify(result), 200
        
    except Exception as e:
        print(f"[get_latest_render] Erro: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/<template_id>/jobs/latest', methods=['GET'])
def get_latest_job(template_id):
    """
    🆕 v2.9.58: Busca o último job de processamento de um template.
    Retorna os vídeos de Fase 1 e Fase 2 se disponíveis.
    Usado pela galeria de templates para mostrar os vídeos renderizados.
    
    🔧 FIX: Jobs são vinculados via conversation_id (não template_id direto).
    Usamos JOIN com video_editing_templates para encontrar os jobs.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 🔧 FIX v2.9.58: Jobs são vinculados via conversation_id
        # Templates têm conversation_id, jobs também têm conversation_id
        cursor.execute("""
            SELECT 
                j.job_id,
                j.status,
                j.phase1_video_url,
                j.phase2_video_url,
                j.output_video_url,
                j.created_at,
                j.updated_at,
                j.completed_at,
                j.error_message
            FROM video_processing_jobs j
            INNER JOIN video_editing_templates t ON t.conversation_id = j.conversation_id
            WHERE t.id = %s
            ORDER BY j.created_at DESC
            LIMIT 1
        """, (template_id,))
        
        job = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not job:
            return jsonify({
                'job': None,
                'message': 'Nenhum job encontrado para este template',
                'template_id': template_id
            }), 200
        
        # Converter para dict e formatar datas
        result = dict(job)
        result['id'] = str(result['job_id'])  # Mapear job_id para id no response
        result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
        result['updated_at'] = result['updated_at'].isoformat() if result['updated_at'] else None
        result['completed_at'] = result['completed_at'].isoformat() if result['completed_at'] else None
        
        return jsonify({'job': result}), 200
        
    except Exception as e:
        print(f"[get_latest_job] Erro: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/render/<job_id>', methods=['GET'])
def get_render_by_job_id(job_id):
    """
    Busca uma renderização específica pelo job_id.
    Usado para rastrear o status de uma renderização específica.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("""
            SELECT 
                r.id,
                r.job_id,
                r.template_id,
                r.video_url,
                r.thumbnail_url,
                r.duration,
                r.resolution,
                r.file_size,
                r.status,
                r.error_message,
                r.metadata,
                r.rendered_at,
                r.created_at,
                COALESCE(vet.name, t.name) as template_name
            FROM template_renders r
            LEFT JOIN video_editing_templates vet ON r.template_id = vet.id
            LEFT JOIN templates t ON r.template_id = t.id
            WHERE r.job_id = %s
        """, (job_id,))
        
        render = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not render:
            return jsonify({'error': 'Renderização não encontrada'}), 404
        
        # Converter para dict e formatar datas
        result = dict(render)
        result['id'] = str(result['id'])
        result['template_id'] = str(result['template_id'])
        result['rendered_at'] = result['rendered_at'].isoformat() if result['rendered_at'] else None
        result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
        
        return jsonify(result), 200
        
    except Exception as e:
        print(f"[get_render_by_job_id] Erro: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/<template_id>/renders', methods=['GET'])
def get_all_renders(template_id):
    """
    🆕 v3.2.8: Busca todas as renderizações de um template (histórico).
    🔧 CORRIGIDO: Usa render_versions ao invés de template_renders
    """
    try:
        limit = request.args.get('limit', 20, type=int)
        
        print(f"[get_all_renders] 🔍 Buscando renders para template: {template_id[:8]}... (limit: {limit})")
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 🔧 v3.2.8: Buscar de render_versions (tabela correta!)
        # 🆕 v3.2.18: JOIN com video_processing_jobs para pegar phase1_video_url
        cursor.execute("""
            SELECT 
                rv.id,
                rv.job_id,
                rv.template_id,
                rv.version_number,
                rv.phase,
                rv.video_url,
                rv.thumbnail_url,
                rv.duration_ms,
                rv.resolution,
                rv.fps,
                rv.file_size_bytes,
                rv.is_current,
                rv.is_featured,
                rv.version_note,
                rv.created_at,
                rv.created_by,
                j.phase1_video_url,
                j.phase1_job_id
            FROM render_versions rv
            LEFT JOIN video_processing_jobs j ON rv.job_id = j.job_id
            WHERE rv.template_id = %s AND rv.phase = 2
            ORDER BY rv.version_number DESC, rv.created_at DESC
            LIMIT %s
        """, (template_id, limit))
        
        renders = cursor.fetchall()
        
        print(f"[get_all_renders] ✅ {len(renders)} versões encontradas para template {template_id[:8]}...")
        
        cursor.close()
        conn.close()
        
        # Converter para lista de dicts
        results = []
        for render in renders:
            result = dict(render)
            result['id'] = str(result['id'])
            result['job_id'] = str(result['job_id'])
            result['template_id'] = str(result['template_id'])
            result['created_by'] = str(result['created_by']) if result['created_by'] else None
            result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
            # 🆕 v3.2.18: Incluir phase1_video_url e phase1_job_id
            result['phase1_video_url'] = result.get('phase1_video_url')
            result['phase1_job_id'] = str(result['phase1_job_id']) if result.get('phase1_job_id') else None
            results.append(result)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'renders': results
        }), 200
        
    except Exception as e:
        print(f"[get_all_renders] ❌ Erro: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"[get_all_renders] Traceback: {traceback.format_exc()}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/<template_id>/approval', methods=['POST'])
def update_template_approval(template_id):
    """
    Aprova ou reprova um template para publicação.
    Usado pelo Site Admin para controlar quais templates ficam disponíveis no site oficial.
    
    Payload esperado:
    {
        "action": "approve" | "reject",
        "rejection_reason": "..." (obrigatório se action = "reject")
    }
    """
    try:
        data = request.get_json()
        print(f"[update_template_approval] Processando aprovação para template {template_id}")
        print(f"[update_template_approval] Payload: {json.dumps(data, indent=2)}")
        
        # Validações
        action = data.get('action')
        if not action or action not in ['approve', 'reject']:
            return jsonify({'error': 'action deve ser "approve" ou "reject"'}), 400
        
        if action == 'reject' and not data.get('rejection_reason'):
            return jsonify({'error': 'rejection_reason é obrigatório ao reprovar'}), 400
        
        # Conectar ao banco
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Verificar se template existe (buscar em video_editing_templates)
        cursor.execute("""
            SELECT id, name FROM video_editing_templates WHERE id = %s
        """, (template_id,))
        template = cursor.fetchone()
        
        if not template:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Template {template_id} não encontrado'}), 404
        
        # Atualizar status de aprovação
        if action == 'approve':
            cursor.execute("""
                UPDATE video_editing_templates
                SET 
                    approved = TRUE,
                    approved_at = %s,
                    approved_by = 'admin',
                    rejection_reason = NULL,
                    updated_at = %s
                WHERE id = %s
            """, (
                datetime.utcnow(),
                datetime.utcnow(),
                template_id
            ))
            
            message = f'Template "{template["name"]}" aprovado com sucesso'
            print(f"[update_template_approval] ✅ {message}")
        else:
            cursor.execute("""
                UPDATE video_editing_templates
                SET 
                    approved = FALSE,
                    approved_at = NULL,
                    approved_by = NULL,
                    rejection_reason = %s,
                    updated_at = %s
                WHERE id = %s
            """, (
                data.get('rejection_reason'),
                datetime.utcnow(),
                template_id
            ))
            
            message = f'Template "{template["name"]}" reprovado'
            print(f"[update_template_approval] ❌ {message}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'template_id': str(template_id),
            'template_name': template['name'],
            'action': action,
            'message': message
        }), 200
        
    except Exception as e:
        print(f"[update_template_approval] ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/approved', methods=['GET'])
def get_approved_templates():
    """
    📋 GET /api/templates/approved
    
    Retorna lista de templates aprovados para exibição no site oficial.
    
    Query params opcionais:
    - theme: Filtrar por tema (slug) - ex: ?theme=marketing
    - limit: Número máximo de resultados - ex: ?limit=20
    - offset: Offset para paginação - ex: ?offset=20
    
    Response:
    {
        "templates": [
            {
                "id": "47dcd417-e8c2-4dc8-bbf3-bf2ca67a09b0",
                "name": "Template_01 v3",
                "description": "Template para marketing digital",
                "video_url": "https://b2.../video.mp4",
                "thumbnail_url": "https://b2.../thumb.jpg",
                "duration": 13.67,
                "resolution": "1080x1920",
                "file_size": 612918,
                "approved_at": "2025-11-07T14:30:00.000Z",
                "themes": [
                    {
                        "slug": "default",
                        "name": "Default",
                        "icon": "📁",
                        "color": "#6B7280"
                    }
                ]
            }
        ],
        "total": 1,
        "limit": 20,
        "offset": 0,
        "theme_filter": null
    }
    """
    try:
        # Query params
        theme_filter = request.args.get('theme')  # Ex: "marketing"
        limit = min(int(request.args.get('limit', 20)), 100)  # Max 100
        offset = int(request.args.get('offset', 0))
        
        print(f"[get_approved_templates] 🔍 Buscando templates aprovados:")
        print(f"  - Theme filter: {theme_filter or 'None'}")
        print(f"  - Limit: {limit}")
        print(f"  - Offset: {offset}")
        
        # Conectar ao banco
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 🆕 DIA-6: Query com LATERAL JOIN para buscar demo featured
        # ❌ REMOVIDO: conversation_id (NULL após migração)
        # ✅ NOVO: Busca demos em template_demo_projects
        if theme_filter:
            # Buscar templates de um tema específico
            query = """
                SELECT DISTINCT
                    vet.id,
                    vet.name,
                    vet.description,
                    vet.approved_at,
                    -- 🆕 DIA-6: Buscar thumbnail e vídeo do demo
                    COALESCE(featured_demo.thumbnail_url, vet.thumbnail_url) as thumbnail_url,
                    COALESCE(featured_demo.video_url, vet.video_url) as video_url,
                    featured_demo.video_duration_ms / 1000.0 as duration,
                    '1080x1920' as resolution,  -- Padrão
                    featured_demo.file_size_bytes as file_size,
                    COALESCE(featured_demo.created_at, vet.approved_at) as rendered_at
                FROM video_editing_templates vet
                INNER JOIN template_theme_relations ttr ON vet.id = ttr.template_id
                INNER JOIN template_themes tt ON ttr.theme_id = tt.id
                LEFT JOIN LATERAL (
                    SELECT 
                        tdp.video_url,
                        tdp.thumbnail_url,
                        tdp.video_duration_ms,
                        tdp.file_size_bytes,
                        tdp.created_at
                    FROM template_demo_projects tdp
                    WHERE tdp.template_id = vet.id
                    ORDER BY tdp.is_featured DESC, tdp.created_at DESC
                    LIMIT 1
                ) featured_demo ON true
                WHERE 
                    vet.approved = TRUE
                    AND tt.slug = %s
                    AND tt.is_active = TRUE
                ORDER BY vet.approved_at DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (theme_filter, limit, offset))
        else:
            # Buscar todos os templates aprovados
            query = """
                SELECT 
                    vet.id,
                    vet.name,
                    vet.description,
                    vet.approved_at,
                    -- 🆕 DIA-6: Buscar thumbnail e vídeo do demo
                    COALESCE(featured_demo.thumbnail_url, vet.thumbnail_url) as thumbnail_url,
                    COALESCE(featured_demo.video_url, vet.video_url) as video_url,
                    featured_demo.video_duration_ms / 1000.0 as duration,
                    '1080x1920' as resolution,  -- Padrão
                    featured_demo.file_size_bytes as file_size,
                    COALESCE(featured_demo.created_at, vet.approved_at) as rendered_at
                FROM video_editing_templates vet
                LEFT JOIN LATERAL (
                    SELECT 
                        tdp.video_url,
                        tdp.thumbnail_url,
                        tdp.video_duration_ms,
                        tdp.file_size_bytes,
                        tdp.created_at
                    FROM template_demo_projects tdp
                    WHERE tdp.template_id = vet.id
                    ORDER BY tdp.is_featured DESC, tdp.created_at DESC
                    LIMIT 1
                ) featured_demo ON true
                WHERE vet.approved = TRUE
                ORDER BY vet.approved_at DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset))
        
        templates_data = cursor.fetchall()
        
        # Buscar temas de cada template
        templates = []
        for template in templates_data:
            template_id = template['id']
            
            # Buscar temas deste template
            cursor.execute("""
                SELECT 
                    tt.slug,
                    tt.name,
                    tt.icon,
                    tt.color,
                    tt.display_order
                FROM template_themes tt
                INNER JOIN template_theme_relations ttr ON tt.id = ttr.theme_id
                WHERE ttr.template_id = %s AND tt.is_active = TRUE
                ORDER BY tt.display_order ASC
            """, (template_id,))
            
            themes = cursor.fetchall()
            
            templates.append({
                'id': str(template['id']),
                'name': template['name'],
                'description': template['description'],
                'video_url': template['video_url'],
                'thumbnail_url': template['thumbnail_url'],
                'duration': float(template['duration']) if template['duration'] else None,
                'resolution': template['resolution'],
                'file_size': template['file_size'],
                'approved_at': template['approved_at'].isoformat() if template['approved_at'] else None,
                'rendered_at': template['rendered_at'].isoformat() if template['rendered_at'] else None,
                'themes': [
                    {
                        'slug': theme['slug'],
                        'name': theme['name'],
                        'icon': theme['icon'],
                        'color': theme['color']
                    }
                    for theme in themes
                ]
            })
        
        # Contar total de templates (para paginação)
        if theme_filter:
            cursor.execute("""
                SELECT COUNT(DISTINCT vet.id)
                FROM video_editing_templates vet
                INNER JOIN template_theme_relations ttr ON vet.id = ttr.template_id
                INNER JOIN template_themes tt ON ttr.theme_id = tt.id
                WHERE vet.approved = TRUE AND tt.slug = %s AND tt.is_active = TRUE
            """, (theme_filter,))
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM video_editing_templates WHERE approved = TRUE
            """)
        
        total = cursor.fetchone()['count']
        
        cursor.close()
        conn.close()
        
        print(f"[get_approved_templates] ✅ Retornando {len(templates)} templates (total: {total})")
        
        return jsonify({
            'templates': templates,
            'total': total,
            'limit': limit,
            'offset': offset,
            'theme_filter': theme_filter
        }), 200
        
    except Exception as e:
        print(f"[get_approved_templates] ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/themes', methods=['GET'])
def get_template_themes():
    """
    📋 GET /api/templates/themes
    
    Retorna lista de temas disponíveis para filtrar templates.
    
    Response:
    {
        "themes": [
            {
                "slug": "default",
                "name": "Default",
                "description": "Templates gerais sem categoria específica",
                "icon": "📁",
                "color": "#6B7280",
                "display_order": 0,
                "template_count": 5
            }
        ]
    }
    """
    try:
        print("[get_template_themes] 🔍 Buscando temas disponíveis...")
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Buscar temas com contagem de templates
        cursor.execute("""
            SELECT 
                tt.slug,
                tt.name,
                tt.description,
                tt.icon,
                tt.color,
                tt.display_order,
                COUNT(DISTINCT ttr.template_id) as template_count
            FROM template_themes tt
            LEFT JOIN template_theme_relations ttr ON tt.id = ttr.theme_id
            LEFT JOIN video_editing_templates vet ON ttr.template_id = vet.id AND vet.approved = TRUE
            WHERE tt.is_active = TRUE
            GROUP BY tt.id, tt.slug, tt.name, tt.description, tt.icon, tt.color, tt.display_order
            ORDER BY tt.display_order ASC
        """)
        
        themes_data = cursor.fetchall()
        
        themes = [
            {
                'slug': theme['slug'],
                'name': theme['name'],
                'description': theme['description'],
                'icon': theme['icon'],
                'color': theme['color'],
                'display_order': theme['display_order'],
                'template_count': theme['template_count']
            }
            for theme in themes_data
        ]
        
        cursor.close()
        conn.close()
        
        print(f"[get_template_themes] ✅ Retornando {len(themes)} temas")
        
        return jsonify({
            'themes': themes
        }), 200
        
    except Exception as e:
        print(f"[get_template_themes] ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


# ==========================================
# 🆕 ROTAS CRUD DE TEMPLATES
# ==========================================

@templates_bp.route('/api/templates/<template_id>', methods=['GET'])
def get_template_by_id(template_id):
    """
    📌 GET /api/templates/<template_id>
    
    Busca um template específico por ID.
    
    🆕 ARQUITETURA ts_* (32 colunas dedicadas):
    - Cada propriedade de text_style em sua própria coluna
    - ts_{estilo}_{propriedade}
    - Exemplo: ts_default_font, ts_default_borders, ts_default_highlight...
    
    Response:
    {
        "template": {
            "id": "...",
            "name": "...",
            "ts_default_font": {...},
            "ts_default_borders": [...],
            "ts_default_highlight": {...},
            ...
            "shadow": {...},
            "enhanced_phrase_rules": {...},
            ...
        }
    }
    """
    try:
        print(f"[get_template_by_id] 🔍 Buscando template: {template_id}")
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Buscar template com TODAS as colunas
        cursor.execute("""
            SELECT 
                id, name, description, 
                -- DEFAULT (9 colunas)
                ts_default_font, ts_default_borders, ts_default_highlight,
                ts_default_alignment, ts_default_positioning, ts_default_animation,
                ts_default_shadow, ts_default_background, ts_default_cartela,
                -- EMPHASIS (9 colunas)
                ts_emphasis_font, ts_emphasis_borders, ts_emphasis_highlight,
                ts_emphasis_alignment, ts_emphasis_positioning, ts_emphasis_animation,
                ts_emphasis_shadow, ts_emphasis_background, ts_emphasis_cartela,
                -- LETTER_EFFECT (9 colunas)
                ts_letter_effect_font, ts_letter_effect_borders, ts_letter_effect_highlight,
                ts_letter_effect_alignment, ts_letter_effect_positioning, ts_letter_effect_animation,
                ts_letter_effect_shadow, ts_letter_effect_background, ts_letter_effect_cartela,
                -- CARTELA (8 colunas)
                ts_cartela_font, ts_cartela_borders, ts_cartela_highlight,
                ts_cartela_alignment, ts_cartela_positioning, ts_cartela_animation,
                ts_cartela_shadow, ts_cartela_background,
                -- Pipeline configs
                enhanced_phrase_rules, 
                phrase_classification, 
                shadow,
                script_data,
                z_index_config,
                -- Configurações básicas (novas colunas dedicadas)
                project_settings,
                project_type,
                template_mode,
                base_layer,
                -- Layout spacing
                layout_spacing,
                -- Matting (recorte de pessoa)
                matting,
                -- 🆕 Estilos de texto habilitados (multi_text_styling)
                multi_text_styling,
                -- Animações globais (LEGADO - manter por enquanto)
                stagger_and_opacity, 
                multi_animations, 
                asset_animations, 
                animation_preset,
                -- 🆕 IDs de projeto/conversa vinculados
                conversation_id,
                project_id,
                -- Metadata
                metadata,
                approved, status, is_active, render_count,
                created_by, created_at, updated_at
            FROM video_editing_templates 
            WHERE id = %s
        """, (template_id,))
        
        template = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not template:
            return jsonify({'error': f'Template {template_id} não encontrado'}), 404
        
        # Construir objeto de resposta
        result = dict(template)
        result['id'] = str(result['id'])
        result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
        result['updated_at'] = result['updated_at'].isoformat() if result['updated_at'] else None
        # 🆕 Converter UUIDs para string
        result['conversation_id'] = str(result['conversation_id']) if result.get('conversation_id') else None
        result['project_id'] = str(result['project_id']) if result.get('project_id') else None
        
        # Contar estilos configurados
        has_default = result.get('ts_default_font') is not None
        has_emphasis = result.get('ts_emphasis_font') is not None
        has_letter = result.get('ts_letter_effect_font') is not None
        has_cartela = result.get('ts_cartela_font') is not None
        
        print(f"[get_template_by_id] ✅ Template '{result['name']}' - styles: default={'✓' if has_default else '✗'} emphasis={'✓' if has_emphasis else '✗'} letter={'✓' if has_letter else '✗'} cartela={'✓' if has_cartela else '✗'}")
        print(f"[get_template_by_id]    Project: {result['project_id'][:8] if result['project_id'] else '❌'} Conversation: {result['conversation_id'][:8] if result['conversation_id'] else '❌'}")
        
        return jsonify({
            'template': result,
            'success': True
        }), 200
        
    except Exception as e:
        print(f"[get_template_by_id] ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates', methods=['POST'])
def create_template():
    """
    📌 POST /api/templates
    
    Cria um novo template.
    
    🆕 ARQUITETURA ts_* (32 colunas dedicadas):
    - Cada propriedade de text_style em sua própria coluna
    - ts_{estilo}_{propriedade}
    
    Payload esperado:
    {
        "name": "Nome do Template",
        "description": "Descrição opcional",
        "ts_default_font": {...},
        "ts_default_borders": [...],
        "ts_default_highlight": {...},
        "ts_default_alignment": {...},
        "ts_default_positioning": {...},
        "ts_default_animation": {...},
        "ts_default_shadow": {...},
        "ts_default_background": {...},
        ... (mesma estrutura para emphasis, letter_effect, cartela)
        "shadow": {...},
        "enhanced_phrase_rules": {...},
        "approved": false,
        "created_by": "user_id_opcional"
    }
    """
    try:
        data = request.get_json()
        print(f"[create_template] 📥 Recebendo novo template: {data.get('name')}")
        
        # Validações
        if not data.get('name'):
            return jsonify({'error': 'name é obrigatório'}), 400
        
        # Conectar ao banco
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # ═══════════════════════════════════════════════════════════════
        # 🎨 EXTRAIR ts_* (32 colunas)
        # ═══════════════════════════════════════════════════════════════
        
        # Função helper para extrair ou None
        def get_ts(style, prop):
            return data.get(f'ts_{style}_{prop}')
        
        # ═══════════════════════════════════════════════════════════════
        # 📋 EXTRAIR OUTRAS CONFIGS
        # ═══════════════════════════════════════════════════════════════
        
        shadow = data.get('shadow')
        enhanced_phrase_rules = data.get('enhanced_phrase_rules')
        phrase_classification = data.get('phrase_classification')
        stagger_and_opacity = data.get('stagger_and_opacity')
        multi_animations = data.get('multi_animations')
        asset_animations = data.get('asset_animations')
        animation_preset = data.get('animation_preset')
        
        # ═══════════════════════════════════════════════════════════════
        # 💾 INSERIR TEMPLATE com 32 colunas ts_*
        # ═══════════════════════════════════════════════════════════════
        
        # 🆕 DIA-6: Remover script_data (agora vai para template_demo_projects)
        cursor.execute("""
            INSERT INTO video_editing_templates (
                name, description,
                -- DEFAULT (9 colunas)
                ts_default_font, ts_default_borders, ts_default_highlight,
                ts_default_alignment, ts_default_positioning, ts_default_animation,
                ts_default_shadow, ts_default_background, ts_default_cartela,
                -- EMPHASIS (9 colunas)
                ts_emphasis_font, ts_emphasis_borders, ts_emphasis_highlight,
                ts_emphasis_alignment, ts_emphasis_positioning, ts_emphasis_animation,
                ts_emphasis_shadow, ts_emphasis_background, ts_emphasis_cartela,
                -- LETTER_EFFECT (9 colunas)
                ts_letter_effect_font, ts_letter_effect_borders, ts_letter_effect_highlight,
                ts_letter_effect_alignment, ts_letter_effect_positioning, ts_letter_effect_animation,
                ts_letter_effect_shadow, ts_letter_effect_background, ts_letter_effect_cartela,
                -- CARTELA (8 colunas)
                ts_cartela_font, ts_cartela_borders, ts_cartela_highlight,
                ts_cartela_alignment, ts_cartela_positioning, ts_cartela_animation,
                ts_cartela_shadow, ts_cartela_background,
                -- Pipeline configs (SEM script_data)
                enhanced_phrase_rules, phrase_classification, shadow,
                z_index_config,
                stagger_and_opacity, multi_animations, asset_animations, animation_preset,
                -- Metadata
                approved, created_by, created_at, updated_at
            ) VALUES (
                %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            RETURNING id, name, created_at, updated_at
        """, (
            data.get('name'),
            data.get('description', ''),
            # DEFAULT
            json.dumps(get_ts('default', 'font')) if get_ts('default', 'font') else None,
            json.dumps(get_ts('default', 'borders')) if get_ts('default', 'borders') else None,
            json.dumps(get_ts('default', 'highlight')) if get_ts('default', 'highlight') else None,
            json.dumps(get_ts('default', 'alignment')) if get_ts('default', 'alignment') else None,
            json.dumps(get_ts('default', 'positioning')) if get_ts('default', 'positioning') else None,
            json.dumps(get_ts('default', 'animation')) if get_ts('default', 'animation') else None,
            json.dumps(get_ts('default', 'shadow')) if get_ts('default', 'shadow') else None,
            json.dumps(get_ts('default', 'background')) if get_ts('default', 'background') else None,
            json.dumps(get_ts('default', 'cartela')) if get_ts('default', 'cartela') else None,
            # EMPHASIS
            json.dumps(get_ts('emphasis', 'font')) if get_ts('emphasis', 'font') else None,
            json.dumps(get_ts('emphasis', 'borders')) if get_ts('emphasis', 'borders') else None,
            json.dumps(get_ts('emphasis', 'highlight')) if get_ts('emphasis', 'highlight') else None,
            json.dumps(get_ts('emphasis', 'alignment')) if get_ts('emphasis', 'alignment') else None,
            json.dumps(get_ts('emphasis', 'positioning')) if get_ts('emphasis', 'positioning') else None,
            json.dumps(get_ts('emphasis', 'animation')) if get_ts('emphasis', 'animation') else None,
            json.dumps(get_ts('emphasis', 'shadow')) if get_ts('emphasis', 'shadow') else None,
            json.dumps(get_ts('emphasis', 'background')) if get_ts('emphasis', 'background') else None,
            json.dumps(get_ts('emphasis', 'cartela')) if get_ts('emphasis', 'cartela') else None,
            # LETTER_EFFECT
            json.dumps(get_ts('letter_effect', 'font')) if get_ts('letter_effect', 'font') else None,
            json.dumps(get_ts('letter_effect', 'borders')) if get_ts('letter_effect', 'borders') else None,
            json.dumps(get_ts('letter_effect', 'highlight')) if get_ts('letter_effect', 'highlight') else None,
            json.dumps(get_ts('letter_effect', 'alignment')) if get_ts('letter_effect', 'alignment') else None,
            json.dumps(get_ts('letter_effect', 'positioning')) if get_ts('letter_effect', 'positioning') else None,
            json.dumps(get_ts('letter_effect', 'animation')) if get_ts('letter_effect', 'animation') else None,
            json.dumps(get_ts('letter_effect', 'shadow')) if get_ts('letter_effect', 'shadow') else None,
            json.dumps(get_ts('letter_effect', 'background')) if get_ts('letter_effect', 'background') else None,
            json.dumps(get_ts('letter_effect', 'cartela')) if get_ts('letter_effect', 'cartela') else None,
            # CARTELA
            json.dumps(get_ts('cartela', 'font')) if get_ts('cartela', 'font') else None,
            json.dumps(get_ts('cartela', 'borders')) if get_ts('cartela', 'borders') else None,
            json.dumps(get_ts('cartela', 'highlight')) if get_ts('cartela', 'highlight') else None,
            json.dumps(get_ts('cartela', 'alignment')) if get_ts('cartela', 'alignment') else None,
            json.dumps(get_ts('cartela', 'positioning')) if get_ts('cartela', 'positioning') else None,
            json.dumps(get_ts('cartela', 'animation')) if get_ts('cartela', 'animation') else None,
            json.dumps(get_ts('cartela', 'shadow')) if get_ts('cartela', 'shadow') else None,
            json.dumps(get_ts('cartela', 'background')) if get_ts('cartela', 'background') else None,
            # Pipeline configs (SEM script_data)
            json.dumps(enhanced_phrase_rules) if enhanced_phrase_rules else None,
            json.dumps(phrase_classification) if phrase_classification else None,
            json.dumps(shadow) if shadow else None,
            json.dumps(data.get('z_index_config')) if data.get('z_index_config') else None,
            json.dumps(stagger_and_opacity) if stagger_and_opacity else None,
            json.dumps(multi_animations) if multi_animations else None,
            json.dumps(asset_animations) if asset_animations else None,
            animation_preset,
            # Metadata
            data.get('approved', False),
            data.get('created_by'),
            datetime.utcnow(),
            datetime.utcnow()
        ))
        
        new_template = cursor.fetchone()
        template_id = str(new_template['id'])
        
        # ═══════════════════════════════════════════════════════════════
        # 🆕 DIA-6: CRIAR DEMO EM template_demo_projects (se script_data fornecido)
        # ═══════════════════════════════════════════════════════════════
        # ❌ REMOVIDO: Criação automática de conversation/project
        # ✅ NOVO: Se script_data for fornecido, criar demo
        
        demo_id = None
        script_data = data.get('script_data')
        
        if script_data:
            ADMIN_USER_ID = '8d04a8bf-0b80-48fa-9fb1-0b42dcb36a11'
            user_id = data.get('created_by') or ADMIN_USER_ID
            
            # 🆕 SAVEPOINT: Permite rollback parcial sem perder o template
            cursor.execute("SAVEPOINT before_demo")
            
            try:
                import uuid
                
                # Criar demo em template_demo_projects
                demo_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO template_demo_projects (
                        id, template_id, name, description, script_data,
                        is_featured, created_by, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    RETURNING id
                """, (
                    demo_id,
                    template_id,
                    f"Demo: {data.get('name')}",
                    f"Demo de demonstração para o template {data.get('name')}",
                    json.dumps(script_data),
                    True,  # Primeiro demo é sempre featured
                    user_id
                ))
                print(f"[create_template] 🎬 Demo criado: {demo_id[:8]}... (featured)")
                
                # Sucesso - libera o savepoint
                cursor.execute("RELEASE SAVEPOINT before_demo")
                
            except Exception as e:
                # 🆕 ROLLBACK parcial - desfaz apenas demo, mantém template
                cursor.execute("ROLLBACK TO SAVEPOINT before_demo")
                print(f"[create_template] ⚠️ Erro ao criar demo (template criado sem demo): {e}")
                import traceback
                traceback.print_exc()
                demo_id = None
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log resumo
        has_default = get_ts('default', 'font') is not None
        has_emphasis = get_ts('emphasis', 'font') is not None
        has_letter = get_ts('letter_effect', 'font') is not None
        has_cartela = get_ts('cartela', 'font') is not None
        
        print(f"[create_template] ✅ Template criado com ID: {template_id}")
        print(f"[create_template]    Text styles: default={'✓' if has_default else '✗'} emphasis={'✓' if has_emphasis else '✗'} letter={'✓' if has_letter else '✗'} cartela={'✓' if has_cartela else '✗'}")
        print(f"[create_template]    Demo: {demo_id[:8] if demo_id else '❌ (nenhum script_data fornecido)'}")
        
        # Converter para dict e formatar datas
        result = dict(new_template)
        result['id'] = str(result['id'])
        result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
        result['updated_at'] = result['updated_at'].isoformat() if result['updated_at'] else None
        result['demo_id'] = demo_id  # 🆕 DIA-6: Retornar demo_id ao invés de conversation_id/project_id
        
        return jsonify({
            'success': True,
            'template': result,
            'demo_id': demo_id  # 🆕 DIA-6: Incluir demo_id na resposta
        }), 201
        
    except Exception as e:
        print(f"[create_template] ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/<template_id>', methods=['PATCH'])
def update_template(template_id):
    """
    📝 PATCH /api/templates/<template_id>
    
    Atualiza um template existente.
    
    🆕 ARQUITETURA ts_* (32 colunas dedicadas):
    - Cada propriedade de text_style em sua própria coluna
    - ts_{estilo}_{propriedade}
    
    Payload esperado (campos opcionais):
    {
        "name": "Novo Nome",
        "ts_default_font": {...},
        "ts_default_borders": [...],
        "ts_emphasis_animation": {...},
        ... (qualquer coluna ts_*)
        "shadow": {...},
        "approved": true
    }
    """
    try:
        data = request.get_json()
        print(f"[update_template] 📝 Atualizando template: {template_id}")
        
        # Conectar ao banco
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Verificar se template existe
        cursor.execute("""
            SELECT id, name FROM video_editing_templates WHERE id = %s
        """, (template_id,))
        
        existing_template = cursor.fetchone()
        
        if not existing_template:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Template {template_id} não encontrado'}), 404
        
        # Construir UPDATE dinâmico
        update_fields = []
        update_values = []
        
        # ═══════════════════════════════════════════════════════════════
        # 🎨 ts_* - 32 Colunas Dedicadas (NOVA ARQUITETURA)
        # ═══════════════════════════════════════════════════════════════
        
        # Lista de todas as 35 colunas ts_* (32 estilos + 3 cartela per-style)
        ts_columns = [
            # DEFAULT (9 colunas)
            'ts_default_font', 'ts_default_borders', 'ts_default_highlight',
            'ts_default_alignment', 'ts_default_positioning', 'ts_default_animation',
            'ts_default_shadow', 'ts_default_background', 'ts_default_cartela',
            # EMPHASIS (9 colunas)
            'ts_emphasis_font', 'ts_emphasis_borders', 'ts_emphasis_highlight',
            'ts_emphasis_alignment', 'ts_emphasis_positioning', 'ts_emphasis_animation',
            'ts_emphasis_shadow', 'ts_emphasis_background', 'ts_emphasis_cartela',
            # LETTER_EFFECT (9 colunas)
            'ts_letter_effect_font', 'ts_letter_effect_borders', 'ts_letter_effect_highlight',
            'ts_letter_effect_alignment', 'ts_letter_effect_positioning', 'ts_letter_effect_animation',
            'ts_letter_effect_shadow', 'ts_letter_effect_background', 'ts_letter_effect_cartela',
            # CARTELA (8 colunas - estilo de texto cartela)
            'ts_cartela_font', 'ts_cartela_borders', 'ts_cartela_highlight',
            'ts_cartela_alignment', 'ts_cartela_positioning', 'ts_cartela_animation',
            'ts_cartela_shadow', 'ts_cartela_background',
        ]
        
        # Processar cada coluna ts_* que veio no payload
        for col in ts_columns:
            if col in data:
                update_fields.append(f'{col} = %s')
                update_values.append(json.dumps(data[col]) if data[col] else None)
                print(f"[update_template] ✅ {col}: ~{len(json.dumps(data[col])) if data[col] else 0} bytes")
        
        # ═══════════════════════════════════════════════════════════════
        # 📋 OUTRAS COLUNAS DEDICADAS
        # ═══════════════════════════════════════════════════════════════
        
        if 'name' in data:
            update_fields.append('name = %s')
            update_values.append(data['name'])
        
        if 'description' in data:
            update_fields.append('description = %s')
            update_values.append(data['description'])
        
        if 'shadow' in data:
            update_fields.append('shadow = %s')
            update_values.append(json.dumps(data['shadow']) if data['shadow'] else None)
            print(f"[update_template] ✅ shadow (global) detectado")
        
        if 'enhanced_phrase_rules' in data:
            update_fields.append('enhanced_phrase_rules = %s')
            update_values.append(json.dumps(data['enhanced_phrase_rules']) if data['enhanced_phrase_rules'] else None)
            print(f"[update_template] ✅ enhanced_phrase_rules detectado")
        
        if 'phrase_classification' in data:
            update_fields.append('phrase_classification = %s')
            update_values.append(json.dumps(data['phrase_classification']) if data['phrase_classification'] else None)
            print(f"[update_template] ✅ phrase_classification detectado")
        
        if 'script_data' in data:
            update_fields.append('script_data = %s')
            update_values.append(json.dumps(data['script_data']) if data['script_data'] else None)
            print(f"[update_template] ✅ script_data detectado")
        
        if 'z_index_config' in data:
            update_fields.append('z_index_config = %s')
            update_values.append(json.dumps(data['z_index_config']) if data['z_index_config'] else None)
            print(f"[update_template] ✅ z_index_config detectado")
        
        if 'stagger_and_opacity' in data:
            update_fields.append('stagger_and_opacity = %s')
            update_values.append(json.dumps(data['stagger_and_opacity']) if data['stagger_and_opacity'] else None)
            print(f"[update_template] ✅ stagger_and_opacity (legado) detectado")
        
        if 'multi_animations' in data:
            update_fields.append('multi_animations = %s')
            update_values.append(json.dumps(data['multi_animations']) if data['multi_animations'] else None)
            print(f"[update_template] ✅ multi_animations (legado) detectado")
        
        if 'asset_animations' in data:
            update_fields.append('asset_animations = %s')
            update_values.append(json.dumps(data['asset_animations']) if data['asset_animations'] else None)
            print(f"[update_template] ✅ asset_animations (legado) detectado")
        
        if 'animation_preset' in data:
            update_fields.append('animation_preset = %s')
            update_values.append(data['animation_preset'])
            print(f"[update_template] ✅ animation_preset (legado): {data['animation_preset']}")
        
        # ═══════════════════════════════════════════════════════════════
        # 📦 CONFIGURAÇÕES BÁSICAS (novas colunas dedicadas)
        # ═══════════════════════════════════════════════════════════════
        
        if 'project_settings' in data:
            update_fields.append('project_settings = %s')
            update_values.append(json.dumps(data['project_settings']) if data['project_settings'] else None)
            print(f"[update_template] ✅ project_settings detectado")
        
        if 'project_type' in data:
            update_fields.append('project_type = %s')
            update_values.append(json.dumps(data['project_type']) if data['project_type'] else None)
            print(f"[update_template] ✅ project_type detectado")
        
        if 'template_mode' in data:
            update_fields.append('template_mode = %s')
            update_values.append(json.dumps(data['template_mode']) if data['template_mode'] else None)
            print(f"[update_template] ✅ template_mode detectado")
        
        if 'base_layer' in data:
            update_fields.append('base_layer = %s')
            update_values.append(json.dumps(data['base_layer']) if data['base_layer'] else None)
            print(f"[update_template] ✅ base_layer detectado")
        
        # layout_spacing
        if 'layout_spacing' in data:
            update_fields.append('layout_spacing = %s')
            update_values.append(json.dumps(data['layout_spacing']) if data['layout_spacing'] else None)
            print(f"[update_template] ✅ layout_spacing detectado")
        
        # creative_layout (tamanhos dinâmicos e shifts de linha)
        if 'creative_layout' in data:
            update_fields.append('creative_layout = %s')
            update_values.append(json.dumps(data['creative_layout']) if data['creative_layout'] else None)
            print(f"[update_template] ✅ creative_layout detectado")
        
        # cartela_presets (presets de cartela por frase)
        if 'cartela_presets' in data:
            update_fields.append('cartela_presets = %s')
            update_values.append(json.dumps(data['cartela_presets']) if data['cartela_presets'] else None)
            print(f"[update_template] ✅ cartela_presets detectado")
        
        # matting (recorte de pessoa / v-matting)
        if 'matting' in data:
            update_fields.append('matting = %s')
            update_values.append(json.dumps(data['matting']) if data['matting'] else None)
            print(f"[update_template] ✅ matting detectado")
        
        # 🆕 multi_text_styling (estilos de texto habilitados por tipo)
        if 'multi_text_styling' in data:
            update_fields.append('multi_text_styling = %s')
            update_values.append(json.dumps(data['multi_text_styling']) if data['multi_text_styling'] else None)
            print(f"[update_template] ✅ multi_text_styling detectado")
        
        # 🆕 marketing_info, template_info e motion_graphics_prompt (salvos dentro do metadata JSONB)
        # A tabela video_editing_templates não tem colunas dedicadas para esses campos,
        # então salvamos dentro do metadata existente
        if 'marketing_info' in data or 'template_info' in data or 'motion_graphics_prompt' in data:
            # Buscar metadata atual
            cursor.execute("SELECT metadata FROM video_editing_templates WHERE id = %s", (template_id,))
            current_row = cursor.fetchone()
            current_metadata = current_row['metadata'] if current_row and current_row.get('metadata') else {}
            
            if 'marketing_info' in data:
                current_metadata['marketing_info'] = data['marketing_info']
                print(f"[update_template] ✅ marketing_info detectado (salvando em metadata)")
            
            if 'template_info' in data:
                current_metadata['template_info'] = data['template_info']
                print(f"[update_template] ✅ template_info detectado (salvando em metadata)")
            
            # 🆕 motion_graphics_prompt (para LLM Director de motion graphics)
            if 'motion_graphics_prompt' in data:
                current_metadata['motion_graphics_prompt'] = data['motion_graphics_prompt']
                print(f"[update_template] ✅ motion_graphics_prompt detectado (salvando em metadata)")
            
            update_fields.append('metadata = %s')
            update_values.append(json.dumps(current_metadata))
        
        if 'approved' in data:
            update_fields.append('approved = %s')
            update_values.append(data['approved'])
        
        # Sempre atualizar updated_at
        update_fields.append('updated_at = %s')
        update_values.append(datetime.utcnow())
        
        # Verificar se há algo para atualizar
        if len(update_fields) == 1:  # Só o updated_at
            print(f"[update_template] ⚠️ Nenhum campo para atualizar")
            cursor.close()
            conn.close()
            return jsonify({'success': True, 'message': 'Nenhum campo para atualizar'}), 200
        
        # Adicionar template_id ao final para o WHERE
        update_values.append(template_id)
        
        # Executar UPDATE
        query = f"""
            UPDATE video_editing_templates
            SET {', '.join(update_fields)}
            WHERE id = %s
            RETURNING id, name, approved, created_at, updated_at
        """
        
        cursor.execute(query, tuple(update_values))
        updated_template = cursor.fetchone()
        
        print(f"[update_template] ✅ Template '{updated_template['name']}' atualizado ({len(update_fields)-1} campos)")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Converter para dict e formatar datas
        result = dict(updated_template)
        result['id'] = str(result['id'])
        result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
        result['updated_at'] = result['updated_at'].isoformat() if result['updated_at'] else None
        
        return jsonify({
            'success': True,
            'template': result
        }), 200
        
    except Exception as e:
        print(f"[update_template] ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


# ==========================================
# 🆕 RENDER VERSIONS API
# ==========================================

@templates_bp.route('/api/templates/<template_id>/render-versions', methods=['GET'])
def get_render_versions(template_id):
    """
    📋 GET /api/templates/<template_id>/render-versions
    
    Busca todas as versões de renders de um template da tabela render_versions.
    Usado pelo Generator V3 para mostrar histórico de renderizações.
    
    Query params:
    - limit: número de versões (default: 50)
    - phase: filtrar por fase (1 ou 2)
    """
    try:
        limit = request.args.get('limit', 50, type=int)
        phase = request.args.get('phase', type=int)
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Query base
        query = """
            SELECT 
                rv.id,
                rv.job_id,
                rv.version_number,
                rv.phase,
                rv.video_url,
                rv.thumbnail_url,
                rv.duration_ms,
                rv.file_size_bytes,
                rv.resolution,
                rv.fps,
                rv.codec,
                rv.worker_id,
                rv.render_duration_ms,
                rv.is_current,
                rv.is_featured,
                rv.created_at,
                rv.version_note,
                vpj.status as job_status,
                vpj.created_at as job_created_at
            FROM render_versions rv
            LEFT JOIN video_processing_jobs vpj ON rv.job_id = vpj.job_id
            WHERE rv.template_id = %s
              AND rv.is_deleted = false
        """
        
        params = [template_id]
        
        # Filtrar por fase se especificado
        if phase:
            query += " AND rv.phase = %s"
            params.append(phase)
        
        query += " ORDER BY rv.created_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        versions = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Converter para lista de dicts
        results = []
        for version in versions:
            result = dict(version)
            result['id'] = str(result['id'])
            result['job_id'] = str(result['job_id']) if result['job_id'] else None
            result['created_at'] = result['created_at'].isoformat() if result['created_at'] else None
            result['job_created_at'] = result['job_created_at'].isoformat() if result['job_created_at'] else None
            results.append(result)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'versions': results
        }), 200
        
    except Exception as e:
        print(f"[get_render_versions] Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/<template_id>/set-featured-render', methods=['PUT'])
def set_featured_render(template_id):
    """
    🌟 PUT /api/templates/<template_id>/set-featured-render
    
    Marca um render específico como "featured" (usado como thumbnail nos carrosséis).
    Desmarca todos os outros renders do mesmo template.
    
    Payload esperado:
    {
        "render_version_id": "uuid-do-render"
    }
    """
    try:
        data = request.get_json()
        render_version_id = data.get('render_version_id')
        
        if not render_version_id:
            return jsonify({'error': 'render_version_id é obrigatório'}), 400
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Verificar se o render existe e pertence ao template
        cursor.execute("""
            SELECT id, template_id, video_url
            FROM render_versions
            WHERE id = %s AND template_id = %s AND is_deleted = false
        """, (render_version_id, template_id))
        
        render = cursor.fetchone()
        
        if not render:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Render não encontrado ou não pertence ao template'}), 404
        
        # 1. Desmarcar todos como featured deste template
        cursor.execute("""
            UPDATE render_versions
            SET is_featured = false
            WHERE template_id = %s
        """, (template_id,))
        
        # 2. Marcar o render escolhido como featured
        cursor.execute("""
            UPDATE render_versions
            SET is_featured = true
            WHERE id = %s
            RETURNING id, is_featured
        """, (render_version_id,))
        
        updated = cursor.fetchone()
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"[set_featured_render] ✅ Render {render_version_id[:8]}... marcado como featured para template {template_id[:8]}...")
        
        return jsonify({
            'success': True,
            'message': 'Render marcado como featured',
            'render_version_id': str(updated['id'])
        }), 200
        
    except Exception as e:
        print(f"[set_featured_render] Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@templates_bp.route('/api/templates/<template_id>/featured-video', methods=['GET'])
def get_featured_video(template_id):
    """
    🆕 v3.3.0: GET /api/templates/<template_id>/featured-video
    
    Retorna o vídeo da versão marcada como featured (estrela) do template.
    Usado pela página de listagem de templates para mostrar o thumbnail correto.
    
    Response:
    {
        "video_url": "https://...",
        "version_number": 10,
        "created_at": "2026-02-04T13:48:09.721442+00:00"
    }
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Buscar versão featured (is_featured = true, phase = 2)
        cursor.execute("""
            SELECT video_url, version_number, created_at
            FROM render_versions
            WHERE template_id = %s
              AND phase = 2
              AND is_featured = true
              AND is_deleted = false
            ORDER BY created_at DESC
            LIMIT 1
        """, (template_id,))
        
        featured = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not featured:
            return jsonify({'video_url': None, 'message': 'Nenhuma versão featured encontrada'}), 200
        
        return jsonify({
            'video_url': featured['video_url'],
            'version_number': featured['version_number'],
            'created_at': featured['created_at'].isoformat() if featured['created_at'] else None
        }), 200
        
    except Exception as e:
        print(f"[get_featured_video] Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500
