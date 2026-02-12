from flask import Blueprint, request, jsonify
from app.db import get_db_connection
from app.utils.params_converter import convert_v2_to_flat
import psycopg2.extras
import uuid

video_templates_bp = Blueprint('video_templates_bp', __name__)


def _is_valid_uuid(val):
    """Verifica se uma string é um UUID válido"""
    try:
        uuid.UUID(str(val))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


# ============================================
# Sanitização segura para params (NÃO remove 'value')
# ============================================

def _sanitize_preserve_values(node):
    """
    Caminha de forma recursiva pelo objeto e NUNCA remove 'value' quando
    o dicionário representa um campo (indicado por 'sidecar_id').

    Também evita transformar números/strings em estruturas acidentalmente.
    """
    if isinstance(node, dict):
        # Se for folha de campo (tem sidecar_id), apenas sanitize filhos
        if 'sidecar_id' in node:
            return {k: _sanitize_preserve_values(v) for k, v in node.items()}
        # Nó interno: não mexe em 'value' se existir por algum motivo
        out = {}
        for k, v in node.items():
            # Mantém todas as chaves (inclusive 'value')
            out[k] = _sanitize_preserve_values(v)
        return out
    if isinstance(node, list):
        return [_sanitize_preserve_values(x) for x in node]
    return node


@video_templates_bp.route('/templates/<template_id>', methods=['GET'])
def get_video_template(template_id):
    """
    Busca um template de edição de vídeo por ID.
    
    🆕 DIA-6: Atualizado para remover campos obsoletos
    ❌ NÃO retorna: conversation_id, project_id, script_data
    ✅ Retorna configs em colunas dedicadas (ts_*, shadow, etc.)
    ✅ Inclui demo featured (se existir)
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute(
            """
            SELECT 
                t.id,
                t.name,
                t.description,
                t.tags,
                t.status,
                t.is_active,
                t.approved,
                t.template_mode,
                t.base_layer,
                t.ts_default_font,
                t.ts_emphasis_font,
                t.ts_letter_effect_font,
                t.ts_cartela_font,
                t.ts_default_animation,
                t.ts_emphasis_animation,
                t.ts_letter_effect_animation,
                t.ts_cartela_animation,
                t.layout_spacing,
                t.creative_layout,
                t.shadow,
                t.z_index_config,
                t.matting,
                t.metadata,
                t.created_at,
                t.updated_at,
                -- 🆕 Incluir demo featured
                featured_demo.video_url as demo_video_url,
                featured_demo.thumbnail_url as demo_thumbnail_url,
                featured_demo.id as demo_id
            FROM public.video_editing_templates t
            LEFT JOIN LATERAL (
                SELECT id, video_url, thumbnail_url
                FROM template_demo_projects
                WHERE template_id = t.id
                ORDER BY is_featured DESC, created_at DESC
                LIMIT 1
            ) featured_demo ON true
            WHERE t.id = %s;
            """,
            (template_id,)
        )
        
        template = cur.fetchone()
        cur.close()

        if not template:
            return jsonify({"error": "Template não encontrado"}), 404

        template_dict = dict(template)
        
        print(f"✅ Retornando template {template_id} (limpo - sem campos obsoletos)")
        
        return jsonify({
            "success": True,
            "template": template_dict
        }), 200

    except Exception as e:
        print(f"Erro ao buscar template: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/templates/<template_id>/latest-video', methods=['GET'])
def get_template_latest_video(template_id):
    """
    Busca o vídeo demo mais recente (featured) de um template.
    
    🆕 DIA-6: Atualizado para usar template_demo_projects
    ❌ REMOVIDO: Busca em project_renders (obsoleto)
    ✅ NOVO: Busca demo featured em template_demo_projects
    
    Retorna:
    {
        "video_url": "https://...",
        "demo_id": "uuid",
        "created_at": "timestamp",
        "thumb_url": "https://...",
        "is_featured": true
    }
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 🆕 Buscar demo featured (ou mais recente) do template
        cur.execute(
            """
            SELECT 
                id,
                name,
                video_url,
                thumbnail_url,
                is_featured,
                created_at
            FROM template_demo_projects
            WHERE template_id = %s
              AND video_url IS NOT NULL
            ORDER BY is_featured DESC, created_at DESC
            LIMIT 1;
            """,
            (template_id,)
        )
        
        demo = cur.fetchone()
        cur.close()

        if not demo:
            return jsonify({
                "video_url": None,
                "message": "Nenhum demo encontrado para este template"
            }), 404

        demo_dict = dict(demo)
        
        return jsonify({
            "video_url": demo_dict.get('video_url'),
            "demo_id": demo_dict.get('id'),
            "demo_name": demo_dict.get('name'),
            "created_at": demo_dict.get('created_at'),
            "thumb_url": demo_dict.get('thumbnail_url'),
            "is_featured": demo_dict.get('is_featured')
        }), 200

    except Exception as e:
        print(f"Erro ao buscar demo do template {template_id}: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/templates', methods=['GET'])
def list_video_templates():
    """
    Lista templates de edição de vídeo com filtros avançados.
    
    Query Parameters:
    - template_mode: filtro por modo (subtitles_simple, subtitles_creative, hyper_dynamic, etc)
    - template_theme: filtro por tema (modern, elegant, bold, minimalist, etc)
    - storytelling_mode: filtro por compatibilidade (vlog, voice_over, music, lyric_video, mixed)
    - tags: filtro por tags (comma-separated)
    - status: filtro por status (draft, pending_approval, approved, published)
    - is_active: filtro por ativo (true/false)
    - search: busca por nome ou descrição
    - limit: limite de resultados (padrão: 50)
    - offset: paginação (padrão: 0)
    - sort_by: ordenação (updated_at, created_at, name) (padrão: updated_at)
    - sort_order: ordem (asc, desc) (padrão: desc)
    
    Exemplos:
    - GET /templates?template_mode=subtitles_creative&storytelling_mode=vlog
    - GET /templates?tags=energetic,modern&is_active=true
    - GET /templates?search=instagram&limit=10
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # ============================================
        # QUERY PARAMETERS
        # ============================================
        template_mode = request.args.get('template_mode')  # ex: subtitles_creative
        template_theme = request.args.get('template_theme')  # ex: modern, bold
        storytelling_mode = request.args.get('storytelling_mode')  # ex: vlog, voice_over
        tags_param = request.args.get('tags')  # ex: "energetic,modern"
        status = request.args.get('status')  # ex: approved, published
        is_active = request.args.get('is_active')  # ex: true, false
        search = request.args.get('search')  # busca por nome/descrição
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        sort_by = request.args.get('sort_by', 'updated_at')  # updated_at, created_at, name
        sort_order = request.args.get('sort_order', 'desc').upper()  # ASC, DESC

        # Validação de sort_order
        if sort_order not in ['ASC', 'DESC']:
            sort_order = 'DESC'

        # Validação de sort_by
        allowed_sort_fields = ['updated_at', 'created_at', 'name', 'id']
        if sort_by not in allowed_sort_fields:
            sort_by = 'updated_at'

        # ============================================
        # CONSTRUIR QUERY DINÂMICA
        # ============================================
        where_clauses = []
        params = []

        # Filtro por template_mode (coluna dedicada JSONB)
        # ✅ Nova arquitetura: usando coluna template_mode dedicada
        # 🆕 v2.9.60: Adicionado alias t. para compatibilidade com LATERAL JOIN
        if template_mode:
            where_clauses.append("t.template_mode->>'template_mode'->>'value' = %s")
            params.append(template_mode)

        # Filtro por template_theme (coluna dedicada base_layer JSONB)
        if template_theme:
            where_clauses.append("t.base_layer->>'template_theme' = %s")
            params.append(template_theme)

        # Filtro por storytelling_mode (coluna dedicada base_layer JSONB)
        if storytelling_mode:
            where_clauses.append("t.base_layer->>'storytelling_mode'->>'value' = %s")
            params.append(storytelling_mode)

        # Filtro por tags (array TEXT[])
        if tags_param:
            tags_list = [tag.strip() for tag in tags_param.split(',') if tag.strip()]
            if tags_list:
                # Verifica se alguma tag do filtro está presente no array tags do template
                where_clauses.append("t.tags && %s")  # Operador && = overlap (interseção)
                params.append(tags_list)

        # Filtro por status
        if status:
            where_clauses.append("t.status = %s")
            params.append(status)

        # Filtro por is_active
        if is_active:
            is_active_bool = is_active.lower() == 'true'
            where_clauses.append("t.is_active = %s")
            params.append(is_active_bool)

        # 🆕 Filtro por approved (coluna boolean)
        approved = request.args.get('approved')
        if approved:
            approved_bool = approved.lower() == 'true'
            where_clauses.append("t.approved = %s")
            params.append(approved_bool)

        # 🆕 Filtro por category (dentro do metadata JSONB)
        category = request.args.get('category')
        if category:
            where_clauses.append("t.metadata->'marketing_info'->'category'->>'value' = %s")
            params.append(category)

        # Busca textual (nome ou descrição)
        if search:
            where_clauses.append("(t.name ILIKE %s OR t.description ILIKE %s)")
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern])

        # ============================================
        # MONTAR QUERY FINAL
        # ============================================
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # 🆕 DIA-6: JOIN com template_demo_projects para buscar demo featured
        # ❌ REMOVIDO: conversation_id (agora é NULL após migração DIA-5)
        # ✅ NOVO: Busca demo principal em template_demo_projects
        query = f"""
            SELECT 
                t.*,
                featured_demo.video_url as demo_video_url,
                featured_demo.thumbnail_url as demo_thumbnail_url,
                featured_demo.id as demo_id,
                featured_demo.name as demo_name
            FROM public.video_editing_templates t
            LEFT JOIN LATERAL (
                SELECT 
                    tdp.id,
                    tdp.name,
                    tdp.video_url,
                    tdp.thumbnail_url,
                    tdp.created_at
                FROM template_demo_projects tdp
                WHERE tdp.template_id = t.id
                ORDER BY tdp.is_featured DESC, tdp.created_at DESC
                LIMIT 1
            ) featured_demo ON true
            {where_sql}
            ORDER BY t.{sort_by} {sort_order}
            LIMIT %s OFFSET %s;
        """
        params.extend([limit, offset])

        print(f"🔍 [TEMPLATES] Executando query com filtros: {where_sql}")
        print(f"📊 [TEMPLATES] Params: {params}")

        cur.execute(query, params)
        
        templates = cur.fetchall()

        # Contar total (para paginação)
        # 🆕 v2.9.60: Já usamos alias t. nas where_clauses
        count_query = f"""
            SELECT COUNT(*) as total FROM public.video_editing_templates t
            {where_sql};
        """
        # Params sem limit/offset para count
        count_params = params[:-2] if where_clauses else []
        cur.execute(count_query, count_params)
        total_count = cur.fetchone()['total']

        cur.close()

        templates_list = [dict(template) for template in templates]
        
        return jsonify({
            "success": True,
            "templates": templates_list,
            "count": len(templates_list),
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "filters_applied": {
                "template_mode": template_mode,
                "template_theme": template_theme,
                "storytelling_mode": storytelling_mode,
                "tags": tags_param,
                "status": status,
                "is_active": is_active,
                "search": search
            }
        }), 200

    except Exception as e:
        print(f"❌ Erro ao listar templates: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/templates', methods=['POST'])
def create_video_template():
    """
    Cria um novo template de edição de vídeo.
    
    ⚠️ ENDPOINT LEGADO (usado pelo N8N)
    Para criar templates com colunas ts_*, use POST /api/templates (templates.py)
    """
    data = request.get_json()

    # Validação básica - name é obrigatório
    if not data or 'name' not in data:
        return jsonify({"error": "Dados inválidos. 'name' é obrigatório."}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Valida created_by: se não for UUID válido, usa None
        created_by = data.get('created_by')
        if created_by and not _is_valid_uuid(created_by):
            created_by = None

        # ✅ Nova arquitetura: não usa mais params_legado
        # Inserir apenas campos básicos
        cur.execute(
            """
            INSERT INTO public.video_editing_templates 
            (name, description, tags, status, created_by, is_active, preview_image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *;
            """,
            (
                data.get('name'),
                data.get('description'),
                data.get('tags', []),
                data.get('status', 'draft'),
                created_by,
                data.get('is_active', True),
                data.get('preview_image_url')
            )
        )
        
        new_template = cur.fetchone()
        conn.commit()
        cur.close()

        # Converte o DictRow para um dicionário padrão para serialização JSON
        if new_template:
            new_template_dict = dict(new_template)
            # O Supabase/PostgREST espera o id como 'id', então garantimos isso
            new_template_dict['template_id'] = new_template_dict.get('id')
            return jsonify({
                "success": True,
                "template_id": new_template_dict.get('id'),
                "template": new_template_dict,
                "message": "Template salvo com sucesso"
            }), 201
        else:
            return jsonify({"error": "Não foi possível criar o template."}), 500

    except psycopg2.errors.UniqueViolation:
        return jsonify({"error": "Já existe um template com este nome."}), 409
    except Exception as e:
        print(f"Erro ao criar template: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/templates/<template_id>', methods=['PATCH'])
def update_video_template(template_id):
    """
    Atualiza um template de edição de vídeo existente.
    """
    data = request.get_json()

    if not data:
        return jsonify({"error": "Dados inválidos."}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Monta dinamicamente o UPDATE baseado nos campos enviados
        update_fields = []
        update_values = []
        
        # ✅ Campos permitidos (sem params_legado que foi removido)
        allowed_fields = ['name', 'description', 'tags', 'status', 'is_active', 'preview_image_url']
        
        for field in allowed_fields:
                if field in data:
                    if field == 'tags':
                        # tags é TEXT[] (array nativo do Postgres)
                        update_fields.append(f"{field} = %s")
                        tags_value = data[field] if isinstance(data[field], list) else []
                        update_values.append(tags_value)  # psycopg2 converte list → ARRAY automaticamente
                    else:
                        update_fields.append(f"{field} = %s")
                        update_values.append(data[field])
        
        # Adiciona updated_at automático
        update_fields.append("updated_at = NOW()")
        
        # Adiciona o template_id ao final
        update_values.append(template_id)
        
        if len(update_fields) <= 1:  # Apenas updated_at
            return jsonify({"error": "Nenhum campo para atualizar"}), 400
        
        query = f"""
            UPDATE public.video_editing_templates
            SET {', '.join(update_fields)}
            WHERE id = %s
            RETURNING *;
        """
        
        cur.execute(query, update_values)
        
        updated_template = cur.fetchone()
        conn.commit()
        cur.close()

        if not updated_template:
            return jsonify({"error": "Template não encontrado"}), 404

        template_dict = dict(updated_template)
        return jsonify({
            "success": True,
            "template": template_dict,
            "message": "Template atualizado com sucesso"
        }), 200

    except Exception as e:
        print(f"Erro ao atualizar template: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


# ============================================
# 🎬 ENDPOINTS PARA JOBS DE RENDERIZAÇÃO
# ============================================
# Usa a tabela existente: public.project_renders

@video_templates_bp.route('/renders', methods=['POST'])
def create_render_job():
    """
    Cria um novo job de renderização usando a tabela project_renders existente.
    Payload: { 
        "id": "uuid" (opcional, será gerado se omitido),
        "project_id": "uuid" (opcional),
        "status": "rendering_started",
        "timeline_json": {...} (payload completo)
    }
    """
    conn = None
    try:
        data = request.get_json()
        
        # job_id pode vir do frontend ou ser gerado aqui
        job_id = data.get('id') or data.get('job_id') or str(uuid.uuid4())
        project_id = data.get('project_id')  # Pode ser null
        status = data.get('status', 'rendering_started')
        timeline_json = data.get('timeline_json', {})
        version = data.get('version', 1)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute(
            """
            INSERT INTO public.project_renders 
            (id, project_id, version, status, timeline_json, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING *;
            """,
            (job_id, project_id, version, status, psycopg2.extras.Json(timeline_json))
        )
        
        job = cur.fetchone()
        conn.commit()
        cur.close()
        
        return jsonify({
            "success": True,
            "render": dict(job)
        }), 201
    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erro ao criar render job: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/renders/<job_id>', methods=['GET'])
def get_render_job(job_id):
    """
    Consulta o status de um job de renderização.
    Retorna: { "status": "completed", "rendered_video_url": "https://...", ... }
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute(
            """
            SELECT * FROM public.project_renders
            WHERE id = %s;
            """,
            (job_id,)
        )
        
        job = cur.fetchone()
        cur.close()
        
        if not job:
            return jsonify({"error": "Render job não encontrado"}), 404
        
        return jsonify({
            "success": True,
            "render": dict(job)
        }), 200
    
    except Exception as e:
        print(f"Erro ao consultar render job: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/renders/<job_id>', methods=['PUT', 'PATCH'])
def update_render_job(job_id):
    """
    Atualiza o status de um job de renderização (chamado pelo webhook do N8N).
    Payload: { 
        "status": "completed", 
        "rendered_video_url": "https://...",
        "rendered_thumb_url": "https://..."
    }
    """
    conn = None
    try:
        data = request.get_json()
        
        status = data.get('status')
        rendered_video_url = data.get('rendered_video_url') or data.get('video_url')
        rendered_thumb_url = data.get('rendered_thumb_url') or data.get('thumb_url')
        
        if not status:
            return jsonify({"error": "status é obrigatório"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Montar query dinâmica
        update_fields = ["status = %s"]
        params = [status]
        
        if rendered_video_url is not None:
            update_fields.append("rendered_video_url = %s")
            params.append(rendered_video_url)
        
        if rendered_thumb_url is not None:
            update_fields.append("rendered_thumb_url = %s")
            params.append(rendered_thumb_url)
        
        params.append(job_id)
        
        cur.execute(
            f"""
            UPDATE public.project_renders
            SET {', '.join(update_fields)}
            WHERE id = %s
            RETURNING *;
            """,
            params
        )
        
        job = cur.fetchone()
        conn.commit()
        cur.close()
        
        if not job:
            return jsonify({"error": "Render job não encontrado"}), 404
        
        return jsonify({
            "success": True,
            "render": dict(job),
            "message": "Render job atualizado com sucesso"
        }), 200
    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erro ao atualizar render job: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


# ============================================
# NOVOS ENDPOINTS - Estrutura Limpa (03/Fev/2026)
# Ref: DIA-5-MIGRACAO-TEMPLATES
# ============================================

@video_templates_bp.route('/templates/for-llm', methods=['GET'])
def list_templates_for_llm():
    """
    Lista templates LIMPOS para uso por LLM.
    
    ✅ Retorna SÓ configs visuais (fontes, animações, layout)
    ❌ NÃO retorna conversation_id, project_id, script_data
    
    Query Parameters:
    - search: busca por nome ou descrição
    - tags: filtro por tags (comma-separated)
    - template_mode: filtro por modo
    - limit: limite de resultados (padrão: 20)
    
    Retorna:
    {
        "templates": [
            {
                "id": "uuid",
                "name": "Template Profissional",
                "description": "...",
                "tags": ["profissional", "corporativo"],
                "visual_config": {
                    "fonts": {...},
                    "animations": {...},
                    "layout": {...}
                },
                "demos": [
                    {
                        "video_url": "https://...",
                        "thumbnail_url": "https://...",
                        "is_featured": true
                    }
                ]
            }
        ],
        "total": 55
    }
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Query parameters
        search = request.args.get('search', '')
        tags = request.args.get('tags', '')
        template_mode = request.args.get('template_mode', '')
        limit = int(request.args.get('limit', 20))
        
        # Buscar templates limpos com demos
        query = """
            SELECT 
                vet.id,
                vet.name,
                vet.description,
                vet.tags,
                vet.template_mode,
                vet.is_active,
                vet.created_at,
                -- Configs visuais (SÓ estes são enviados)
                jsonb_build_object(
                    'fonts', jsonb_build_object(
                        'default', vet.ts_default_font,
                        'emphasis', vet.ts_emphasis_font,
                        'letter_effect', vet.ts_letter_effect_font,
                        'cartela', vet.ts_cartela_font
                    ),
                    'animations', jsonb_build_object(
                        'default', vet.ts_default_animation,
                        'emphasis', vet.ts_emphasis_animation,
                        'letter_effect', vet.ts_letter_effect_animation,
                        'cartela', vet.ts_cartela_animation
                    ),
                    'layout', vet.layout_spacing,
                    'creative_layout', vet.creative_layout,
                    'shadows', vet.shadow,
                    'z_index', vet.z_index_config,
                    'matting', vet.matting
                ) as visual_config,
                -- Buscar demos (subquery)
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'id', tdp.id,
                            'video_url', tdp.video_url,
                            'thumbnail_url', tdp.thumbnail_url,
                            'is_featured', tdp.is_featured,
                            'name', tdp.name,
                            'created_at', tdp.created_at
                        )
                        ORDER BY tdp.is_featured DESC, tdp.created_at DESC
                    )
                    FROM template_demo_projects tdp
                    WHERE tdp.template_id = vet.id
                ) as demos
            FROM video_editing_templates vet
            WHERE vet.is_active = true
        """
        
        # Filtros
        params = []
        if search:
            query += " AND (vet.name ILIKE %s OR vet.description ILIKE %s)"
            params.extend([f'%{search}%', f'%{search}%'])
        
        if tags:
            tag_list = [t.strip() for t in tags.split(',')]
            query += " AND vet.tags && %s"
            params.append(tag_list)
        
        if template_mode:
            query += " AND vet.template_mode->>'mode' = %s"
            params.append(template_mode)
        
        query += " ORDER BY vet.created_at DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        templates = cur.fetchall()
        cur.close()
        
        templates_list = [dict(t) for t in templates]
        
        print(f"✅ Retornando {len(templates_list)} templates limpos para LLM")
        
        return jsonify({
            "success": True,
            "templates": templates_list,
            "total": len(templates_list)
        }), 200
        
    except Exception as e:
        print(f"❌ Erro ao buscar templates para LLM: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/templates/<template_id>/demos', methods=['GET'])
def get_template_demos(template_id):
    """
    Busca demos de um template específico.
    
    Retorna todos os demos criados para este template (projetos de demonstração).
    
    Retorna:
    {
        "demos": [
            {
                "id": "uuid",
                "name": "Demo: Template Profissional",
                "video_url": "https://...",
                "thumbnail_url": "https://...",
                "script_data": {...},
                "is_featured": true,
                "created_at": "timestamp"
            }
        ],
        "total": 1
    }
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute(
            """
            SELECT 
                id,
                template_id,
                name,
                description,
                script_data,
                video_url,
                thumbnail_url,
                is_featured,
                display_order,
                render_duration_ms,
                video_duration_ms,
                file_size_bytes,
                created_by,
                created_at,
                updated_at
            FROM template_demo_projects
            WHERE template_id = %s
            ORDER BY is_featured DESC, display_order ASC, created_at DESC;
            """,
            (template_id,)
        )
        
        demos = cur.fetchall()
        cur.close()
        
        demos_list = [dict(d) for d in demos]
        
        print(f"✅ Retornando {len(demos_list)} demos do template {template_id}")
        
        return jsonify({
            "success": True,
            "demos": demos_list,
            "total": len(demos_list)
        }), 200
        
    except Exception as e:
        print(f"❌ Erro ao buscar demos do template {template_id}: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()


@video_templates_bp.route('/templates/<template_id>/featured-demo', methods=['PUT'])
def set_featured_demo(template_id):
    """
    Define qual demo é o principal (featured) de um template.
    
    Body:
    {
        "demo_id": "uuid"
    }
    
    Retorna:
    {
        "success": true,
        "message": "Demo principal atualizado"
    }
    """
    conn = None
    try:
        data = request.get_json()
        demo_id = data.get('demo_id')
        
        if not demo_id:
            return jsonify({"error": "demo_id é obrigatório"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Remover featured de todos os demos deste template
        cur.execute(
            """
            UPDATE template_demo_projects
            SET is_featured = false
            WHERE template_id = %s;
            """,
            (template_id,)
        )
        
        # 2. Marcar novo demo como featured
        cur.execute(
            """
            UPDATE template_demo_projects
            SET is_featured = true
            WHERE id = %s AND template_id = %s;
            """,
            (demo_id, template_id)
        )
        
        if cur.rowcount == 0:
            conn.rollback()
            cur.close()
            return jsonify({"error": "Demo não encontrado"}), 404
        
        conn.commit()
        cur.close()
        
        print(f"✅ Demo {demo_id} marcado como featured do template {template_id}")
        
        return jsonify({
            "success": True,
            "message": "Demo principal atualizado"
        }), 200
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Erro ao atualizar demo featured: {e}")
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()
