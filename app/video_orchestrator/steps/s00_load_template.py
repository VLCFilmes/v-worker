"""Step 00: Carrega template config e injeta no state."""

from ._base import *

@register_step(
    name="load_template",
    description="Carrega configuração do template (estilos, dimensões, animações)",
    category="setup",
    depends_on=[],
    produces=["template_config", "text_styles", "enabled_types", "video_width", "video_height"],
    estimated_duration_s=3,
    cost_category="free",
    retryable=True,
    max_retries=3,
    sse_step_name="TEMPLATE",
    tool_schema={
        "description": "Carrega template do banco (estilos, dimensões, fontes)",
        "input": {},
        "output": {"template_id": "str", "enabled_types": "list", "dimensions": "str"}
    }
)
def load_template_step(state: PipelineState, params: dict) -> PipelineState:
    from ..services.template_loader import TemplateLoaderService

    template_id = params.get('template_id') or state.template_id

    if not template_id:
        # Tentar buscar do projeto
        template_id = _get_template_id_for_project(state.project_id)

    if not template_id:
        # ═══ Motion Graphics: template é opcional ═══
        # Para motion_graphics, o LLM Director gera os visuais.
        # Template só forneceria palette/fonts/mood, mas não é obrigatório.
        if getattr(state, 'storytelling_mode', '') == 'motion_graphics':
            logger.info(f"🎨 [TEMPLATE] motion_graphics sem template → usando defaults (1080x1920)")
            default_config = {
                'project-settings': {
                    'video_settings': {
                        'width': 1080,
                        'height': 1920,
                    }
                },
                'multi-text-styling': {
                    'text_styles': {}
                },
                '_text_styles': {},
            }
            return state.with_updates(
                template_id='motion_graphics_default',
                template_config=default_config,
                text_styles={},
                enabled_types=['default'],
                video_width=1080,
                video_height=1920,
            )
        raise ValueError(f"Nenhum template_id encontrado para projeto {state.project_id}")

    loader = TemplateLoaderService()

    # Carregar com overrides se existirem
    overrides = state.options.get('overrides')
    if overrides:
        logger.info(f"🔧 [TEMPLATE] Aplicando overrides: {list(overrides.keys())}")
        template_config = loader.load_template_with_overrides(template_id, overrides)
    else:
        template_config = loader.load_template(template_id)

    if not template_config:
        raise ValueError(f"Template {template_id} não encontrado no banco")

    # Extrair dimensões do vídeo
    ps = template_config.get('project-settings', {})
    vs = ps.get('video_settings', {})
    height = extract_value(vs.get('height'))
    width = extract_value(vs.get('width'))

    if not height or not width:
        raise ValueError(
            f"Dimensões do vídeo não definidas no template! "
            f"height={height}, width={width}"
        )

    # Extrair enabled_types do multi-text-styling
    mts = template_config.get('multi-text-styling', {})
    text_styles_raw = mts.get('text_styles', {})
    enabled_types = []
    for st in ['default', 'emphasis', 'letter_effect']:
        sc = text_styles_raw.get(st, {})
        ec = sc.get('enabled', {})
        is_en = ec.get('value', True) if isinstance(ec, dict) else (ec if ec is not None else True)
        if is_en:
            enabled_types.append(st)

    if not enabled_types:
        enabled_types = ['default']

    # Carregar text_styles completos
    text_styles = loader.load_multi_text_styling(template_id)

    # Injetar _text_styles no template_config (compat com serviços existentes)
    template_config['_text_styles'] = text_styles

    logger.info(f"✅ [TEMPLATE] Carregado: {template_id[:8]}...")
    logger.info(f"   Dimensões template: {width}x{height}")
    logger.info(f"   Enabled types: {enabled_types}")
    logger.info(f"   Text styles: {list(text_styles.keys()) if text_styles else 'nenhum'}")

    # ═══ v4.5.0: Resolver canvas baseado na resolução do upload ═══
    # O template define o ESTILO, mas o canvas é determinado pelo upload.
    # Se upload_width/upload_height estão no state (definidos pelo bridge),
    # recalcular video_width/video_height para evitar upscale desnecessário.
    final_width = int(width)
    final_height = int(height)

    if state.upload_width > 0 and state.upload_height > 0:
        final_width, final_height = _resolve_canvas(
            upload_w=state.upload_width,
            upload_h=state.upload_height,
            template_w=int(width),
            template_h=int(height),
            target_aspect_ratio=state.target_aspect_ratio,
        )
        if final_width != int(width) or final_height != int(height):
            logger.info(
                f"📐 [CANVAS] Resolvido: template {width}x{height} → "
                f"projeto {final_width}x{final_height} "
                f"(upload={state.upload_width}x{state.upload_height}, "
                f"aspect={state.target_aspect_ratio or 'auto'})"
            )
        
        # Atualizar video_settings no template_config para que downstream
        # (subtitle_pipeline_service, render_service) usem as dimensões corretas
        if template_config.get('project-settings', {}).get('video_settings'):
            template_config['project-settings']['video_settings']['width'] = final_width
            template_config['project-settings']['video_settings']['height'] = final_height
    else:
        logger.info(f"   📐 [CANVAS] Usando dimensões do template (sem info de upload)")

    return state.with_updates(
        template_id=template_id,
        template_config=template_config,
        text_styles=text_styles,
        enabled_types=enabled_types,
        video_width=final_width,
        video_height=final_height,
    )


# ═══ v4.5.0: Regras de Resolução do Canvas ═══

# Tiers de resolução suportados
RESOLUTION_TIERS = {
    "9:16": [
        (720, 1280),    # HD
        (1080, 1920),   # Full HD
    ],
    "16:9": [
        (1280, 720),    # HD
        (1920, 1080),   # Full HD
    ],
    "1:1": [
        (720, 720),     # HD
        (1080, 1080),   # Full HD
    ],
}


def _detect_aspect_ratio(w: int, h: int) -> str:
    """Detecta o aspect ratio mais próximo de uma resolução."""
    if w == 0 or h == 0:
        return "9:16"
    ratio = w / h
    if ratio < 0.7:      # Muito vertical (< 0.7) → 9:16
        return "9:16"
    elif ratio > 1.4:     # Muito horizontal (> 1.4) → 16:9
        return "16:9"
    else:                 # Perto de 1.0 → 1:1
        return "1:1"


def _get_resolution_tier(w: int, h: int) -> str:
    """Retorna 'hd' ou 'fullhd' baseado no total de pixels."""
    pixels = w * h
    if pixels <= 1_000_000:  # ~921.600 (720x1280)
        return "hd"
    return "fullhd"


def _resolve_canvas(
    upload_w: int,
    upload_h: int,
    template_w: int,
    template_h: int,
    target_aspect_ratio: str = "",
) -> tuple:
    """
    Resolve a resolução final do canvas baseado no upload.
    
    Regras:
    1. Aspect ratio: usa target_aspect_ratio se definido, senão auto-detecta do upload
    2. Tier (HD/FHD): usa o menor entre upload e Full HD (nunca upscala)
    3. Retorna (width, height) do tier correspondente
    
    Exemplos:
    - Upload 720x1280, template 1080x1920 → 720x1280 (HD, não upscala)
    - Upload 1080x1920, template 1080x1920 → 1080x1920 (Full HD match)
    - Upload 1920x1080, template 1080x1920, aspect="16:9" → 1920x1080
    - Upload 4K, template 1080x1920 → 1080x1920 (cap em Full HD)
    """
    # 1. Determinar aspect ratio
    if target_aspect_ratio and target_aspect_ratio in RESOLUTION_TIERS:
        aspect = target_aspect_ratio
    else:
        aspect = _detect_aspect_ratio(upload_w, upload_h)
    
    tiers = RESOLUTION_TIERS.get(aspect, RESOLUTION_TIERS["9:16"])
    
    # 2. Determinar tier baseado na resolução do upload
    upload_tier = _get_resolution_tier(upload_w, upload_h)
    
    if upload_tier == "hd":
        # Upload é HD → projeto em HD (não upscalar)
        return tiers[0]
    else:
        # Upload é Full HD ou maior → projeto em Full HD (cap)
        return tiers[-1]


def _get_template_id_for_project(project_id: str) -> Optional[str]:
    """Busca template_id a partir do project_id (mesmo padrão do orchestrator legado)."""
    if not project_id:
        return None
    try:
        from app.supabase_client import get_direct_db_connection
        conn = get_direct_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT template_id FROM projects WHERE project_id = %s",
                    (str(project_id),)
                )
                row = cur.fetchone()
                if row:
                    val = row['template_id'] if isinstance(row, dict) else row[0]
                    if val:
                        logger.info(f"🎨 [TEMPLATE] Projeto {project_id} → template: {val}")
                        return str(val)
                logger.warning(f"⚠️ [TEMPLATE] Projeto {project_id} sem template_id no banco")
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"⚠️ Não conseguiu buscar template_id do projeto: {e}")
    return None
