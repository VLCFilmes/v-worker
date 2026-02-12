"""
Step: Title Generation — Gera PNG do título e monta track items.

Lê o título do roteiro editado (prioridade) ou do title_director_result
e gera PNG(s) que serão renderizados no vídeo como overlay.

Usa o MESMO endpoint de legendas: POST /png-subtitles/generate_subtitles
com o mesmo payload format (words, text_style, text_border_config, etc.).

O estilo (fonte, cor, borda, gradiente) vem do Title Director (LLM) via
png_style no style_suggestion, e pode ser editado pelo usuário no roteiro.

O título aparece nos primeiros 3-5 segundos do vídeo, acima de tudo (z=3500).

Fluxo:
  subtitle_pipeline [s17]
  ├── title_generation (opcional, roda após subtitle_pipeline)
  └── render [s18] (injeta tracks['titles'] = state.title_track)
"""

from ._base import *

@register_step(
    name="title_generation",
    description="Gera PNG do título do vídeo e monta track items",
    category="creative",
    depends_on=["subtitle_pipeline"],
    produces=["title_track"],
    optional=True,
    estimated_duration_s=5,
    cost_category="cpu",
    retryable=True,
    max_retries=1,
    timeout_s=30,
    sse_step_name="TITLE_GENERATION",
    tool_schema={
        "description": "Gera PNG do título e posiciona como overlay no vídeo",
        "input": {"force_regenerate": "bool?"},
        "output": {"has_title": "bool", "status": "str"}
    }
)
def title_generation_step(state: PipelineState, params: dict) -> PipelineState:
    """
    Gera track de título para o vídeo.

    1. Lê título do roteiro editado ou do title_director_result
    2. Aplica title_overrides do state (para replay/alterações)
    3. Gera PNG(s) via v-services (ou PngGeneratorService)
    4. Posiciona (center_top default)
    5. Monta track items com timing + animation
    """
    import json

    # ═══ 1. Buscar título ═══
    title_data = _get_title_data(state.project_id)

    if not title_data:
        logger.info("⏭️ [TITLE_GEN] Sem título configurado, pulando")
        return state

    # ═══ 1.5. Aplicar overrides do state (replay/alterações) ═══
    overrides = state.title_overrides or {}
    if overrides:
        logger.info(f"🔄 [TITLE_GEN] Aplicando {len(overrides)} override(s) do replay")
        # Override de texto
        if 'line_1' in overrides:
            title_data['line_1'] = overrides['line_1']
        if 'line_2' in overrides:
            title_data['line_2'] = overrides['line_2']
        if 'highlight_words' in overrides:
            title_data['highlight_words'] = overrides['highlight_words']
        # Override de posição/timing
        style = title_data.get('style_suggestion', {})
        if 'position' in overrides:
            style['position'] = overrides['position']
        if 'timing_start_ms' in overrides:
            style['timing_start_ms'] = overrides['timing_start_ms']
        if 'timing_end_ms' in overrides:
            style['timing_end_ms'] = overrides['timing_end_ms']
        if 'animation' in overrides:
            style['animation'] = overrides['animation']
        # Override de estilo PNG (cor, fonte, borda, etc.)
        if 'png_style' in overrides:
            existing_png_style = style.get('png_style', {})
            existing_png_style.update(overrides['png_style'])
            style['png_style'] = existing_png_style
        title_data['style_suggestion'] = style

    line_1 = title_data.get('line_1', '').strip()
    line_2 = title_data.get('line_2', '').strip()

    if not line_1:
        logger.info("⏭️ [TITLE_GEN] Título vazio (line_1), pulando")
        return state

    logger.info(
        f"🏷️ [TITLE_GEN] Gerando título: \"{line_1}\" / \"{line_2}\""
        f"{' (com overrides)' if overrides else ''}"
    )

    # ═══ 2. Gerar PNG(s) do título ═══
    style = title_data.get('style_suggestion', {})
    highlight_words = title_data.get('highlight_words', [])

    png_results = _generate_title_pngs(
        state=state,
        line_1=line_1,
        line_2=line_2,
        highlight_words=highlight_words,
        style_suggestion=style,  # Passa estilo JÁ com overrides aplicados
    )

    if not png_results:
        logger.warning("⚠️ [TITLE_GEN] Falha ao gerar PNGs do título")
        return state

    # ═══ 3. Montar track items ═══
    timing_start = style.get('timing_start_ms', 0)
    timing_end = style.get('timing_end_ms', 4000)
    animation = style.get('animation', 'fade_in_up')
    position = style.get('position', 'center_top')

    canvas_w = state.video_width or 1080
    canvas_h = state.video_height or 1920

    # Nota: v-services já gera os PNGs com largura máxima correta
    # (render_mode='full_phrase' + max_width). O clamp aqui é apenas safety net.

    title_items = []

    for i, png in enumerate(png_results):
        png_url = png.get('url', '')
        png_width = png.get('width', 0)
        png_height = png.get('height', 0)

        if not png_url:
            continue

        # Calcular posição
        pos = _calculate_position(
            position, canvas_w, canvas_h,
            png_width, png_height, line_index=i
        )

        item = {
            'id': f'title_{i}',
            'type': 'title',
            'src': png_url,
            'start_time': timing_start,
            'end_time': timing_end,
            'position': pos,
            'zIndex': 3500 + i,
            'animation': {
                'entry': animation,
                'exit': 'fade_out',
                'entry_duration_ms': 500,
                'exit_duration_ms': 300,
            },
        }
        title_items.append(item)

    if title_items:
        logger.info(
            f"✅ [TITLE_GEN] {len(title_items)} PNG(s) de título gerados | "
            f"timing={timing_start}-{timing_end}ms | "
            f"position={position}"
        )
    else:
        logger.warning("⚠️ [TITLE_GEN] Nenhum track item gerado")

    return state.with_updates(title_track=title_items if title_items else None)


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════


def _get_title_data(project_id: str) -> dict:
    """
    Busca dados do título (prioridade: roteiro editado > title_director_result).

    Returns:
        Dict com line_1, line_2, highlight_words, style_suggestion
    """
    import json

    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Tentar do content_documents (roteiro editado pelo usuário)
        cursor.execute("""
            SELECT content_data
            FROM content_documents
            WHERE project_id = %s
              AND document_type = 'script'
            ORDER BY updated_at DESC NULLS LAST, created_at DESC
            LIMIT 1
        """, (project_id,))
        doc_row = cursor.fetchone()

        if doc_row and doc_row[0]:
            content_data = doc_row[0] if isinstance(doc_row[0], dict) else json.loads(doc_row[0])
            rows = content_data.get('rows', [])
            for row in rows:
                if row.get('type') == 'title_suggestion':
                    # png_style pode estar no pngStyle (editado) ou dentro de styleSuggestion
                    style_suggestion = row.get('styleSuggestion', {})
                    png_style = row.get('pngStyle', style_suggestion.get('png_style', {}))

                    title_data = {
                        'line_1': row.get('titleLine1', ''),
                        'line_2': row.get('titleLine2', ''),
                        'highlight_words': row.get('highlightWords', []),
                        'style_suggestion': {
                            **style_suggestion,
                            'png_style': png_style,
                        },
                    }
                    cursor.close()
                    conn.close()
                    logger.info(f"🏷️ [TITLE_GEN] Título do roteiro editado")
                    return title_data

        # 2. Tentar do title_director_result
        cursor.execute("""
            SELECT project_config->'title_director_result'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        config_row = cursor.fetchone()
        cursor.close()
        conn.close()

        if config_row and config_row[0]:
            result = config_row[0] if isinstance(config_row[0], dict) else json.loads(config_row[0])
            if result.get('status') == 'success' and result.get('title'):
                title = result['title']
                return {
                    'line_1': title.get('line_1', ''),
                    'line_2': title.get('line_2', ''),
                    'highlight_words': title.get('highlight_words', []),
                    'style_suggestion': result.get('style_suggestion', {}),
                }

        return {}

    except Exception as e:
        logger.warning(f"⚠️ [TITLE_GEN] Erro ao buscar título: {e}")
        return {}


def _generate_title_pngs(
    state: PipelineState,
    line_1: str,
    line_2: str,
    highlight_words: list,
    style_suggestion: dict = None,
) -> list:
    """
    Gera PNG(s) do título usando o MESMO endpoint de legendas:
    POST /png-subtitles/generate_subtitles

    Usa o mesmo payload format que o PngGeneratorService já usa.
    Cada linha do título vira um "word" no payload (modo frase inteira).

    O estilo vem do parâmetro style_suggestion (que já inclui overrides
    do replay), ou fallback para o estilo do DB/defaults.

    Args:
        style_suggestion: Dict com png_style já resolvido (inclui overrides).
                          Se None, busca do DB como fallback.

    Returns:
        Lista de dicts com {url, width, height, line_index}
    """
    import requests

    v_services_url = get_env('V_SERVICES_URL', 'http://v-services:5000')
    endpoint = f'{v_services_url}/png-subtitles/generate_subtitles'
    video_height = state.video_height or 1920
    video_width = state.video_width or 1080

    # Largura máxima para títulos: 90% do canvas (margem de segurança)
    MAX_TITLE_WIDTH_PERCENT = 0.90
    max_title_width = int(video_width * MAX_TITLE_WIDTH_PERCENT)

    # ─── Resolver estilo: parâmetro (com overrides) > DB > defaults ───
    if style_suggestion is None:
        logger.info("🔄 [TITLE_PNG] style_suggestion não recebido, buscando do DB (fallback)")
        title_data = _get_title_data(state.project_id)
        style_suggestion = title_data.get('style_suggestion', {})
    else:
        logger.info(f"🎨 [TITLE_PNG] Usando style_suggestion recebido (com possíveis overrides): {list(style_suggestion.keys())}")
    png_style = style_suggestion.get('png_style', {})
    if png_style:
        logger.info(f"🎨 [TITLE_PNG] png_style keys: {list(png_style.keys())}")

    # ─── Defaults para título (se não tiver estilo customizado) ───
    # Default: texto preto, borda branca grossa (30), bg de frase
    # Título usa ~5% do video_height para line_1 e ~3.5% para line_2
    default_font_family = 'Poppins:style=Black'
    default_text_style = {
        'render_type': 'solid',
        'solid_color_rgb': '0,0,0',
    }
    default_border_config = {
        'line_join': 'round',
        'border_1_inner': {
            'enabled': True,
            'thickness_value': 30,
            'thickness_unit': 'percent_font',
            'color_rgb': '255,255,255',
            'blur_radius': 0,
        },
    }

    # ─── Montar words (cada linha do título = 1 "word" no payload) ───
    words = []
    lines_data = [(line_1, 0)]
    if line_2:
        lines_data.append((line_2, 1))

    for line_text, line_idx in lines_data:
        if not line_text.strip():
            continue

        is_main_line = (line_idx == 0)

        # Resolver estilos: custom do LLM > defaults
        font_family = (
            png_style.get('fontFamily')
            or default_font_family
        )
        size = (
            png_style.get('size_line1' if is_main_line else 'size_line2')
            or png_style.get('size')
            or ('5%' if is_main_line else '3.5%')
        )
        uppercase = png_style.get('uppercase', True)
        padding_x = png_style.get('padding_x', 30 if is_main_line else 20)
        padding_y = png_style.get('padding_y', 15 if is_main_line else 10)

        # text_style (cor / gradiente)
        text_style = png_style.get('text_style', default_text_style)
        # Suporte a cor simples: se vier "color_rgb": "255,255,0" direto
        if 'color_rgb' in text_style and 'render_type' not in text_style:
            text_style = {
                'render_type': 'solid',
                'solid_color_rgb': text_style['color_rgb'],
            }

        # border config
        border_config = png_style.get('text_border_config', default_border_config)

        # highlight (se palavra está na highlight_words)
        has_highlight = (
            is_main_line
            and highlight_words
            and png_style.get('highlight_text_style')
        )

        word_payload = {
            'text': line_text.strip(),
            'fontFamily': font_family,
            'size': size,
            'padding_x': padding_x,
            'padding_y': padding_y,
            'uppercase': uppercase,
            'is_highlight': False,
            'letter_by_letter': False,
            'text_style': text_style,
            'text_border_config': border_config,
            'quality': 100,
            'dpi': 300,
            'line_join': border_config.get('line_join', 'round'),
            'start_time': 0,
            'end_time': 0,
            'word_index': line_idx,
            'phrase_info': {
                'index': 0,
                'text': f'{line_1} {line_2}'.strip(),
            },
            'style_type': 'title',
            # ═══ Controle de largura máxima (v-services auto-scale) ═══
            # render_mode='full_phrase' → frase inteira, v-services reduz
            # fonte se exceder max_width. Diferente de 'word' (legendas),
            # onde cada palavra é um PNG separado e o posicionamento faz wrap.
            'render_mode': 'full_phrase',
            'max_width': max_title_width,
        }
        words.append(word_payload)

        # Se tiver highlight, gerar versão highlight separada (para karaoke)
        if has_highlight:
            hl_word = {**word_payload}
            hl_word['is_highlight'] = True
            hl_word['highlight_text_style'] = png_style['highlight_text_style']
            if png_style.get('highlight_border_config'):
                hl_word['highlight_border_config'] = png_style['highlight_border_config']
            words.append(hl_word)

    if not words:
        logger.warning("⚠️ [TITLE_GEN] Sem words para gerar PNGs")
        return []

    # ─── Chamar v-services ───
    payload = {
        'words': words,
        'video_height': video_height,
    }

    logger.info(
        f"🏷️ [TITLE_GEN] Enviando {len(words)} word(s) para "
        f"png-subtitles/generate_subtitles | video_height={video_height} | "
        f"video_width={video_width} | max_title_width={max_title_width}"
    )
    # Debug: confirmar que max_width e render_mode estão no payload
    for w in words:
        logger.info(
            f"   📐 word[{w.get('word_index')}]: render_mode={w.get('render_mode')}, "
            f"max_width={w.get('max_width')}, size={w.get('size')}"
        )

    try:
        resp = requests.post(endpoint, json=payload, timeout=30)

        if resp.status_code != 200:
            logger.error(
                f"❌ [TITLE_GEN] v-services retornou {resp.status_code}: "
                f"{resp.text[:300]}"
            )
            return []

        data = resp.json()

        if data.get('status') != 'success':
            logger.error(
                f"❌ [TITLE_GEN] v-services status: {data.get('status')} | "
                f"{data.get('error', 'unknown')}"
            )
            return []

        # Processar response (mesmo formato das legendas)
        result_words = data.get('words', [])
        results = []

        for w in result_words:
            # Pular highlights (usamos só o base por enquanto)
            if w.get('is_highlight', False):
                continue

            url = w.get('url', '')
            if not url:
                continue

            results.append({
                'url': url,
                'width': w.get('scaled_width', w.get('width', 0)),
                'height': w.get('scaled_height', w.get('height', 0)),
                'anchor_x': w.get('anchor_x', 0),
                'anchor_y': w.get('anchor_y', 0),
                'line_index': w.get('word_index', 0),
                'text': w.get('text', ''),
            })

        logger.info(
            f"✅ [TITLE_GEN] v-services gerou {len(results)} PNG(s) | "
            f"total_images={data.get('metadata', {}).get('total_images', '?')}"
        )

        return results

    except Exception as e:
        logger.error(f"❌ [TITLE_GEN] Erro ao chamar v-services: {e}")
        return []


def _calculate_position(
    position_hint: str,
    canvas_w: int,
    canvas_h: int,
    png_width: int,
    png_height: int,
    line_index: int = 0,
) -> dict:
    """
    Calcula posição X,Y do título no canvas.

    IMPORTANTE: v-editor-python espera coordenadas de CENTRO para layers
    que não são background (subtitles, highlights, titles).
    Ou seja, position.x e position.y = centro do elemento em pixels.

    O título é centralizado horizontalmente.
    Verticalmente, segue o position_hint:
      - "center_top": parte superior com margem de safe area (~8% do topo)
        Num vídeo 1080x1920, isso dá ~154px do topo — fora da barra de status
        de celulares e UI do Instagram/TikTok.
      - "center": centralizado verticalmente
      - "bottom_third": terço inferior (~65% do topo)

    Para múltiplas linhas: line_2 fica logo abaixo de line_1 com espaçamento.
    """
    w = png_width or 100
    h = png_height or 100

    # ─── Centro horizontal: sempre no meio do canvas ───
    center_x = canvas_w // 2

    # ─── Posição vertical (top-left da bounding box) ───
    # Safe area: Instagram/TikTok cobrem ~5% no topo (barra de status, nome do usuário)
    # Usamos 8% como margem segura para o título ficar visível em todas as plataformas
    TOP_SAFE_MARGIN = 0.08  # 8% do canvas height

    if position_hint == 'center_top':
        # top-left Y = safe margin do topo
        top_y = int(canvas_h * TOP_SAFE_MARGIN)
    elif position_hint == 'center':
        # Centralizado vertical (para text_video/narração sem pessoa na tela)
        total_h = h
        if line_index == 0:
            total_h = int(h * 2.5)  # Estimativa generosa (line1 + line2 + spacing)
        top_y = (canvas_h - total_h) // 2
    elif position_hint == 'bottom_third':
        top_y = int(canvas_h * 0.65)
    else:
        top_y = int(canvas_h * TOP_SAFE_MARGIN)

    # ─── Offset para linhas múltiplas (line_2 abaixo de line_1) ───
    line_spacing = 12  # px entre linhas
    top_y = top_y + (line_index * (h + line_spacing))

    # ─── Converter top-left para CENTRO (formato que v-editor-python espera) ───
    center_y = top_y + (h // 2)

    return {
        'x': center_x,
        'y': center_y,
        'width': w,
        'height': h,
    }
