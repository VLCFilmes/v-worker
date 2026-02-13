"""
Step: generate_visual_layout — Gera layers visuais via LLM + Playwright.

Exclusivo do STM motion_graphics. Substitui os steps:
classify, generate_pngs, add_shadows, apply_animations,
calculate_positions, generate_backgrounds, cartelas.

Fluxo:
1. Lê scene_overrides (do format_script) + template_style
2. Chama v-llm-directors /render/full-pipeline (LLM gera HTML → Playwright renderiza PNGs)
3. Salva PNGs no B2 storage
4. Monta png_results + phrase_groups com metadata de animação
5. Passa para subtitle_pipeline → render
"""

from ._base import *

import json
import base64
import requests
import time


V_LLM_DIRECTORS_URL = get_env('V_LLM_DIRECTORS_URL', 'http://v-llm-directors:5025')
FULL_PIPELINE_ENDPOINT = f"{V_LLM_DIRECTORS_URL}/render/full-pipeline"
LLM_DIRECTOR_TIMEOUT = int(get_env('LLM_DIRECTOR_TIMEOUT', '120'))


@register_step(
    name="generate_visual_layout",
    description="Gera layouts visuais via LLM (HTML/CSS → PNGs transparentes)",
    category="rendering",
    depends_on=["fraseamento"],
    produces=["visual_layout_result", "png_results"],
    estimated_duration_s=30,
    cost_category="llm",
    retryable=True,
    sse_step_name="VISUAL_LAYOUT",
    tool_schema={
        "description": "Gera motion graphics visuais via LLM + Playwright renderer",
        "input": {"user_prompt": "str", "scene_descriptions": "list"},
        "output": {"scenes": "int", "total_layers": "int", "llm_cost": "dict"},
    },
)
def generate_visual_layout_step(state: PipelineState, params: dict) -> PipelineState:
    """
    Step principal do STM motion_graphics.
    Chama v-llm-directors para gerar HTML/CSS e renderizar PNGs.
    """
    start = time.time()

    # ─── Extrair contexto do state ───
    template_config = state.template_config or {}
    options = state.options or {}
    scene_overrides = getattr(state, "scene_overrides", None) or []
    script_text = state.transcription_text or ""

    # Canvas: prioridade → template → default
    canvas_w = template_config.get("canvas_width", 720)
    canvas_h = template_config.get("canvas_height", 1280)
    canvas = {"width": canvas_w, "height": canvas_h}

    # Template style (cores, fontes, mood)
    template_style = _extract_template_style(template_config)

    # Prompt do usuário (pode vir das options ou do motion_graphics_prompt)
    user_prompt = (
        options.get("motion_graphics_prompt")
        or options.get("user_prompt")
        or "Crie um layout visual moderno e bonito para cada cena do roteiro."
    )

    # Scene descriptions (do format_script)
    scene_descriptions = []
    if scene_overrides:
        for i, scene in enumerate(scene_overrides):
            scene_descriptions.append({
                "scene_id": f"scene_{i + 1:02d}",
                "text": scene.get("text", ""),
                "visual_hint": scene.get("visual_hint", ""),
                "cartela_override": scene.get("cartela_override", {}),
                "background_override": scene.get("background_override", {}),
            })

    # Timestamps (se disponíveis, para sync de animações)
    timestamps = []
    if state.transcription_words:
        timestamps = [
            {
                "text": w.get("word", w.get("text", "")),
                "start": w.get("start", w.get("start_time", 0)),
                "end": w.get("end", w.get("end_time", 0)),
            }
            for w in (state.transcription_words or [])[:50]  # Limitar
        ]

    logger.info(f"🎨 [VISUAL_LAYOUT] Gerando layout visual...")
    logger.info(f"   Prompt: {user_prompt[:100]}...")
    logger.info(f"   Canvas: {canvas_w}x{canvas_h}")
    logger.info(f"   Scenes: {len(scene_descriptions)}")
    logger.info(f"   Style: {template_style.get('mood', 'N/A')}")

    # ─── Chamar v-llm-directors /render/full-pipeline ───
    payload = {
        "user_prompt": user_prompt,
        "canvas": canvas,
        "template_style": template_style,
        "script_text": script_text,
        "scene_descriptions": scene_descriptions,
        "timestamps": timestamps,
    }

    try:
        logger.info(f"🌐 Chamando v-llm-directors: {FULL_PIPELINE_ENDPOINT}")
        response = requests.post(
            FULL_PIPELINE_ENDPOINT,
            json=payload,
            timeout=LLM_DIRECTOR_TIMEOUT,
        )

        if response.status_code != 200:
            logger.error(
                f"❌ [VISUAL_LAYOUT] HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )
            return state.with_updates(
                error_message=f"Visual Layout Director returned HTTP {response.status_code}",
            )

        result = response.json()

        if result.get("status") != "success":
            error_msg = result.get("error", "Unknown error")
            logger.error(f"❌ [VISUAL_LAYOUT] Erro: {error_msg}")
            return state.with_updates(error_message=f"Visual Layout Director: {error_msg}")

    except requests.exceptions.Timeout:
        logger.error(f"❌ [VISUAL_LAYOUT] Timeout ({LLM_DIRECTOR_TIMEOUT}s)")
        return state.with_updates(
            error_message=f"Visual Layout Director timeout after {LLM_DIRECTOR_TIMEOUT}s",
        )
    except Exception as e:
        logger.error(f"❌ [VISUAL_LAYOUT] Erro de conexão: {e}", exc_info=True)
        return state.with_updates(error_message=f"Visual Layout Director: {e}")

    # ─── Processar resultado ───
    rendered_scenes = result.get("rendered_scenes", [])
    llm_result = result.get("llm_result", {})
    llm_usage = result.get("llm_usage", {})

    total_layers = sum(len(s.get("layers", [])) for s in rendered_scenes)
    total_strokes = sum(len(s.get("stroke_reveals", [])) for s in rendered_scenes)

    logger.info(
        f"✅ [VISUAL_LAYOUT] Resultado: "
        f"{len(rendered_scenes)} cenas, {total_layers} layers, "
        f"{total_strokes} strokes"
    )

    if llm_usage:
        logger.info(
            f"💰 [VISUAL_LAYOUT] LLM: {llm_usage.get('total_tokens', 0)} tokens "
            f"(model: {llm_usage.get('model', 'N/A')})"
        )

    # ─── Upload PNGs para B2 e montar png_results ───
    png_results = _upload_and_build_png_results(
        rendered_scenes, state, canvas_w, canvas_h
    )

    elapsed = time.time() - start
    logger.info(f"⏱️ [VISUAL_LAYOUT] Concluído em {elapsed:.1f}s")

    # ─── Atualizar state ───
    return state.with_updates(
        png_results=png_results,
        visual_layout_result={
            "scenes": [s.get("scene_id") for s in rendered_scenes],
            "total_layers": total_layers,
            "total_strokes": total_strokes,
            "llm_usage": llm_usage,
            "llm_reasoning": llm_result.get("reasoning", ""),
            "elapsed_s": round(elapsed, 1),
        },
    )


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════


def _extract_template_style(template_config: Dict) -> Dict:
    """Extrai cores, fontes e mood do template_config para o Director."""
    style = {}

    # Cores
    colors = {}
    tc = template_config
    if tc.get("primary_color"):
        colors["primary"] = tc["primary_color"]
    if tc.get("secondary_color"):
        colors["secondary"] = tc["secondary_color"]
    if tc.get("accent_color"):
        colors["accent"] = tc["accent_color"]
    if tc.get("background_color"):
        colors["background"] = tc["background_color"]
    if tc.get("text_color"):
        colors["text"] = tc["text_color"]
    style["colors"] = colors or {
        "primary": "#00e5ff",
        "secondary": "#7c3aed",
        "text": "#ffffff",
        "background": "#0a0a2e",
    }

    # Fontes
    fonts = {}
    if tc.get("title_font"):
        fonts["title"] = tc["title_font"]
    if tc.get("body_font"):
        fonts["body"] = tc["body_font"]
    style["fonts"] = fonts or {"title": "Space Grotesk", "body": "Inter"}

    # Mood
    style["mood"] = tc.get("mood", tc.get("style", "modern, clean, professional"))

    return style


def _upload_and_build_png_results(
    rendered_scenes: List[Dict],
    state: PipelineState,
    canvas_w: int,
    canvas_h: int,
) -> List[Dict]:
    """
    Converte rendered_scenes em png_results compatível com subtitle_pipeline.

    Para a Fase 1, salvamos PNGs como base64 no state (sem upload B2).
    O upload B2 será adicionado quando integrarmos com o render final.
    """
    png_results = []
    layer_index = 0

    for scene in rendered_scenes:
        scene_id = scene.get("scene_id", "unknown")

        for layer in scene.get("layers", []):
            layer_id = layer.get("id", f"layer_{layer_index}")
            animation = layer.get("animation")

            png_entry = {
                "phrase_index": layer_index,
                "layer_id": layer_id,
                "scene_id": scene_id,
                "type": layer.get("type", "unknown"),
                "description": layer.get("description", ""),
                "z_index": layer.get("z_index", 100),
                "is_static": layer.get("is_static", True),
                "width": layer.get("width", canvas_w),
                "height": layer.get("height", canvas_h),
                "position": layer.get("position", {"x": 0, "y": 0}),
                "anchor_point": layer.get("anchor_point", {"x": canvas_w / 2, "y": canvas_h / 2}),
                "source": "visual_layout_director",
            }

            # PNG data (base64 ou path)
            if layer.get("png_base64"):
                png_entry["png_base64"] = layer["png_base64"]
            elif layer.get("png_path"):
                png_entry["png_path"] = layer["png_path"]

            # Animation metadata (para v-editor-python)
            if animation:
                png_entry["animation"] = animation

            png_results.append(png_entry)
            layer_index += 1

        # Stroke reveals (masks)
        for stroke in scene.get("stroke_reveals", []):
            png_entry = {
                "phrase_index": layer_index,
                "layer_id": stroke.get("id", f"stroke_{layer_index}"),
                "scene_id": scene_id,
                "type": "stroke_reveal",
                "z_index": 350,
                "is_static": False,
                "source": "visual_layout_director",
                "hq_png_base64": stroke.get("hq_png_base64"),
                "masks": stroke.get("masks", []),
                "reveal": stroke.get("reveal", {}),
                "total_frames": stroke.get("total_frames", 0),
                "fps": stroke.get("fps", 30),
            }
            png_results.append(png_entry)
            layer_index += 1

    logger.info(f"   📦 png_results: {len(png_results)} entries")
    return png_results
