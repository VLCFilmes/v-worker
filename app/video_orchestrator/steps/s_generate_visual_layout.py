"""
Step: generate_visual_layout — Gera layers visuais via LLM + Playwright.

Exclusivo do STM motion_graphics. Substitui os steps:
classify, generate_pngs, add_shadows, apply_animations,
calculate_positions, generate_backgrounds, cartelas.

Fluxo:
1. Lê scene_overrides (do format_script) + template_style
2. Chama v-llm-directors /render/full-pipeline (LLM gera HTML → Playwright renderiza PNGs)
3. PNGs são salvos diretamente no volume compartilhado (/app/shared)
4. Monta png_results com paths + timing compatíveis com v-editor-python
5. Passa para subtitle_pipeline → render (v-editor-python)
"""

from ._base import *

import requests
import time


V_LLM_DIRECTORS_URL = get_env('V_LLM_DIRECTORS_URL', 'http://v-llm-directors:5025')
FULL_PIPELINE_ENDPOINT = f"{V_LLM_DIRECTORS_URL}/render/full-pipeline"
LLM_DIRECTOR_TIMEOUT = int(get_env('LLM_DIRECTOR_TIMEOUT', '120'))

# Diretório base no volume compartilhado (montado em v-llm-directors e v-editor-python)
SHARED_VOLUME_BASE = "/app/shared/temp_frames"


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
    PNGs são salvos no volume compartilhado para acesso direto pelo v-editor-python.
    """
    start = time.time()

    # ─── Extrair contexto do state ───
    template_config = state.template_config or {}
    options = state.options or {}
    scene_overrides = getattr(state, "scene_overrides", None) or []
    script_text = state.transcription_text or ""

    # Canvas: prioridade → template → default
    project_settings = template_config.get("project-settings", template_config.get("project_settings", {}))
    video_settings = project_settings.get("video_settings", {})

    canvas_w = _get_value(video_settings, "width") or template_config.get("canvas_width", 720)
    canvas_h = _get_value(video_settings, "height") or template_config.get("canvas_height", 1280)
    fps = _get_value(video_settings, "fps") or 30
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

    # ─── Diretório de saída no volume compartilhado ───
    output_dir = f"{SHARED_VOLUME_BASE}/{state.job_id}/visual_layout"

    logger.info(f"🎨 [VISUAL_LAYOUT] Gerando layout visual...")
    logger.info(f"   Prompt: {user_prompt[:100]}...")
    logger.info(f"   Canvas: {canvas_w}x{canvas_h} @{fps}fps")
    logger.info(f"   Scenes: {len(scene_descriptions)}")
    logger.info(f"   Style: {template_style.get('mood', 'N/A')}")
    logger.info(f"   Output: {output_dir}")

    # ─── Chamar v-llm-directors /render/full-pipeline ───
    payload = {
        "user_prompt": user_prompt,
        "canvas": canvas,
        "template_style": template_style,
        "script_text": script_text,
        "scene_descriptions": scene_descriptions,
        "timestamps": timestamps,
        "output_dir": output_dir,
        "render_animations": True,
        "fps": fps,
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

    # ─── Calcular timing das cenas ───
    duration_ms = getattr(state, "duration_ms", None) or 0
    if not duration_ms and timestamps:
        # Calcular duração total a partir dos timestamps
        # Timestamps podem estar em segundos (ex: 17.3) ou ms (ex: 17300)
        max_end = max(t.get("end", 0) for t in timestamps)
        # Se < 1000, provavelmente em segundos → converter para ms
        duration_ms = max_end * 1000 if max_end < 1000 else max_end

    scene_timings = _compute_scene_timings(
        rendered_scenes, scene_overrides, timestamps, duration_ms
    )

    # ─── Montar png_results com paths e timing ───
    layers_list = _build_png_results_with_paths(
        rendered_scenes, scene_timings, canvas_w, canvas_h, fps
    )

    # Wrap em dict compatível com subtitle_pipeline_service
    png_results = {
        "status": "success",
        "sentences": [],
        "positioned_sentences": [],
        "backgrounds": [],
        "motion_graphics": layers_list,
        "total_pngs": len(layers_list),
        "phrases": [],
        "source": "visual_layout_director",
    }

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


def _get_value(settings_dict: Dict, key: str) -> Any:
    """Extrai valor de formato {value: x} ou valor direto."""
    raw = settings_dict.get(key)
    if isinstance(raw, dict):
        return raw.get("value")
    return raw


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


def _compute_scene_timings(
    rendered_scenes: List[Dict],
    scene_overrides: List[Dict],
    timestamps: List[Dict],
    total_duration_ms: float,
) -> List[Dict]:
    """
    Calcula start_ms e end_ms para cada cena.

    Estratégia:
    1. Se temos timestamps, divide proporcionalmente pela quantidade de texto.
    2. Senão, divide igualmente pela quantidade de cenas.
    """
    num_scenes = len(rendered_scenes)
    if num_scenes == 0:
        return []

    if total_duration_ms <= 0:
        # Fallback: 5 segundos por cena
        total_duration_ms = num_scenes * 5000

    # Divisão igual por cena (v1 simplificada)
    scene_duration_ms = total_duration_ms / num_scenes
    timings = []

    for i in range(num_scenes):
        timings.append({
            "scene_index": i,
            "start_ms": round(i * scene_duration_ms),
            "end_ms": round((i + 1) * scene_duration_ms),
        })

    logger.info(f"   ⏱️ Scene timings: {num_scenes} cenas, "
                f"~{scene_duration_ms:.0f}ms cada, total={total_duration_ms:.0f}ms")

    return timings


def _build_png_results_with_paths(
    rendered_scenes: List[Dict],
    scene_timings: List[Dict],
    canvas_w: int,
    canvas_h: int,
    fps: int,
) -> List[Dict]:
    """
    Converte rendered_scenes em motion_graphics list para v-editor-python.

    Cada layer gera um entry compatível com process_motion_graphics():
    - src: path no volume compartilhado (png_path do v-llm-directors)
    - start_time / end_time: em milissegundos
    - position: {x, y, width, height}
    - animation_sequence: frames animados (se existirem)
    """
    results = []
    layer_index = 0

    for scene_idx, scene in enumerate(rendered_scenes):
        scene_id = scene.get("scene_id", f"scene_{scene_idx:02d}")

        # Timing da cena
        timing = scene_timings[scene_idx] if scene_idx < len(scene_timings) else {
            "start_ms": 0, "end_ms": 5000
        }
        scene_start_ms = timing["start_ms"]
        scene_end_ms = timing["end_ms"]

        for layer in scene.get("layers", []):
            layer_id = layer.get("id", f"layer_{layer_index}")
            position = layer.get("position", {"x": 0, "y": 0})
            animation = layer.get("animation")
            animation_sequence = layer.get("animation_sequence")

            # Obter src (path no volume compartilhado)
            src = layer.get("png_path") or layer.get("png_base64")

            if not src:
                logger.warning(f"   ⚠️ Layer {layer_id} sem png_path nem png_base64, pulando")
                layer_index += 1
                continue

            # Se recebemos base64 (fallback), logar aviso
            is_path = not src.startswith("/9j/") and not src.startswith("iVBOR")
            if not is_path:
                logger.warning(
                    f"   ⚠️ Layer {layer_id} retornou base64 ao invés de path. "
                    f"Verifique se o volume compartilhado está montado em v-llm-directors."
                )
                layer_index += 1
                continue

            entry = {
                "id": layer_id,
                "scene_id": scene_id,
                "type": layer.get("type", "motion_graphic"),
                "src": src,
                "start_time": scene_start_ms,
                "end_time": scene_end_ms,
                "position": {
                    "x": position.get("x", 0),
                    "y": position.get("y", 0),
                    "width": layer.get("width", canvas_w),
                    "height": layer.get("height", canvas_h),
                },
                "zIndex": layer.get("z_index", 2200 + layer_index),
                "is_static": layer.get("is_static", True),
                "description": layer.get("description", ""),
                "source": "visual_layout_director",
            }

            # Animação CSS (metadata para futuro)
            if animation:
                entry["animation"] = animation

            # PNG sequence animada (para v-editor-python)
            if animation_sequence:
                entry["animation_sequence"] = animation_sequence
                entry["is_static"] = False
                # Duração da animação pode ser mais curta que a cena
                anim_duration_ms = animation_sequence.get("duration_ms", 0)
                if anim_duration_ms > 0:
                    entry["anim_duration_ms"] = anim_duration_ms

            results.append(entry)
            layer_index += 1

            logger.info(
                f"   📦 MG #{layer_index - 1}: {layer_id} | "
                f"src={src[-50:]} | "
                f"time={scene_start_ms}-{scene_end_ms}ms | "
                f"static={entry['is_static']} | "
                f"anim_seq={'yes' if animation_sequence else 'no'}"
            )

        # Stroke reveals (masks animados)
        for stroke in scene.get("stroke_reveals", []):
            stroke_id = stroke.get("id", f"stroke_{layer_index}")
            results.append({
                "id": stroke_id,
                "scene_id": scene_id,
                "type": "stroke_reveal",
                "start_time": scene_start_ms,
                "end_time": scene_end_ms,
                "zIndex": 350,
                "is_static": False,
                "source": "visual_layout_director",
                "hq_png_base64": stroke.get("hq_png_base64"),
                "masks": stroke.get("masks", []),
                "reveal": stroke.get("reveal", {}),
                "total_frames": stroke.get("total_frames", 0),
                "fps": stroke.get("fps", 30),
            })
            layer_index += 1

    logger.info(f"   📦 motion_graphics total: {len(results)} entries")
    return results
