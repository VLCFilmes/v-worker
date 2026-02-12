"""Step 14: Motion Graphics (planejamento LLM + renderização Manim)."""

from ._base import *

@register_step(
    name="motion_graphics",
    description="Planeja (via LLM) e renderiza motion graphics com Manim",
    category="rendering",
    depends_on=["calculate_positions"],
    produces=["motion_graphics_plan", "motion_graphics_rendered", "png_results"],
    optional=True,
    estimated_duration_s=60,
    cost_category="llm",
    retryable=True,
    max_retries=2,
    timeout_s=180,
    sse_step_name="MG",
    tool_schema={
        "description": "Gera motion graphics animados (setas, highlights, círculos, etc.)",
        "input": {"prompt": "str?"},
        "output": {"planned": "int", "rendered": "int", "status": "str"}
    }
)
def motion_graphics_step(state: PipelineState, params: dict) -> PipelineState:
    # Motion graphics só roda se houver prompt do usuário ou do LLM Director
    mg_prompt = (
        params.get('prompt')
        or (state.options.get('motion_graphics_prompt') if state.options else None)
    )

    if not mg_prompt:
        logger.info("⏭️ [MG] Sem motion_graphics_prompt, pulando step")
        return state

    if not state.png_results:
        logger.warning("⚠️ [MG] Sem png_results para motion graphics")
        return state

    from ..services.motion_graphics_director_service import MotionGraphicsDirectorService
    from ..services.motion_graphics_render_service import MotionGraphicsRenderService

    canvas = {
        'width': state.video_width,
        'height': state.video_height,
    }

    positioned_sentences = state.png_results.get('positioned_sentences', [])
    if not positioned_sentences and state.positioning_results:
        positioned_sentences = state.positioning_results.get('sentences', [])

    # ═══ FASE 1: Planejamento (LLM Director v0) ═══
    logger.info(f"🎬 [MG] Planejando motion graphics...")
    logger.info(f"   Prompt: {mg_prompt[:100]}...")

    director = MotionGraphicsDirectorService()

    # Calcular duração em segundos
    duration_s = (state.total_duration_ms or 0) / 1000.0

    # Preparar words_with_timestamps do fraseamento
    words_with_ts = []
    if state.phrase_groups:
        for pg in state.phrase_groups:
            for w in pg.get('words', []):
                words_with_ts.append(w)

    plan = director.plan_motion_graphics(
        user_prompt=mg_prompt,
        transcription=state.transcription_text or "",
        words_with_timestamps=words_with_ts,
        text_layout=positioned_sentences,
        canvas=canvas,
        duration=duration_s,
    )

    if not plan or not plan.get('motion_graphics'):
        logger.warning("⚠️ [MG] Director não retornou plano válido")
        return state.with_updates(
            motion_graphics_plan=[],
            motion_graphics_rendered=[],
        )

    mg_list = plan.get('motion_graphics', [])
    logger.info(f"✅ [MG] Plano: {len(mg_list)} motion graphics")

    # Enriquecer com posições das frases
    for mg in mg_list:
        sentence_idx = mg.get('sentence_index')
        if sentence_idx is not None and sentence_idx < len(positioned_sentences):
            sentence = positioned_sentences[sentence_idx]
            bbox = sentence.get('bounding_box', {})
            mg_type = mg.get('type', '')

            # Posicionar baseado no tipo
            if mg_type == 'arrow':
                mg['x'] = bbox.get('center_x', canvas['width'] // 2)
                mg['y'] = bbox.get('y', 0) - 50
            elif mg_type in ('highlight', 'underline'):
                mg['x'] = bbox.get('x', 0)
                mg['y'] = bbox.get('y', 0)
                mg['width'] = bbox.get('width', 200)
                mg['height'] = bbox.get('height', 50)
            elif mg_type == 'circle':
                mg['x'] = bbox.get('center_x', canvas['width'] // 2)
                mg['y'] = bbox.get('center_y', canvas['height'] // 2)
            else:
                mg['x'] = bbox.get('center_x', canvas['width'] // 2)
                mg['y'] = bbox.get('center_y', canvas['height'] // 2)

    # ═══ FASE 2: Renderização (Manim) ═══
    logger.info(f"🎨 [MG] Renderizando {len(mg_list)} motion graphics...")

    renderer = MotionGraphicsRenderService()
    render_result = renderer.render_motion_graphics(
        motion_graphics_plan=mg_list,
        job_id=state.job_id,
        project_id=state.project_id,
        user_id=state.user_id,
        text_layout=positioned_sentences,
        canvas=canvas,
    )

    rendered_mgs = render_result.get('motion_graphics', [])
    logger.info(f"✅ [MG] Renderizados: {len(rendered_mgs)} motion graphics")

    # Adicionar ao png_results para uso pelo subtitle_pipeline
    updated_png_results = state.png_results.copy()
    updated_png_results['motion_graphics'] = rendered_mgs

    return state.with_updates(
        motion_graphics_plan=mg_list,
        motion_graphics_rendered=rendered_mgs,
        png_results=updated_png_results,
    )
