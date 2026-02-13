"""Step 18: Renderização final (envia payload para o v-editor)."""

from ._base import *

@register_step(
    name="render",
    description="Envia payload para o v-editor e inicia renderização do vídeo",
    category="output",
    depends_on=["subtitle_pipeline"],
    produces=["output_video_url"],
    estimated_duration_s=300,
    cost_category="gpu",
    retryable=True,
    max_retries=2,
    timeout_s=900,
    sse_step_name="RENDER",
    await_async=["video_clipper"],  # 🆕 v4.4.0: Espera video_clipper async terminar
    tool_schema={
        "description": "Renderiza o vídeo final com todas as legendas e efeitos",
        "input": {"worker_preference": "str?", "quality": "str?", "preset": "str?"},
        "output": {"render_id": "str", "status": "str"}
    }
)
def render_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.subtitle_payload:
        raise ValueError("Sem subtitle_payload para renderizar")

    from ..services.render_service import RenderService

    # ═══ Selecionar worker de render ═══
    worker_preference = (
        params.get('worker_preference')
        or (state.options.get('editor_worker_id') if state.options else None)
        or get_env('DEFAULT_EDITOR_WORKER', 'python')
    )

    # ═══ Motion Graphics: forçar v-editor-python (Hetzner) ═══
    # Render-pod não suporta PNGs via path do volume compartilhado.
    # Toda renderização MG é feita no v-editor-python (Hetzner-first).
    if state.storytelling_mode == "motion_graphics" and worker_preference == "render-pod":
        logger.info(f"🎨 [RENDER] STM=motion_graphics → forçando v-editor-python (era: render-pod)")
        worker_preference = "python"

    service = RenderService(editor_worker_id=worker_preference)
    payload = dict(state.subtitle_payload)  # cópia

    # ═══ Extrair configurações de qualidade do template ═══
    project_settings = (state.template_config or {}).get('project-settings', {})
    video_settings = project_settings.get('video_settings', {})

    quality = params.get('quality') or extract_value(video_settings.get('quality')) or 'medium'
    preset = params.get('preset') or extract_value(video_settings.get('preset')) or 'balanced'

    payload['quality'] = quality
    payload['preset'] = preset

    # ═══ Motion Graphics (adicionar ao payload se existirem) ═══
    if state.motion_graphics_rendered:
        mg_track = payload.get('tracks', {}).get('motion_graphics', [])
        if not mg_track:
            tracks = payload.setdefault('tracks', {})
            tracks['motion_graphics'] = state.motion_graphics_rendered
            logger.info(f"🎬 [RENDER] Adicionados {len(state.motion_graphics_rendered)} MGs ao payload")

    # ═══ B-roll Overlay (Video Clipper Director — via async step) ═══
    if state.video_clipper_track:
        tracks = payload.setdefault('tracks', {})
        tracks['b_roll_overlay'] = state.video_clipper_track
        logger.info(
            f"🎬 [RENDER] Adicionados {len(state.video_clipper_track)} b-rolls ao payload"
        )
    else:
        logger.info("🎬 [RENDER] Sem b-rolls (video_clipper_track vazio ou step não executado)")

    # ═══ Title Track (Título do vídeo — via Title Director) ═══
    if state.title_track:
        tracks = payload.setdefault('tracks', {})
        tracks['titles'] = state.title_track
        logger.info(
            f"🏷️ [RENDER] Adicionados {len(state.title_track)} items de título ao payload"
        )
    else:
        logger.info("🏷️ [RENDER] Sem título (title_track vazio ou step não executado)")

    logger.info(f"🎬 [RENDER] Iniciando renderização...")
    logger.info(f"   Quality: {quality} | Preset: {preset}")
    logger.info(f"   Worker: {worker_preference}")
    logger.info(f"   Tracks: {list(payload.get('tracks', {}).keys())}")

    result = service.submit_render_job(
        job_id=state.job_id,
        payload=payload,
        user_id=state.user_id,
        project_id=state.project_id,
        template_id=state.template_id,
    )

    status = result.get('status', 'unknown')

    if status in ('queued', 'rendering_started', 'success'):
        logger.info(f"✅ [RENDER] Job enviado | Status: {status} | "
                     f"Render ID: {result.get('job_id', '?')}")
        return state.with_updates(
            output_video_url=result.get('output_url'),
        )

    if status == 'error':
        raise Exception(f"Render falhou: {result.get('message', 'desconhecido')}")

    # Status intermediário (queued, etc.)
    logger.info(f"📋 [RENDER] Status: {status} | {result.get('message', '')}")
    return state
