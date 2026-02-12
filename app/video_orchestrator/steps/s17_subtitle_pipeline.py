"""Step 17: Subtitle Pipeline (posicionamento + payload builder)."""

from ._base import *

@register_step(
    name="subtitle_pipeline",
    description="Monta payload final (posicionamento + tracks) para o v-editor",
    category="rendering",
    depends_on=["calculate_positions", "generate_backgrounds"],
    produces=["subtitle_payload"],
    estimated_duration_s=15,
    cost_category="cpu",
    retryable=True,
    max_retries=2,
    timeout_s=120,
    sse_step_name="SUBTITLE_PIPELINE",
    tool_schema={
        "description": "Constrói o payload final com todas as tracks para renderização",
        "input": {},
        "output": {"total_sentences": "int", "has_matting": "bool", "status": "str"}
    }
)
def subtitle_pipeline_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.png_results:
        raise ValueError("Sem png_results para montar payload")

    from ..services.subtitle_pipeline_service import SubtitlePipelineService

    service = SubtitlePipelineService()

    # Determinar video_url para o payload
    video_url = state.get_video_url_for_processing()
    if not video_url:
        logger.warning("⚠️ [SUBTITLE_PIPELINE] Sem video_url, usando modo TEXT_VIDEO")

    # Duração
    duration_ms = state.total_duration_ms or 0

    # Matting data (se disponível)
    matting_data = None
    if state.matted_video_url or state.matting_segments:
        matting_data = {
            'foreground_url': state.matted_video_url,
            'segments': state.matting_segments or [],
            # 🔧 v3.5.0: Incluir foreground_segments (URLs do Modal) para payload_builder
            'foreground_segments': state.foreground_segments or [],
        }
        logger.info(f"👤 [SUBTITLE_PIPELINE] Matting: {len(matting_data['segments'])} segmentos, "
                     f"{len(matting_data['foreground_segments'])} foregrounds")

    logger.info(f"🎬 [SUBTITLE_PIPELINE] Montando payload final...")
    logger.info(f"   Video: {video_url[:50] if video_url else 'N/A'}...")
    logger.info(f"   Duração: {duration_ms}ms")
    logger.info(f"   PNGs: {state.png_results.get('total_pngs', '?')}")

    result = service.execute_pipeline(
        job_id=state.job_id,
        png_results=state.png_results,
        video_url=video_url or "",
        duration_ms=duration_ms,
        template_config=state.template_config or {},
        user_id=state.user_id,
        project_id=state.project_id,
        matting_data=matting_data,
        speech_segments=state.speech_segments,
    )

    if result.get('status') not in ('success', 'partial'):
        raise Exception(f"Subtitle pipeline falhou: {result.get('error', 'desconhecido')}")

    payload = result.get('payload', {})
    sentences = result.get('sentences', [])

    # 🔧 v3.5.0: Injetar foreground_segments na track person_overlay se vazia
    # O payload_builder pode não ter populado person_overlay com as URLs do Modal.
    # Os foreground_segments do MattingOrchestratorService já possuem todos os campos
    # necessários (src, mask_url, original_video_url, zIndex, start_time, end_time, position)
    if state.foreground_segments:
        tracks = payload.setdefault('tracks', {})
        existing_overlay = tracks.get('person_overlay', [])

        if not existing_overlay:
            # Converter foreground_segments para formato de person_overlay items
            overlay_items = []
            for fg in state.foreground_segments:
                item = {
                    'id': fg.get('id', f"person_overlay_{fg.get('segment_index', 0)}"),
                    'type': 'person_overlay',
                    'src': fg.get('src') or fg.get('foreground_url', ''),
                    'zIndex': fg.get('zIndex', 600),
                    'start_time': fg.get('start_time', 0),
                    'end_time': fg.get('end_time', 0),
                    'position': fg.get('position', {'x': 0, 'y': 0, 'width': '100%', 'height': '100%'}),
                }
                # Luma matte: incluir mask_url e original_video_url
                if fg.get('mask_url'):
                    item['mask_url'] = fg['mask_url']
                if fg.get('original_video_url'):
                    item['original_video_url'] = fg['original_video_url']
                overlay_items.append(item)

            tracks['person_overlay'] = overlay_items
            logger.info(f"🎭 [SUBTITLE_PIPELINE] Injetados {len(overlay_items)} person_overlay items "
                        f"(foreground_segments do Modal)")
        else:
            logger.info(f"👤 [SUBTITLE_PIPELINE] person_overlay já populado: {len(existing_overlay)} items")

    logger.info(f"✅ [SUBTITLE_PIPELINE] Payload montado | "
                f"{len(sentences)} sentenças | "
                f"Tracks: {list(payload.get('tracks', {}).keys()) if payload.get('tracks') else '?'}")

    return state.with_updates(
        subtitle_payload=payload,
    )
