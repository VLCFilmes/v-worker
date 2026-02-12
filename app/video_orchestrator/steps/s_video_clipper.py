"""
Step: Video Clipper — Gera EDL de b-roll overlay via cruzamento semântico.

🆕 v4.4.0: @register_step com async_mode=True (Fire-and-Wait).
Dispara após transcribe e roda em paralelo com fraseamento, classify, PNGs, etc.
O render step espera (await_async) antes de montar o payload.

Fluxo:
  1. Verifica se há transcrição e b-rolls analisados
  2. Gera overlay EDL via LLM (cruzamento semântico: transcrição ↔ visual)
  3. Resolve URLs dos b-rolls (B2 ou shared-assets)
  4. Converte EDL → track items (formato v-editor-python)
  5. Retorna state com video_clipper_track

Timeline:
  transcribe [s06]
  ├── [FIRE] video_clipper (async ~5-15s) ─────────┐
  ├── fraseamento [s07] → ... → subtitle [s17]     │
  └── render [s18] (AWAIT video_clipper) ◄─────────┘
"""

from ._base import *

@register_step(
    name="video_clipper",
    description="Gera EDL de b-roll overlay via cruzamento semântico (LLM)",
    category="creative",
    depends_on=["transcribe"],
    produces=["video_clipper_track"],
    optional=True,
    estimated_duration_s=30,
    cost_category="llm",
    retryable=True,
    max_retries=1,
    timeout_s=120,
    sse_step_name="VIDEO_CLIPPER",
    async_mode=True,  # 🆕 Roda em thread separada (Fire-and-Wait)
    tool_schema={
        "description": "Posiciona b-rolls no timeline via cruzamento semântico transcrição ↔ visual",
        "input": {"force_regenerate": "bool?"},
        "output": {"b_rolls_placed": "int", "status": "str"}
    }
)
def video_clipper_step(state: PipelineState, params: dict) -> PipelineState:
    """
    Gera track de b-roll overlay via Video Clipper LLM.

    Usa cruzamento semântico entre transcrição (o que é falado/escrito)
    e análise visual dos b-rolls (o que é mostrado).
    """

    # ═══ 1. Verificar pré-requisitos ═══
    if not state.transcription_words:
        logger.info("⏭️ [VIDEO_CLIPPER] Sem transcrição, pulando")
        return state

    from app.services.video_clipper_service import (
        get_video_clipper_service,
        edl_to_track_items,
        resolve_broll_urls,
    )

    # ═══ 2. Verificar cache (overlay EDL já gerada?) ═══
    if not params.get('force_regenerate'):
        existing = _get_overlay_edl(state.project_id)
        if existing and existing.get('edit_sequence'):
            logger.info(
                f"✅ [VIDEO_CLIPPER] Overlay EDL já existe "
                f"({len(existing['edit_sequence'])} b-rolls), usando cache"
            )
            broll_urls = resolve_broll_urls(state.project_id)
            items = edl_to_track_items(existing, broll_urls)
            if items:
                logger.info(f"✅ [VIDEO_CLIPPER] {len(items)} track items do cache")
                return state.with_updates(video_clipper_track=items)

    # ═══ 3. Buscar dados de análise do projeto (DB) ═══
    results = _get_analysis_results(state.project_id)
    vision_result = results.get('vision')
    transcript_result = results.get('transcript')
    triage_result = results.get('triage')

    # ═══ 4. Filtrar b-rolls analisados ═══
    broll_analyses = _extract_broll_analyses(vision_result, triage_result)
    if not broll_analyses:
        logger.info("⏭️ [VIDEO_CLIPPER] Sem b-rolls analisados, pulando")
        return state

    # ═══ 5. Extrair dados do pipeline state ═══
    speech_segments = state.speech_segments or []
    cut_timestamps = state.cut_timestamps or []
    total_duration_ms = state.total_duration_ms or 0
    phase1_source = state.phase1_source or 'unknown'
    transcription_words = state.transcription_words or []

    if not total_duration_ms:
        logger.warning("⚠️ [VIDEO_CLIPPER] Sem total_duration_ms, pulando")
        return state

    # ═══ 6. Resolver storytelling mode e idioma ═══
    storytelling_mode = _get_storytelling_mode(state.project_id)
    response_language = _get_project_locale(state.project_id)

    logger.info(
        f"🎬 [VIDEO_CLIPPER] Gerando EDL overlay: "
        f"mode={storytelling_mode}, duration={total_duration_ms}ms, "
        f"b-rolls={len(broll_analyses)}, words={len(transcription_words)}"
    )

    # ═══ 7. Gerar overlay EDL via LLM ═══
    service = get_video_clipper_service()
    overlay_edl = service.generate_broll_overlay_edl(
        storytelling_mode=storytelling_mode,
        total_duration_ms=total_duration_ms,
        phase1_source=phase1_source,
        transcription_words=transcription_words,
        cut_timestamps=cut_timestamps,
        speech_segments=speech_segments,
        broll_analyses=broll_analyses,
        transcript_analysis=transcript_result,
        response_language=response_language,
    )

    if overlay_edl.get('status') != 'success':
        error_msg = overlay_edl.get('error', overlay_edl.get('status', 'unknown'))
        raise Exception(f"Video Clipper LLM falhou: {error_msg}")

    # ═══ 8. Persistir EDL no project_config ═══
    _persist_overlay_edl(state.project_id, overlay_edl)

    # ═══ 9. Registrar custos ═══
    _log_costs(state, overlay_edl)

    # ═══ 10. Resolver URLs dos b-rolls ═══
    broll_urls = resolve_broll_urls(state.project_id)

    # ═══ 11. Converter EDL → track items ═══
    items = edl_to_track_items(overlay_edl, broll_urls)

    placed = len(overlay_edl.get('edit_sequence', []))
    resolved = len(items)
    logger.info(
        f"✅ [VIDEO_CLIPPER] Completo: {placed} b-rolls na EDL, "
        f"{resolved} track items com URL resolvida"
    )

    return state.with_updates(video_clipper_track=items)


# ═══════════════════════════════════════════════════════════════
# HELPERS — DB (reutilizados do video_clipper_trigger.py)
# ═══════════════════════════════════════════════════════════════

def _get_analysis_results(project_id: str) -> dict:
    """Busca resultados de análise do project_config."""
    try:
        from app.services.video_clipper_trigger import _get_analysis_results as _trigger_get
        return _trigger_get(project_id)
    except ImportError:
        logger.warning("⚠️ [VIDEO_CLIPPER] Fallback: buscando análises diretamente")
        return _get_analysis_results_fallback(project_id)


def _get_analysis_results_fallback(project_id: str) -> dict:
    """Fallback se video_clipper_trigger não estiver disponível."""
    import json
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                project_config->'asset_triage_result',
                project_config->'transcript_analysis_result',
                project_config->'vision_analysis_result'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return {}

        def parse_jsonb(val):
            if val is None:
                return None
            if isinstance(val, dict):
                return val
            try:
                return json.loads(val)
            except (json.JSONDecodeError, TypeError):
                return None

        return {
            'triage': parse_jsonb(row[0]),
            'transcript': parse_jsonb(row[1]),
            'vision': parse_jsonb(row[2]),
        }
    except Exception as e:
        logger.error(f"❌ [VIDEO_CLIPPER] Erro ao buscar análises: {e}")
        return {}


def _extract_broll_analyses(vision_result, triage_result) -> list:
    """Filtra análises visuais de b-rolls."""
    try:
        from app.services.video_clipper_trigger import _extract_broll_analyses as _trigger_extract
        return _trigger_extract(vision_result, triage_result)
    except ImportError:
        pass

    # Fallback inline
    if not vision_result:
        return []

    videos = vision_result.get('videos', [])
    if not videos and vision_result.get('status') == 'success':
        videos = [vision_result]

    if not triage_result:
        return [v for v in videos if v.get('content_type') == 'b_roll']

    broll_ids = set()
    for asset in triage_result.get('assets', []):
        if asset.get('classification') == 'b_roll':
            broll_ids.add(asset.get('asset_id'))

    routing = triage_result.get('routing', {})
    for vid in routing.get('vision_analysis', []):
        broll_ids.add(str(vid))

    return [v for v in videos if v.get('asset_id') in broll_ids]


def _get_overlay_edl(project_id: str):
    """Busca overlay EDL existente."""
    try:
        from app.services.video_clipper_trigger import _get_overlay_edl as _trigger_get_edl
        return _trigger_get_edl(project_id)
    except ImportError:
        pass

    import json
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT project_config->'video_clipper_overlay_edl'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row or not row[0]:
            return None
        if isinstance(row[0], dict):
            return row[0]
        return json.loads(row[0])
    except Exception:
        return None


def _persist_overlay_edl(project_id: str, result: dict) -> None:
    """Salva overlay EDL em project_config."""
    try:
        from app.services.video_clipper_trigger import _persist_overlay_edl as _trigger_persist
        _trigger_persist(project_id, result)
    except ImportError:
        import json
        try:
            from app.db import get_db_connection
            conn = get_db_connection()
            cursor = conn.cursor()
            result_json = json.dumps(result, ensure_ascii=False)
            cursor.execute("""
                UPDATE projects
                SET project_config = jsonb_set(
                    COALESCE(project_config, '{}'::jsonb),
                    '{video_clipper_overlay_edl}',
                    %s::jsonb
                )
                WHERE project_id = %s
            """, (result_json, project_id))
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("💾 [VIDEO_CLIPPER] Overlay EDL salvo")
        except Exception as e:
            logger.error(f"❌ [VIDEO_CLIPPER] Erro ao persistir overlay EDL: {e}")


def _get_storytelling_mode(project_id: str) -> str:
    """Busca storytelling_mode do projeto."""
    try:
        from app.services.video_clipper_trigger import _get_storytelling_mode as _trigger_mode
        return _trigger_mode(project_id)
    except ImportError:
        pass

    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                project_config->'base_layer'->>'storytelling_mode',
                project_config->>'storytelling_mode'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return row[0] or row[1] or 'talking_head'
    except Exception:
        pass
    return 'talking_head'


def _get_project_locale(project_id: str) -> str:
    """Resolve idioma do projeto."""
    try:
        from app.services.asset_triage_service import get_project_locale
        return get_project_locale(project_id)
    except Exception:
        return 'Portuguese (pt-BR)'


def _log_costs(state: PipelineState, overlay_edl: dict) -> None:
    """Registra custos da chamada LLM."""
    try:
        from app.services.ai_cost_tracker import log_ai_usage
        log_ai_usage(
            service_type="video_clipper_overlay",
            provider="openai",
            model=overlay_edl.get('model', 'gpt-4o-mini'),
            project_id=state.project_id,
            conversation_id=state.conversation_id or '',
            tokens_in=overlay_edl.get('tokens_in', 0),
            tokens_out=overlay_edl.get('tokens_out', 0),
            duration_ms=overlay_edl.get('llm_time_ms', 0),
            input_units=len(overlay_edl.get('edit_sequence', [])),
            metadata={
                'mode': overlay_edl.get('mode'),
                'b_rolls_placed': len(overlay_edl.get('edit_sequence', [])),
                'strategy': overlay_edl.get('strategy'),
                'step': 'video_clipper',
            },
        )
    except Exception as cost_err:
        logger.warning(f"⚠️ [VIDEO_CLIPPER] Cost tracking: {cost_err}")
