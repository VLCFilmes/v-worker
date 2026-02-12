"""
🎬 VideoClipper Trigger — Gera EDL combinando Transcript + Vision + Triage.

Dispara APÓS Vision Director e Transcript Director completarem.
Lê os resultados de project_config e chama o VideoClipper Service.

Fluxo:
  1. Busca resultados: asset_triage_result, transcript_analysis_result, vision_analysis_result
  2. Verifica se ambos análises estão prontas
  3. Chama VideoClipperService para gerar EDL
  4. Salva em project_config.video_clipper_edl
  5. Envia mensagem no chat + dados para modal revisor

Autor: Vinicius + Claude
Data: 09/Fev/2026
"""

import logging
import threading
import json
import uuid
import time
from typing import Optional, Dict

logger = logging.getLogger(__name__)


def trigger_video_clipper_async(
    project_id: str,
    conversation_id: str,
) -> None:
    """Dispara geração de EDL em background thread."""
    thread = threading.Thread(
        target=_run_video_clipper,
        args=(project_id, conversation_id),
        daemon=True,
        name=f"video-clipper-{project_id[:8]}",
    )
    thread.start()
    logger.info(
        f"🎬 [CLIPPER-TRIGGER] Background thread iniciada: "
        f"project={project_id[:8]}... conv={conversation_id[:8]}..."
    )


def check_and_trigger_clipper(
    project_id: str,
    conversation_id: str,
) -> bool:
    """
    Verifica se ambos análises (Vision + Transcript) estão prontas
    e dispara o VideoClipper automaticamente se estiverem.

    Retorna True se disparou, False se ainda falta algum resultado.
    """
    try:
        results = _get_analysis_results(project_id)

        has_triage = results.get('triage') is not None
        has_transcript = results.get('transcript') is not None
        has_vision = results.get('vision') is not None

        # Precisa pelo menos do triage + pelo menos um dos directors
        if not has_triage:
            logger.debug(
                f"🎬 [CLIPPER-CHECK] Aguardando triage para {project_id[:8]}"
            )
            return False

        # Verificar se há assets que precisam de cada tipo de análise
        triage = results['triage']
        routing = triage.get('routing', {})
        needs_vision = len(routing.get('vision_analysis', [])) > 0
        needs_transcript = len(routing.get('pipeline_ready', [])) > 0

        # Se precisa de vision mas não tem, aguardar
        if needs_vision and not has_vision:
            logger.debug(
                f"🎬 [CLIPPER-CHECK] Aguardando vision para {project_id[:8]}"
            )
            return False

        # Se precisa de transcript mas não tem, aguardar
        if needs_transcript and not has_transcript:
            logger.debug(
                f"🎬 [CLIPPER-CHECK] Aguardando transcript para {project_id[:8]}"
            )
            return False

        # Verificar se EDL já foi gerado (evitar duplicatas)
        if results.get('edl') is not None:
            logger.info(
                f"🎬 [CLIPPER-CHECK] EDL já existe para {project_id[:8]}, pulando"
            )
            return False

        # Tudo pronto! Disparar VideoClipper
        logger.info(
            f"🎬 [CLIPPER-CHECK] Ambos análises prontas! "
            f"Disparando VideoClipper para {project_id[:8]} "
            f"(vision={has_vision}, transcript={has_transcript})"
        )
        trigger_video_clipper_async(project_id, conversation_id)
        return True

    except Exception as e:
        logger.error(
            f"❌ [CLIPPER-CHECK] Erro ao verificar: {e}"
        )
        return False


def _run_video_clipper(
    project_id: str,
    conversation_id: str,
) -> None:
    """Executa a geração de EDL (roda em background thread)."""
    t0 = time.time()

    try:
        # ─── 1. Buscar resultados dos directors ──────────────────
        results = _get_analysis_results(project_id)

        triage_result = results.get('triage')
        transcript_result = results.get('transcript')
        vision_result = results.get('vision')

        if not triage_result:
            _send_error_message(
                conversation_id,
                "Não encontrei resultado da triagem. Execute a triagem primeiro."
            )
            return

        if not transcript_result and not vision_result:
            _send_error_message(
                conversation_id,
                "Nenhuma análise (transcrição ou visual) encontrada. "
                "Execute as análises primeiro."
            )
            return

        sources = []
        if transcript_result:
            sources.append("transcrição")
        if vision_result:
            sources.append("visual")

        logger.info(
            f"🎬 [CLIPPER-TRIGGER] Fontes disponíveis: {', '.join(sources)}"
        )

        # ─── 2. Typing indicator ──────────────────────────────────
        _send_typing_message(
            conversation_id,
            f"Montando plano de edição com base na análise de {' + '.join(sources)}..."
        )

        # ─── 3. Resolver idioma ───────────────────────────────────
        try:
            from app.services.asset_triage_service import get_project_locale
            response_language = get_project_locale(project_id)
        except Exception:
            response_language = 'Portuguese (pt-BR)'

        # ─── 4. Gerar EDL ────────────────────────────────────────
        from app.services.video_clipper_service import get_video_clipper_service
        service = get_video_clipper_service()

        result = service.analyze(
            triage_result=triage_result,
            transcript_result=transcript_result or {},
            vision_result=vision_result,
            response_language=response_language,
        )

        elapsed = time.time() - t0

        if result.get('status') != 'success':
            _send_error_message(
                conversation_id,
                f"Erro ao gerar plano de edição: {result.get('error', 'desconhecido')}"
            )
            return

        # ─── 5. Persistir EDL ────────────────────────────────────
        _persist_edl_result(project_id, result)

        # ─── 5b. Registrar custos ────────────────────────────────
        try:
            from app.services.ai_cost_tracker import log_ai_usage
            log_ai_usage(
                service_type="video_clipper_llm",
                provider="openai",
                model=result.get('model', 'gpt-4o-mini'),
                project_id=project_id,
                conversation_id=conversation_id,
                tokens_in=result.get('tokens_in', 0),
                tokens_out=result.get('tokens_out', 0),
                duration_ms=result.get('llm_time_ms', 0),
                input_units=len(result.get('edit_sequence', [])),
                metadata={
                    'format': result.get('format'),
                    'edit_sequence_count': len(result.get('edit_sequence', [])),
                    'b_roll_placements': len(result.get('b_roll_placement', [])),
                    'unused_segments': len(result.get('unused_segments', [])),
                    'sources': sources,
                },
            )
        except Exception as cost_err:
            logger.warning(
                f"⚠️ [CLIPPER-TRIGGER] Cost tracking: {cost_err}"
            )

        # ─── 6. Gerar roteiro pré-populado para o editor ─────
        doc_id = None
        script_data = None
        script_rows_count = 0
        try:
            from app.services.script_generator import generate_script_from_directors
            doc_id = generate_script_from_directors(
                project_id=project_id,
                conversation_id=conversation_id,
                triage_result=triage_result,
                transcript_result=transcript_result,
                vision_result=vision_result,
                edl_result=result,
            )
            if doc_id:
                logger.info(
                    f"📝 [CLIPPER-TRIGGER] Roteiro gerado: {doc_id[:8]}"
                )
        except Exception as script_err:
            logger.warning(
                f"⚠️ [CLIPPER-TRIGGER] Script generation: {script_err}"
            )

        # ─── 7. Enviar ÚNICA mensagem consolidada no chat ──────
        # Resume o pipeline todo (triage + análises + EDL + roteiro)
        # e abre o editor automaticamente
        _send_consolidated_message(
            conversation_id=conversation_id,
            edl_result=result,
            transcript_result=transcript_result,
            vision_result=vision_result,
            triage_result=triage_result,
            document_id=doc_id,
            total_elapsed=elapsed,
        )

        logger.info(
            f"✅ [CLIPPER-TRIGGER] Completo em {elapsed:.1f}s | "
            f"sequence={len(result.get('edit_sequence', []))} cortes | "
            f"b_roll={len(result.get('b_roll_placement', []))}"
        )

    except Exception as e:
        elapsed = time.time() - t0
        logger.error(
            f"❌ [CLIPPER-TRIGGER] Falha após {elapsed:.1f}s: {e}",
            exc_info=True,
        )
        _send_error_message(
            conversation_id,
            "Ocorreu um erro ao gerar o plano de edição."
        )


# ═══════════════════════════════════════════════════════════════
# B-ROLL OVERLAY EDL (chamado do render step com pipeline state)
# ═══════════════════════════════════════════════════════════════


def generate_overlay_edl_for_render(
    project_id: str,
    conversation_id: str,
    pipeline_state: dict,
) -> Optional[Dict]:
    """
    Gera EDL de overlay (b-roll positions) usando dados do pipeline.
    Chamado SINCRONAMENTE do render step (s18_render).

    Args:
        project_id: ID do projeto
        conversation_id: ID da conversa
        pipeline_state: Dict com speech_segments, cut_timestamps,
                        total_duration_ms, phase1_source, transcription_words

    Returns:
        EDL de overlay (ou None se não houver b-rolls/dados)
    """
    t0 = time.time()

    try:
        # ─── 1. Buscar resultados dos directors ──────────────
        results = _get_analysis_results(project_id)

        vision_result = results.get('vision')
        transcript_result = results.get('transcript')
        triage_result = results.get('triage')

        # Verificar se já existe overlay EDL
        existing_overlay = _get_overlay_edl(project_id)
        if existing_overlay and existing_overlay.get('edit_sequence'):
            logger.info(
                f"🎬 [OVERLAY-EDL] Overlay EDL já existe para {project_id[:8]} "
                f"({len(existing_overlay['edit_sequence'])} b-rolls)"
            )
            return existing_overlay

        # ─── 2. Filtrar b-rolls do vision analysis ──────────
        broll_analyses = _extract_broll_analyses(vision_result, triage_result)

        if not broll_analyses:
            logger.info(
                f"🎬 [OVERLAY-EDL] Sem b-rolls analisados para {project_id[:8]}, pulando"
            )
            return None

        # ─── 3. Extrair dados do pipeline state ─────────────
        speech_segments = pipeline_state.get('speech_segments', [])
        cut_timestamps = pipeline_state.get('cut_timestamps', [])
        total_duration_ms = pipeline_state.get('total_duration_ms', 0)
        phase1_source = pipeline_state.get('phase1_source', 'unknown')
        transcription_words = pipeline_state.get('transcription_words', [])

        # Fallback: buscar transcrição do project_config se não vier no state
        if not transcription_words:
            transcription_words = _get_transcription_words(project_id)

        storytelling_mode = _get_storytelling_mode(project_id)

        if not total_duration_ms:
            logger.warning(
                f"⚠️ [OVERLAY-EDL] Sem total_duration_ms para {project_id[:8]}"
            )
            return None

        logger.info(
            f"🎬 [OVERLAY-EDL] Gerando para {project_id[:8]}: "
            f"mode={storytelling_mode}, duration={total_duration_ms}ms, "
            f"b-rolls={len(broll_analyses)}, words={len(transcription_words)}, "
            f"cuts={len(cut_timestamps)}"
        )

        # ─── 4. Resolver idioma ─────────────────────────────
        try:
            from app.services.asset_triage_service import get_project_locale
            response_language = get_project_locale(project_id)
        except Exception:
            response_language = 'Portuguese (pt-BR)'

        # ─── 5. Gerar overlay EDL ───────────────────────────
        from app.services.video_clipper_service import get_video_clipper_service
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

        elapsed = time.time() - t0

        if overlay_edl.get('status') != 'success':
            logger.warning(
                f"⚠️ [OVERLAY-EDL] Falha: {overlay_edl.get('error', overlay_edl.get('status'))}"
            )
            return None

        # ─── 6. Persistir ───────────────────────────────────
        _persist_overlay_edl(project_id, overlay_edl)

        logger.info(
            f"✅ [OVERLAY-EDL] Completo em {elapsed:.1f}s | "
            f"b-rolls={len(overlay_edl.get('edit_sequence', []))}"
        )

        return overlay_edl

    except Exception as e:
        logger.error(
            f"❌ [OVERLAY-EDL] Erro para {project_id[:8]}: {e}",
            exc_info=True,
        )
        return None


def _extract_broll_analyses(
    vision_result: Optional[dict],
    triage_result: Optional[dict],
) -> list:
    """Filtra apenas as análises visuais de b-rolls."""
    if not vision_result:
        return []

    # Vision pode ser single ou multi
    videos = vision_result.get('videos', [])
    if not videos and vision_result.get('status') == 'success':
        videos = [vision_result]

    if not triage_result:
        # Sem triage, considerar todos como b-roll potencial
        return [v for v in videos if v.get('content_type') == 'b_roll']

    # Com triage, filtrar pelos IDs classificados como b_roll
    broll_ids = set()
    for asset in triage_result.get('assets', []):
        if asset.get('classification') == 'b_roll':
            broll_ids.add(asset.get('asset_id'))

    # Também pegar do routing
    routing = triage_result.get('routing', {})
    for vid in routing.get('vision_analysis', []):
        broll_ids.add(str(vid))

    return [v for v in videos if v.get('asset_id') in broll_ids]


def _get_overlay_edl(project_id: str) -> Optional[dict]:
    """Busca overlay EDL existente."""
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
    """Salva overlay EDL em project_config.video_clipper_overlay_edl."""
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

        logger.info(f"💾 [OVERLAY-EDL] Salvo em project_config")
    except Exception as e:
        logger.error(f"❌ [OVERLAY-EDL] Erro ao persistir: {e}")


def _get_transcription_words(project_id: str) -> list:
    """Busca palavras da transcrição do projeto."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        # Tentar de múltiplas fontes
        cursor.execute("""
            SELECT
                project_config->'transcription_result'->'words',
                project_config->'transcription_result'->'segments'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return []

        # Tentar words primeiro
        words = row[0]
        if words:
            if isinstance(words, list):
                return words
            try:
                return json.loads(words)
            except (json.JSONDecodeError, TypeError):
                pass

        # Fallback: extrair de segments
        segments = row[1]
        if segments:
            if isinstance(segments, str):
                segments = json.loads(segments)
            words_list = []
            for seg in segments:
                for w in seg.get('words', []):
                    words_list.append(w)
            return words_list

        return []
    except Exception as e:
        logger.warning(f"⚠️ [OVERLAY-EDL] Erro ao buscar transcrição: {e}")
        return []


def _get_storytelling_mode(project_id: str) -> str:
    """Busca storytelling_mode do projeto."""
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
        return 'talking_head'
    except Exception:
        return 'talking_head'


# ═══════════════════════════════════════════════════════════════
# HELPERS — DB
# ═══════════════════════════════════════════════════════════════


def _get_analysis_results(project_id: str) -> Dict:
    """Busca todos os resultados de análise do project_config."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                project_config->'asset_triage_result',
                project_config->'transcript_analysis_result',
                project_config->'vision_analysis_result',
                project_config->'video_clipper_edl'
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
            'edl': parse_jsonb(row[3]),
        }
    except Exception as e:
        logger.error(
            f"❌ [CLIPPER-TRIGGER] Erro ao buscar resultados: {e}"
        )
        return {}


def _persist_edl_result(project_id: str, result: dict) -> None:
    """Salva EDL em project_config.video_clipper_edl."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        result_json = json.dumps(result, ensure_ascii=False)
        cursor.execute("""
            UPDATE projects
            SET project_config = jsonb_set(
                COALESCE(project_config, '{}'::jsonb),
                '{video_clipper_edl}',
                %s::jsonb
            )
            WHERE project_id = %s
        """, (result_json, project_id))
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"💾 [CLIPPER-TRIGGER] EDL salvo em project_config")
    except Exception as e:
        logger.error(
            f"❌ [CLIPPER-TRIGGER] Erro ao persistir EDL: {e}"
        )


# ═══════════════════════════════════════════════════════════════
# FORMATAÇÃO DA MENSAGEM
# ═══════════════════════════════════════════════════════════════


def _format_edl_message(result: dict, elapsed: float) -> str:
    """Formata EDL como mensagem resumida no chat."""
    edit_seq = result.get('edit_sequence', [])
    broll = result.get('b_roll_placement', [])
    unused = result.get('unused_segments', [])
    retake_decisions = result.get('retake_decisions', [])

    lines = [
        f"**Plano de Edição Gerado** ({elapsed:.0f}s)",
        "",
    ]

    # Formato e estilo
    fmt = result.get('format', 'unknown')
    style = result.get('editing_style', 'balanced')
    target_ms = result.get('target_duration_ms', 0)
    raw_ms = result.get('total_raw_duration_ms', 0)
    ratio = result.get('compression_ratio', '')

    lines.append(f"**Formato:** {fmt} | **Estilo:** {style}")
    if target_ms > 0:
        lines.append(
            f"**Duração:** ~{target_ms / 1000:.0f}s "
            f"(de {raw_ms / 1000:.0f}s bruto, compressão {ratio})"
        )
    lines.append("")

    # Sequência de edição
    if edit_seq:
        lines.append(f"**Sequência de edição ({len(edit_seq)} cortes):**")
        for cut in edit_seq[:8]:  # Max 8 no chat
            order = cut.get('order', '?')
            cut_type = cut.get('type', '?')
            speaker = cut.get('speaker', '')
            in_ms = cut.get('in_ms', 0)
            out_ms = cut.get('out_ms', 0)
            purpose = cut.get('editorial_purpose', '')
            text = cut.get('text', '')[:50]

            type_icon = {
                'talking_head': '🎙️',
                'b_roll': '🎬',
                'audio_narration': '🎤',
                'screen_capture': '🖥️',
            }.get(cut_type, '📎')

            duration_s = (out_ms - in_ms) / 1000

            if speaker:
                lines.append(
                    f"  {order}. {type_icon} **{speaker}** "
                    f"({duration_s:.1f}s) — {purpose}"
                )
                if text:
                    lines.append(f"     _{text}_")
            else:
                lines.append(
                    f"  {order}. {type_icon} {cut_type} "
                    f"({duration_s:.1f}s) — {purpose}"
                )

        if len(edit_seq) > 8:
            lines.append(f"  ... e mais {len(edit_seq) - 8} cortes")
        lines.append("")

    # B-roll placement
    if broll:
        lines.append(f"**B-roll ({len(broll)} posicionamentos)**")
        lines.append("")

    # Retake decisions
    if retake_decisions:
        lines.append(f"**Retakes resolvidos ({len(retake_decisions)}):**")
        for rd in retake_decisions:
            reason = rd.get('reason', '')[:80]
            lines.append(f"  - {reason}")
        lines.append("")

    # Unused
    if unused:
        lines.append(
            f"**{len(unused)} segmento(s) não utilizado(s)**"
        )
        lines.append("")

    # Resumo
    summary = result.get('summary', '')
    if summary:
        lines.append(f"_{summary}_")
        lines.append("")

    # Métricas
    llm_ms = result.get('llm_time_ms', 0)
    model = result.get('model', '?')
    tokens_in = result.get('tokens_in', 0)
    tokens_out = result.get('tokens_out', 0)

    lines.append(
        f"_LLM: {model} ({llm_ms}ms) | "
        f"Tokens: {tokens_in}→{tokens_out}_"
    )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# HELPERS — SSE / MENSAGENS
# ═══════════════════════════════════════════════════════════════


def _send_consolidated_message(
    conversation_id: str,
    edl_result: dict,
    transcript_result: Optional[dict],
    vision_result: Optional[dict],
    triage_result: dict,
    document_id: Optional[str],
    total_elapsed: float,
) -> None:
    """
    Envia UMA ÚNICA mensagem consolidada no chat resumindo todo o pipeline.
    Se document_id estiver disponível, inclui show_script + open_editor
    para abrir o editor automaticamente.
    """
    try:
        from app.db import get_db_connection

        # ─── Montar resumo curto ──────────────────────────────
        edit_seq = edl_result.get('edit_sequence', [])
        broll = edl_result.get('b_roll_placement', [])
        total_assets = len(triage_result.get('assets', []))

        # Contar etapas realizadas
        steps = ['classificação']
        if transcript_result:
            steps.append('transcrição')
        if vision_result:
            steps.append('visual')
        steps.append('montagem')

        content = (
            f"**Análise completa!** ({total_elapsed:.0f}s)\n\n"
            f"Analisei {total_assets} uploads e gerei um roteiro com "
            f"**{len(edit_seq)} cortes**"
        )
        if broll:
            content += f" e **{len(broll)} b-roll(s)**"
        content += ".\n\n"

        # Detalhes curtos
        target_ms = edl_result.get('target_duration_ms', 0)
        if target_ms > 0:
            content += f"Duração estimada: ~{target_ms / 1000:.0f}s\n"

        content += f"Etapas: {' → '.join(steps)}\n\n"

        if document_id:
            content += "_O roteiro foi aberto automaticamente no editor. Revise e ajuste como quiser._"
        else:
            content += "_Os resultados estão salvos no projeto._"

        # ─── Component props para abrir editor ────────────────
        component_props = {
            'analysis_complete': True,
            'edit_sequence_count': len(edit_seq),
            'b_roll_count': len(broll),
            'total_assets': total_assets,
            'steps': steps,
        }

        # Se temos um documento, incluir flags para abrir o editor
        if document_id:
            component_props['show_script'] = True
            component_props['open_editor'] = True
            component_props['document_id'] = document_id

        msg_id = str(uuid.uuid4())
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO chatbot_messages
                (id, message_id, conversation_id, sender, content,
                 component_type, component_props, created_at)
            VALUES (%s, %s, %s, 'bot', %s, %s, %s, NOW())
        """, (
            msg_id, msg_id, conversation_id, content,
            'analysis_complete',
            json.dumps(component_props, ensure_ascii=False),
        ))
        conn.commit()
        cursor.close()
        conn.close()

        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': msg_id,
            'sender': 'bot',
            'content': content,
            'component_type': 'analysis_complete',
            'component_props': component_props,
        })

        logger.info(
            f"📡 [CLIPPER-TRIGGER] Mensagem consolidada enviada: {msg_id[:8]} | "
            f"doc={document_id[:8] if document_id else 'None'}"
        )
    except Exception as e:
        logger.error(
            f"❌ [CLIPPER-TRIGGER] Erro ao enviar mensagem consolidada: {e}"
        )


def _send_edl_message(
    conversation_id: str,
    content: str,
    edl_result: dict,
) -> None:
    """LEGACY: Salva mensagem + emite SSE com dados do EDL. Usado apenas para erros."""
    try:
        from app.db import get_db_connection
        msg_id = str(uuid.uuid4())
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO chatbot_messages
                (id, message_id, conversation_id, sender, content,
                 component_type, component_props, created_at)
            VALUES (%s, %s, %s, 'bot', %s, 'text', '{}', NOW())
        """, (
            msg_id, msg_id, conversation_id, content,
        ))
        conn.commit()
        cursor.close()
        conn.close()

        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': msg_id,
            'sender': 'bot',
            'content': content,
            'component_type': 'text',
        })
    except Exception as e:
        logger.error(
            f"❌ [CLIPPER-TRIGGER] Erro ao enviar mensagem: {e}"
        )


def _send_error_message(conversation_id: str, content: str) -> None:
    """Envia mensagem de erro."""
    _send_edl_message(conversation_id, content, {})


def _send_typing_message(
    conversation_id: str, message: str = ""
) -> None:
    """Envia indicador de digitação."""
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'typing', {
            'sender': 'bot',
            'message': message or 'Montando plano de edição com IA...',
        })
    except Exception as e:
        logger.warning(
            f"⚠️ [CLIPPER-TRIGGER] Erro typing: {e}"
        )
