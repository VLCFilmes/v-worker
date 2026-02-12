"""
🎙️ Transcript Analysis Trigger — Dispara análise de transcrições assíncrona.

Arquitetura: GPT-4o-mini (texto puro, sem visão)

Quando ativado (em paralelo com Vision Director):
  1. Busca assets pipeline_ready da triagem (talking_head, audio_narration)
  2. Coleta transcrições COMPLETAS do banco (project_assets.metadata)
  3. Busca retakes detectados pela triagem (para refinamento)
  4. Chama GPT-4o-mini com todas as transcrições
  5. Análise: narrativa, sound bites, retakes refinados, ordem, segmentação
  6. Salva em project_config.transcript_analysis_result
  7. Envia mensagem no chat via SSE

Autor: Vinicius + Claude
Data: 09/Fev/2026
"""

import logging
import threading
import json
import uuid
import time
from typing import Optional, List

logger = logging.getLogger(__name__)


def trigger_transcript_analysis_async(
    project_id: str,
    conversation_id: str,
    batch_id: str = None,
) -> None:
    """Dispara análise de transcrição em background thread."""
    thread = threading.Thread(
        target=_run_transcript_analysis,
        args=(project_id, conversation_id, batch_id),
        daemon=True,
        name=f"transcript-analysis-{project_id[:8]}",
    )
    thread.start()
    logger.info(
        f"🎙️ [TRANSCRIPT-TRIGGER] Background thread iniciada: "
        f"project={project_id[:8]}... conv={conversation_id[:8]}..."
    )


def _run_transcript_analysis(
    project_id: str,
    conversation_id: str,
    batch_id: str = None,
) -> None:
    """Executa a análise de transcrição completa (roda em background thread)."""
    t0 = time.time()

    try:
        # ─── 1. Buscar IDs dos assets para análise (routing da triagem) ──
        transcript_asset_ids = _get_triage_transcript_ids(project_id)
        triage_data = _get_triage_data(project_id)

        if not transcript_asset_ids:
            logger.warning(
                f"⚠️ [TRANSCRIPT-TRIGGER] Nenhum asset pipeline_ready "
                f"encontrado na triagem para project={project_id[:8]}"
            )
            _send_info_message(
                conversation_id,
                "Não encontrei assets com fala para analisar. "
                "A triagem não identificou talking-heads ou narrações neste projeto."
            )
            return

        logger.info(
            f"🎙️ [TRANSCRIPT-TRIGGER] Triage routing: "
            f"{len(transcript_asset_ids)} asset(s) para análise de transcrição"
        )

        # ─── 2. Buscar transcrições completas do banco ────────────────
        assets_with_transcriptions = _get_assets_with_transcriptions(
            project_id, transcript_asset_ids
        )

        if not assets_with_transcriptions:
            _send_info_message(
                conversation_id,
                "Não encontrei transcrições disponíveis para os assets selecionados."
            )
            return

        # Enriquecer com classificação da triagem
        triage_assets = {
            a.get('asset_id', ''): a
            for a in (triage_data.get('assets', []) if triage_data else [])
        }
        for asset in assets_with_transcriptions:
            triage_info = triage_assets.get(asset['id'], {})
            asset['classification'] = triage_info.get('classification', 'unknown')

        # Contar palavras totais para log
        total_words = sum(
            len((a.get('transcription_text', '') or '').split())
            for a in assets_with_transcriptions
        )
        logger.info(
            f"🎙️ [TRANSCRIPT-TRIGGER] {len(assets_with_transcriptions)} asset(s) "
            f"com transcrição | ~{total_words} palavras total"
        )

        # ─── 3. Typing indicator + Visualizer ─────────────────────────
        n = len(assets_with_transcriptions)
        _send_typing_message(
            conversation_id,
            f"Analisando transcrições de {n} vídeo{'s' if n > 1 else ''} com IA..."
        )
        
        # 🆕 11/Fev/2026: Atualizar Analysis Visualizer (step: transcript_analysis → active)
        if batch_id:
            try:
                from app.routes.visualizer_persistence import update_batch_final_step
                update_batch_final_step(
                    conversation_id, batch_id, 'transcript_analysis', 'active',
                    f"Analisando transcrições de {n} vídeo{'s' if n > 1 else ''} com IA..."
                )
            except Exception as viz_err:
                logger.warning(f"⚠️ [TRANSCRIPT-TRIGGER] Erro ao atualizar visualizer: {viz_err}")

        # ─── 4. Analisar transcrições (LLM) ───────────────────────────
        from app.services.transcript_analysis_service import (
            get_transcript_analysis_service,
        )
        service = get_transcript_analysis_service()

        # Resolver idioma de resposta
        try:
            from app.services.asset_triage_service import get_project_locale
            response_language = get_project_locale(project_id)
        except Exception:
            response_language = 'Portuguese (pt-BR)'

        # Retakes da triagem (para refinamento)
        retakes = triage_data.get('retakes', []) if triage_data else []
        format_detected = (
            triage_data.get('format_detected', 'unknown')
            if triage_data else 'unknown'
        )

        result = service.analyze(
            assets=assets_with_transcriptions,
            retakes=retakes,
            format_detected=format_detected,
            response_language=response_language,
        )

        elapsed = time.time() - t0

        # ─── 5. Verificar resultado ───────────────────────────────────
        if result.get('status') != 'success':
            _send_error_message(
                conversation_id,
                f"Erro na análise de transcrição: {result.get('error', 'desconhecido')}"
            )
            return

        # ─── 5b. Resolver intra-retakes (DETERMINÍSTICO) ──────────────
        # Padrão híbrido: LLM detectou frases repetidas (semântico),
        # agora o IntraRetakeResolver calcula timestamps e segmentos
        # usando os word timestamps reais do AssemblyAI.
        # Montar dict asset_id → words[] do AssemblyAI (usado por LLM e fallback)
        assets_words = {
            a['id']: a.get('words', [])
            for a in assets_with_transcriptions
            if a.get('words')
        }

        llm_intra = result.get('intra_retakes', [])

        # 🆕 Fallback determinístico: se a LLM não detectou, escanear automaticamente
        if not llm_intra and assets_words:
            try:
                from app.services.intra_retake_resolver import detect_repeated_phrases
                deterministic_detections = detect_repeated_phrases(assets_words)
                if deterministic_detections:
                    llm_intra = deterministic_detections
                    logger.info(
                        f"🔍 [TRANSCRIPT-TRIGGER] Fallback determinístico: "
                        f"{len(deterministic_detections)} asset(s) com repetições detectadas "
                        f"(LLM não detectou)"
                    )
            except Exception as det_err:
                logger.warning(
                    f"⚠️ [TRANSCRIPT-TRIGGER] Fallback determinístico falhou: {det_err}"
                )

        if llm_intra:
            try:
                from app.services.intra_retake_resolver import resolve_intra_retakes

                resolved = resolve_intra_retakes(llm_intra, assets_words)

                if resolved:
                    # Substituir detecções semânticas por segmentos determinísticos
                    result['intra_retakes'] = resolved
                    logger.info(
                        f"🔧 [TRANSCRIPT-TRIGGER] IntraRetakeResolver: "
                        f"{len(resolved)} asset(s) com segmentos resolvidos"
                    )
                else:
                    # Resolver não encontrou matches — limpar para não propagar dados ruins
                    result['intra_retakes'] = []
                    logger.warning(
                        f"⚠️ [TRANSCRIPT-TRIGGER] Resolver não encontrou matches, "
                        f"limpando intra_retakes"
                    )
            except Exception as resolver_err:
                logger.warning(
                    f"⚠️ [TRANSCRIPT-TRIGGER] IntraRetakeResolver falhou: "
                    f"{resolver_err} — mantendo sem intra_retakes"
                )
                result['intra_retakes'] = []

        # ─── 6. Persistir resultado ───────────────────────────────────
        _persist_analysis_result(project_id, result)

        # ─── 6b. Registrar custos ─────────────────────────────────────
        try:
            from app.services.ai_cost_tracker import log_ai_usage
            log_ai_usage(
                service_type="transcript_analysis_llm",
                provider="openai",
                model=result.get('model', 'gpt-4o-mini'),
                project_id=project_id,
                conversation_id=conversation_id,
                tokens_in=result.get('tokens_in', 0),
                tokens_out=result.get('tokens_out', 0),
                duration_ms=result.get('llm_time_ms', 0),
                input_units=len(assets_with_transcriptions),
                metadata={
                    'total_assets': len(assets_with_transcriptions),
                    'total_words': total_words,
                    'format_detected': format_detected,
                    'sound_bites_found': len(result.get('sound_bites', [])),
                },
            )
        except Exception as cost_err:
            logger.warning(
                f"⚠️ [TRANSCRIPT-TRIGGER] Cost tracking: {cost_err}"
            )

        # ─── 7. NÃO enviar mensagem individual no chat ─────────────
        # Os resultados ficam salvos em project_config.
        # A mensagem consolidada será enviada pelo VideoClipper/ScriptGenerator.
        # Apenas enviar SSE de progresso (sem criar mensagem no banco).
        try:
            from app.routes.chat_sse import emit_chat_event
            emit_chat_event(conversation_id, 'analysis_progress', {
                'type': 'transcript_complete',
                'assets_analyzed': len(assets_with_transcriptions),
                'sound_bites': len(result.get('sound_bites', [])),
                'elapsed_s': round(elapsed, 1),
            })
        except Exception:
            pass

        logger.info(
            f"✅ [TRANSCRIPT-TRIGGER] Completo em {elapsed:.1f}s | "
            f"{len(assets_with_transcriptions)} assets | "
            f"~{total_words} palavras | "
            f"sound_bites={len(result.get('sound_bites', []))}"
        )

        # 🆕 11/Fev/2026: Atualizar Analysis Visualizer (step: transcript_analysis → complete)
        if batch_id:
            try:
                from app.routes.visualizer_persistence import update_batch_final_step
                update_batch_final_step(
                    conversation_id, batch_id, 'transcript_analysis', 'complete',
                    f"Transcrições analisadas ({elapsed:.0f}s) — {len(assets_with_transcriptions)} assets, ~{total_words} palavras"
                )
            except Exception as viz_err:
                logger.warning(f"⚠️ [TRANSCRIPT-TRIGGER] Erro ao completar visualizer: {viz_err}")

        # ─── 8. Disparar Title Director (assíncrono) ─────────
        # O título é gerado em paralelo enquanto os próximos passos rodam.
        # Se falhar, não bloqueia — título é optional no pipeline.
        try:
            from app.services.title_director_trigger import trigger_title_director_async
            trigger_title_director_async(project_id, conversation_id)
        except Exception as title_err:
            logger.warning(
                f"⚠️ [TRANSCRIPT-TRIGGER] Title Director: {title_err}"
            )

        # ─── 9. Verificar se VideoClipper pode ser disparado ────
        # Se o Vision Director também já completou, gerar EDL
        try:
            from app.services.video_clipper_trigger import check_and_trigger_clipper
            check_and_trigger_clipper(project_id, conversation_id)
        except Exception as clipper_err:
            logger.warning(
                f"⚠️ [TRANSCRIPT-TRIGGER] Clipper check: {clipper_err}"
            )

    except Exception as e:
        elapsed = time.time() - t0
        logger.error(
            f"❌ [TRANSCRIPT-TRIGGER] Falha após {elapsed:.1f}s: {e}",
            exc_info=True,
        )
        _send_error_message(
            conversation_id,
            "Ocorreu um erro durante a análise de transcrições."
        )


# ═══════════════════════════════════════════════════════════════
# HELPERS — DB
# ═══════════════════════════════════════════════════════════════


def _get_triage_transcript_ids(project_id: str) -> List[str]:
    """
    Busca IDs dos assets pipeline_ready da triagem.

    Lê project_config.asset_triage_result.routing.pipeline_ready
    para saber quais assets precisam de análise de transcrição.
    """
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT project_config->'asset_triage_result'->'routing'->'pipeline_ready'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row or not row[0]:
            return []

        pipeline_ids = row[0] if isinstance(row[0], list) else json.loads(row[0])

        if isinstance(pipeline_ids, list) and len(pipeline_ids) > 0:
            return [str(pid) for pid in pipeline_ids]

        return []
    except Exception as e:
        logger.warning(
            f"⚠️ [TRANSCRIPT-TRIGGER] Erro ao ler triage routing: {e}"
        )
        return []


def _get_triage_data(project_id: str) -> Optional[dict]:
    """Busca resultado completo da triagem para retakes e format_detected."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT project_config->'asset_triage_result'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row or not row[0]:
            return None

        data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        return data
    except Exception as e:
        logger.warning(
            f"⚠️ [TRANSCRIPT-TRIGGER] Erro ao ler triage data: {e}"
        )
        return None


def _get_assets_with_transcriptions(
    project_id: str,
    asset_ids: List[str],
) -> List[dict]:
    """Busca transcrições completas dos assets selecionados."""
    if not asset_ids:
        return []

    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Usar IN clause para buscar só os assets necessários
        placeholders = ','.join(['%s'] * len(asset_ids))
        cursor.execute(f"""
            SELECT id, file_path, asset_type, metadata, created_at
            FROM project_assets
            WHERE project_id = %s AND id IN ({placeholders})
            ORDER BY created_at ASC
        """, [project_id] + asset_ids)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        assets = []
        for row in rows:
            metadata = row[3] if row[3] else {}

            # Extrair transcrição COMPLETA
            transcription_text = ""
            trans_result = metadata.get("transcription_result", {})
            if isinstance(trans_result, dict):
                transcription_text = (
                    trans_result.get("transcript", "")
                    or trans_result.get("text", "")
                    or ""
                )
            elif isinstance(trans_result, str):
                transcription_text = trans_result

            # Extrair duração
            duration_ms = metadata.get("duration_ms", 0)
            if not duration_ms:
                duration_ms = metadata.get("duration", 0)
            if not duration_ms and isinstance(trans_result, dict):
                duration_s = trans_result.get("duration_s", 0)
                if duration_s:
                    duration_ms = int(float(duration_s) * 1000)

            # Word timestamps (se disponíveis)
            words = []
            if isinstance(trans_result, dict):
                words = trans_result.get("words", [])

            # Filename
            file_path = row[1] or ''
            original_name = (
                metadata.get("original_name") or file_path.split('/')[-1]
            )

            # Pular assets sem transcrição
            if not transcription_text:
                logger.warning(
                    f"⚠️ [TRANSCRIPT-TRIGGER] Asset {str(row[0])[:8]} "
                    f"sem transcrição, pulando"
                )
                continue

            assets.append({
                'id': str(row[0]),
                'file_path': file_path,
                'asset_type': row[2],
                'filename': original_name,
                'transcription_text': transcription_text,
                'duration_ms': duration_ms,
                'words': words,
                'created_at': str(row[4]) if row[4] else None,
            })

        return assets
    except Exception as e:
        logger.error(
            f"❌ [TRANSCRIPT-TRIGGER] Erro ao buscar transcrições: {e}"
        )
        return []


def _persist_analysis_result(project_id: str, result: dict) -> None:
    """Salva resultado em project_config.transcript_analysis_result."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        result_json = json.dumps(result, ensure_ascii=False)
        cursor.execute("""
            UPDATE projects
            SET project_config = jsonb_set(
                COALESCE(project_config, '{}'::jsonb),
                '{transcript_analysis_result}',
                %s::jsonb
            )
            WHERE project_id = %s
        """, (result_json, project_id))
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"💾 [TRANSCRIPT-TRIGGER] Resultado salvo em project_config")
    except Exception as e:
        logger.error(
            f"❌ [TRANSCRIPT-TRIGGER] Erro ao persistir: {e}"
        )


# ═══════════════════════════════════════════════════════════════
# FORMATAÇÃO DA MENSAGEM NO CHAT
# ═══════════════════════════════════════════════════════════════


def _format_analysis_message(result: dict, elapsed: float) -> str:
    """Formata resultado como mensagem legível no chat."""
    n = result.get('total_assets_analyzed', 0)
    lines = [
        f"**Análise de Transcrição Concluída** ({elapsed:.0f}s) — "
        f"{n} vídeo{'s' if n > 1 else ''}",
        "",
    ]

    # Narrativa
    narrative = result.get('narrative_analysis', {})
    main_theme = narrative.get('main_theme', '')
    if main_theme:
        lines.append(f"**Tema principal:** {main_theme}")

    themes = narrative.get('themes', [])
    if themes:
        theme_names = [t.get('theme', '') for t in themes if t.get('theme')]
        if theme_names:
            lines.append(f"**Temas:** {', '.join(theme_names)}")
    lines.append("")

    # Sound bites
    sound_bites = result.get('sound_bites', [])
    if sound_bites:
        lines.append(f"**Sound Bites ({len(sound_bites)}):**")
        for sb in sound_bites[:5]:  # Max 5 no chat
            text = sb.get('text', '')[:80]
            strength = sb.get('strength', '')
            icon = '🔥' if strength == 'high' else '💡'
            lines.append(f"  {icon} _{text}_")
        if len(sound_bites) > 5:
            lines.append(f"  ... e mais {len(sound_bites) - 5}")
        lines.append("")

    # Retakes refinados
    retakes = result.get('retakes_refined', [])
    if retakes:
        lines.append(f"**Retakes refinados ({len(retakes)}):**")
        for r in retakes:
            rec = r.get('recommendation', 'review_both')
            reason = r.get('reason', '')[:80]
            rec_labels = {
                'use_latest': 'usar mais recente',
                'use_first': 'usar primeiro',
                'review_both': 'revisar ambos',
            }
            lines.append(
                f"  - Recomendação: {rec_labels.get(rec, rec)} — {reason}"
            )
        lines.append("")

    # Ordem
    order = result.get('order_analysis', {})
    if order.get('reorder_needed'):
        lines.append(
            f"**Reordenação sugerida:** {order.get('reason', '')[:100]}"
        )
        lines.append("")

    # Per-asset resumo
    per_asset = result.get('per_asset_analysis', [])
    if per_asset:
        lines.append(f"**Análise por vídeo ({len(per_asset)}):**")
        for pa in per_asset:
            name = pa.get('speaker_name', 'Desconhecido')
            topics = pa.get('topics', [])
            quality = pa.get('quality', {})
            fluency = quality.get('fluency', 0)
            energy = quality.get('energy', 'medium')
            usable = len(pa.get('usable_segments', []))
            dead = len(pa.get('dead_segments', []))

            topic_str = ', '.join(topics[:3]) if topics else 'N/A'
            lines.append(
                f"  - **{name}**: {topic_str} | "
                f"Fluência: {fluency:.0%} | Energia: {energy} | "
                f"{usable} trecho(s) útil(is), {dead} pausas"
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


def _send_analysis_message(
    conversation_id: str,
    content: str,
    analysis_result: dict,
) -> None:
    """Salva mensagem + emite SSE."""
    try:
        from app.db import get_db_connection
        msg_id = str(uuid.uuid4())
        conn = get_db_connection()
        cursor = conn.cursor()

        component_props = {
            'transcript_analysis': True,
            'total_assets': analysis_result.get('total_assets_analyzed', 0),
            'processing_time_ms': analysis_result.get('processing_time_ms', 0),
            'sound_bites_count': len(analysis_result.get('sound_bites', [])),
            'has_retakes': len(analysis_result.get('retakes_refined', [])) > 0,
            'reorder_needed': analysis_result.get(
                'order_analysis', {}
            ).get('reorder_needed', False),
        }

        cursor.execute("""
            INSERT INTO chatbot_messages
                (id, message_id, conversation_id, sender, content,
                 component_type, component_props, created_at)
            VALUES (%s, %s, %s, 'bot', %s, %s, %s, NOW())
        """, (
            msg_id, msg_id, conversation_id, content,
            'transcript_analysis_result',
            json.dumps(component_props),
        ))
        conn.commit()
        cursor.close()
        conn.close()

        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': msg_id,
            'sender': 'bot',
            'content': content,
            'component_type': 'transcript_analysis_result',
            'component_props': component_props,
        })

        logger.info(
            f"📡 [TRANSCRIPT-TRIGGER] Mensagem enviada: {msg_id[:8]}..."
        )
    except Exception as e:
        logger.error(
            f"❌ [TRANSCRIPT-TRIGGER] Erro ao enviar mensagem: {e}"
        )


def _send_error_message(conversation_id: str, content: str) -> None:
    """Envia mensagem de erro no chat."""
    _send_analysis_message(conversation_id, content, {})


def _send_info_message(conversation_id: str, content: str) -> None:
    """Envia mensagem informativa (sem componente especial)."""
    _send_analysis_message(conversation_id, content, {})


def _send_typing_message(
    conversation_id: str, message: str = ""
) -> None:
    """Envia indicador de digitação."""
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'typing', {
            'sender': 'bot',
            'message': message or 'Analisando transcrições com IA...',
        })
    except Exception as e:
        logger.warning(
            f"⚠️ [TRANSCRIPT-TRIGGER] Erro typing: {e}"
        )
