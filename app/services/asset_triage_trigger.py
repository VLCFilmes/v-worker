"""
📋 Asset Triage Trigger — Dispara classificação de uploads assíncrona.

Quando o usuário ativa "Classificar Uploads" no chatbot:
  1. Busca TODOS os assets do projeto (vídeos, áudios, imagens)
  2. Gera URLs temporárias do B2 para vídeos
  3. Coleta transcrições existentes de project_assets.metadata
  4. Extrai 1 frame por vídeo (ffmpeg direto da URL, sem Modal)
  5. Chama GPT-4o-mini com frames + transcrições (1 chamada única)
  6. Classifica, detecta retakes, verifica ordem
  7. Salva em project_config.asset_triage_result
  8. Envia diagnóstico no chat via SSE

Autor: Claude + Vinicius
Data: 08/Fev/2026
"""

import logging
import threading
import json
import uuid
import time
from typing import Optional, List

logger = logging.getLogger(__name__)


def trigger_asset_triage_async(
    project_id: str,
    conversation_id: str,
    batch_id: str = None,
) -> None:
    """Dispara triagem de assets em background thread."""
    thread = threading.Thread(
        target=_run_asset_triage,
        args=(project_id, conversation_id, batch_id),
        daemon=True,
        name=f"asset-triage-{project_id[:8]}",
    )
    thread.start()
    logger.info(
        f"📋 [TRIAGE-TRIGGER] Background thread iniciada: "
        f"project={project_id[:8]}... conv={conversation_id[:8]}... "
        f"batch={batch_id[:8] if batch_id else 'none'}"
    )


def _run_asset_triage(
    project_id: str,
    conversation_id: str,
    batch_id: str = None,
) -> None:
    """Executa a triagem completa (roda em background thread)."""
    t0 = time.time()

    try:
        # ─── 0a. Resolver batch_id se não foi passado ───
        if not batch_id:
            batch_id = _resolve_batch_id(conversation_id)
            if batch_id:
                logger.info(f"📋 [TRIAGE-TRIGGER] batch_id resolvido: {batch_id[:8]}")

        # ─── 0b. Guard: Lock atômico para evitar execução concorrente ───
        # Usa UPDATE ... SET flag = true WHERE flag IS NULL para garantir
        # que apenas UMA thread executa a triage por projeto.
        # Também verifica se o resultado já existe (re-check duplicado).
        try:
            from app.db import get_db_connection
            conn = get_db_connection()
            cur = conn.cursor()

            # 1) Verificar se resultado já existe
            cur.execute(
                "SELECT project_config->>'asset_triage_result' FROM projects WHERE project_id = %s",
                (project_id,)
            )
            row = cur.fetchone()
            if row and row[0] and row[0] != 'null':
                cur.close()
                conn.close()
                logger.info(
                    f"⏭️ [TRIAGE-TRIGGER] Triage já executada para project={project_id[:8]}, "
                    f"pulando re-execução (guard: resultado existe)"
                )
                return

            # 2) Lock atômico: tentar setar flag "triage_running"
            # Só prosseguir se conseguir setar (ou se já estiver setada por nós)
            cur.execute("""
                UPDATE projects
                SET project_config = jsonb_set(
                    COALESCE(project_config, '{}'::jsonb),
                    '{asset_triage_running}',
                    'true'::jsonb
                )
                WHERE project_id = %s
                AND (
                    project_config->>'asset_triage_running' IS NULL
                    OR project_config->>'asset_triage_running' = 'false'
                )
                RETURNING project_id
            """, (project_id,))
            locked = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()

            if not locked:
                logger.info(
                    f"⏭️ [TRIAGE-TRIGGER] Triage já em execução para project={project_id[:8]}, "
                    f"pulando (guard: lock atômico)"
                )
                return
        except Exception as guard_err:
            logger.warning(f"⚠️ [TRIAGE-TRIGGER] Falha no guard de deduplicação: {guard_err}")
            # Continuar mesmo se o guard falhar — melhor duplicar do que não executar

        # ─── 1. Buscar TODOS os assets do projeto ────────────
        assets = _get_project_assets(project_id)
        if not assets:
            _send_error_message(
                conversation_id,
                "Não encontrei uploads no projeto para classificar. "
                "Faça upload de vídeos ou áudios primeiro."
            )
            return

        video_count = sum(1 for a in assets if a.get('asset_type') == 'video')
        audio_count = sum(1 for a in assets if a.get('asset_type') == 'audio')
        image_count = sum(1 for a in assets if a.get('asset_type') == 'image')
        logger.info(
            f"📋 [TRIAGE-TRIGGER] {len(assets)} asset(s): "
            f"{video_count} vídeo(s), {audio_count} áudio(s), "
            f"{image_count} imagem(ns)"
        )

        # ─── 2. Gerar URLs do B2 para vídeos ─────────────────
        video_urls = {}
        for asset in assets:
            if asset.get('asset_type') == 'video':
                url = _generate_download_url(
                    asset['bucket'], asset['file_path']
                )
                if url:
                    video_urls[asset['id']] = url
                else:
                    logger.warning(
                        f"⚠️ [TRIAGE-TRIGGER] Falha URL: "
                        f"{asset['file_path'][:60]}"
                    )

        # ─── 3. SSE: triage iniciando ─────────────────────────
        n = len(assets)
        _send_triage_sse(conversation_id, 'asset_triage_start', {
            'total_assets': n,
            'video_count': video_count,
            'audio_count': audio_count,
            'message': f"Classificando {n} upload{'s' if n > 1 else ''}...",
        })
        # 🆕 v4.7.1: Emitir upload_step para o visualizer mostrar triage
        _send_triage_sse(conversation_id, 'upload_step_start', {
            'step': 'triage',
            'message': f"Classificando {n} upload{'s' if n > 1 else ''} com IA...",
            'metadata': {
                'total_assets': n,
                'video_count': video_count,
                'audio_count': audio_count,
                'image_count': image_count,
            },
        })
        # 🐛 FIX v4.7.2: Atualizar batch visualizer no banco via update_batch_final_step
        # Sem isso, o visualizer persistido no banco não mostra triage/vision steps
        if batch_id:
            try:
                from app.routes.visualizer_persistence import update_batch_final_step
                update_batch_final_step(
                    conversation_id, batch_id, 'triage', 'active',
                    f"Classificando {n} upload{'s' if n > 1 else ''} com IA..."
                )
            except Exception as viz_err:
                logger.warning(f"⚠️ [TRIAGE-TRIGGER] Erro ao atualizar visualizer: {viz_err}")
        _send_typing_message(
            conversation_id,
            f"Classificando {n} upload{'s' if n > 1 else ''} com IA..."
        )

        # ─── 4. Executar triagem ──────────────────────────────
        from app.services.asset_triage_service import get_asset_triage_service
        service = get_asset_triage_service()

        result = service.analyze(
            assets=assets,
            video_urls=video_urls,
            project_id=project_id,
        )

        elapsed = time.time() - t0

        if result.get('status') != 'success':
            _send_error_message(
                conversation_id,
                f"Erro na classificação: {result.get('error', 'desconhecido')}"
            )
            return

        # ─── 5. Persistir resultado ──────────────────────────
        _persist_triage_result(project_id, result)

        # ─── 5b. Registrar custos ────────────────────────────
        try:
            from app.services.ai_cost_tracker import log_ai_usage
            # Custo do LLM
            log_ai_usage(
                service_type="triage_llm",
                provider="openai",
                model=result.get('model', 'gpt-4o-mini'),
                project_id=project_id,
                conversation_id=conversation_id,
                tokens_in=result.get('tokens_in', 0),
                tokens_out=result.get('tokens_out', 0),
                duration_ms=result.get('llm_time_ms', 0),
                input_units=len(assets),
                metadata={
                    'total_assets': len(assets),
                    'frames_extracted': result.get('frames_extracted', 0),
                    'format_detected': result.get('format_detected'),
                },
            )
            # Custo da extração de frames (se houve fallback)
            frame_ms = result.get('frame_extraction_ms', 0)
            if frame_ms > 0 and result.get('frames_extracted', 0) > 0:
                log_ai_usage(
                    service_type="triage_frames",
                    provider="ffmpeg_local",
                    model="ffmpeg",
                    project_id=project_id,
                    conversation_id=conversation_id,
                    duration_ms=frame_ms,
                    input_units=result.get('frames_extracted', 0),
                    cost_usd=0.0,  # ffmpeg local = sem custo externo
                    metadata={'fallback_extraction': True},
                )
        except Exception as cost_err:
            logger.warning(f"⚠️ [TRIAGE-TRIGGER] Erro cost tracking: {cost_err}")

        # ─── 6. SSE: triage completa ─────────────────────────
        _send_triage_sse(conversation_id, 'asset_triage_complete', {
            'total_assets': len(assets),
            'format_detected': result.get('format_detected', 'unknown'),
            'processing_time_ms': result.get('processing_time_ms', 0),
        })
        # 🆕 v4.7.1: Emitir upload_step_complete para o visualizer
        _send_triage_sse(conversation_id, 'upload_step_complete', {
            'step': 'triage',
            'message': f"Classificação concluída ({elapsed:.0f}s)",
            'metadata': {
                'format_detected': result.get('format_detected', 'unknown'),
                'total_assets': len(assets),
                'elapsed_s': round(elapsed, 1),
                'model': result.get('model', '?'),
                'tokens_in': result.get('tokens_in', 0),
                'tokens_out': result.get('tokens_out', 0),
                'llm_time_ms': result.get('llm_time_ms', 0),
                'frame_extraction_ms': result.get('frame_extraction_ms', 0),
            },
        })
        # 🐛 FIX v4.7.2: Persistir step no banco
        if batch_id:
            try:
                from app.routes.visualizer_persistence import update_batch_final_step
                update_batch_final_step(
                    conversation_id, batch_id, 'triage', 'complete',
                    f"Classificação concluída ({elapsed:.0f}s)"
                )
            except Exception as viz_err:
                logger.warning(f"⚠️ [TRIAGE-TRIGGER] Erro ao completar visualizer: {viz_err}")

        # ─── 7. Enviar formulário editável no chat ───────────
        summary = _format_triage_message(result, elapsed)
        _send_triage_message(conversation_id, summary, result)

        # Limpar lock atômico (resultado salvo já serve como guard permanente)
        _clear_triage_lock(project_id)

        logger.info(
            f"✅ [TRIAGE-TRIGGER] Completo em {elapsed:.1f}s | "
            f"{len(assets)} assets | format={result.get('format_detected')}"
        )

    except Exception as e:
        elapsed = time.time() - t0
        logger.error(
            f"❌ [TRIAGE-TRIGGER] Falha após {elapsed:.1f}s: {e}",
            exc_info=True,
        )
        _send_error_message(
            conversation_id,
            "Ocorreu um erro durante a classificação dos uploads."
        )
        # Limpar lock atômico em caso de erro
        _clear_triage_lock(project_id)


# ═══════════════════════════════════════════════════════════════
# HELPERS — DB
# ═══════════════════════════════════════════════════════════════


def _get_project_assets(project_id: str) -> List[dict]:
    """Busca TODOS os assets do projeto com transcrições existentes."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, file_path, bucket, asset_type, metadata,
                   created_at
            FROM project_assets
            WHERE project_id = %s
            ORDER BY created_at ASC
        """, (project_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        assets = []
        for row in rows:
            metadata = row[4] if row[4] else {}

            # Extrair transcrição se existir
            # AssemblyAI salva como "transcript", não "text"
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

            # Extrair duração se existir
            # Pode estar em metadata.duration_ms, metadata.duration,
            # ou transcription_result.duration_s (AssemblyAI)
            duration_ms = metadata.get("duration_ms", 0)
            if not duration_ms:
                duration_ms = metadata.get("duration", 0)
            if not duration_ms and isinstance(trans_result, dict):
                duration_s = trans_result.get("duration_s", 0)
                if duration_s:
                    duration_ms = int(float(duration_s) * 1000)

            # Derivar filename do file_path (coluna file_name não existe)
            file_path = row[1] or ''
            original_name = metadata.get("original_name") or file_path.split('/')[-1]

            assets.append({
                'id': str(row[0]),
                'file_path': file_path,
                'bucket': row[2],
                'asset_type': row[3],
                'metadata': metadata,
                'filename': original_name,
                'created_at': str(row[5]) if row[5] else None,
                'transcription_text': transcription_text,
                'duration_ms': duration_ms,
            })

        return assets
    except Exception as e:
        logger.error(f"❌ [TRIAGE-TRIGGER] Erro ao buscar assets: {e}")
        return []


def _generate_download_url(
    bucket_name: str, file_path: str
) -> Optional[str]:
    """Gera URL temporária do B2 (1 hora)."""
    try:
        from app.routes.upload import generate_temp_download_url_internal
        return generate_temp_download_url_internal(
            bucket_name=bucket_name,
            file_path=file_path,
            duration_seconds=3600,
        )
    except Exception as e:
        logger.error(f"❌ [TRIAGE-TRIGGER] Erro URL: {e}")
        return None


def _persist_triage_result(project_id: str, result: dict) -> None:
    """Salva resultado em project_config.asset_triage_result."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        result_json = json.dumps(result, ensure_ascii=False)
        cursor.execute("""
            UPDATE projects
            SET project_config = jsonb_set(
                COALESCE(project_config, '{}'::jsonb),
                '{asset_triage_result}',
                %s::jsonb
            )
            WHERE project_id = %s
        """, (result_json, project_id))
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"💾 [TRIAGE-TRIGGER] Resultado salvo em project_config")
    except Exception as e:
        logger.error(f"❌ [TRIAGE-TRIGGER] Erro ao persistir: {e}")


# ═══════════════════════════════════════════════════════════════
# FORMATAÇÃO DA MENSAGEM NO CHAT
# ═══════════════════════════════════════════════════════════════


def _format_triage_message(result: dict, elapsed: float) -> str:
    """Formata resultado da triagem como mensagem legível no chat."""
    type_labels = {
        'talking_head': 'Talking Head (fala)',
        'b_roll': 'B-Roll (cobertura)',
        'audio_narration': 'Narração (áudio)',
        'screen_capture': 'Captura de Tela',
        'image_static': 'Imagem Estática',
        'music_only': 'Música / Efeitos',
        'briefing_prompt': 'Briefing / Referência',
    }

    format_labels = {
        'talking_head_solo': 'Talking Head Solo',
        'multi_interview': 'Entrevista Múltipla',
        'narration_broll': 'Narração + B-Roll',
        'humor_dialogue': 'Diálogo / Humor',
        'tutorial': 'Tutorial',
        'mixed': 'Conteúdo Misto',
        'unknown': 'Não determinado',
    }

    lines = [
        f"**Classificação de Uploads** ({elapsed:.0f}s)",
        "",
    ]

    # Formato detectado
    fmt = result.get('format_detected', 'unknown')
    fmt_label = format_labels.get(fmt, fmt)
    lines.append(f"**Formato:** {fmt_label}")
    lines.append("")

    # Assets classificados
    assets_list = result.get('assets', [])
    if assets_list:
        lines.append(f"**{len(assets_list)} upload(s):**")
        for a in assets_list:
            cls = a.get('classification', 'unknown')
            label = type_labels.get(cls, cls)
            conf = a.get('confidence', 0)
            filename = a.get('filename', '?')
            notes = a.get('notes', '')
            lines.append(
                f"  - **{filename}** → {label} ({conf * 100:.0f}%)"
            )
            if notes:
                lines.append(f"    _{notes}_")
        lines.append("")

    # Retakes
    retakes = result.get('retakes', [])
    if retakes:
        lines.append(f"**Retakes detectados ({len(retakes)}):**")
        for r in retakes:
            reason = r.get('reason', '')
            recommendation = r.get('recommendation', 'review_both')
            rec_labels = {
                'use_latest': 'usar o mais recente',
                'use_first': 'usar o primeiro',
                'review_both': 'revisar ambos',
            }
            rec = rec_labels.get(recommendation, recommendation)
            lines.append(f"  - Recomendação: {rec} — {reason}")
        lines.append("")

    # Ordem
    order = result.get('order', {})
    if order.get('reorder_needed'):
        lines.append(
            f"**Reordenação sugerida:** {order.get('reason', '')}"
        )
        lines.append("")

    # Roteamento
    routing = result.get('routing', {})
    vision_ids = routing.get('vision_analysis', [])
    if vision_ids:
        lines.append(
            f"**{len(vision_ids)} arquivo(s) precisam análise visual (RAFT)**"
        )
        lines.append("")

    # Resumo
    summary = result.get('summary', '')
    if summary:
        lines.append(f"_{summary}_")
        lines.append("")

    # Métricas
    llm_ms = result.get('llm_time_ms', 0)
    frame_ms = result.get('frame_extraction_ms', 0)
    model = result.get('model', '?')
    tokens_in = result.get('tokens_in', 0)
    tokens_out = result.get('tokens_out', 0)

    lines.append(
        f"_Frames: {frame_ms}ms | "
        f"LLM: {model} ({llm_ms}ms) | "
        f"Tokens: {tokens_in}→{tokens_out}_"
    )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# HELPER — LOCK ATÔMICO
# ═══════════════════════════════════════════════════════════════


def _clear_triage_lock(project_id: str) -> None:
    """Remove o flag asset_triage_running do project_config."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE projects
            SET project_config = project_config - 'asset_triage_running'
            WHERE project_id = %s
        """, (project_id,))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.warning(f"⚠️ [TRIAGE-TRIGGER] Erro ao limpar lock: {e}")


# ═══════════════════════════════════════════════════════════════
# HELPER — RESOLVER BATCH_ID
# ═══════════════════════════════════════════════════════════════


def _resolve_batch_id(conversation_id: str) -> Optional[str]:
    """
    Resolve o batch_id mais recente para uma conversa.
    Busca no upload_visualizer persistido no chatbot_messages.
    """
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT component_props->>'batch_id'
            FROM chatbot_messages
            WHERE conversation_id = %s
            AND component_type = 'upload_visualizer'
            ORDER BY created_at DESC
            LIMIT 1
        """, (conversation_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0]:
            return row[0]
        return None
    except Exception as e:
        logger.warning(f"⚠️ [TRIAGE-TRIGGER] Erro ao resolver batch_id: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# HELPERS — SSE / MENSAGENS
# ═══════════════════════════════════════════════════════════════


def _send_triage_message(
    conversation_id: str,
    content: str,
    triage_result: dict,
) -> None:
    """Salva mensagem + emite SSE com formulário editável + botões de ação."""
    try:
        from app.db import get_db_connection
        msg_id = str(uuid.uuid4())
        conn = get_db_connection()
        cursor = conn.cursor()

        # Montar dados do formulário editável para o frontend
        # Cada asset tem classificação + opções de correção
        classification_options = [
            {"value": "talking_head", "label": "Talking Head (fala)"},
            {"value": "b_roll", "label": "B-Roll (cobertura)"},
            {"value": "audio_narration", "label": "Narração (áudio)"},
            {"value": "screen_capture", "label": "Captura de Tela"},
            {"value": "image_static", "label": "Imagem Estática"},
            {"value": "music_only", "label": "Música / Efeitos"},
            {"value": "briefing_prompt", "label": "Briefing / Referência"},
        ]

        # Botões de ação pós-triagem (determinístico via RoutingValidator)
        from app.services.routing_validator import get_vision_asset_count
        vision_count = get_vision_asset_count(triage_result.get('assets', []))

        # Contar assets para análise de transcrição
        from app.services.routing_validator import get_transcript_asset_count
        transcript_count = get_transcript_asset_count(triage_result.get('assets', []))

        action_buttons = []
        if vision_count > 0 or transcript_count > 0:
            # Botão único dispara AMBAS análises em paralelo
            label_parts = []
            if vision_count > 0:
                label_parts.append(f"{vision_count} visual")
            if transcript_count > 0:
                label_parts.append(f"{transcript_count} transcrição")
            analysis_label = f"Prosseguir para Análise ({' + '.join(label_parts)})"

            action_buttons.append({
                "label": analysis_label,
                "text": "Prosseguir para Análise Visual",
                "field": "vision_analysis",
                "value": True,
                "action": "activate_vision",
            })
        action_buttons.append({
            "label": "Ver Templates",
            "text": "Ver Templates",
            "action": "open_templates",
        })

        component_props = {
            'asset_triage': True,
            'editable': True,
            'total_assets': triage_result.get('total_assets', 0),
            'format_detected': triage_result.get('format_detected', 'unknown'),
            'processing_time_ms': triage_result.get('processing_time_ms', 0),
            'classification_options': classification_options,
            'assets': triage_result.get('assets', []),
            'retakes': triage_result.get('retakes', []),
            'order': triage_result.get('order', {}),
            'routing': triage_result.get('routing', {}),
            'summary': triage_result.get('summary', ''),
            'actionButtons': action_buttons,
        }

        cursor.execute("""
            INSERT INTO chatbot_messages
                (id, message_id, conversation_id, sender, content,
                 component_type, component_props, created_at)
            VALUES (%s, %s, %s, 'bot', %s, %s, %s, NOW())
        """, (
            msg_id, msg_id, conversation_id, content,
            'asset_triage_form', json.dumps(component_props),
        ))
        conn.commit()
        cursor.close()
        conn.close()

        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': msg_id,
            'sender': 'bot',
            'content': content,
            'component_type': 'asset_triage_form',
            'component_props': component_props,
            'buttons': action_buttons,
        })

        logger.info(f"📡 [TRIAGE-TRIGGER] Formulário enviado: {msg_id[:8]}...")
    except Exception as e:
        logger.error(f"❌ [TRIAGE-TRIGGER] Erro ao enviar mensagem: {e}")


def _send_error_message(conversation_id: str, content: str) -> None:
    """Envia mensagem de erro no chat."""
    _send_triage_message(conversation_id, content, {})


def _send_triage_sse(
    conversation_id: str, event_type: str, data: dict
) -> None:
    """Emite evento SSE de progresso da triagem (para Upload Visualizer)."""
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, event_type, data)
    except Exception as e:
        logger.warning(f"⚠️ [TRIAGE-TRIGGER] Erro SSE {event_type}: {e}")


def _send_typing_message(
    conversation_id: str, message: str = ""
) -> None:
    """Envia indicador de digitação."""
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'typing', {
            'sender': 'bot',
            'message': message or 'Classificando uploads com IA...',
        })
    except Exception as e:
        logger.warning(f"⚠️ [TRIAGE-TRIGGER] Erro typing: {e}")
