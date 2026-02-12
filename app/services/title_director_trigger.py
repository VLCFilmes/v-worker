"""
🏷️ Title Director Trigger — Dispara geração de título assíncrona.

Fluxo:
  1. Busca transcript_analysis_result do project_config
  2. Busca user_inputs do chat (se houver instruções sobre título)
  3. Busca storytelling_mode e format_detected
  4. Chama TitleDirectorService.generate()
  5. Salva em project_config.title_director_result
  6. Registra custos via ai_cost_tracker
  7. Emite SSE de progresso

Trigger:
  - Após Transcript Director completar (auto-trigger)
  - Via chat quando usuário pede título
  - Via script_generator quando monta o editor de roteiro

Autor: Vinicius + Claude
Data: 11/Fev/2026
"""

import logging
import threading
import json
import time
from typing import Optional

logger = logging.getLogger(__name__)


def trigger_title_director_async(
    project_id: str,
    conversation_id: str,
    user_input: Optional[str] = None,
) -> None:
    """Dispara geração de título em background thread."""
    thread = threading.Thread(
        target=_run_title_director,
        args=(project_id, conversation_id, user_input),
        daemon=True,
        name=f"title-director-{project_id[:8]}",
    )
    thread.start()
    logger.info(
        f"🏷️ [TITLE-TRIGGER] Background thread iniciada: "
        f"project={project_id[:8]}... conv={conversation_id[:8]}... "
        f"user_input={'sim' if user_input else 'não'}"
    )


def generate_title_sync(
    project_id: str,
    conversation_id: str,
    user_input: Optional[str] = None,
) -> Optional[dict]:
    """
    Gera título de forma síncrona (para uso no script_generator).

    Returns:
        title_director_result dict, ou None se falhar
    """
    return _run_title_director(
        project_id, conversation_id, user_input,
        sync_mode=True,
    )


def _run_title_director(
    project_id: str,
    conversation_id: str,
    user_input: Optional[str] = None,
    sync_mode: bool = False,
) -> Optional[dict]:
    """Executa a geração de título (roda em background thread ou síncrono)."""
    t0 = time.time()

    try:
        # ─── 1. Buscar transcript_analysis_result ─────────────
        transcript_analysis = _get_transcript_analysis(project_id)

        if not transcript_analysis:
            logger.warning(
                f"⚠️ [TITLE-TRIGGER] Sem transcript_analysis_result "
                f"para project={project_id[:8]}. "
                f"Title Director precisa que o Transcript Director rode primeiro."
            )
            # Se não tem análise de transcrição, não podemos gerar título
            if not sync_mode:
                _send_info_message(
                    conversation_id,
                    "Não foi possível gerar título: análise de transcrição não disponível."
                )
            return None

        # ─── 2. Buscar contexto do projeto ────────────────────
        project_context = _get_project_context(project_id)
        storytelling_mode = project_context.get('storytelling_mode', 'talking_head')
        format_detected = project_context.get('format_detected', 'unknown')

        # ─── 3. Resolver idioma ───────────────────────────────
        try:
            from app.services.asset_triage_service import get_project_locale
            language = get_project_locale(project_id)
        except Exception:
            language = 'Portuguese (pt-BR)'

        # ─── 4. Gerar título (LLM) ───────────────────────────
        from app.services.title_director_service import get_title_director_service
        service = get_title_director_service()

        result = service.generate(
            transcript_analysis=transcript_analysis,
            storytelling_mode=storytelling_mode,
            format_detected=format_detected,
            language=language,
            user_input=user_input,
        )

        elapsed = time.time() - t0

        # ─── 5. Verificar resultado ──────────────────────────
        if result.get('status') != 'success':
            logger.warning(
                f"⚠️ [TITLE-TRIGGER] Geração falhou: "
                f"{result.get('error', 'desconhecido')}"
            )
            if not sync_mode:
                _send_info_message(
                    conversation_id,
                    f"Erro ao gerar título: {result.get('error', 'desconhecido')}"
                )
            return None

        # ─── 6. Persistir resultado ──────────────────────────
        _persist_title_result(project_id, result)

        # ─── 7. Registrar custos ─────────────────────────────
        try:
            from app.services.ai_cost_tracker import log_ai_usage
            log_ai_usage(
                service_type="title_director",
                provider="openai",
                model=result.get('model', 'gpt-4o-mini'),
                project_id=project_id,
                conversation_id=conversation_id,
                tokens_in=result.get('tokens_in', 0),
                tokens_out=result.get('tokens_out', 0),
                duration_ms=result.get('llm_time_ms', 0),
                input_units=1,
                metadata={
                    'source': result.get('source', 'transcript_analysis'),
                    'user_input_used': result.get('user_input_used', False),
                    'title_line_1': result.get('title', {}).get('line_1', '')[:50],
                },
            )
        except Exception as cost_err:
            logger.warning(
                f"⚠️ [TITLE-TRIGGER] Cost tracking: {cost_err}"
            )

        # ─── 8. Emitir SSE de progresso ──────────────────────
        if not sync_mode:
            try:
                from app.routes.chat_sse import emit_chat_event
                title = result.get('title', {})
                emit_chat_event(conversation_id, 'title_generated', {
                    'type': 'title_generated',
                    'title_line_1': title.get('line_1', ''),
                    'title_line_2': title.get('line_2', ''),
                    'elapsed_s': round(elapsed, 1),
                })
            except Exception:
                pass

        logger.info(
            f"✅ [TITLE-TRIGGER] Completo em {elapsed:.1f}s | "
            f"title=\"{result.get('title', {}).get('line_1', '')[:50]}\" | "
            f"model={result.get('model', '?')} | "
            f"tokens={result.get('tokens_in', 0)}→{result.get('tokens_out', 0)}"
        )

        return result

    except Exception as e:
        elapsed = time.time() - t0
        logger.error(
            f"❌ [TITLE-TRIGGER] Falha após {elapsed:.1f}s: {e}",
            exc_info=True,
        )
        if not sync_mode:
            _send_info_message(
                conversation_id,
                "Ocorreu um erro ao gerar o título do vídeo."
            )
        return None


# ═══════════════════════════════════════════════════════════════
# HELPERS — DB
# ═══════════════════════════════════════════════════════════════


def _get_transcript_analysis(project_id: str) -> Optional[dict]:
    """Busca transcript_analysis_result do project_config."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT project_config->'transcript_analysis_result'
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
            f"⚠️ [TITLE-TRIGGER] Erro ao ler transcript_analysis: {e}"
        )
        return None


def _get_project_context(project_id: str) -> dict:
    """Busca storytelling_mode e format_detected do projeto."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                project_config->>'storytelling_mode' as mode,
                project_config->'asset_triage_result'->>'format_detected' as format
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return {}

        return {
            'storytelling_mode': row[0] or 'talking_head',
            'format_detected': row[1] or 'unknown',
        }
    except Exception as e:
        logger.warning(
            f"⚠️ [TITLE-TRIGGER] Erro ao ler project context: {e}"
        )
        return {}


def _persist_title_result(project_id: str, result: dict) -> None:
    """Salva resultado em project_config.title_director_result."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        result_json = json.dumps(result, ensure_ascii=False)
        cursor.execute("""
            UPDATE projects
            SET project_config = jsonb_set(
                COALESCE(project_config, '{}'::jsonb),
                '{title_director_result}',
                %s::jsonb
            )
            WHERE project_id = %s
        """, (result_json, project_id))
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"💾 [TITLE-TRIGGER] Resultado salvo em project_config")
    except Exception as e:
        logger.error(
            f"❌ [TITLE-TRIGGER] Erro ao persistir: {e}"
        )


# ═══════════════════════════════════════════════════════════════
# HELPERS — SSE
# ═══════════════════════════════════════════════════════════════


def _send_info_message(conversation_id: str, content: str) -> None:
    """Envia mensagem informativa via SSE (sem salvar no banco)."""
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'typing', {
            'sender': 'bot',
            'message': content,
        })
    except Exception as e:
        logger.warning(
            f"⚠️ [TITLE-TRIGGER] Erro SSE: {e}"
        )
