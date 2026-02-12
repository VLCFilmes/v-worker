"""
🆕 v4.4.1: Asset Reprocess Service — Reprocessa pipeline com novos assets.

Quando o usuário envia novos b-rolls após o pipeline já ter renderizado,
este serviço orquestra:
1. Asset Triage (reclassifica todos os assets incluindo novos)
2. Vision Analysis (analisa visualmente os novos b-rolls)
3. Limpa cache do Video Clipper (força regeneração do EDL)
4. Replay from video_clipper (re-gera EDL + re-renderiza)

Anti-monolito: serviço dedicado, cada responsabilidade é delegada
ao serviço existente (triage trigger, vision trigger, replay endpoint).

Trigger: botão "Reprocessar com novos uploads" no chat.
Fluxo determinístico — sem LLM envolvida.
"""

import json
import logging
import threading
import uuid as uuid_module
from typing import Optional

logger = logging.getLogger(__name__)


def trigger_reprocess_async(
    project_id: str,
    conversation_id: str,
) -> None:
    """
    Dispara reprocessamento com novos assets em background thread.
    
    Args:
        project_id: ID do projeto
        conversation_id: ID da conversa (para SSE)
    """
    logger.info(
        f"🔄 [REPROCESS] Disparando reprocessamento async "
        f"project={project_id[:8]}, conv={conversation_id[:8]}"
    )
    thread = threading.Thread(
        target=_run_reprocess_pipeline,
        args=(project_id, conversation_id),
        daemon=True,
        name=f"reprocess_{project_id[:8]}",
    )
    thread.start()


def _run_reprocess_pipeline(
    project_id: str,
    conversation_id: str,
) -> None:
    """
    Pipeline de reprocessamento (roda em thread separada):
    
    1. Asset Triage → reclassifica todos os assets
    2. Vision Analysis → analisa b-rolls (novos e existentes)
    3. Limpa cache do Video Clipper overlay EDL
    4. Replay from video_clipper → re-gera EDL + re-renderiza
    """
    try:
        _emit_progress(conversation_id, "Classificando novos uploads...", step=1, total=4)

        # ═══ 1. Asset Triage (sync) ═══
        logger.info(f"🔄 [REPROCESS] Step 1/4: Asset Triage para project={project_id[:8]}")
        try:
            from app.services.asset_triage_trigger import _run_asset_triage
            _run_asset_triage(
                project_id=project_id,
                conversation_id=conversation_id,
            )
            logger.info(f"✅ [REPROCESS] Asset Triage concluída")
        except Exception as e:
            logger.error(f"❌ [REPROCESS] Asset Triage falhou: {e}")
            _emit_error(conversation_id, f"Erro na classificação dos uploads: {e}")
            return

        # ═══ 2. Vision Analysis (sync) ═══
        _emit_progress(conversation_id, "Analisando conteúdo visual dos vídeos...", step=2, total=4)
        logger.info(f"🔄 [REPROCESS] Step 2/4: Vision Analysis para project={project_id[:8]}")
        try:
            from app.services.vision_analysis_trigger import _run_vision_analysis
            _run_vision_analysis(
                project_id=project_id,
                conversation_id=conversation_id,
            )
            logger.info(f"✅ [REPROCESS] Vision Analysis concluída")
        except Exception as e:
            logger.error(f"❌ [REPROCESS] Vision Analysis falhou: {e}")
            _emit_error(conversation_id, f"Erro na análise visual: {e}")
            return

        # ═══ 3. Limpar cache do Video Clipper EDL ═══
        _emit_progress(conversation_id, "Preparando reposicionamento de b-rolls...", step=3, total=4)
        logger.info(f"🔄 [REPROCESS] Step 3/4: Limpando cache do Video Clipper EDL")
        _clear_video_clipper_cache(project_id)

        # ═══ 4. Replay from video_clipper ═══
        _emit_progress(conversation_id, "Remontando edição e re-renderizando...", step=4, total=4)
        logger.info(f"🔄 [REPROCESS] Step 4/4: Replay from video_clipper")

        job_id = _get_latest_completed_job_id(project_id)
        if not job_id:
            logger.error(f"❌ [REPROCESS] Nenhum job completo encontrado para project={project_id[:8]}")
            _emit_error(conversation_id, "Não foi possível encontrar o vídeo anterior para reprocessar.")
            return

        new_job_id = _trigger_replay(job_id, step_name="video_clipper")
        if not new_job_id:
            _emit_error(conversation_id, "Erro ao iniciar o reprocessamento do vídeo.")
            return

        logger.info(
            f"✅ [REPROCESS] Replay disparado: new_job={new_job_id[:8]}, "
            f"original_job={job_id[:8]}, target=video_clipper"
        )

        # Mensagem final — o pipeline engine vai emitir seus próprios SSE de progresso
        _emit_success(
            conversation_id,
            f"Reprocessamento iniciado! Os novos b-rolls estão sendo integrados ao vídeo. "
            f"Acompanhe o progresso acima."
        )

    except Exception as e:
        logger.error(f"❌ [REPROCESS] Pipeline falhou: {e}", exc_info=True)
        _emit_error(conversation_id, f"Erro inesperado no reprocessamento: {e}")


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def _get_latest_completed_job_id(project_id: str) -> Optional[str]:
    """Busca o job_id do último pipeline completo do projeto."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT job_id FROM video_processing_jobs
            WHERE project_id = %s
              AND status = 'completed'
              AND output_video_url IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            return row[0] if not isinstance(row, dict) else row.get('job_id')
        return None
    except Exception as e:
        logger.error(f"❌ [REPROCESS] Erro ao buscar job completo: {e}")
        return None


def _clear_video_clipper_cache(project_id: str) -> None:
    """
    Limpa o cache do Video Clipper overlay EDL no project_config.
    
    Sem isso, o step video_clipper encontra o cache e não regenera
    o EDL com os novos b-rolls.
    """
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE projects
            SET project_config = project_config - 'video_clipper_overlay_edl'
            WHERE project_id = %s
              AND project_config ? 'video_clipper_overlay_edl'
        """, (project_id,))
        conn.commit()
        affected = cursor.rowcount
        cursor.close()
        conn.close()
        if affected:
            logger.info(f"🗑️ [REPROCESS] Cache video_clipper_overlay_edl limpo")
        else:
            logger.info(f"ℹ️ [REPROCESS] Sem cache de video_clipper_overlay_edl para limpar")
    except Exception as e:
        logger.warning(f"⚠️ [REPROCESS] Erro ao limpar cache do Video Clipper: {e}")


def _trigger_replay(job_id: str, step_name: str) -> Optional[str]:
    """
    Dispara replay do pipeline a partir de um step.
    
    Usa a mesma lógica do endpoint replay-from mas chamado internamente
    (sem HTTP) para evitar dependência circular.
    
    Returns:
        new_job_id se sucesso, None se falha
    """
    try:
        from app.video_orchestrator.engine.replay import (
            prepare_replay,
            get_previous_step,
        )
        from app.video_orchestrator.jobs import VideoJob, JobStatus
        from app.video_orchestrator.engine.state_manager import StateManager
        from app.supabase_client import get_direct_db_connection

        # 1. Preparar replay
        modified_state, steps_to_run, error = prepare_replay(
            job_id=job_id,
            target_step=step_name,
            modifications={},
        )

        if error:
            logger.error(f"❌ [REPROCESS] prepare_replay falhou: {error}")
            return None

        # 2. Criar novo job
        new_job_id = str(uuid_module.uuid4())

        from app.video_orchestrator.endpoints import get_job_manager
        jm = get_job_manager()

        original_job = jm.get_job(job_id, force_reload=True)
        if not original_job:
            logger.error(f"❌ [REPROCESS] Job original {job_id} não encontrado")
            return None

        new_job = VideoJob(
            job_id=new_job_id,
            conversation_id=original_job.conversation_id,
            project_id=original_job.project_id,
            user_id=original_job.user_id,
            status=JobStatus.PROCESSING,
            phase1_video_url=modified_state.phase1_video_url,
            phase1_audio_url=modified_state.phase1_audio_url,
            phase1_source=modified_state.phase1_source,
            phase1_metadata=modified_state.phase1_metadata,
            phase1_video_concatenated_url=modified_state.phase1_video_concatenated_url,
            original_video_url=modified_state.original_video_url,
            transcription_text=modified_state.transcription_text,
            transcription_words=modified_state.transcription_words,
            phrase_groups=modified_state.phrase_groups,
            speech_segments=modified_state.speech_segments,
            cut_timestamps=modified_state.cut_timestamps,
            total_duration_ms=modified_state.total_duration_ms,
            untranscribed_segments=modified_state.untranscribed_segments,
            template_id=modified_state.template_id,
            phase2_video_url=None,
            output_video_url=None,
            options={
                **(original_job.options or {}),
                '_replay_params': {
                    'original_job_id': job_id,
                    'target_step': step_name,
                    'steps_to_run': steps_to_run,
                    'modifications': {},
                }
            },
            steps=[],
            current_step=0,
        )

        # 3. Persistir job
        jm._jobs_cache[new_job_id] = new_job
        jm._persist_job(new_job)

        # 4. Salvar PipelineState
        sm = StateManager(db_connection_func=get_direct_db_connection)
        replay_options = {
            **(modified_state.options or {}),
            '_replay_params': {
                'original_job_id': job_id,
                'target_step': step_name,
                'steps_to_run': steps_to_run,
                'modifications': {},
            }
        }
        replay_state = modified_state.with_updates(
            job_id=new_job_id,
            options=replay_options,
        )
        sm.save(new_job_id, replay_state, step_name=f"replay_init_{step_name}")

        # 5. Checkpoint para replays encadeados
        previous_step = get_previous_step(step_name)
        if previous_step:
            from app.video_orchestrator.debug_logger import get_debug_logger
            debug = get_debug_logger()
            debug.log_checkpoint(
                job_id=new_job_id,
                step_name=previous_step,
                state_dict=replay_state.to_dict(),
                duration_ms=0,
                attempt=1,
            )

        jm.update_job_status(new_job_id, JobStatus.PROCESSING)

        # 6. Enfileirar
        from app.video_orchestrator.queue import enqueue_replay_job
        enqueued = enqueue_replay_job(new_job_id)

        if not enqueued:
            logger.warning(f"⚠️ [REPROCESS] Redis indisponível, executando replay localmente")
            from concurrent.futures import ThreadPoolExecutor
            from app.video_orchestrator.engine.bridge import get_engine_bridge
            executor = ThreadPoolExecutor(max_workers=1)
            bridge = get_engine_bridge()
            executor.submit(bridge.replay_pipeline, new_job_id)

        return new_job_id

    except Exception as e:
        logger.error(f"❌ [REPROCESS] Erro ao disparar replay: {e}", exc_info=True)
        return None


# ═══════════════════════════════════════════════════════════════
# SSE Helpers
# ═══════════════════════════════════════════════════════════════

def _emit_progress(conversation_id: str, message: str, step: int, total: int) -> None:
    """Emite evento SSE de progresso do reprocessamento."""
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'reprocess_progress', {
            'message': message,
            'step': step,
            'total': total,
        })
    except Exception as e:
        logger.warning(f"⚠️ [REPROCESS] Erro ao emitir SSE progress: {e}")


def _emit_error(conversation_id: str, message: str) -> None:
    """Emite mensagem de erro no chat."""
    try:
        from app.routes.chat_sse import emit_chat_event
        bot_id = str(uuid_module.uuid4())
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': bot_id,
            'sender': 'bot',
            'content': f"❌ {message}",
        })
    except Exception as e:
        logger.warning(f"⚠️ [REPROCESS] Erro ao emitir SSE error: {e}")

    # Persistir no banco
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO chatbot_messages (id, message_id, conversation_id, sender, content, created_at)
            VALUES (%s, %s, %s, 'bot', %s, NOW())
        """, (bot_id, bot_id, conversation_id, f"❌ {message}"))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


def _emit_success(conversation_id: str, message: str) -> None:
    """Emite mensagem de sucesso no chat."""
    bot_id = str(uuid_module.uuid4())
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': bot_id,
            'sender': 'bot',
            'content': message,
        })
    except Exception as e:
        logger.warning(f"⚠️ [REPROCESS] Erro ao emitir SSE success: {e}")

    # Persistir no banco
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO chatbot_messages (id, message_id, conversation_id, sender, content, created_at)
            VALUES (%s, %s, %s, 'bot', %s, NOW())
        """, (bot_id, bot_id, conversation_id, message))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass
