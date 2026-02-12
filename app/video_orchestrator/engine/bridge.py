"""
Bridge: ponto de entrada para o Pipeline Engine v3.

Responsabilidades:
1. Criar jobs e enfileirar no Redis (start_processing)
2. Executar AutoRunner (run_full, run_phase1_only, run_phase2)
3. Sincronizar resultado de volta para VideoJob (endpoints.py e frontend leem daí)
"""

import logging
import time
from typing import Optional, List, Dict

from .models import PipelineState
from .pipeline_engine import PipelineEngine
from .state_manager import StateManager
from .auto_runner import AutoRunner

logger = logging.getLogger(__name__)


class EngineBridge:
    """
    Ponto de entrada do Pipeline Engine v3.
    
    Uso:
        bridge = get_engine_bridge()
        job = bridge.start_processing(...)        # Cria job + enfileira
        bridge.execute_pipeline(job_id)            # Worker: pipeline completo
        bridge.continue_pipeline(job_id)           # Worker: Fase 2
    """

    def __init__(self):
        from app.supabase_client import get_direct_db_connection
        self.db_connection_func = get_direct_db_connection
        self.state_manager = StateManager(db_connection_func=self.db_connection_func)
        self.engine = PipelineEngine(db_connection_func=self.db_connection_func)
        self.runner = AutoRunner(engine=self.engine)
        logger.info("🚀 [ENGINE] Bridge inicializada")

    # ═══════════════════════════════════════════════════════════════
    # API Layer — chamado pelos endpoints / routes
    # ═══════════════════════════════════════════════════════════════

    def start_processing(
        self,
        conversation_id: Optional[str],
        project_id: Optional[str],
        user_id: str,
        videos: Optional[List[Dict]] = None,
        options: Optional[Dict] = None,
        webhook_url: Optional[str] = None,
        template_id: Optional[str] = None,
        text: Optional[str] = None,
        phrases: Optional[List[Dict]] = None
    ):
        """
        Cria um VideoJob e enfileira para processamento.
        
        Chamado por: projects.py, chat.py, chat_flask.py
        
        Returns:
            VideoJob criado (com job_id para polling)
        """
        from ..jobs import JobManager
        
        options = options or {}
        jm = JobManager(db_connection_func=self.db_connection_func)
        
        # 1. Criar job
        job = jm.create_job(
            conversation_id=conversation_id,
            project_id=project_id,
            user_id=user_id,
            videos=videos or [],
            options=options,
            webhook_url=webhook_url,
            template_id=template_id,
            text=text,
            phrases=phrases
        )
        
        storytelling_mode = options.get('storytelling_mode', 'unknown')
        logger.info(f"🎬 Job {job.job_id} criado: mode={storytelling_mode}, "
                     f"videos={bool(videos)}, text={bool(text)}")
        
        # 2. Criar Pipeline Visualizer (UI no chat)
        if conversation_id:
            try:
                from app.routes.visualizer_persistence import create_pipeline_visualizer
                create_pipeline_visualizer(
                    conversation_id=conversation_id,
                    job_id=job.job_id,
                    project_id=project_id
                )
            except Exception as e:
                logger.warning(f"⚠️ Erro ao criar pipeline visualizer: {e}")
        
        # 3. Enfileirar no Redis
        from ..queue import enqueue_job
        if not enqueue_job(job.job_id):
            # Fallback: executar localmente se Redis indisponível
            logger.warning(f"⚠️ Redis indisponível, executando job {job.job_id} localmente")
            import threading
            thread = threading.Thread(
                target=self.execute_pipeline,
                args=(job.job_id,),
                daemon=True
            )
            thread.start()
        
        return job

    # ═══════════════════════════════════════════════════════════════
    # Worker Layer — chamado pelo worker.py via Redis
    # ═══════════════════════════════════════════════════════════════

    def execute_pipeline(self, job_id: str, phase_1_only: bool = None):
        """
        Executa pipeline completo (Fase 1 + Fase 2, ou só Fase 1).
        
        Chamado pelo worker quando pega job da fila Redis.
        
        O parâmetro phase_1_only é lido automaticamente do job.options
        (definido pelo endpoint start-processing). Se passado explicitamente,
        o valor explícito tem prioridade.
        
        Args:
            job_id: ID do job
            phase_1_only: Se True, roda apenas Fase 1 (AWAITING_REVIEW).
                          Se None (default), lê de state.options['phase_1_only'].
        """
        start = time.time()

        try:
            # 1. Carregar job e criar PipelineState
            state = self._load_state_from_job(job_id)

            # 1.5 🆕 v4.5.0: Enriquecer state com resolução do upload
            state = self._enrich_upload_resolution(state)

            # 2. Determinar phase_1_only: parâmetro explícito > job.options
            if phase_1_only is None:
                opts = state.options or {}
                phase_1_only = opts.get('phase_1_only', False)
            
            logger.info(f"🎬 [ENGINE] execute_pipeline job={job_id[:8]}... "
                         f"(phase_1_only={phase_1_only})")

            # 3. Determinar storytelling_mode e ajustar state
            opts = state.options or {}
            storytelling_mode = opts.get('storytelling_mode', 'talking_head')
            if storytelling_mode and storytelling_mode != state.storytelling_mode:
                state = state.with_updates(storytelling_mode=storytelling_mode)

            # 3b. Verificar se análise visual está ativada
            include_visual = bool(opts.get('vision_analysis'))
            if include_visual:
                logger.info(f"👁️ [ENGINE] vision_analysis=True → incluindo step visual_analysis")

            # 4. Executar via AutoRunner (branching por STM)
            if storytelling_mode == "text_video":
                logger.info(f"📝 [ENGINE] STM=text_video → usando TEXT_VIDEO_STEPS")
                final_state = self.runner.run_text_video(job_id, state=state)
            elif phase_1_only:
                final_state = self.runner.run_phase1_only(job_id, state=state)
            else:
                final_state = self.runner.run_full(
                    job_id, state=state, include_visual=include_visual
                )

            # 4. Sincronizar resultado de volta para o VideoJob
            self._sync_state_to_job(job_id, final_state)

            elapsed = time.time() - start
            logger.info(f"✅ [ENGINE] Pipeline concluído em {elapsed:.1f}s | "
                         f"Steps: {final_state.completed_steps}")

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"❌ [ENGINE] Pipeline falhou após {elapsed:.1f}s: {e}")
            self._mark_job_failed(job_id, str(e))
            raise

    def replay_pipeline(self, job_id: str):
        """
        🆕 v3.10.0: Re-executa pipeline a partir de um step específico.
        
        O state já está preparado (checkpoint + modifications) e salvo
        no pipeline_state do novo job. Os steps a executar e o step
        alvo estão em job.options['_replay_params'].
        
        Args:
            job_id: ID do NOVO job (criado pelo endpoint replay-from)
        """
        start = time.time()
        logger.info(f"🔄 [ENGINE] replay_pipeline job={job_id[:8]}...")

        try:
            # 1. Carregar state do job (já tem checkpoint + modifications)
            state = self._load_state_from_job(job_id)

            # 2. Ler parâmetros de replay do job.options
            opts = dict(state.options or {})
            replay_params = opts.pop('_replay_params', {})
            
            target_step = replay_params.get('target_step')
            steps_to_run = replay_params.get('steps_to_run', [])
            original_job_id = replay_params.get('original_job_id')
            
            if not target_step or not steps_to_run:
                raise ValueError(
                    f"Parâmetros de replay inválidos: target_step={target_step}, "
                    f"steps_to_run={steps_to_run}"
                )
            
            logger.info(f"🔄 [REPLAY] target={target_step}, "
                         f"steps={steps_to_run}, "
                         f"original_job={original_job_id[:8] if original_job_id else 'N/A'}...")

            # 3. Limpar _replay_params do options (não precisa mais)
            if opts != (state.options or {}):
                state = state.with_updates(options=opts)

            # 3.1 🆕 v4.4.2: Limpar cache do Video Clipper EDL quando replay é a partir
            # desse step — sem isso, o step usaria o EDL cacheado ao invés de regenerar via LLM
            if target_step == "video_clipper" and state.project_id:
                try:
                    _clear_video_clipper_edl_cache(state.project_id)
                    logger.info(f"🗑️ [REPLAY] Cache video_clipper_overlay_edl limpo para replay")
                except Exception as cache_err:
                    logger.warning(f"⚠️ [REPLAY] Falha ao limpar cache do Video Clipper: {cache_err}")

            # 4. Executar pipeline via AutoRunner com custom steps
            final_state = self.runner.run_custom(
                job_id, steps_to_run, state=state
            )

            # 5. Sincronizar resultado de volta para o VideoJob
            self._sync_state_to_job(job_id, final_state)

            elapsed = time.time() - start
            logger.info(f"✅ [ENGINE] Replay concluído em {elapsed:.1f}s | "
                         f"Steps: {final_state.completed_steps}")

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"❌ [ENGINE] Replay falhou após {elapsed:.1f}s: {e}")
            self._mark_job_failed(job_id, str(e))
            raise

    def continue_pipeline(self, job_id: str):
        """
        Executa Fase 2 do pipeline (após revisão do usuário).
        
        O engine decide quais steps rodar (PHASE_2_STEPS). O caller não
        precisa informar steps — essa responsabilidade é do auto_runner.
        
        Overrides de template_id e worker vêm do job.options (definidos pelo endpoint).
        
        Args:
            job_id: ID do NOVO job (Fase 2)
        """
        start = time.time()
        logger.info(f"🔄 [ENGINE] continue_pipeline job={job_id[:8]}...")

        try:
            # 1. Carregar state do job (já tem dados da Fase 1 copiados)
            state = self._load_state_from_job(job_id)

            # 2. Aplicar overrides do job.options (definidos pelo endpoint)
            opts = dict(state.options or {})
            continue_params = opts.pop('_continue_params', {})
            
            if continue_params.get('template_id'):
                state = state.with_updates(template_id=continue_params['template_id'])
            if continue_params.get('worker_override'):
                opts['worker_override'] = continue_params['worker_override']
            if continue_params.get('editor_worker_id'):
                opts['editor_worker_id'] = continue_params['editor_worker_id']
            if opts != (state.options or {}):
                state = state.with_updates(options=opts)

            # 3. Executar Fase 2 via AutoRunner (engine decide os steps)
            final_state = self.runner.run_phase2(job_id, state=state)

            # 4. Sincronizar resultado de volta para o VideoJob
            self._sync_state_to_job(job_id, final_state)

            elapsed = time.time() - start
            logger.info(f"✅ [ENGINE] Continue concluído em {elapsed:.1f}s | "
                         f"Steps: {final_state.completed_steps}")

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"❌ [ENGINE] Continue falhou após {elapsed:.1f}s: {e}")
            self._mark_job_failed(job_id, str(e))
            raise

    def _enrich_upload_resolution(self, state: PipelineState) -> PipelineState:
        """
        🆕 v4.5.0: Busca resolução do vídeo principal (upload) e injeta no state.
        
        Fontes (prioridade):
        1. state.options['target_aspect_ratio'] (escolha explícita do usuário)
        2. project_assets.metadata.video_analysis.width/height (ffprobe via v-services)
        
        Se não encontrar, retorna state inalterado (fallback para dimensões do template).
        """
        # Se já tem resolução, não buscar novamente (ex: replay)
        if state.upload_width > 0 and state.upload_height > 0:
            return state
        
        # Ler aspect_ratio das options (escolha do usuário via frontend)
        opts = state.options or {}
        target_aspect = opts.get('target_aspect_ratio', '')
        
        upload_w = 0
        upload_h = 0
        
        try:
            conn = self.db_connection_func()
            try:
                from psycopg2.extras import RealDictCursor
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Buscar resolução do primeiro vídeo via project_assets
                # (metadata.video_analysis vem do ffprobe, mais preciso)
                if state.project_id:
                    cursor.execute("""
                        SELECT metadata
                        FROM project_assets
                        WHERE project_id = %s 
                          AND asset_type = 'video'
                        ORDER BY asset_order ASC, created_at ASC
                        LIMIT 1
                    """, (state.project_id,))
                    row = cursor.fetchone()
                    
                    if row and row.get('metadata'):
                        meta = row['metadata'] if isinstance(row['metadata'], dict) else {}
                        va = meta.get('video_analysis', {})
                        upload_w = int(va.get('width', 0))
                        upload_h = int(va.get('height', 0))
                        
                        if upload_w > 0 and upload_h > 0:
                            logger.info(
                                f"📐 [RESOLUTION] Upload detectado via project_assets: "
                                f"{upload_w}x{upload_h}"
                            )
                
                # Fallback: tentar parsear campo resolution ("720x1280")
                if upload_w == 0 and state.project_id:
                    cursor.execute("""
                        SELECT resolution
                        FROM project_assets
                        WHERE project_id = %s 
                          AND asset_type = 'video'
                          AND resolution IS NOT NULL
                          AND resolution != ''
                        ORDER BY asset_order ASC, created_at ASC
                        LIMIT 1
                    """, (state.project_id,))
                    row = cursor.fetchone()
                    
                    if row and row.get('resolution'):
                        try:
                            parts = str(row['resolution']).split('x')
                            if len(parts) == 2:
                                upload_w = int(parts[0])
                                upload_h = int(parts[1])
                                logger.info(
                                    f"📐 [RESOLUTION] Upload detectado via resolution field: "
                                    f"{upload_w}x{upload_h}"
                                )
                        except (ValueError, IndexError):
                            pass
                
                cursor.close()
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"⚠️ [RESOLUTION] Erro ao buscar resolução do upload: {e}")
        
        if upload_w > 0 and upload_h > 0:
            return state.with_updates(
                upload_width=upload_w,
                upload_height=upload_h,
                target_aspect_ratio=target_aspect,
            )
        
        logger.info(f"📐 [RESOLUTION] Sem resolução de upload detectada, usando template")
        return state

    def _load_state_from_job(self, job_id: str) -> PipelineState:
        """Carrega PipelineState do banco (via StateManager ou VideoJob)."""
        # Tentar carregar state v3 do banco primeiro
        state = self.state_manager.load(job_id)
        if state:
            logger.info(f"   State v3 carregado (completed_steps={state.completed_steps})")
            return state

        # Fallback: criar state a partir do VideoJob legado
        from ..jobs import JobManager
        job_manager = JobManager(db_connection_func=self.db_connection_func)
        job = job_manager.get_job(job_id, force_reload=True)
        if not job:
            raise ValueError(f"Job {job_id} não encontrado")

        state = PipelineState.from_job(job)
        logger.info(f"   State criado a partir de VideoJob "
                     f"(phase1_source={state.phase1_source}, "
                     f"phrases={len(state.phrase_groups or [])})")
        return state

    def _sync_state_to_job(self, job_id: str, state: PipelineState):
        """
        Sincroniza PipelineState de volta para o VideoJob.
        
        Necessário para backward compat: endpoints.py e callbacks.py
        ainda leem do VideoJob.
        """
        from ..jobs import JobManager, JobStatus
        job_manager = JobManager(db_connection_func=self.db_connection_func)
        job = job_manager.get_job(job_id)
        if not job:
            logger.warning(f"⚠️ Job {job_id} não encontrado para sync")
            return

        # Atualizar campos do job com dados do state
        fields_to_sync = {
            'phase1_video_url': state.phase1_video_url,
            'phase1_audio_url': state.phase1_audio_url,
            'phase2_video_url': state.phase2_video_url,
            'output_video_url': state.output_video_url,
            'matted_video_url': state.matted_video_url,
            'normalized_video_url': state.normalized_video_url,
            'base_normalized_url': state.base_normalized_url,
            'transcription_text': state.transcription_text,
            'transcription_words': state.transcription_words,
            'phrase_groups': state.phrase_groups,
            'png_results': state.png_results,
            'speech_segments': state.speech_segments,
            'cut_timestamps': state.cut_timestamps,
            'total_duration_ms': state.total_duration_ms,
            'phase1_source': state.phase1_source,
            'phase1_metadata': state.phase1_metadata,
            'matting_segments': state.matting_segments,
            'foreground_segments': state.foreground_segments,
            'matting_config_hash': state.matting_config_hash,
        }

        for field_name, value in fields_to_sync.items():
            if value is not None and hasattr(job, field_name):
                setattr(job, field_name, value)

        # Determinar status final
        if state.failed_step:
            job_manager.update_job_status(job_id, JobStatus.FAILED)
        elif state.error_message:
            job_manager.update_job_status(job_id, JobStatus.FAILED)
        else:
            # Se o último step completado é 'classify' e não é pipeline completo,
            # então estamos em AWAITING_REVIEW
            if (state.completed_steps
                and state.completed_steps[-1] == 'classify'
                and 'render' not in state.completed_steps):
                job_manager.update_job_status(job_id, JobStatus.AWAITING_REVIEW)
            else:
                job_manager.update_job_status(job_id, JobStatus.COMPLETED)

        # Persistir mudanças
        job_manager._persist_job(job)
        logger.info(f"   ✅ State sincronizado para VideoJob (status={job.status})")

    def _mark_job_failed(self, job_id: str, error: str):
        """Marca job como FAILED no banco."""
        try:
            from ..jobs import JobManager, JobStatus
            job_manager = JobManager(db_connection_func=self.db_connection_func)
            job = job_manager.get_job(job_id)
            if job:
                job.error_message = error
                job_manager.update_job_status(job_id, JobStatus.FAILED)
                job_manager._persist_job(job)
        except Exception as e:
            logger.error(f"⚠️ Falha ao marcar job como failed: {e}")
        
        # 🆕 v4.4.2: Notificar admin sobre falha
        try:
            from app.services.admin_notification_service import notify_pipeline_failure
            notify_pipeline_failure(
                job_id=job_id,
                step_name="bridge",
                error_message=error,
            )
        except Exception:
            pass  # Nunca bloquear por causa de notificação


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def _clear_video_clipper_edl_cache(project_id: str) -> None:
    """
    🆕 v4.4.2: Limpa o cache do Video Clipper EDL em project_config.
    
    Necessário para que o replay do step video_clipper force a LLM
    a regenerar o EDL ao invés de usar o resultado cacheado.
    """
    from app.db import get_db_connection
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE projects
            SET project_config = project_config - 'video_clipper_overlay_edl'
            WHERE id = %s
              AND project_config ? 'video_clipper_overlay_edl'
        """, (project_id,))
        conn.commit()
        rows = cur.rowcount
        cur.close()
        conn.close()
        if rows:
            logger.info(f"🗑️ [REPLAY] Removido video_clipper_overlay_edl do project_config")
        else:
            logger.debug(f"[REPLAY] video_clipper_overlay_edl não encontrado (ok, nada a limpar)")
    except Exception as e:
        logger.warning(f"⚠️ [REPLAY] Erro ao limpar cache EDL: {e}")
        raise


# ═══════════════════════════════════════════════════════════════
# Singleton
# ═══════════════════════════════════════════════════════════════

_bridge = None


def get_engine_bridge() -> EngineBridge:
    """Retorna instância singleton da bridge."""
    global _bridge
    if _bridge is None:
        _bridge = EngineBridge()
    return _bridge
