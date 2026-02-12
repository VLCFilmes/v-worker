"""
Pipeline Engine v3 - Motor principal.

Executa steps em sequência, persiste estado após cada um,
emite eventos SSE, suporta retry e timeout.

🆕 v3.10.0: Salva checkpoints automáticos após cada step
para suportar Pipeline Replay (Director).

🆕 v4.3.0: Async Subflows (Fire-and-Wait) — steps marcados com
async_mode=True rodam em thread separada enquanto o pipeline continua.
Steps com await_async=["step_name"] esperam o resultado antes de rodar.
"""

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .events import EngineEvents
from .models import PipelineState, StepResult
from .state_manager import StateManager
from .step_registry import StepRegistry

logger = logging.getLogger(__name__)

# 🆕 v3.10.0: Debug logger para checkpoints
try:
    from ..debug_logger import get_debug_logger
    _checkpoint_logger = get_debug_logger()
except ImportError:
    _checkpoint_logger = None
    logger.warning("⚠️ debug_logger não disponível, checkpoints desabilitados")


class PipelineEngine:
    """
    Motor principal do pipeline.
    
    Responsabilidades:
    - Executar steps na ordem correta (respeitando dependências)
    - Persistir estado após CADA step (crash recovery)
    - Emitir eventos SSE para o frontend
    - Retry automático com exponential backoff
    - Timeout por step
    
    Não faz:
    - Decisões sobre QUAIS steps executar (isso é do AutoRunner ou LLMDirector)
    - Criar jobs (isso é do JobManager)
    - Gerenciar filas (isso é do worker.py)
    """

    def __init__(self, db_connection_func: Callable):
        self.state_manager = StateManager(db_connection_func)
        self.registry = StepRegistry
        self.events = EngineEvents()

    def run(self, job_id: str, steps: List[str],
            initial_state: PipelineState = None,
            stop_after: str = None) -> PipelineState:
        """
        Executa uma lista de steps em sequência.
        
        Args:
            job_id: ID do job
            steps: Lista de nomes de steps a executar
            initial_state: Estado inicial (se None, carrega do banco)
            stop_after: Para após este step (para phase_1_only)
            
        Returns:
            PipelineState final após todos os steps
            
        Raises:
            Exception: Se um step não-opcional falhar definitivamente
        """
        # 1. Carregar ou usar estado fornecido
        state = initial_state
        if state is None:
            state = self.state_manager.load(job_id)
            if state is None:
                raise ValueError(f"Job {job_id} não encontrado no banco")

        # 2. Resolver ordem (topological sort com dependências)
        ordered_steps = self.registry.resolve_order(steps)
        logger.info(f"🚀 [ENGINE] Iniciando pipeline para {job_id[:8]}...")
        logger.info(f"   Steps: {ordered_steps}")
        logger.info(f"   Já completados: {state.completed_steps}")

        # 3. Atualizar status do job
        self.state_manager.update_job_status(job_id, 'processing')

        # 4. Emitir evento de início
        self.events.job_start(job_id, total_steps=len(ordered_steps))

        pipeline_start = time.time()

        # 🆕 v4.3.0: Async Subflows — rastreia steps rodando em background
        async_futures: Dict[str, Future] = {}  # step_name → Future

        # 5. Executar cada step
        for step_name in ordered_steps:
            step_def = self.registry.get(step_name)
            if step_def is None:
                logger.warning(f"⚠️ [ENGINE] Step '{step_name}' não registrado, pulando")
                continue

            # Pular se já completado (crash recovery)
            if step_name in state.completed_steps:
                logger.info(f"⏭️ [{step_name}] Já completado anteriormente, pulando")
                continue

            # Pular se já estava nos skipped
            if step_name in state.skipped_steps:
                logger.info(f"⏭️ [{step_name}] Previamente skipped, pulando")
                continue

            # 🆕 v4.3.0: AWAIT — Se este step precisa de async steps, esperar
            if step_def.await_async:
                for async_name in step_def.await_async:
                    if async_name in async_futures:
                        state = self._await_async_step(
                            job_id, async_name, async_futures, state
                        )

            # 🆕 v4.3.0: FIRE — Se este step é async, disparar em background
            if step_def.async_mode:
                self._fire_async_step(
                    job_id, step_def, state, async_futures
                )
                continue  # NÃO bloqueia, vai para o próximo step

            # Executar step normalmente (sequencial)
            try:
                state = self._execute_step(job_id, step_def, state)
            except Exception as e:
                # Step não-opcional falhou definitivamente
                logger.error(f"❌ [ENGINE] Pipeline falhou no step '{step_name}': {e}")
                state = state.with_updates(
                    failed_step=step_name,
                    error_message=str(e),
                )
                self.state_manager.save(job_id, state, step_name)
                self.state_manager.update_job_status(job_id, 'failed', error_message=str(e))
                self.events.job_error(job_id, str(e), step=step_name)
                
                # 🆕 v4.4.2: Notificar admin sobre falha
                try:
                    from app.services.admin_notification_service import notify_pipeline_failure
                    notify_pipeline_failure(
                        job_id=job_id,
                        step_name=step_name,
                        error_message=str(e),
                        project_id=getattr(state, 'project_id', None),
                        user_id=getattr(state, 'user_id', None),
                    )
                except Exception:
                    pass  # Nunca bloquear o pipeline por causa de notificação
                
                raise

            # Verificar stop_after (phase_1_only)
            if stop_after and step_name == stop_after:
                logger.info(f"🛑 [ENGINE] Parando após '{step_name}' (stop_after)")
                self.state_manager.update_job_status(job_id, 'awaiting_review')
                break

        # 🆕 v4.3.0: Esperar TODOS os async pendentes antes de finalizar
        for async_name in list(async_futures.keys()):
            state = self._await_async_step(
                job_id, async_name, async_futures, state
            )

        # 6. Pipeline completo
        total_ms = int((time.time() - pipeline_start) * 1000)
        logger.info(f"✅ [ENGINE] Pipeline completo para {job_id[:8]}... ({total_ms}ms)")

        # Se não parou por stop_after e tem video URL, marcar como completo
        if not stop_after and state.output_video_url:
            self.state_manager.update_job_status(job_id, 'completed')
            self.events.job_complete(job_id, state.output_video_url, duration_ms=total_ms)

        return state

    def run_step(self, job_id: str, step_name: str,
                 params: Dict = None) -> StepResult:
        """
        Executa um único step (usado pelo LLM Director).
        
        Args:
            job_id: ID do job
            step_name: Nome do step
            params: Parâmetros extras (do tool call da LLM)
            
        Returns:
            StepResult com resumo para a LLM inspecionar
        """
        state = self.state_manager.load(job_id)
        if state is None:
            return StepResult(
                step_name=step_name,
                success=False,
                error=f"Job {job_id} não encontrado",
            )

        step_def = self.registry.get(step_name)
        if step_def is None:
            return StepResult(
                step_name=step_name,
                success=False,
                error=f"Step '{step_name}' não registrado",
            )

        try:
            started = time.time()
            state = self._execute_step(job_id, step_def, state, params)
            duration_ms = int((time.time() - started) * 1000)

            return StepResult(
                step_name=step_name,
                success=True,
                duration_ms=duration_ms,
                state_summary=state.summary(),
            )
        except Exception as e:
            return StepResult(
                step_name=step_name,
                success=False,
                error=str(e),
                state_summary=state.summary(),
            )

    def _execute_step(self, job_id: str, step_def, state: PipelineState,
                      params: dict = None) -> PipelineState:
        """
        Executa um step com retry, timeout, logging e persistência.
        
        Args:
            job_id: ID do job
            step_def: StepDefinition com metadata
            state: Estado atual
            params: Parâmetros extras (opcional)
            
        Returns:
            Novo PipelineState após execução
            
        Raises:
            Exception: Se falhou definitivamente e step não é opcional
        """
        step_name = step_def.name
        sse_name = step_def.sse_step_name or step_name.upper()
        started_at = time.time()
        last_error = None

        self.events.step_start(job_id, sse_name)
        logger.info(f"▶️ [{step_name}] Iniciando... "
                     f"(category={step_def.category}, retries={step_def.max_retries})")

        for attempt in range(step_def.max_retries + 1):
            try:
                # Executar a função do step
                new_state = step_def.fn(state, params or {})

                if new_state is None:
                    logger.warning(f"⚠️ [{step_name}] Retornou None, mantendo state anterior")
                    new_state = state

                # Registrar timing
                duration_ms = int((time.time() - started_at) * 1000)
                new_state = new_state.with_updates(
                    completed_steps=list(set(state.completed_steps + [step_name])),
                    step_timings={
                        **state.step_timings,
                        step_name: {
                            'started_at': datetime.fromtimestamp(
                                started_at, tz=timezone.utc
                            ).isoformat(),
                            'duration_ms': duration_ms,
                            'attempt': attempt + 1,
                        }
                    }
                )

                # Persistir estado (APÓS CADA STEP - crash recovery)
                self.state_manager.save(job_id, new_state, step_name)

                # 🆕 v3.10.0: Salvar checkpoint para Pipeline Replay
                if _checkpoint_logger:
                    try:
                        _checkpoint_logger.log_checkpoint(
                            job_id=job_id,
                            step_name=step_name,
                            state_dict=new_state.to_dict(),
                            duration_ms=duration_ms,
                            attempt=attempt + 1,
                        )
                    except Exception as cp_err:
                        # Checkpoint é best-effort, não pode quebrar o pipeline
                        logger.warning(f"⚠️ [{step_name}] Erro ao salvar checkpoint: {cp_err}")

                # Emitir evento SSE
                self.events.step_complete(job_id, sse_name, duration_ms=duration_ms)
                logger.info(f"✅ [{step_name}] Completo em {duration_ms}ms "
                            f"(tentativa {attempt + 1})")

                return new_state

            except Exception as e:
                last_error = e
                if attempt < step_def.max_retries and step_def.retryable:
                    wait = 2 ** attempt  # 1s, 2s, 4s
                    logger.warning(f"⚠️ [{step_name}] Tentativa {attempt + 1} falhou: {e}. "
                                   f"Retry em {wait}s...")
                    time.sleep(wait)
                    continue

                # Falha definitiva
                duration_ms = int((time.time() - started_at) * 1000)
                logger.error(f"❌ [{step_name}] Falhou após {attempt + 1} tentativas: {e}")
                self.events.step_error(job_id, sse_name, str(e))

                if step_def.optional:
                    logger.info(f"⏭️ [{step_name}] Step opcional, continuando pipeline")
                    return state.with_updates(
                        skipped_steps=list(set(state.skipped_steps + [step_name])),
                        step_timings={
                            **state.step_timings,
                            step_name: {
                                'started_at': datetime.fromtimestamp(
                                    started_at, tz=timezone.utc
                                ).isoformat(),
                                'duration_ms': duration_ms,
                                'error': str(e),
                                'attempt': attempt + 1,
                                'skipped': True,
                            }
                        }
                    )

                # Step obrigatório falhou → propagar exceção
                raise

    # ═══════════════════════════════════════════════════════════════
    # 🆕 v4.3.0: Async Subflows (Fire-and-Wait)
    # ═══════════════════════════════════════════════════════════════

    def _fire_async_step(
        self, job_id: str, step_def, state: PipelineState,
        async_futures: Dict[str, Future]
    ) -> None:
        """
        Dispara um step em thread separada (background).

        O step recebe um snapshot do state atual (imutável).
        O resultado fica no Future para ser coletado pelo _await_async_step.

        Custo do fire: <1ms (criar thread).
        """
        step_name = step_def.name
        sse_name = step_def.sse_step_name or step_name.upper()

        # Emitir SSE de início imediatamente
        self.events.step_start(job_id, sse_name)

        logger.info(f"🔀 [{step_name}] Disparado em ASYNC (thread) — "
                     f"pipeline continua sem esperar")

        # Executar em thread separada via ThreadPoolExecutor
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"async_{step_name}")
        future = executor.submit(self._execute_step, job_id, step_def, state)

        # Shutdown do executor quando a thread terminar (non-blocking)
        future.add_done_callback(lambda _: executor.shutdown(wait=False))

        async_futures[step_name] = future

    def _await_async_step(
        self, job_id: str, async_name: str,
        async_futures: Dict[str, Future], current_state: PipelineState
    ) -> PipelineState:
        """
        Espera um step async terminar e faz merge do resultado no state atual.

        O merge usa o campo `produces` do StepDefinition para copiar apenas
        os campos que o step async produziu. Também merge completed_steps e step_timings.

        Se o step falhou e é opcional, marca como skipped. Se obrigatório, re-lança exceção.
        """
        future = async_futures.pop(async_name)
        step_def = self.registry.get(async_name)

        if future.done():
            logger.info(f"✅ [AWAIT] '{async_name}' já terminou — coletando resultado")
        else:
            timeout_s = step_def.timeout_s if step_def else 600
            logger.info(f"⏳ [AWAIT] Esperando '{async_name}' terminar "
                         f"(timeout={timeout_s}s)...")

        try:
            async_state = future.result(timeout=step_def.timeout_s if step_def else 600)
        except Exception as e:
            # Step async falhou
            if step_def and step_def.optional:
                logger.warning(f"⚠️ [AWAIT] '{async_name}' falhou (opcional): {e}")
                return current_state.with_updates(
                    skipped_steps=list(set(current_state.skipped_steps + [async_name])),
                )
            else:
                logger.error(f"❌ [AWAIT] '{async_name}' falhou (obrigatório): {e}")
                raise

        # Merge: copiar apenas os campos que o async step PRODUZIU
        updates = {}
        if step_def and step_def.produces:
            async_dict = async_state.to_dict()
            for field_name in step_def.produces:
                value = async_dict.get(field_name)
                if value is not None:
                    updates[field_name] = value

        # Merge completed_steps
        updates['completed_steps'] = list(set(
            current_state.completed_steps + [async_name]
        ))

        # Merge step_timings do async
        if hasattr(async_state, 'step_timings') and async_name in async_state.step_timings:
            updates['step_timings'] = {
                **current_state.step_timings,
                async_name: async_state.step_timings[async_name],
            }

        # Merge campos extras de matting (não estão em produces mas são importantes)
        for extra_field in ['matted_video_url']:
            val = async_state.to_dict().get(extra_field)
            if val is not None and extra_field not in updates:
                updates[extra_field] = val

        logger.info(f"✅ [AWAIT] '{async_name}' merge completo: "
                     f"{list(updates.keys())}")

        merged_state = current_state.with_updates(**updates)

        # Persistir state com merge (importante para crash recovery)
        self.state_manager.save(job_id, merged_state, f"await_{async_name}")

        # 🆕 v4.4.1: Salvar checkpoint do merge para Pipeline Replay
        # Sem este checkpoint, replay a partir de steps com await_async
        # (ex: render, cartelas) perde os outputs dos async steps porque
        # o checkpoint do step anterior (ex: subtitle_pipeline) não os contém.
        if _checkpoint_logger:
            try:
                _checkpoint_logger.log_checkpoint(
                    job_id=job_id,
                    step_name=f"await_{async_name}",
                    state_dict=merged_state.to_dict(),
                    duration_ms=0,
                    attempt=1,
                )
            except Exception as cp_err:
                logger.warning(f"⚠️ [AWAIT] Erro ao salvar checkpoint de merge: {cp_err}")

        return merged_state

    def get_state(self, job_id: str) -> Optional[PipelineState]:
        """Carrega estado atual de um job."""
        return self.state_manager.load(job_id)

    def get_debug_info(self, job_id: str) -> Dict:
        """Retorna debug completo de um job (para suporte em < 2 min)."""
        state = self.state_manager.load(job_id)
        if state is None:
            return {'error': f'Job {job_id} não encontrado'}

        return {
            'job_id': job_id,
            'completed_steps': state.completed_steps,
            'skipped_steps': state.skipped_steps,
            'failed_step': state.failed_step,
            'error_message': state.error_message,
            'step_timings': state.step_timings,
            'template_id': state.template_id,
            'phase1_source': state.phase1_source,
            'total_duration_ms': state.total_duration_ms,
            'phrase_count': len(state.phrase_groups) if state.phrase_groups else 0,
            'has_pngs': state.png_results is not None,
            'has_transcription': state.transcription_text is not None,
            'video_dimensions': f"{state.video_width}x{state.video_height}",
            'engine_version': state.engine_version,
            'state_keys_with_data': [
                k for k, v in state.to_dict().items()
                if v is not None and v != [] and v != {} and v != ''
            ],
        }
