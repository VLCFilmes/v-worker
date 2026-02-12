"""
State Manager - Persiste e carrega PipelineState do PostgreSQL.

Fonte de verdade = PostgreSQL (coluna pipeline_state JSONB).
Também atualiza colunas legacy para compatibilidade com frontend/admin.
"""

import json
import logging
from typing import Any, Callable, Dict, Optional

from psycopg2.extras import Json

from .models import PipelineState

logger = logging.getLogger(__name__)


class StateManager:
    """
    Gerencia persistência do PipelineState.
    
    Estratégia de storage:
    - pipeline_state JSONB: estado completo (fonte de verdade para o engine)
    - Colunas legacy: atualizadas em paralelo para compat com frontend
    """

    def __init__(self, db_connection_func: Callable):
        self.db_connection_func = db_connection_func

    def load(self, job_id: str) -> Optional[PipelineState]:
        """
        Carrega PipelineState do banco.
        
        Tenta pipeline_state primeiro (engine novo).
        Se não existir, reconstroi a partir das colunas legacy (migração).
        """
        conn = None
        try:
            conn = self.db_connection_func()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM video_processing_jobs WHERE job_id = %s",
                    (job_id,)
                )
                row = cur.fetchone()

                if not row:
                    logger.error(f"❌ [STATE] Job não encontrado: {job_id}")
                    return None

                # Converter row para dict (compatível com RealDictCursor e tuple)
                if hasattr(row, 'keys'):
                    row_dict = dict(row)
                else:
                    # Fallback: usar column names do cursor
                    col_names = [desc[0] for desc in cur.description]
                    row_dict = dict(zip(col_names, row))

                # Tentar carregar do campo pipeline_state (engine novo)
                pipeline_state_json = row_dict.get('pipeline_state')
                if pipeline_state_json and isinstance(pipeline_state_json, dict):
                    state = PipelineState.from_dict(pipeline_state_json)
                    logger.info(f"✅ [STATE] Carregado do pipeline_state: {job_id[:8]}... "
                                f"(steps: {state.completed_steps})")
                    return state

                # Fallback: reconstruir das colunas legacy
                state = self._reconstruct_from_legacy(row_dict)
                logger.info(f"✅ [STATE] Reconstruído das colunas legacy: {job_id[:8]}...")
                return state

        except Exception as e:
            logger.error(f"❌ [STATE] Erro ao carregar state: {e}")
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def save(self, job_id: str, state: PipelineState, step_name: str = None) -> bool:
        """
        Salva PipelineState no banco após cada step.
        
        Atualiza:
        1. pipeline_state JSONB (estado completo)
        2. Colunas legacy (para compat com frontend/admin)
        3. steps JSONB (para exibição de progresso)
        """
        conn = None
        try:
            conn = self.db_connection_func()
            state_dict = state.to_dict()

            # Construir updates: pipeline_state JSONB (fonte de verdade) + colunas legacy
            updates = {
                'pipeline_state': Json(state_dict),  # Estado completo para engine/replay
            }

            # Mapear campos do state para colunas legacy
            legacy_mappings = {
                'transcription_text': state.transcription_text,
                'transcription_words': Json(state.transcription_words) if state.transcription_words else None,
                'phrase_groups': Json(state.phrase_groups) if state.phrase_groups else None,
                'png_results': Json(state.png_results) if state.png_results else None,
                'shadow_results': Json(state.shadow_results) if state.shadow_results else None,
                'phase1_video_url': state.phase1_video_url,
                'phase2_video_url': state.phase2_video_url,
                'output_video_url': state.output_video_url,
                'matted_video_url': state.matted_video_url,
                'base_normalized_url': state.base_normalized_url,
                'original_video_url': state.original_video_url,
                'phase1_audio_url': state.phase1_audio_url,
                'total_duration_ms': state.total_duration_ms,
                'speech_segments': Json(state.speech_segments) if state.speech_segments else None,
                'cut_timestamps': Json(state.cut_timestamps) if state.cut_timestamps else None,
                'foreground_segments': Json(state.foreground_segments) if state.foreground_segments else None,
                'matting_segments': Json(state.matting_segments) if state.matting_segments else None,
                'normalization_stats': Json(state.normalization_stats) if state.normalization_stats else None,
                'untranscribed_segments': Json(state.untranscribed_segments) if state.untranscribed_segments else None,
                'phase1_source': state.phase1_source,
                'phase1_metadata': Json(state.phase1_metadata) if state.phase1_metadata else None,
                'error_message': state.error_message,
            }

            # Só atualizar legacy se o valor não for None (COALESCE behavior)
            for col, val in legacy_mappings.items():
                if val is not None:
                    updates[col] = val

            # Construir steps JSONB para exibição de progresso no frontend
            steps_json = self._build_steps_json(state)
            updates['steps'] = Json(steps_json)

            # Construir SQL dinâmico
            set_clauses = []
            values = []
            for col, val in updates.items():
                set_clauses.append(f"{col} = %s")
                values.append(val)

            values.append(job_id)
            sql = f"UPDATE video_processing_jobs SET {', '.join(set_clauses)} WHERE job_id = %s"

            with conn.cursor() as cur:
                cur.execute(sql, values)
            conn.commit()

            logger.info(f"💾 [STATE] Salvo: {job_id[:8]}... "
                        f"(step={step_name}, completed={len(state.completed_steps)})")
            return True

        except Exception as e:
            logger.error(f"❌ [STATE] Erro ao salvar state: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def update_job_status(self, job_id: str, status: str,
                          error_message: str = None) -> bool:
        """Atualiza apenas o status do job (sem alterar pipeline_state)."""
        conn = None
        try:
            conn = self.db_connection_func()
            with conn.cursor() as cur:
                if error_message:
                    cur.execute(
                        "UPDATE video_processing_jobs SET status = %s, error_message = %s WHERE job_id = %s",
                        (status, error_message, job_id)
                    )
                else:
                    cur.execute(
                        "UPDATE video_processing_jobs SET status = %s WHERE job_id = %s",
                        (status, job_id)
                    )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ [STATE] Erro ao atualizar status: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _reconstruct_from_legacy(self, row: Dict) -> PipelineState:
        """Reconstroi PipelineState a partir das colunas legacy do banco."""
        # Desserializar campos JSONB
        def _parse_json(val):
            if val is None:
                return None
            if isinstance(val, (dict, list)):
                return val
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    return None
            return val

        # Inferir completed_steps dos steps legados
        completed = []
        steps_raw = _parse_json(row.get('steps')) or []
        for s in steps_raw:
            if isinstance(s, dict) and s.get('status') == 'completed':
                completed.append(s.get('name', ''))

        return PipelineState(
            job_id=str(row.get('job_id', '')),
            project_id=str(row.get('project_id', '')),
            user_id=str(row.get('user_id', '')),
            conversation_id=str(row.get('conversation_id', '')) if row.get('conversation_id') else None,
            template_id=None,  # Não disponível na tabela diretamente
            videos=_parse_json(row.get('videos')) or [],
            options=_parse_json(row.get('options')) or {},
            webhook_url=row.get('webhook_url'),
            original_video_url=row.get('original_video_url'),
            phase1_video_url=row.get('phase1_video_url'),
            phase1_audio_url=row.get('phase1_audio_url'),
            phase2_video_url=row.get('phase2_video_url'),
            output_video_url=row.get('output_video_url'),
            matted_video_url=row.get('matted_video_url'),
            base_normalized_url=row.get('base_normalized_url'),
            transcription_text=row.get('transcription_text'),
            transcription_words=_parse_json(row.get('transcription_words')),
            phrase_groups=_parse_json(row.get('phrase_groups')),
            png_results=_parse_json(row.get('png_results')),
            shadow_results=_parse_json(row.get('shadow_results')),
            speech_segments=_parse_json(row.get('speech_segments')),
            cut_timestamps=_parse_json(row.get('cut_timestamps')),
            foreground_segments=_parse_json(row.get('foreground_segments')),
            matting_segments=_parse_json(row.get('matting_segments')),
            normalization_stats=_parse_json(row.get('normalization_stats')),
            untranscribed_segments=_parse_json(row.get('untranscribed_segments')),
            phase1_source=row.get('phase1_source'),
            phase1_metadata=_parse_json(row.get('phase1_metadata')),
            total_duration_ms=row.get('total_duration_ms'),
            error_message=row.get('error_message'),
            completed_steps=completed,
            created_at=str(row.get('created_at', '')) if row.get('created_at') else None,
        )

    def _build_steps_json(self, state: PipelineState) -> list:
        """Constroi array de steps para a coluna legacy (frontend display)."""
        steps = []
        all_step_names = state.completed_steps + (
            [state.failed_step] if state.failed_step else []
        )
        for name in all_step_names:
            timing = state.step_timings.get(name, {})
            status = 'failed' if name == state.failed_step else 'completed'
            steps.append({
                'name': name,
                'status': status,
                'started_at': timing.get('started_at'),
                'duration_ms': timing.get('duration_ms'),
                'error': timing.get('error'),
            })
        return steps
