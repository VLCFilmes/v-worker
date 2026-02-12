"""
Pipeline Logger Service - Sistema de Observabilidade do Pipeline

Este serviço permite rastrear cada execução do pipeline de vídeo,
salvando inputs, outputs, payloads e métricas de cada etapa.

Uso:
    logger = PipelineLogger(job_id, user_id, phase=2)
    
    step_id = logger.start_step('matting', service_url='https://modal.run/...')
    logger.log_request(step_id, payload)
    
    # ... chamar serviço ...
    
    logger.log_response(step_id, response, status_code=200, duration_ms=1234)
    logger.log_artifact(step_id, 'foreground_webm', url, duration_ms=7400)
    logger.log_metric(step_id, 'matting_fps', 24.6, 'fps')
    logger.complete_step(step_id)

Versão: 1.0.0
Data: 23/Jan/2026
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import psycopg2
from psycopg2.extras import RealDictCursor, Json

logger = logging.getLogger(__name__)

# Versão do backend (pode ser sobrescrita via env)
BACKEND_VERSION = os.environ.get('BACKEND_VERSION', 'v2.9.172')


class PipelineLogger:
    """
    Logger para rastreabilidade completa do pipeline de vídeo.
    
    Salva no banco de dados:
    - Execuções (runs)
    - Etapas (steps)
    - Arquivos gerados (artifacts)
    - Payloads de request/response
    - Métricas de performance
    """
    
    def __init__(
        self,
        job_id: str,
        user_id: str,
        phase: int,
        project_id: str = None,
        template_id: str = None,
        conversation_id: str = None,
        options: Dict[str, Any] = None,
        template_config: Dict[str, Any] = None,
        worker_id: str = None,
        auto_start: bool = True
    ):
        """
        Inicializa o logger e cria um novo run no banco.
        
        Args:
            job_id: ID do job de processamento
            user_id: ID do usuário
            phase: Fase do pipeline (1 ou 2)
            project_id: ID do projeto (opcional)
            template_id: ID do template (opcional)
            conversation_id: ID da conversa (opcional)
            options: Opções de processamento (silence_cut, auto_pipeline, etc.)
            template_config: Configuração do template (snapshot)
            worker_id: ID do worker (hetzner, linux-home, modal)
            auto_start: Se True, marca o run como 'running' automaticamente
        """
        self.job_id = job_id
        self.user_id = user_id
        self.phase = phase
        self.project_id = project_id
        self.template_id = template_id
        self.conversation_id = conversation_id
        self.options = options or {}
        self.template_config = template_config
        self.worker_id = worker_id or os.environ.get('WORKER_ID', 'unknown')
        
        # Estado interno
        self.run_id: Optional[str] = None
        self.step_counter = 0
        self._conn = None
        
        # Criar run no banco
        try:
            self.run_id = self._create_run()
            if auto_start:
                self._start_run()
            logger.info(f"📊 [PipelineLogger] Run criado: {self.run_id} (job={job_id}, phase={phase})")
        except Exception as e:
            logger.error(f"❌ [PipelineLogger] Erro ao criar run: {e}")
            # Não falhar - logging é opcional
            self.run_id = str(uuid4())  # Fallback ID
    
    def _get_connection(self):
        """Obtém conexão com o banco de dados."""
        if self._conn is None or self._conn.closed:
            db_url = os.environ.get('DATABASE_URL')
            if not db_url:
                raise ValueError("DATABASE_URL não configurada")
            self._conn = psycopg2.connect(db_url)
        return self._conn
    
    def _execute(self, query: str, params: tuple = None, fetch: bool = False):
        """Executa query no banco de dados."""
        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch:
                    result = cur.fetchone()
                else:
                    result = None
                conn.commit()
                return result
        except Exception as e:
            logger.error(f"❌ [PipelineLogger] Erro no banco: {e}")
            if self._conn:
                self._conn.rollback()
            raise
    
    def _create_run(self) -> str:
        """Cria um novo run no banco e retorna o ID."""
        # Calcular run_number
        result = self._execute(
            """
            SELECT COALESCE(MAX(run_number), 0) + 1 as next_run
            FROM pipeline_runs 
            WHERE job_id = %s AND phase = %s
            """,
            (self.job_id, self.phase),
            fetch=True
        )
        run_number = result['next_run'] if result else 1
        
        # Inserir run
        run_id = str(uuid4())
        self._execute(
            """
            INSERT INTO pipeline_runs (
                id, job_id, user_id, phase, project_id, template_id, 
                conversation_id, run_number, options, template_config,
                worker_id, worker_hostname, backend_version, status, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s,
                %s, %s, %s, 'pending', NOW()
            )
            """,
            (
                run_id, self.job_id, self.user_id, self.phase, 
                self.project_id, self.template_id, self.conversation_id,
                run_number, Json(self.options), Json(self.template_config) if self.template_config else None,
                self.worker_id, os.environ.get('HOSTNAME', 'unknown'), BACKEND_VERSION
            )
        )
        
        return run_id
    
    def _start_run(self):
        """Marca o run como iniciado."""
        self._execute(
            """
            UPDATE pipeline_runs 
            SET status = 'running', started_at = NOW()
            WHERE id = %s
            """,
            (self.run_id,)
        )
    
    def set_input_video(self, url: str, duration_ms: int = None):
        """Define o vídeo de input do run."""
        self._execute(
            """
            UPDATE pipeline_runs 
            SET input_video_url = %s, input_video_duration_ms = %s
            WHERE id = %s
            """,
            (url, duration_ms, self.run_id)
        )
    
    def start_step(
        self,
        step_name: str,
        service_name: str = None,
        service_url: str = None,
        service_method: str = 'POST'
    ) -> str:
        """
        Inicia uma etapa do pipeline.
        
        Args:
            step_name: Nome da etapa (normalize, transcribe, matting, etc.)
            service_name: Nome do serviço (v-services, modal, assemblyai)
            service_url: URL do endpoint
            service_method: Método HTTP (POST, GET)
            
        Returns:
            step_id: ID da etapa criada
        """
        self.step_counter += 1
        step_id = str(uuid4())
        
        try:
            self._execute(
                """
                INSERT INTO pipeline_steps (
                    id, run_id, step_name, step_order, status,
                    service_name, service_url, service_method,
                    queued_at, started_at
                ) VALUES (
                    %s, %s, %s, %s, 'running',
                    %s, %s, %s,
                    NOW(), NOW()
                )
                """,
                (
                    step_id, self.run_id, step_name, self.step_counter,
                    service_name, service_url, service_method
                )
            )
            logger.info(f"   📊 [Step] {step_name} iniciado (id={step_id[:8]}...)")
        except Exception as e:
            logger.error(f"   ❌ [Step] Erro ao criar step {step_name}: {e}")
        
        return step_id
    
    def log_request(
        self,
        step_id: str,
        payload: Dict[str, Any],
        endpoint_url: str = None,
        method: str = 'POST',
        headers: Dict[str, str] = None
    ):
        """
        Salva o payload de request de uma etapa.
        
        Args:
            step_id: ID da etapa
            payload: Payload JSON enviado
            endpoint_url: URL do endpoint
            method: Método HTTP
            headers: Headers relevantes (sem auth!)
        """
        try:
            # Sanitizar headers (remover tokens)
            safe_headers = self._sanitize_headers(headers) if headers else None
            
            # Calcular tamanho
            body_str = json.dumps(payload, default=str)
            body_size = len(body_str.encode('utf-8'))
            
            self._execute(
                """
                INSERT INTO pipeline_payloads (
                    id, step_id, direction, endpoint_url, method, 
                    headers, body, body_size_bytes, created_at
                ) VALUES (
                    %s, %s, 'request', %s, %s, 
                    %s, %s, %s, NOW()
                )
                """,
                (
                    str(uuid4()), step_id, endpoint_url, method,
                    Json(safe_headers), Json(payload), body_size
                )
            )
            logger.debug(f"   📊 [Payload] Request salvo ({body_size} bytes)")
        except Exception as e:
            logger.error(f"   ❌ [Payload] Erro ao salvar request: {e}")
    
    def log_response(
        self,
        step_id: str,
        payload: Dict[str, Any],
        status_code: int = 200,
        duration_ms: int = None,
        headers: Dict[str, str] = None
    ):
        """
        Salva o payload de response de uma etapa.
        
        Args:
            step_id: ID da etapa
            payload: Payload JSON recebido
            status_code: Código HTTP de resposta
            duration_ms: Tempo de resposta em ms
            headers: Headers de resposta
        """
        try:
            # Calcular tamanho
            body_str = json.dumps(payload, default=str)
            body_size = len(body_str.encode('utf-8'))
            
            self._execute(
                """
                INSERT INTO pipeline_payloads (
                    id, step_id, direction, headers, body, 
                    body_size_bytes, status_code, response_time_ms, created_at
                ) VALUES (
                    %s, %s, 'response', %s, %s, 
                    %s, %s, %s, NOW()
                )
                """,
                (
                    str(uuid4()), step_id, Json(headers) if headers else None, Json(payload),
                    body_size, status_code, duration_ms
                )
            )
            logger.debug(f"   📊 [Payload] Response salvo ({body_size} bytes, {status_code})")
        except Exception as e:
            logger.error(f"   ❌ [Payload] Erro ao salvar response: {e}")
    
    def log_artifact(
        self,
        step_id: str,
        artifact_name: str,
        url: str = None,
        artifact_type: str = 'output',
        b2_path: str = None,
        local_path: str = None,
        file_size_bytes: int = None,
        content_type: str = None,
        duration_ms: int = None,
        width: int = None,
        height: int = None,
        fps: float = None,
        codec: str = None,
        has_alpha: bool = False,
        frame_count: int = None,
        is_preserved: bool = True,
        metadata: Dict[str, Any] = None
    ):
        """
        Salva um artifact (arquivo gerado) de uma etapa.
        
        Args:
            step_id: ID da etapa
            artifact_name: Nome do artifact (foreground_webm, final_video, etc.)
            url: URL do arquivo
            artifact_type: Tipo (input, output, intermediate)
            b2_path: Path no Backblaze
            local_path: Path local
            file_size_bytes: Tamanho em bytes
            content_type: MIME type
            duration_ms: Duração (para vídeos)
            width, height: Dimensões
            fps: Frames por segundo
            codec: Codec de vídeo/áudio
            has_alpha: Se tem canal alpha
            frame_count: Número de frames
            is_preserved: Se deve ser preservado
            metadata: Metadados extras
        """
        try:
            # Extrair b2_path da URL se não fornecido
            if not b2_path and url and 'backblazeb2.com' in url:
                b2_path = self._extract_b2_path(url)
            
            self._execute(
                """
                INSERT INTO pipeline_artifacts (
                    id, step_id, artifact_name, artifact_type, url, b2_path, local_path,
                    file_size_bytes, content_type, duration_ms, width, height,
                    fps, codec, has_alpha, frame_count, is_preserved, metadata, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, NOW()
                )
                """,
                (
                    str(uuid4()), step_id, artifact_name, artifact_type, url, b2_path, local_path,
                    file_size_bytes, content_type, duration_ms, width, height,
                    fps, codec, has_alpha, frame_count, is_preserved, Json(metadata) if metadata else None
                )
            )
            logger.debug(f"   📊 [Artifact] {artifact_name} salvo")
        except Exception as e:
            logger.error(f"   ❌ [Artifact] Erro ao salvar {artifact_name}: {e}")
    
    def log_metric(
        self,
        step_id: str,
        metric_name: str,
        metric_value: float,
        metric_unit: str = None
    ):
        """
        Salva uma métrica de performance.
        
        Args:
            step_id: ID da etapa
            metric_name: Nome da métrica (matting_fps, frames_processed, cost_usd)
            metric_value: Valor da métrica
            metric_unit: Unidade (fps, frames, usd, percent, mb)
        """
        try:
            self._execute(
                """
                INSERT INTO pipeline_metrics (id, step_id, metric_name, metric_value, metric_unit, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (str(uuid4()), step_id, metric_name, metric_value, metric_unit)
            )
            logger.debug(f"   📊 [Metric] {metric_name}={metric_value}{metric_unit or ''}")
        except Exception as e:
            logger.error(f"   ❌ [Metric] Erro ao salvar {metric_name}: {e}")
    
    def complete_step(
        self,
        step_id: str,
        status: str = 'completed',
        error_message: str = None,
        error_code: str = None
    ):
        """
        Marca uma etapa como completa.
        
        Args:
            step_id: ID da etapa
            status: Status final (completed, failed, skipped)
            error_message: Mensagem de erro (se failed)
            error_code: Código de erro (se failed)
        """
        try:
            self._execute(
                """
                UPDATE pipeline_steps SET
                    status = %s,
                    completed_at = NOW(),
                    duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
                    error_message = %s,
                    error_code = %s
                WHERE id = %s
                """,
                (status, error_message, error_code, step_id)
            )
            emoji = '✅' if status == 'completed' else '❌' if status == 'failed' else '⏭️'
            logger.info(f"   📊 [Step] {emoji} Completo (status={status})")
        except Exception as e:
            logger.error(f"   ❌ [Step] Erro ao completar step: {e}")
    
    def fail_step(self, step_id: str, error_message: str, error_code: str = None):
        """Atalho para marcar step como falho."""
        self.complete_step(step_id, status='failed', error_message=error_message, error_code=error_code)
    
    def skip_step(self, step_id: str, reason: str = None):
        """Atalho para marcar step como pulado."""
        self.complete_step(step_id, status='skipped', error_message=reason)
    
    def complete_run(
        self,
        status: str = 'completed',
        output_url: str = None,
        output_duration_ms: int = None,
        error_message: str = None,
        error_code: str = None
    ):
        """
        Marca o run como completo.
        
        Args:
            status: Status final (completed, failed, cancelled)
            output_url: URL do vídeo final
            output_duration_ms: Duração do vídeo final
            error_message: Mensagem de erro
            error_code: Código de erro
        """
        try:
            self._execute(
                """
                UPDATE pipeline_runs SET
                    status = %s,
                    completed_at = NOW(),
                    total_duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
                    output_video_url = COALESCE(%s, output_video_url),
                    output_video_duration_ms = %s,
                    error_message = %s,
                    error_code = %s
                WHERE id = %s
                """,
                (status, output_url, output_duration_ms, error_message, error_code, self.run_id)
            )
            emoji = '✅' if status == 'completed' else '❌' if status == 'failed' else '🚫'
            logger.info(f"📊 [PipelineLogger] Run {emoji} completo (status={status})")
        except Exception as e:
            logger.error(f"❌ [PipelineLogger] Erro ao completar run: {e}")
    
    def fail_run(self, error_message: str, error_code: str = None):
        """Atalho para marcar run como falho."""
        self.complete_run(status='failed', error_message=error_message, error_code=error_code)
    
    def add_metadata(self, key: str, value: Any):
        """Adiciona metadado ao run."""
        try:
            self._execute(
                """
                UPDATE pipeline_runs 
                SET metadata = jsonb_set(COALESCE(metadata, '{}'), %s, %s::jsonb)
                WHERE id = %s
                """,
                (f'{{{key}}}', json.dumps(value, default=str), self.run_id)
            )
        except Exception as e:
            logger.error(f"❌ [PipelineLogger] Erro ao adicionar metadata: {e}")
    
    # =========================================================================
    # Métodos auxiliares
    # =========================================================================
    
    def _sanitize_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Remove tokens e dados sensíveis dos headers."""
        sensitive_keys = ['authorization', 'x-api-key', 'cookie', 'x-auth-token']
        return {
            k: '***REDACTED***' if k.lower() in sensitive_keys else v
            for k, v in headers.items()
        }
    
    def _extract_b2_path(self, url: str) -> str:
        """Extrai o path B2 de uma URL do Backblaze."""
        try:
            # URL format: https://f001.backblazeb2.com/file/bucket-name/path/to/file.ext?auth...
            if '/file/' in url:
                path_start = url.index('/file/') + len('/file/')
                path_end = url.index('?') if '?' in url else len(url)
                full_path = url[path_start:path_end]
                # Remover bucket name
                parts = full_path.split('/', 1)
                return parts[1] if len(parts) > 1 else full_path
        except:
            pass
        return None
    
    def close(self):
        """Fecha a conexão com o banco."""
        if self._conn and not self._conn.closed:
            self._conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.fail_run(str(exc_val), exc_type.__name__ if exc_type else None)
        self.close()


# =============================================================================
# Funções de conveniência
# =============================================================================

def create_pipeline_logger(
    job_id: str,
    user_id: str,
    phase: int,
    **kwargs
) -> PipelineLogger:
    """
    Cria um PipelineLogger.
    
    Uso:
        with create_pipeline_logger(job_id, user_id, 2) as logger:
            step_id = logger.start_step('matting')
            # ...
    """
    return PipelineLogger(job_id, user_id, phase, **kwargs)


def get_run_summary(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtém resumo de um run.
    
    Returns:
        Dict com informações do run, steps, artifacts e métricas
    """
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        return None
    
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Run
            cur.execute("SELECT * FROM pipeline_runs WHERE id = %s", (run_id,))
            run = dict(cur.fetchone()) if cur.rowcount > 0 else None
            
            if not run:
                return None
            
            # Steps
            cur.execute(
                "SELECT * FROM pipeline_steps WHERE run_id = %s ORDER BY step_order",
                (run_id,)
            )
            steps = [dict(row) for row in cur.fetchall()]
            
            # Artifacts e payloads para cada step
            for step in steps:
                cur.execute(
                    "SELECT * FROM pipeline_artifacts WHERE step_id = %s",
                    (step['id'],)
                )
                step['artifacts'] = [dict(row) for row in cur.fetchall()]
                
                cur.execute(
                    "SELECT * FROM pipeline_payloads WHERE step_id = %s ORDER BY direction",
                    (step['id'],)
                )
                step['payloads'] = [dict(row) for row in cur.fetchall()]
                
                cur.execute(
                    "SELECT * FROM pipeline_metrics WHERE step_id = %s",
                    (step['id'],)
                )
                step['metrics'] = [dict(row) for row in cur.fetchall()]
            
            run['steps'] = steps
            return run
            
    except Exception as e:
        logger.error(f"Erro ao buscar run: {e}")
        return None
    finally:
        if conn:
            conn.close()
