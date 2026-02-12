"""
🔍 Pipeline Debug Logger

Sistema para salvar payloads de entrada/saída de cada step do pipeline.
Permite investigar o fluxo de dados entre steps.

🆕 v3.10.0: Suporte a CHECKPOINTS para Pipeline Replay.
- Checkpoint = estado COMPLETO do PipelineState salvo após cada step
- Usado pelo LLM Director para replay parcial do pipeline
- Limite de payload maior (1MB) para checkpoints

🔧 AUTO-LIMPEZA:
- Mantém apenas os últimos MAX_LOGS_TOTAL logs no total
- Mantém logs por no máximo MAX_AGE_DAYS dias
- Limpeza automática a cada CLEANUP_INTERVAL inserções

Uso:
    from .debug_logger import PipelineDebugLogger
    
    debug = PipelineDebugLogger()
    debug.log_step(job_id, "render_service", "input", payload)
    debug.log_step(job_id, "render_service", "output", result)
    
    # 🆕 Checkpoints (usado pelo PipelineEngine automaticamente)
    debug.log_checkpoint(job_id, "generate_pngs", state_dict, duration_ms=1500)
    checkpoints = debug.get_checkpoints(job_id)
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PipelineDebugLogger:
    """
    Salva logs de debug do pipeline no banco de dados.
    
    Cada log contém:
    - job_id: ID do job
    - step_name: Nome do step (ex: "silence_cut", "render_service")
    - direction: "input", "output", ou "state_after" (checkpoint)
    - payload: Dados do payload (truncado se muito grande)
    - timestamp: Quando foi logado
    
    Auto-limpeza:
    - Mantém MAX_LOGS_TOTAL logs no total
    - Remove logs mais antigos que MAX_AGE_DAYS dias
    
    🆕 Checkpoints (Pipeline Replay):
    - direction="state_after" → estado completo após step
    - metadata.checkpoint = True
    - Limite de payload maior (1MB)
    """
    
    MAX_PAYLOAD_SIZE = 50000          # 50KB max por payload (default)
    MAX_PAYLOAD_SIZE_RENDER = 500000  # 500KB max para render_service
    MAX_PAYLOAD_SIZE_CHECKPOINT = 1000000  # 1MB max para checkpoints (state completo)
    MAX_LOGS_TOTAL = 5000      # 🆕 v3.10.0: Aumentado para suportar checkpoints (~38 por job)
    MAX_AGE_DAYS = 7           # 🆕 v3.10.0: 7 dias para permitir replay tardio
    CLEANUP_INTERVAL = 50      # 🆕 v3.10.0: Limpar a cada 50 inserções
    
    _insert_count = 0  # Contador de inserções para trigger de limpeza
    
    def __init__(self):
        self.enabled = True
    
    def log_step(
        self,
        job_id: str,
        step_name: str,
        direction: str,  # "input" ou "output"
        payload: Any,
        metadata: Dict[str, Any] = None
    ) -> bool:
        """
        Salva log de um step do pipeline.
        
        Args:
            job_id: ID do job
            step_name: Nome do step
            direction: "input" ou "output"
            payload: Dados do payload
            metadata: Metadados adicionais (opcional)
            
        Returns:
            True se salvou com sucesso
        """
        if not self.enabled:
            return False
            
        try:
            from app.supabase_client import get_direct_db_connection
            
            # Serializar payload (limite escalonado por tipo)
            payload_json = self._serialize_payload(payload, step_name, direction)
            
            # Extrair campos importantes para busca rápida
            extracted = self._extract_important_fields(payload)
            
            db_conn = get_direct_db_connection()
            cursor = db_conn.cursor()
            
            try:
                cursor.execute("""
                    INSERT INTO pipeline_debug_logs 
                    (job_id, step_name, direction, payload, extracted_fields, metadata, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    job_id,
                    step_name,
                    direction,
                    payload_json,
                    json.dumps(extracted) if extracted else None,
                    json.dumps(metadata) if metadata else None,
                    datetime.now(timezone.utc).isoformat()
                ))
                
                db_conn.commit()
                logger.debug(f"📝 Debug log: {step_name}/{direction} para job {job_id[:8]}...")
                
                # Auto-limpeza periódica
                PipelineDebugLogger._insert_count += 1
                if PipelineDebugLogger._insert_count >= self.CLEANUP_INTERVAL:
                    self._auto_cleanup(cursor, db_conn)
                    PipelineDebugLogger._insert_count = 0
                
                return True
                
            finally:
                cursor.close()
                db_conn.close()
                
        except Exception as e:
            # Não deixar erros de debug quebrar o pipeline
            logger.warning(f"⚠️ Erro ao salvar debug log: {e}")
            return False
    
    def _auto_cleanup(self, cursor, db_conn):
        """
        Remove logs antigos automaticamente.
        
        Regras:
        1. Remove logs mais antigos que MAX_AGE_DAYS dias
        2. Se ainda tiver mais que MAX_LOGS_TOTAL, remove os mais antigos
        """
        try:
            # 1. Remover logs antigos por data
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=self.MAX_AGE_DAYS)).isoformat()
            cursor.execute("""
                DELETE FROM pipeline_debug_logs 
                WHERE created_at < %s
            """, (cutoff_date,))
            deleted_by_age = cursor.rowcount
            
            # 2. Se ainda tiver muitos, remover os mais antigos
            cursor.execute("SELECT COUNT(*) FROM pipeline_debug_logs")
            total_count = cursor.fetchone()[0]
            
            if total_count > self.MAX_LOGS_TOTAL:
                excess = total_count - self.MAX_LOGS_TOTAL
                cursor.execute("""
                    DELETE FROM pipeline_debug_logs 
                    WHERE id IN (
                        SELECT id FROM pipeline_debug_logs 
                        ORDER BY created_at ASC 
                        LIMIT %s
                    )
                """, (excess,))
                deleted_by_count = cursor.rowcount
            else:
                deleted_by_count = 0
            
            db_conn.commit()
            
            if deleted_by_age > 0 or deleted_by_count > 0:
                logger.info(f"🧹 Auto-cleanup: removidos {deleted_by_age} por idade + {deleted_by_count} por excesso")
                
        except Exception as e:
            logger.warning(f"⚠️ Erro no auto-cleanup: {e}")
    
    def log_checkpoint(
        self,
        job_id: str,
        step_name: str,
        state_dict: Dict[str, Any],
        duration_ms: int = 0,
        attempt: int = 1
    ) -> bool:
        """
        🆕 v3.10.0: Salva checkpoint de estado completo após step.
        
        Diferente de log_step (que salva dados cherry-picked),
        log_checkpoint salva o PipelineState COMPLETO.
        
        Usado pelo PipelineEngine._execute_step() automaticamente.
        O LLM Director usa get_checkpoints() para ler estes dados.
        
        Args:
            job_id: ID do job
            step_name: Nome do step que acabou de executar
            state_dict: PipelineState.to_dict() completo
            duration_ms: Tempo de execução do step
            attempt: Número da tentativa (retry)
            
        Returns:
            True se salvou com sucesso
        """
        return self.log_step(
            job_id=job_id,
            step_name=step_name,
            direction="state_after",
            payload=state_dict,
            metadata={
                "checkpoint": True,
                "duration_ms": duration_ms,
                "attempt": attempt,
                "engine_version": "3.10.0",
                "completed_steps": state_dict.get("completed_steps", []),
            }
        )

    def get_checkpoints(self, job_id: str) -> List[Dict[str, Any]]:
        """
        🆕 v3.10.0: Retorna checkpoints de um job para Pipeline Replay.
        
        Retorna lista de steps com checkpoints salvos, ordenados
        pela ordem de execução.
        
        Args:
            job_id: ID do job
            
        Returns:
            Lista de dicts com info dos checkpoints:
            [
                {
                    "step_name": "normalize",
                    "direction": "state_after",
                    "created_at": "2026-02-06T...",
                    "duration_ms": 1500,
                    "completed_steps": ["load_template", "normalize"],
                    "has_payload": True,
                    "payload_size": 45000
                },
                ...
            ]
        """
        try:
            from app.supabase_client import get_direct_db_connection
            
            db_conn = get_direct_db_connection()
            cursor = db_conn.cursor()
            
            try:
                cursor.execute("""
                    SELECT step_name, direction, created_at, metadata,
                           LENGTH(payload) as payload_size
                    FROM pipeline_debug_logs
                    WHERE job_id = %s 
                      AND direction = 'state_after'
                      AND metadata->>'checkpoint' = 'true'
                    ORDER BY created_at ASC
                """, (job_id,))
                
                rows = cursor.fetchall()
                
                checkpoints = []
                for row in rows:
                    meta = row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {}
                    checkpoints.append({
                        "step_name": row[0],
                        "direction": row[1],
                        "created_at": row[2].isoformat() if hasattr(row[2], 'isoformat') else str(row[2]),
                        "duration_ms": meta.get("duration_ms", 0),
                        "attempt": meta.get("attempt", 1),
                        "completed_steps": meta.get("completed_steps", []),
                        "has_payload": True,
                        "payload_size": row[4] or 0,
                    })
                
                return checkpoints
                
            finally:
                cursor.close()
                db_conn.close()
                
        except Exception as e:
            logger.warning(f"⚠️ Erro ao buscar checkpoints para {job_id}: {e}")
            return []

    def get_step_checkpoint(self, job_id: str, step_name: str) -> Optional[Dict[str, Any]]:
        """
        🆕 v3.10.0: Retorna payload completo de um checkpoint específico.
        
        Usado pelo Director para inspecionar/modificar o estado
        de um step específico antes de replay.
        
        Args:
            job_id: ID do job
            step_name: Nome do step
            
        Returns:
            Dict com o PipelineState completo do checkpoint, ou None
        """
        try:
            from app.supabase_client import get_direct_db_connection
            
            db_conn = get_direct_db_connection()
            cursor = db_conn.cursor()
            
            try:
                cursor.execute("""
                    SELECT payload, metadata, created_at
                    FROM pipeline_debug_logs
                    WHERE job_id = %s 
                      AND step_name = %s
                      AND direction = 'state_after'
                      AND metadata->>'checkpoint' = 'true'
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (job_id, step_name))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                # Desserializar payload
                payload_raw = row[0]
                if isinstance(payload_raw, dict):
                    payload = payload_raw
                elif isinstance(payload_raw, str):
                    # Verificar se foi truncado
                    if '... [TRUNCADO' in payload_raw:
                        logger.warning(f"⚠️ Checkpoint truncado para {step_name} de {job_id}")
                        return None
                    payload = json.loads(payload_raw)
                else:
                    return None
                
                return payload
                
            finally:
                cursor.close()
                db_conn.close()
                
        except Exception as e:
            logger.warning(f"⚠️ Erro ao buscar checkpoint {step_name} de {job_id}: {e}")
            return None

    def _serialize_payload(self, payload: Any, step_name: str = None,
                           direction: str = None) -> str:
        """
        Serializa payload para JSON, truncando se necessário.
        
        🆕 v3.10.0: Limite escalonado:
        - Checkpoints (state_after): 1MB
        - render_service: 500KB
        - Demais: 50KB
        """
        try:
            if payload is None:
                return "null"
            
            json_str = json.dumps(payload, default=str, ensure_ascii=False)
            
            # 🆕 v3.10.0: Limite escalonado por tipo
            if direction == "state_after":
                max_size = self.MAX_PAYLOAD_SIZE_CHECKPOINT
            elif step_name == "render_service":
                max_size = self.MAX_PAYLOAD_SIZE_RENDER
            else:
                max_size = self.MAX_PAYLOAD_SIZE
            
            # Truncar se muito grande
            if len(json_str) > max_size:
                truncated = json_str[:max_size]
                return truncated + f'... [TRUNCADO - total: {len(json_str)} chars]'
            
            return json_str
            
        except Exception as e:
            return json.dumps({"error": f"Não foi possível serializar: {str(e)}"})
    
    def _extract_important_fields(self, payload: Any) -> Dict[str, Any]:
        """
        Extrai campos importantes para busca rápida.
        Retorna um dict com valores resumidos.
        """
        if not isinstance(payload, dict):
            return {}
        
        extracted = {}
        
        # Campos importantes para debug
        important_keys = [
            "template_id",
            "project_id", 
            "user_id",
            "job_id",
            "duration_ms",
            "duration_in_frames",
            "video_url",
            "status",
            "error"
        ]
        
        for key in important_keys:
            if key in payload:
                val = payload[key]
                # Truncar strings longas
                if isinstance(val, str) and len(val) > 100:
                    extracted[key] = val[:100] + "..."
                else:
                    extracted[key] = val
        
        # 🔧 Buscar template_id dentro de template_config (onde geralmente está)
        if "template_config" in payload and isinstance(payload["template_config"], dict):
            tc = payload["template_config"]
            if "template_id" in tc:
                extracted["template_id"] = tc["template_id"]
            # Também buscar em template-mode (fallback)
            if "template-mode" in tc and isinstance(tc["template-mode"], dict):
                tm = tc["template-mode"]
                if "template_id" in tm and not extracted.get("template_id"):
                    extracted["template_id"] = tm["template_id"]
        
        # Contar elementos em arrays
        if "subtitles" in payload.get("tracks", {}):
            extracted["subtitles_count"] = len(payload["tracks"]["subtitles"])
        
        if "sentences" in payload:
            extracted["sentences_count"] = len(payload["sentences"])
        
        return extracted


# Instância global
_debug_logger = None

def get_debug_logger() -> PipelineDebugLogger:
    """Retorna instância do debug logger."""
    global _debug_logger
    if _debug_logger is None:
        _debug_logger = PipelineDebugLogger()
    return _debug_logger


# 🆕 Instância para import direto: from .debug_logger import debug_logger
debug_logger = get_debug_logger()

