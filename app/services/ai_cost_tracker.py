"""
💰 AI Cost Tracker — Rastreamento centralizado de custos de IA.

Registra cada chamada a serviços de IA (LLM, transcrição, motion analysis)
com custos estimados, tokens, duração, e metadados.

Tabela: ai_usage_log (criada automaticamente no primeiro uso)

Uso:
    from app.services.ai_cost_tracker import log_ai_usage

    log_ai_usage(
        project_id="uuid",
        service_type="triage_llm",
        provider="openai",
        model="gpt-4o-mini",
        tokens_in=5000,
        tokens_out=800,
        cost_usd=0.0054,
        duration_ms=3200,
        metadata={"assets_count": 5},
    )

Autor: Claude + Vinicius
Data: 08/Fev/2026
"""

import logging
import json
import uuid
from typing import Optional, Dict, Any
from decimal import Decimal

logger = logging.getLogger(__name__)

# Tabela já criada?
_table_ensured = False


# ═══════════════════════════════════════════════════════════════
# CUSTOS POR UNIDADE (para estimativa automática)
# ═══════════════════════════════════════════════════════════════

# OpenAI pricing (USD per 1M tokens)
OPENAI_COSTS = {
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4-turbo": {"input": 10.00, "output": 30.00},
}

# AssemblyAI pricing (USD per hour of audio)
ASSEMBLYAI_COST_PER_HOUR = 0.37  # Standard tier

# Modal pricing (USD per hour)
MODAL_COSTS = {
    "cpu-4vcpu-16gb": 0.096,  # v-motion-analyzer
    "gpu-a10g": 1.67,  # v-vision-diretor (descontinuado)
}


def estimate_openai_cost(
    model: str, tokens_in: int, tokens_out: int
) -> float:
    """Estima custo de chamada OpenAI em USD."""
    costs = OPENAI_COSTS.get(model, OPENAI_COSTS.get("gpt-4o-mini"))
    cost = (tokens_in * costs["input"] / 1_000_000) + (
        tokens_out * costs["output"] / 1_000_000
    )
    return round(cost, 6)


def estimate_assemblyai_cost(duration_ms: int) -> float:
    """Estima custo de transcrição AssemblyAI em USD."""
    hours = duration_ms / 1000 / 3600
    return round(hours * ASSEMBLYAI_COST_PER_HOUR, 6)


def estimate_modal_cost(
    resource_type: str, duration_ms: int
) -> float:
    """Estima custo de processamento Modal em USD."""
    cost_per_hour = MODAL_COSTS.get(resource_type, 0.096)
    hours = duration_ms / 1000 / 3600
    return round(hours * cost_per_hour, 6)


# ═══════════════════════════════════════════════════════════════
# TABELA
# ═══════════════════════════════════════════════════════════════

def _ensure_table():
    """Cria tabela ai_usage_log se não existir."""
    global _table_ensured
    if _table_ensured:
        return

    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ai_usage_log (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                project_id UUID,
                conversation_id UUID,
                service_type VARCHAR(50) NOT NULL,
                provider VARCHAR(50) NOT NULL,
                model VARCHAR(100),
                tokens_in INTEGER DEFAULT 0,
                tokens_out INTEGER DEFAULT 0,
                duration_ms INTEGER DEFAULT 0,
                cost_usd DECIMAL(10,6) DEFAULT 0,
                input_units INTEGER DEFAULT 0,
                output_units INTEGER DEFAULT 0,
                metadata JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_ai_usage_project
                ON ai_usage_log(project_id);
            CREATE INDEX IF NOT EXISTS idx_ai_usage_service
                ON ai_usage_log(service_type);
            CREATE INDEX IF NOT EXISTS idx_ai_usage_created
                ON ai_usage_log(created_at);
        """)
        conn.commit()
        cursor.close()
        conn.close()
        _table_ensured = True
        logger.info("💰 [COST] Tabela ai_usage_log OK")
    except Exception as e:
        logger.error(f"❌ [COST] Falha ao criar tabela: {e}")


# ═══════════════════════════════════════════════════════════════
# LOG DE USO
# ═══════════════════════════════════════════════════════════════

def log_ai_usage(
    service_type: str,
    provider: str,
    model: str = "",
    project_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    tokens_in: int = 0,
    tokens_out: int = 0,
    duration_ms: int = 0,
    cost_usd: float = 0.0,
    input_units: int = 0,
    output_units: int = 0,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Registra uma chamada a serviço de IA no banco.

    Args:
        service_type: Tipo do serviço. Valores padronizados:
            - "triage_llm"          → Asset Triage (GPT-4o-mini)
            - "triage_frames"       → Frame extraction (ffmpeg)
            - "vision_motion"       → Vision Director - RAFT (Modal CPU)
            - "vision_llm"          → Vision Director - LLM (GPT-4o-mini)
            - "transcription"       → Transcrição (AssemblyAI)
            - "chatbot_llm"         → Chatbot (GPT-4o-mini)
            - "router_llm"          → Router (GPT-4o-mini)
        provider: "openai", "assemblyai", "modal", "ffmpeg_local"
        model: Nome do modelo ("gpt-4o-mini", "raft_small", "whisper")
        project_id: ID do projeto (opcional)
        conversation_id: ID da conversa (opcional)
        tokens_in: Tokens de entrada (LLM)
        tokens_out: Tokens de saída (LLM)
        duration_ms: Duração da chamada
        cost_usd: Custo estimado em USD (se 0, tenta estimar)
        input_units: Unidades de entrada (ex: num_frames, audio_seconds)
        output_units: Unidades de saída
        metadata: Dados extras (JSONB)

    Returns:
        ID do registro criado, ou None se falhou
    """
    _ensure_table()

    # Auto-estimar custo se não fornecido
    if cost_usd == 0.0:
        if provider == "openai" and tokens_in > 0:
            cost_usd = estimate_openai_cost(model, tokens_in, tokens_out)
        elif provider == "assemblyai" and duration_ms > 0:
            cost_usd = estimate_assemblyai_cost(duration_ms)
        elif provider == "modal" and duration_ms > 0:
            cost_usd = estimate_modal_cost(model, duration_ms)

    try:
        from app.db import get_db_connection
        record_id = str(uuid.uuid4())
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO ai_usage_log
                (id, project_id, conversation_id, service_type, provider,
                 model, tokens_in, tokens_out, duration_ms, cost_usd,
                 input_units, output_units, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record_id,
            project_id,
            conversation_id,
            service_type,
            provider,
            model or "",
            tokens_in,
            tokens_out,
            duration_ms,
            cost_usd,
            input_units,
            output_units,
            json.dumps(metadata or {}, ensure_ascii=False),
        ))
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(
            f"💰 [COST] {service_type}/{provider}"
            f"{'/' + model if model else ''} "
            f"${cost_usd:.6f} "
            f"({duration_ms}ms, {tokens_in}→{tokens_out}t)"
        )

        return record_id

    except Exception as e:
        logger.error(f"❌ [COST] Falha ao registrar: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# CONSULTAS
# ═══════════════════════════════════════════════════════════════

def get_project_costs(project_id: str) -> Dict:
    """Retorna resumo de custos de um projeto."""
    _ensure_table()

    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                service_type,
                COUNT(*) as calls,
                SUM(tokens_in) as total_tokens_in,
                SUM(tokens_out) as total_tokens_out,
                SUM(duration_ms) as total_duration_ms,
                SUM(cost_usd) as total_cost_usd
            FROM ai_usage_log
            WHERE project_id = %s
            GROUP BY service_type
            ORDER BY total_cost_usd DESC
        """, (project_id,))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        services = {}
        total_cost = 0.0
        total_calls = 0

        for row in rows:
            svc = row[0]
            cost = float(row[5] or 0)
            calls = row[1] or 0
            services[svc] = {
                "calls": calls,
                "tokens_in": row[2] or 0,
                "tokens_out": row[3] or 0,
                "duration_ms": row[4] or 0,
                "cost_usd": cost,
            }
            total_cost += cost
            total_calls += calls

        return {
            "project_id": project_id,
            "total_cost_usd": round(total_cost, 6),
            "total_calls": total_calls,
            "services": services,
        }

    except Exception as e:
        logger.error(f"❌ [COST] Erro ao consultar custos: {e}")
        return {"project_id": project_id, "total_cost_usd": 0, "services": {}}
