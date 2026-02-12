"""
🔀 Routing Validator — Lógica determinística de roteamento pós-classificação.

Princípio: A LLM CLASSIFICA (decisão semântica), a lógica ROTEIA (regra de negócio).
Nunca pedir ao LLM para decidir routing — isso é determinístico.

Regras de roteamento:
  - vision_analysis: b_roll, screen_capture, image_static → RAFT + análise visual
  - pipeline_ready: talking_head, audio_narration → transcrição é o conteúdo principal
  - background_audio: music_only → áudio de fundo, sem processamento visual
  - skip: briefing_prompt → referência do usuário, não entra no vídeo final

Autor: Vinicius + Claude
Data: 09/Fev/2026
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# REGRAS DE ROTEAMENTO (determinísticas)
# ═══════════════════════════════════════════════════════════════

# Assets que precisam de análise visual (RAFT motion + GPT-4o-mini)
VISION_ANALYSIS_TYPES = {'b_roll', 'screen_capture', 'image_static'}

# Assets prontos para o pipeline de renderização (fala é o conteúdo)
PIPELINE_READY_TYPES = {'talking_head', 'audio_narration'}

# Assets de áudio de fundo (sem processamento visual)
BACKGROUND_AUDIO_TYPES = {'music_only'}

# Assets que são referência/instrução (não entram no vídeo)
SKIP_TYPES = {'briefing_prompt'}


def build_routing(classified_assets: List[Dict]) -> Dict:
    """
    Constrói routing determinístico baseado na classificação dos assets.

    Args:
        classified_assets: Lista de assets com campo 'classification' e 'asset_id'
                          (saída da LLM, já classificados)

    Returns:
        Dict com listas de asset_ids por categoria de roteamento:
        {
            'vision_analysis': [...],   # Vão para RAFT + análise visual
            'pipeline_ready': [...],    # Prontos para pipeline
            'retake_review': [...],     # Retakes detectados (vem da LLM)
            'background_audio': [...],  # Áudio de fundo
        }
    """
    vision_ids = []
    pipeline_ids = []
    audio_ids = []

    for asset in classified_assets:
        aid = asset.get('asset_id', '')
        cls = asset.get('classification', '')

        if cls in VISION_ANALYSIS_TYPES:
            vision_ids.append(aid)
        elif cls in PIPELINE_READY_TYPES:
            pipeline_ids.append(aid)
        elif cls in BACKGROUND_AUDIO_TYPES:
            audio_ids.append(aid)
        else:
            # briefing_prompt, unknown → pipeline_ready (não gasta com RAFT)
            pipeline_ids.append(aid)

    logger.info(
        f"🔀 [ROUTING] Determinístico: "
        f"vision={len(vision_ids)}, "
        f"pipeline={len(pipeline_ids)}, "
        f"audio={len(audio_ids)} "
        f"(total={len(classified_assets)})"
    )

    return {
        'vision_analysis': vision_ids,
        'pipeline_ready': pipeline_ids,
        'retake_review': [],  # Preenchido pelo caller com base nos retakes da LLM
        'background_audio': audio_ids,
    }


def build_routing_with_retakes(
    classified_assets: List[Dict],
    retakes: List[Dict],
) -> Dict:
    """
    Constrói routing incluindo retakes identificados pela LLM.

    Args:
        classified_assets: Assets classificados pela LLM
        retakes: Lista de retakes detectados pela LLM

    Returns:
        Routing completo com retake_review preenchido
    """
    routing = build_routing(classified_assets)

    # Extrair IDs de retakes
    retake_ids = set()
    for retake in retakes:
        retake_ids.add(retake.get('original_asset_id', ''))
        retake_ids.add(retake.get('retake_asset_id', ''))
    retake_ids.discard('')

    routing['retake_review'] = list(retake_ids)

    if retake_ids:
        logger.info(
            f"🔀 [ROUTING] {len(retake_ids)} asset(s) em retake_review"
        )

    return routing


def needs_vision_analysis(classification: str) -> bool:
    """Verifica se um tipo de classificação precisa de análise visual."""
    return classification in VISION_ANALYSIS_TYPES


def get_vision_asset_count(classified_assets: List[Dict]) -> int:
    """Conta quantos assets precisam de análise visual."""
    return sum(
        1 for a in classified_assets
        if a.get('classification', '') in VISION_ANALYSIS_TYPES
    )


# ═══════════════════════════════════════════════════════════════
# TRANSCRIPT ANALYSIS — Assets que precisam análise de transcrição
# ═══════════════════════════════════════════════════════════════

# Assets cujo conteúdo principal é a fala/narração
TRANSCRIPT_ANALYSIS_TYPES = {'talking_head', 'audio_narration'}


def needs_transcript_analysis(classification: str) -> bool:
    """Verifica se um tipo de classificação precisa de análise de transcrição."""
    return classification in TRANSCRIPT_ANALYSIS_TYPES


def get_transcript_asset_count(classified_assets: List[Dict]) -> int:
    """Conta quantos assets precisam de análise de transcrição."""
    return sum(
        1 for a in classified_assets
        if a.get('classification', '') in TRANSCRIPT_ANALYSIS_TYPES
    )


def get_transcript_asset_ids(classified_assets: List[Dict]) -> List[str]:
    """Retorna IDs dos assets que precisam de análise de transcrição."""
    return [
        a.get('asset_id', '')
        for a in classified_assets
        if a.get('classification', '') in TRANSCRIPT_ANALYSIS_TYPES
    ]
