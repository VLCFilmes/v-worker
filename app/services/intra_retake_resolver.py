"""
🔧 Intra-Retake Resolver — Módulo DETERMINÍSTICO para segmentação de retakes.

Segue o padrão routing_validator.py:
  - LLM detecta repetições (decisão SEMÂNTICA)
  - Este módulo calcula timestamps e segmentos (lógica DETERMINÍSTICA)

A LLM é boa em entender que "Tenho aqui o lado das coisas digitais"
é uma repetição. Mas é RUIM em calcular timestamps e construir
segmentos sem sobreposição.

Fluxo:
  1. Recebe decisões semânticas da LLM (qual frase repetiu, qual manter)
  2. Recebe word timestamps reais do AssemblyAI
  3. Encontra ocorrências exatas via text matching
  4. Constrói segmentos precisos, sem sobreposição, cronológicos
  5. Retorna no formato que o frontend espera (segments[])

Autor: Vinicius + Claude
Data: 09/Fev/2026
"""

import re
import uuid
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# TEXT MATCHING
# ═══════════════════════════════════════════════════════════════

def _normalize(text: str) -> str:
    """Normaliza texto para comparação: lowercase, sem pontuação."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s]', '', text)  # Remove pontuação
    text = re.sub(r'\s+', ' ', text)     # Colapsa espaços
    return text


def _find_phrase_occurrences(
    words: List[Dict],
    phrase: str,
) -> List[Tuple[int, int]]:
    """
    Encontra todas as ocorrências de uma frase no array de words.

    Usa sliding window com text matching normalizado.

    Args:
        words: Lista de {text, start, end} do AssemblyAI
        phrase: Frase a buscar (da LLM)

    Returns:
        Lista de (start_word_idx, end_word_idx) — inclusive em ambos
    """
    if not words or not phrase:
        return []

    normalized_phrase = _normalize(phrase)
    phrase_words = normalized_phrase.split()
    phrase_len = len(phrase_words)

    if phrase_len == 0:
        return []

    occurrences = []
    word_texts = [_normalize(w.get('text', '')) for w in words]

    for i in range(len(word_texts) - phrase_len + 1):
        window = word_texts[i:i + phrase_len]
        if window == phrase_words:
            occurrences.append((i, i + phrase_len - 1))

    # Se não encontrou match exato, tentar match parcial (fuzzy)
    if not occurrences:
        occurrences = _find_fuzzy_occurrences(word_texts, phrase_words)

    return occurrences


def _find_fuzzy_occurrences(
    word_texts: List[str],
    phrase_words: List[str],
) -> List[Tuple[int, int]]:
    """
    Fallback: busca com tolerância a palavras levemente diferentes.

    Aceita match se >= 80% das palavras coincidem na posição.
    """
    phrase_len = len(phrase_words)
    threshold = 0.8
    occurrences = []

    for i in range(len(word_texts) - phrase_len + 1):
        window = word_texts[i:i + phrase_len]
        matches = sum(1 for a, b in zip(window, phrase_words) if a == b)
        score = matches / phrase_len

        if score >= threshold:
            occurrences.append((i, i + phrase_len - 1))

    return occurrences


# ═══════════════════════════════════════════════════════════════
# SEGMENT BUILDER
# ═══════════════════════════════════════════════════════════════

def _build_segments_from_words(
    words: List[Dict],
    removed_ranges: List[Tuple[int, int]],
    removal_reasons: Dict[Tuple[int, int], str],
) -> List[Dict]:
    """
    Constrói segmentos não-sobrepostos que cobrem TODOS os words.

    Cada word é marcado como 'keep' ou 'removed'. Words consecutivos
    com o mesmo status são agrupados em um único segmento.

    Args:
        words: Lista de {text, start, end} do AssemblyAI
        removed_ranges: Lista de (start_idx, end_idx) a remover
        removal_reasons: Dict de (start_idx, end_idx) → reason

    Returns:
        Lista de segments no formato do frontend
    """
    if not words:
        return []

    # 1. Marcar cada word como keep/removed
    word_status = ['keep'] * len(words)
    word_reason = [''] * len(words)

    for (start_idx, end_idx) in removed_ranges:
        reason = removal_reasons.get((start_idx, end_idx), 'Repetição detectada')
        for i in range(start_idx, min(end_idx + 1, len(words))):
            word_status[i] = 'removed'
            word_reason[i] = reason

    # 2. Agrupar words consecutivos com mesmo status
    segments = []
    current_status = word_status[0]
    current_reason = word_reason[0]
    current_start_idx = 0

    for i in range(1, len(words)):
        if word_status[i] != current_status:
            # Fechar segmento atual
            segments.append(
                _create_segment(
                    words, current_start_idx, i - 1,
                    current_status, current_reason
                )
            )
            # Iniciar novo segmento
            current_status = word_status[i]
            current_reason = word_reason[i]
            current_start_idx = i

    # Fechar último segmento
    segments.append(
        _create_segment(
            words, current_start_idx, len(words) - 1,
            current_status, current_reason
        )
    )

    return segments


def _create_segment(
    words: List[Dict],
    start_idx: int,
    end_idx: int,
    status: str,
    reason: str,
) -> Dict:
    """Cria um segmento a partir de um range de words."""
    # Timestamps: start do primeiro word, end do último word
    start_s = _get_timestamp(words[start_idx], 'start')
    end_s = _get_timestamp(words[end_idx], 'end')

    # Texto: concatenar todos os words
    text = ' '.join(w.get('text', '') for w in words[start_idx:end_idx + 1])

    return {
        'id': f'seg-{uuid.uuid4().hex[:8]}',
        'start_s': round(start_s, 2),
        'end_s': round(end_s, 2),
        'text': text,
        'status': status,
        'removedBy': 'llm' if status == 'removed' else None,
        'removedReason': reason if status == 'removed' else '',
    }


def _get_timestamp(word: Dict, field: str) -> float:
    """
    Extrai timestamp de um word. Lida com ms e seconds.

    AssemblyAI pode retornar em milissegundos (770) ou
    segundos (0.77) dependendo da versão/config.
    """
    val = word.get(field, 0)
    if isinstance(val, (int, float)):
        # Se > 1000, provavelmente está em ms → converter para seconds
        if val > 1000:
            return val / 1000.0
        return float(val)
    return 0.0


# ═══════════════════════════════════════════════════════════════
# MAIN RESOLVER
# ═══════════════════════════════════════════════════════════════

def detect_repeated_phrases(
    assets_words: Dict[str, List[Dict]],
    min_phrase_len: int = 4,
    max_phrase_len: int = 15,
) -> List[Dict]:
    """
    🆕 Detecção DETERMINÍSTICA de frases repetidas (fallback da LLM).

    Escaneia cada transcrição com sliding window de N-grams.
    Se encontra uma sequência de >= min_phrase_len palavras que aparece
    >= 2 vezes, reporta como intra-retake (mantém a primeira, remove a segunda).

    Não é semântico — é puramente textual. Mas garante que repetições
    óbvias sejam capturadas mesmo se a LLM falhar.

    Returns:
        Lista no formato llm_detections (compatível com resolve_intra_retakes):
        [{"asset_id": "...", "detections": [{"repeated_text": "...", ...}]}]
    """
    results = []

    for asset_id, words in assets_words.items():
        if not words or len(words) < min_phrase_len * 2:
            continue

        word_texts = [_normalize(w.get('text', '')) for w in words]
        detections = []
        already_found = set()  # Evitar duplicar detecções

        # Tentar do maior para o menor para pegar a frase mais longa primeiro
        for phrase_len in range(min(max_phrase_len, len(word_texts) // 2), min_phrase_len - 1, -1):
            for i in range(len(word_texts) - phrase_len + 1):
                window = tuple(word_texts[i:i + phrase_len])

                # Já encontramos? Ou parte de uma detecção maior?
                window_key = ' '.join(window)
                if any(window_key in af for af in already_found):
                    continue

                # Buscar segunda ocorrência (não sobreposta)
                for j in range(i + phrase_len, len(word_texts) - phrase_len + 1):
                    candidate = tuple(word_texts[j:j + phrase_len])
                    if candidate == window:
                        # Encontrou repetição! Reconstruir texto original
                        orig_text = ' '.join(w.get('text', '') for w in words[i:i + phrase_len])
                        detections.append({
                            'repeated_text': orig_text,
                            'keep_occurrence': 1,
                            'remove_occurrence': 2,
                            'reason': 'Repetição detectada automaticamente (fallback determinístico)',
                        })
                        already_found.add(window_key)
                        logger.info(
                            f"🔍 [DETERMINISTIC] Asset {asset_id[:8]}: "
                            f"frase repetida detectada ({phrase_len} palavras): "
                            f"'{orig_text[:50]}...'"
                        )
                        break  # Não precisa buscar mais ocorrências desta frase

        if detections:
            results.append({
                'asset_id': asset_id,
                'detections': detections,
            })

    return results


def resolve_intra_retakes(
    llm_detections: List[Dict],
    assets_words: Dict[str, List[Dict]],
) -> List[Dict]:
    """
    Resolve intra-retakes combinando decisões semânticas da LLM
    com word timestamps determinísticos do AssemblyAI.

    Segue o padrão routing_validator.py:
        LLM detecta (semântico) → Resolver segmenta (determinístico)

    Args:
        llm_detections: Lista de detecções da LLM, cada uma com:
            {
                "asset_id": "...",
                "detections": [
                    {
                        "repeated_text": "frase repetida",
                        "keep_occurrence": 1,
                        "remove_occurrence": 2,
                        "reason": "motivo"
                    }
                ]
            }
        assets_words: Dict mapeando asset_id → words[] do AssemblyAI

    Returns:
        Lista no formato intra_retakes[] para o script_generator:
        [
            {
                "asset_id": "...",
                "segments": [
                    {"id", "start_s", "end_s", "text", "status", ...}
                ]
            }
        ]
    """
    if not llm_detections or not assets_words:
        return []

    results = []

    for detection in llm_detections:
        asset_id = detection.get('asset_id', '')
        detections = detection.get('detections', [])

        if not asset_id or not detections:
            continue

        words = assets_words.get(asset_id, [])
        if not words:
            logger.warning(
                f"⚠️ [RETAKE-RESOLVER] Sem words para asset {asset_id[:8]}..."
            )
            continue

        # Coletar todos os ranges a remover
        removed_ranges = []
        removal_reasons = {}

        for det in detections:
            phrase = det.get('repeated_text', '')
            keep_occ = det.get('keep_occurrence', 1)
            remove_occ = det.get('remove_occurrence', 2)
            reason = det.get('reason', 'Repetição detectada')

            if not phrase:
                continue

            # Encontrar todas as ocorrências
            occurrences = _find_phrase_occurrences(words, phrase)

            if len(occurrences) < 2:
                logger.warning(
                    f"⚠️ [RETAKE-RESOLVER] Frase '{phrase[:40]}...' encontrada "
                    f"{len(occurrences)}x (esperado >= 2) em {asset_id[:8]}"
                )
                # Se encontrou 0, não podemos fazer nada
                # Se encontrou 1, não é repetição real
                continue

            # Determinar qual ocorrência remover (1-indexed)
            remove_idx = remove_occ - 1  # Converter para 0-indexed

            if remove_idx < 0 or remove_idx >= len(occurrences):
                remove_idx = len(occurrences) - 1  # Fallback: remover última

            range_to_remove = occurrences[remove_idx]
            removed_ranges.append(range_to_remove)
            removal_reasons[range_to_remove] = reason

            keep_idx = keep_occ - 1 if keep_occ - 1 != remove_idx else 0
            keep_range = occurrences[keep_idx] if keep_idx < len(occurrences) else occurrences[0]

            logger.info(
                f"✅ [RETAKE-RESOLVER] Asset {asset_id[:8]}: "
                f"'{phrase[:30]}...' — "
                f"mantendo ocorrência {keep_occ} [{_get_timestamp(words[keep_range[0]], 'start'):.1f}s], "
                f"removendo ocorrência {remove_occ} [{_get_timestamp(words[range_to_remove[0]], 'start'):.1f}s]"
            )

        if not removed_ranges:
            continue

        # Construir segmentos determinísticos
        segments = _build_segments_from_words(words, removed_ranges, removal_reasons)

        if segments:
            results.append({
                'asset_id': asset_id,
                'segments': segments,
            })

            # Log resumo
            keep_count = sum(1 for s in segments if s['status'] == 'keep')
            remove_count = sum(1 for s in segments if s['status'] == 'removed')
            logger.info(
                f"📊 [RETAKE-RESOLVER] Asset {asset_id[:8]}: "
                f"{len(segments)} segmentos ({keep_count} keep, {remove_count} removed)"
            )

    return results
