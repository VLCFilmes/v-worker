"""
🌍 Tectonic Plates Service v1.1.0

Lógica simplificada para atribuir features (cartela, v-matting) às placas tectônicas.

Conceito:
- "Placas Tectônicas" = speech_segments gerados pelo silence_cut
- Cada placa é IMUTÁVEL após geração
- Features são atribuídas por PLACA, não por frase
- Se QUALQUER frase dentro de uma placa tem a feature, a placa inteira recebe
- 🆕 v1.1.0: Se TODAS as frases têm a feature, TODAS as placas recebem (modo "vídeo inteiro")
- 🆕 v1.1.0: Merge de placas muito próximas para evitar flicker

Substitui o BlockGrouperService que era muito complexo e trabalhava com timestamps de frases.
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# 🆕 v1.1.0: Threshold para merge de placas muito próximas (evitar flicker)
DEFAULT_MERGE_GAP_THRESHOLD_MS = 300  # 300ms = 9 frames a 30fps


def assign_features_to_plates(
    speech_segments: List[Dict],
    phrase_groups: List[Dict],
    features: List[str] = None,
    merge_gap_threshold_ms: int = DEFAULT_MERGE_GAP_THRESHOLD_MS
) -> Dict[str, Any]:
    """
    Atribui features às placas tectônicas baseado nas frases que contêm.
    
    🆕 v1.1.0: Se TODAS as frases têm uma feature, TODAS as placas recebem
               (modo "vídeo inteiro" - quando usuário marca tudo no modal).
    
    Args:
        speech_segments: Lista de placas tectônicas (do hybrid silence cut)
            [{url, shared_path, original_start, original_end, audio_offset, duration, index}]
        phrase_groups: Lista de frases classificadas
            [{start_time, end_time, use_cartela, person_overlay_enabled, ...}]
        features: Lista de features a verificar (default: ['use_cartela', 'person_overlay_enabled'])
        merge_gap_threshold_ms: Gap máximo entre placas para considerar "contínuo" (anti-flicker)
    
    Returns:
        {
            "plates": [
                {
                    "index": 0,
                    "url": "...",
                    "shared_path": "...",
                    "original_start": 0.5,
                    "original_end": 5.2,
                    "audio_offset": 0.0,
                    "duration": 4.7,
                    "has_cartela": True,
                    "has_matting": False,
                    "phrase_indices": [0, 1, 2]
                },
                ...
            ],
            "stats": {
                "total_plates": 20,
                "plates_with_cartela": 15,
                "plates_with_matting": 10,
                "all_cartela": False,
                "all_matting": True
            }
        }
    """
    if features is None:
        features = ['use_cartela', 'person_overlay_enabled']
    
    logger.info(f"🌍 [TECTONIC PLATES] Atribuindo features às placas...")
    logger.info(f"   📊 {len(speech_segments)} placas, {len(phrase_groups)} frases")
    logger.info(f"   🔍 Features: {features}")
    
    if not speech_segments:
        logger.warning(f"   ⚠️ Sem placas tectônicas disponíveis")
        return {
            "plates": [],
            "stats": {"error": "No speech_segments (tectonic plates) available"}
        }
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 🆕 v1.1.0: Verificar se TODAS as frases têm a feature
    # Se sim, aplicar a TODAS as placas (modo "vídeo inteiro")
    # ═══════════════════════════════════════════════════════════════════════════
    all_cartela = False
    all_matting = False
    
    if phrase_groups:
        if 'use_cartela' in features:
            all_cartela = all(p.get('use_cartela', False) for p in phrase_groups)
            if all_cartela:
                logger.info(f"   🎴 [MODO VÍDEO INTEIRO] TODAS as {len(phrase_groups)} frases têm cartela → aplicar a TODAS as placas")
        
        if 'person_overlay_enabled' in features:
            all_matting = all(p.get('person_overlay_enabled', False) for p in phrase_groups)
            if all_matting:
                logger.info(f"   👤 [MODO VÍDEO INTEIRO] TODAS as {len(phrase_groups)} frases têm matting → aplicar a TODAS as placas")
    
    # Mapear frases para placas baseado em audio_offset
    # Cada frase tem start_time (tempo virtual/transcrição)
    # Cada placa tem audio_offset (início no tempo virtual) e duration
    
    plates_result = []
    stats = {
        "total_plates": len(speech_segments),
        "plates_with_cartela": 0,
        "plates_with_matting": 0,
        "all_cartela": all_cartela,
        "all_matting": all_matting
    }
    
    for seg in speech_segments:
        plate = dict(seg)  # Copiar para não modificar original
        seg_index = seg.get('index', 0)
        seg_audio_offset = seg.get('audio_offset', 0)
        seg_duration = seg.get('duration', 0)
        seg_end = seg_audio_offset + seg_duration
        
        # Encontrar frases que pertencem a esta placa
        # Uma frase pertence à placa se seu start cai dentro do intervalo [audio_offset, audio_offset + duration)
        # 🔧 FIX v2.9.65: SEMPRE usar 'start' primeiro (tempo virtual da transcrição)
        # 'start_time' pode ter valores diferentes se a frase foi regroupada
        plate_phrases = []
        for i, phrase in enumerate(phrase_groups):
            phrase_start = phrase.get('start', phrase.get('start_time', 0))
            
            # 🔍 DEBUG: Verificar discrepância entre start e start_time (apenas para primeiras 5 frases)
            if i < 5:
                start_time_val = phrase.get('start_time')
                if start_time_val is not None and start_time_val != phrase_start:
                    logger.warning(f"   ⚠️ [DEBUG] Phrase {i}: start={phrase_start:.3f}s DIFERENTE de start_time={start_time_val:.3f}s")
            
            # Tolerância de 0.15s para lidar com arredondamentos (aumentada de 0.1s)
            if seg_audio_offset - 0.15 <= phrase_start < seg_end + 0.15:
                plate_phrases.append(i)
        
        plate['phrase_indices'] = plate_phrases
        
        # ═══════════════════════════════════════════════════════════════════════════
        # 🆕 v1.1.0: Se modo "vídeo inteiro", aplicar feature a TODAS as placas
        # ═══════════════════════════════════════════════════════════════════════════
        plate['has_cartela'] = all_cartela  # Se all_cartela=True, todas recebem
        plate['has_matting'] = all_matting  # Se all_matting=True, todas recebem
        
        # Se não for modo "vídeo inteiro", verificar normalmente
        if not all_cartela or not all_matting:
            for phrase_idx in plate_phrases:
                phrase = phrase_groups[phrase_idx]
                
                if not all_cartela and 'use_cartela' in features and phrase.get('use_cartela', False):
                    plate['has_cartela'] = True
                
                if not all_matting and 'person_overlay_enabled' in features and phrase.get('person_overlay_enabled', False):
                    plate['has_matting'] = True
        
        # Atualizar stats
        if plate['has_cartela']:
            stats['plates_with_cartela'] += 1
        if plate['has_matting']:
            stats['plates_with_matting'] += 1
        
        plates_result.append(plate)
        
        # Log detalhado - SEMPRE mostrar todas as placas para diagnóstico
        features_str = []
        if plate['has_cartela']:
            features_str.append('🎴 cartela')
        if plate['has_matting']:
            features_str.append('👤 matting')
        
        if features_str:
            logger.info(f"   [Placa {seg_index}] {seg_audio_offset:.2f}s → {seg_end:.2f}s | {', '.join(features_str)} | frases: {plate_phrases}")
        else:
            # 🆕 v2.9.42: Log de placas sem features para diagnóstico
            logger.warning(f"   [Placa {seg_index}] {seg_audio_offset:.2f}s → {seg_end:.2f}s | ⚠️ SEM FEATURES (nenhuma frase associada)")
    
    logger.info(f"✅ [TECTONIC PLATES] Resultado:")
    logger.info(f"   📊 {stats['plates_with_cartela']}/{stats['total_plates']} placas com cartela {'(MODO VÍDEO INTEIRO)' if all_cartela else ''}")
    logger.info(f"   📊 {stats['plates_with_matting']}/{stats['total_plates']} placas com matting {'(MODO VÍDEO INTEIRO)' if all_matting else ''}")
    
    return {
        "plates": plates_result,
        "stats": stats
    }


def get_matting_plates(
    speech_segments: List[Dict],
    phrase_groups: List[Dict],
    merge_adjacent: bool = True,
    merge_gap_threshold_ms: int = DEFAULT_MERGE_GAP_THRESHOLD_MS
) -> List[Dict]:
    """
    Retorna lista de placas que devem ir para v-matting.
    
    🆕 v1.1.0: Adiciona merge_adjacent para evitar flicker em placas muito próximas.
    
    Args:
        speech_segments: Lista de placas tectônicas
        phrase_groups: Lista de frases classificadas
        merge_adjacent: Se True, mescla placas com gap pequeno
        merge_gap_threshold_ms: Gap máximo para merge (default: 300ms)
    
    Returns:
        Lista de placas com has_matting=True
    """
    result = assign_features_to_plates(
        speech_segments=speech_segments,
        phrase_groups=phrase_groups,
        features=['person_overlay_enabled']
    )
    
    matting_plates = [p for p in result['plates'] if p.get('has_matting', False)]
    
    # 🆕 v1.1.0: Merge de placas adjacentes para evitar flicker
    if merge_adjacent and len(matting_plates) > 1:
        matting_plates = _merge_adjacent_plates(
            matting_plates, 
            merge_gap_threshold_ms,
            feature_name='matting'
        )
    
    logger.info(f"👤 [MATTING PLATES] {len(matting_plates)} placas selecionadas para v-matting")
    
    return matting_plates


def get_cartela_plates(
    speech_segments: List[Dict],
    phrase_groups: List[Dict],
    merge_adjacent: bool = True,
    merge_gap_threshold_ms: int = DEFAULT_MERGE_GAP_THRESHOLD_MS
) -> List[Dict]:
    """
    Retorna lista de placas que devem receber cartela.
    
    🆕 v1.1.0: Adiciona merge_adjacent para evitar flicker em placas muito próximas.
    
    Args:
        speech_segments: Lista de placas tectônicas
        phrase_groups: Lista de frases classificadas
        merge_adjacent: Se True, mescla placas com gap pequeno
        merge_gap_threshold_ms: Gap máximo para merge (default: 300ms)
    
    Returns:
        Lista de placas com has_cartela=True
    """
    result = assign_features_to_plates(
        speech_segments=speech_segments,
        phrase_groups=phrase_groups,
        features=['use_cartela']
    )
    
    cartela_plates = [p for p in result['plates'] if p.get('has_cartela', False)]
    
    # 🆕 v1.1.0: Merge de placas adjacentes para evitar flicker
    if merge_adjacent and len(cartela_plates) > 1:
        cartela_plates = _merge_adjacent_plates(
            cartela_plates, 
            merge_gap_threshold_ms,
            feature_name='cartela'
        )
    
    logger.info(f"🎴 [CARTELA PLATES] {len(cartela_plates)} placas selecionadas para cartela")
    
    return cartela_plates


def _merge_adjacent_plates(
    plates: List[Dict],
    gap_threshold_ms: int,
    feature_name: str = 'feature'
) -> List[Dict]:
    """
    🆕 v1.1.0: Mescla placas adjacentes com gap pequeno para evitar flicker.
    
    Se duas placas consecutivas têm um gap menor que gap_threshold_ms,
    o final da primeira é estendido até o início da segunda.
    
    Isso evita o efeito de "piscar" onde a feature aparece, some rapidamente,
    e reaparece na próxima placa.
    
    Args:
        plates: Lista de placas com a feature
        gap_threshold_ms: Gap máximo para considerar merge (em ms)
        feature_name: Nome da feature para logs
    
    Returns:
        Lista de placas com gaps pequenos eliminados
    """
    if not plates or len(plates) < 2:
        return plates
    
    # Ordenar por audio_offset
    sorted_plates = sorted(plates, key=lambda p: p.get('audio_offset', 0))
    
    merged_count = 0
    gap_threshold_s = gap_threshold_ms / 1000.0
    
    for i in range(len(sorted_plates) - 1):
        current = sorted_plates[i]
        next_plate = sorted_plates[i + 1]
        
        current_end = current.get('audio_offset', 0) + current.get('duration', 0)
        next_start = next_plate.get('audio_offset', 0)
        gap = next_start - current_end
        
        # Se gap é pequeno, estender a placa atual até o início da próxima
        if 0 < gap <= gap_threshold_s:
            old_duration = current.get('duration', 0)
            new_duration = old_duration + gap
            current['duration'] = new_duration
            current['_merged_gap'] = gap  # Marcar que foi estendida
            merged_count += 1
            logger.debug(f"   🔗 [{feature_name}] Placa {current.get('index')} estendida: {old_duration:.2f}s → {new_duration:.2f}s (gap={gap*1000:.0f}ms)")
    
    if merged_count > 0:
        logger.info(f"   🔗 [{feature_name.upper()}] {merged_count} gaps pequenos eliminados (threshold={gap_threshold_ms}ms)")
    
    return sorted_plates


def get_all_plates_with_features(
    speech_segments: List[Dict],
    phrase_groups: List[Dict],
    merge_adjacent: bool = True,
    merge_gap_threshold_ms: int = DEFAULT_MERGE_GAP_THRESHOLD_MS
) -> Dict[str, Any]:
    """
    🆕 v1.1.0: Retorna todas as placas com ambas as features verificadas de uma vez.
    
    Mais eficiente que chamar get_matting_plates e get_cartela_plates separadamente.
    
    Returns:
        {
            "all_plates": [...],
            "matting_plates": [...],
            "cartela_plates": [...],
            "stats": {...}
        }
    """
    result = assign_features_to_plates(
        speech_segments=speech_segments,
        phrase_groups=phrase_groups,
        features=['use_cartela', 'person_overlay_enabled']
    )
    
    all_plates = result['plates']
    matting_plates = [p for p in all_plates if p.get('has_matting', False)]
    cartela_plates = [p for p in all_plates if p.get('has_cartela', False)]
    
    # Merge de placas adjacentes
    if merge_adjacent:
        if len(matting_plates) > 1:
            matting_plates = _merge_adjacent_plates(
                matting_plates, 
                merge_gap_threshold_ms,
                feature_name='matting'
            )
        if len(cartela_plates) > 1:
            cartela_plates = _merge_adjacent_plates(
                cartela_plates, 
                merge_gap_threshold_ms,
                feature_name='cartela'
            )
    
    return {
        "all_plates": all_plates,
        "matting_plates": matting_plates,
        "cartela_plates": cartela_plates,
        "stats": result['stats']
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 🆕 v1.2.0: PLACAS VIRTUAIS - Para vídeos SEM corte de silêncio
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_VIRTUAL_GAP_THRESHOLD_MS = 500  # Gap para merge de placas virtuais


def create_virtual_matting_plates(
    phrase_groups: List[Dict],
    original_video_duration: float,
    gap_threshold_ms: int = DEFAULT_VIRTUAL_GAP_THRESHOLD_MS,
    padding_ms: int = 100
) -> List[Dict]:
    """
    🆕 v1.2.0: Cria "placas virtuais" para v-matting quando não há corte de silêncio.
    
    Usado quando:
    - Corte de silêncio desabilitado
    - Vídeo sem silêncios (fala contínua)
    - Apenas 1 placa tectônica muito grande
    
    Agrupa frases consecutivas com person_overlay_enabled em blocos contínuos,
    mesclando frases com gap < gap_threshold_ms.
    
    Args:
        phrase_groups: Lista de frases com timestamps e features
            Deve ter _mapped_start_time/_mapped_end_time (tempo original do vídeo)
        original_video_duration: Duração do vídeo original em segundos
        gap_threshold_ms: Gap máximo entre frases para considerar contínuas (default: 500ms)
        padding_ms: Margem adicional no início/fim de cada placa (default: 100ms)
    
    Returns:
        Lista de placas virtuais:
        [
            {
                "index": 0,
                "original_start": 5.0,      # Tempo no vídeo original (segundos)
                "original_end": 25.0,
                "duration": 20.0,
                "phrase_indices": [0, 1, 2],
                "is_virtual": True,         # Marca como placa virtual
                "has_matting": True
            },
            ...
        ]
    """
    logger.info(f"🎭 [VIRTUAL PLATES] Criando placas virtuais para matting...")
    logger.info(f"   📊 {len(phrase_groups)} frases, vídeo: {original_video_duration:.2f}s")
    logger.info(f"   🔗 Gap threshold: {gap_threshold_ms}ms, Padding: {padding_ms}ms")
    
    # Filtrar frases com matting
    matting_phrases = []
    for i, phrase in enumerate(phrase_groups):
        if phrase.get('person_overlay_enabled', False):
            # 🔧 FIX v2.9.65: Prioridade de timestamps para matting:
            # 1. _mapped_start_time/_mapped_end_time (tempo ORIGINAL do vídeo)
            # 2. start/end (tempo virtual da transcrição - mais confiável que start_time)
            # 3. start_time/end_time (pode ter valores incorretos se a frase foi regroupada)
            start = phrase.get('_mapped_start_time', phrase.get('start', phrase.get('start_time', 0)))
            end = phrase.get('_mapped_end_time', phrase.get('end', phrase.get('end_time', 0)))
            
            # Converter de ms para segundos se necessário
            if start > 1000:  # Provavelmente em ms
                start = start / 1000
                end = end / 1000
            
            matting_phrases.append({
                'index': i,
                'start': start,
                'end': end,
                'text': phrase.get('text', phrase.get('phrase', ''))[:30]
            })
    
    if not matting_phrases:
        logger.info(f"   ⚠️ Nenhuma frase com matting encontrada")
        return []
    
    logger.info(f"   🎭 {len(matting_phrases)} frases com matting")
    
    # Ordenar por tempo de início
    matting_phrases.sort(key=lambda p: p['start'])
    
    # Agrupar frases consecutivas (gap < threshold)
    gap_threshold_s = gap_threshold_ms / 1000.0
    padding_s = padding_ms / 1000.0
    
    virtual_plates = []
    current_plate = None
    
    for phrase in matting_phrases:
        if current_plate is None:
            # Primeira frase do grupo
            current_plate = {
                'start': phrase['start'],
                'end': phrase['end'],
                'phrase_indices': [phrase['index']]
            }
        else:
            # Verificar gap com a frase anterior
            gap = phrase['start'] - current_plate['end']
            
            if gap <= gap_threshold_s:
                # Gap pequeno - estender placa atual
                current_plate['end'] = max(current_plate['end'], phrase['end'])
                current_plate['phrase_indices'].append(phrase['index'])
            else:
                # Gap grande - finalizar placa atual e iniciar nova
                virtual_plates.append(current_plate)
                current_plate = {
                    'start': phrase['start'],
                    'end': phrase['end'],
                    'phrase_indices': [phrase['index']]
                }
    
    # Adicionar última placa
    if current_plate:
        virtual_plates.append(current_plate)
    
    # Formatar como placas tectônicas, adicionando padding
    result = []
    is_last_plate_index = len(virtual_plates) - 1
    
    for i, plate in enumerate(virtual_plates):
        # Aplicar padding (com limites no vídeo)
        start_with_padding = max(0, plate['start'] - padding_s)
        end_with_padding = min(original_video_duration, plate['end'] + padding_s)
        
        # 🆕 v1.4.0: Se é a ÚLTIMA placa e ainda há tempo até o fim do vídeo,
        # estender até o final do vídeo para evitar corte abrupto
        is_last_plate = (i == is_last_plate_index)
        gap_to_end = original_video_duration - end_with_padding
        
        if is_last_plate and gap_to_end > 0 and gap_to_end < 1.0:  # Gap menor que 1s
            logger.info(f"   🔧 [EXTEND_TO_END] Última placa: estendendo {end_with_padding:.2f}s → {original_video_duration:.2f}s (gap={gap_to_end:.2f}s)")
            end_with_padding = original_video_duration
        
        duration = end_with_padding - start_with_padding
        
        virtual_plate = {
            "index": i,
            "original_start": round(start_with_padding, 3),
            "original_end": round(end_with_padding, 3),
            "duration": round(duration, 3),
            "phrase_indices": plate['phrase_indices'],
            "is_virtual": True,  # Marca como placa virtual (não veio do silence_cut)
            "has_matting": True,
            "has_cartela": False  # Cartelas usam timestamps virtuais, não precisam de corte
        }
        result.append(virtual_plate)
        
        logger.info(f"   [Placa Virtual {i}] {start_with_padding:.2f}s → {end_with_padding:.2f}s | duração: {duration:.2f}s | frases: {plate['phrase_indices']}")
    
    logger.info(f"✅ [VIRTUAL PLATES] {len(result)} placas virtuais criadas")
    logger.info(f"   📊 Tempo total para matting: {sum(p['duration'] for p in result):.2f}s (vs {original_video_duration:.2f}s do vídeo)")
    
    return result


def needs_virtual_plates(
    speech_segments: List[Dict],
    phrase_groups: List[Dict]
) -> bool:
    """
    🆕 v1.2.0: Verifica se precisa criar placas virtuais.
    
    Retorna True se:
    - Não há speech_segments (corte de silêncio desabilitado)
    - Há frases com matting que precisam ser processadas
    
    Args:
        speech_segments: Lista de placas tectônicas (pode estar vazia)
        phrase_groups: Lista de frases
    
    Returns:
        True se precisa criar placas virtuais para matting
    """
    # Se não há speech_segments, precisa de placas virtuais
    if not speech_segments:
        # Mas só se houver frases com matting
        has_matting = any(p.get('person_overlay_enabled', False) for p in phrase_groups)
        if has_matting:
            logger.info(f"🎭 [NEEDS_VIRTUAL] Sem placas tectônicas + frases com matting → PRECISA de placas virtuais")
            return True
        else:
            logger.info(f"🎭 [NEEDS_VIRTUAL] Sem placas tectônicas e sem matting → NÃO precisa")
            return False
    
    # Se há speech_segments, usar as placas normais
    logger.info(f"🎭 [NEEDS_VIRTUAL] {len(speech_segments)} placas tectônicas disponíveis → NÃO precisa de virtuais")
    return False


def get_matting_segments_for_phase2(
    speech_segments: List[Dict],
    phrase_groups: List[Dict],
    original_video_url: str,
    original_video_duration: float,
    gap_threshold_ms: int = DEFAULT_VIRTUAL_GAP_THRESHOLD_MS
) -> Dict[str, Any]:
    """
    🆕 v1.2.0: Obtém segmentos para v-matting na Fase 2.
    
    Decide automaticamente entre:
    - Usar placas tectônicas reais (se disponíveis)
    - Criar placas virtuais (se não houver corte de silêncio)
    
    Args:
        speech_segments: Lista de placas tectônicas (pode estar vazia)
        phrase_groups: Lista de frases com features
        original_video_url: URL do vídeo original/concatenado
        original_video_duration: Duração do vídeo em segundos
        gap_threshold_ms: Gap para merge de placas virtuais
    
    Returns:
        {
            "mode": "tectonic" | "virtual",
            "plates": [...],           # Placas para processar
            "video_url": "...",        # URL do vídeo fonte
            "needs_cutting": bool,     # Se precisa cortar o vídeo antes do matting
            "stats": {...}
        }
    """
    logger.info(f"🎭 [PHASE2_MATTING] Preparando segmentos para v-matting...")
    
    # Verificar se alguma frase tem matting
    matting_phrases = [p for p in phrase_groups if p.get('person_overlay_enabled', False)]
    
    if not matting_phrases:
        logger.info(f"   ⚠️ Nenhuma frase com matting - pulando")
        return {
            "mode": "none",
            "plates": [],
            "video_url": original_video_url,
            "needs_cutting": False,
            "stats": {"total_phrases_with_matting": 0}
        }
    
    logger.info(f"   📊 {len(matting_phrases)} frases com matting")
    
    # Decidir modo
    if needs_virtual_plates(speech_segments, phrase_groups):
        # MODO VIRTUAL: Criar placas a partir das frases
        logger.info(f"   🔄 MODO VIRTUAL: Criando placas a partir das frases")
        
        virtual_plates = create_virtual_matting_plates(
            phrase_groups=phrase_groups,
            original_video_duration=original_video_duration,
            gap_threshold_ms=gap_threshold_ms
        )
        
        # 🔧 v2.9.51: Evitar divisão por zero quando video_duration é 0
        total_matting_duration = sum(p['duration'] for p in virtual_plates)
        if original_video_duration > 0:
            efficiency = f"{(1 - total_matting_duration / original_video_duration) * 100:.1f}% economizado"
        else:
            efficiency = "N/A (duração não disponível)"
        
        return {
            "mode": "virtual",
            "plates": virtual_plates,
            "video_url": original_video_url,
            "needs_cutting": True,  # Precisa cortar o vídeo nos timestamps das placas virtuais
            "stats": {
                "total_phrases_with_matting": len(matting_phrases),
                "virtual_plates_created": len(virtual_plates),
                "total_matting_duration": total_matting_duration,
                "video_duration": original_video_duration,
                "efficiency": efficiency
            }
        }
    else:
        # MODO TECTÔNICO: Usar placas do silence_cut
        logger.info(f"   🌍 MODO TECTÔNICO: Usando placas do silence_cut")
        
        matting_plates = get_matting_plates(
            speech_segments=speech_segments,
            phrase_groups=phrase_groups,
            merge_adjacent=True,
            merge_gap_threshold_ms=gap_threshold_ms
        )
        
        return {
            "mode": "tectonic",
            "plates": matting_plates,
            "video_url": None,  # Cada placa já tem sua própria URL
            "needs_cutting": False,  # Já está cortado
            "stats": {
                "total_phrases_with_matting": len(matting_phrases),
                "tectonic_plates_with_matting": len(matting_plates),
                "total_plates": len(speech_segments)
            }
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 🆕 v1.3.0: PLACAS VIRTUAIS PARA CARTELAS - Quando não há speech_segments
# ═══════════════════════════════════════════════════════════════════════════════

def create_virtual_cartela_plates(
    phrase_groups: List[Dict],
    video_duration: float,
    gap_threshold_ms: int = DEFAULT_VIRTUAL_GAP_THRESHOLD_MS
) -> List[Dict]:
    """
    🆕 v1.3.0: Cria "placas virtuais" para CARTELAS quando não há speech_segments.
    
    Usado quando:
    - Corte de silêncio desabilitado
    - Vídeo sem silêncios (fala contínua) → apenas 1 placa tectônica
    - Usuário quer cartelas em algumas frases (não necessariamente todas)
    
    Diferente do matting, cartelas NÃO precisam de corte físico do vídeo.
    Elas só precisam de timestamps virtuais para posicionamento no v-editor.
    
    Lógica:
    1. Identificar frases consecutivas com use_cartela=True
    2. Agrupar frases em blocos (se gap entre elas < gap_threshold)
    3. Para cada bloco: criar UMA cartela do início da primeira ao fim da última frase
    4. Isso evita flicker de gaps pequenos entre cartelas individuais
    
    Args:
        phrase_groups: Lista de frases com timestamps e features
            Usa start_time/end_time (tempo virtual - do audio concatenado)
        video_duration: Duração total do vídeo em segundos
        gap_threshold_ms: Gap máximo entre frases para considerar contínuas (default: 500ms)
    
    Returns:
        Lista de placas virtuais para cartela:
        [
            {
                "index": 0,
                "audio_offset": 0.5,        # Tempo virtual (segundos) - início
                "duration": 10.0,           # Duração em segundos
                "phrase_indices": [0, 1, 2],# Índices das frases no grupo
                "is_virtual": True,         # Marca como placa virtual
                "has_cartela": True
            },
            ...
        ]
    """
    logger.info(f"🎴 [VIRTUAL CARTELA PLATES] Criando placas virtuais para cartelas...")
    logger.info(f"   📊 {len(phrase_groups)} frases, vídeo: {video_duration:.2f}s")
    logger.info(f"   🔗 Gap threshold: {gap_threshold_ms}ms")
    
    # Filtrar frases com cartela
    cartela_phrases = []
    for i, phrase in enumerate(phrase_groups):
        if phrase.get('use_cartela', False) or phrase.get('cartela_info') is not None:
            # ═══════════════════════════════════════════════════════════════════════════
            # 🔧 FIX v2.9.67: PROBLEMA IDENTIFICADO!
            # 
            # O FRONTEND (TranscriptionReviewModal) define frases com start_time/end_time
            # O BACKEND (v-services/fraseamento) define frases com start/end
            # 
            # Quando o usuário edita no modal e salva, as frases do FRONTEND 
            # sobrescrevem as do job, perdendo o campo 'start'.
            #
            # SOLUÇÃO: Verificar AMBOS os campos e escolher o que faz sentido:
            # 1. Se 'start' existe e != 0 → usar 'start' (veio do backend/fraseamento)
            # 2. Se não, usar 'start_time' (veio do frontend)
            # ═══════════════════════════════════════════════════════════════════════════
            start_val = phrase.get('start')
            start_time_val = phrase.get('start_time')
            end_val = phrase.get('end')
            end_time_val = phrase.get('end_time')
            
            # 🔍 DEBUG v2.9.67: Logar TODOS os campos de timing (INFO para garantir visibilidade)
            logger.info(f"   🔍 [TIMING] Phrase {i}: start={start_val}, start_time={start_time_val}, end={end_val}, end_time={end_time_val}")
            
            # 🔧 v2.9.67: Escolher o valor correto
            # Priorizar 'start' se existir e for > 0, senão usar 'start_time'
            if start_val is not None and start_val > 0:
                start = start_val
                end = end_val if end_val is not None else (end_time_val if end_time_val is not None else 0)
                source = "start/end"
            elif start_time_val is not None:
                start = start_time_val
                end = end_time_val if end_time_val is not None else (end_val if end_val is not None else 0)
                source = "start_time/end_time"
            else:
                start = 0
                end = 0
                source = "FALLBACK (0)"
                logger.warning(f"   ⚠️ [TIMING] Phrase {i}: SEM TIMESTAMPS VÁLIDOS!")
            
            logger.info(f"   📊 [TIMING] Phrase {i}: usando {source} → start={start:.3f}s, end={end:.3f}s")
            
            # Converter de ms para segundos se necessário
            if start > 1000:  # Provavelmente em ms
                start = start / 1000
                end = end / 1000
            
            cartela_phrases.append({
                'index': i,
                'start': start,
                'end': end,
                'text': phrase.get('text', phrase.get('phrase', ''))[:30]
            })
    
    if not cartela_phrases:
        logger.info(f"   ⚠️ Nenhuma frase com cartela encontrada")
        return []
    
    logger.info(f"   🎴 {len(cartela_phrases)} frases com cartela")
    
    # 🔍 DEBUG v2.9.66: Logar timestamps ANTES de ordenar (usar INFO para garantir que apareça)
    logger.info(f"   📊 [ANTES DE ORDENAR] Timestamps das frases com cartela:")
    for cp in cartela_phrases:
        logger.info(f"      Phrase {cp['index']}: start={cp['start']:.3f}s, end={cp['end']:.3f}s, text='{cp['text'][:20]}'")
    
    # Ordenar por tempo de início
    cartela_phrases.sort(key=lambda p: p['start'])
    
    # Agrupar frases consecutivas (gap < threshold)
    gap_threshold_s = gap_threshold_ms / 1000.0
    
    virtual_plates = []
    current_plate = None
    
    # 🔍 DEBUG v2.9.66: Logar timestamps DEPOIS de ordenar (usar INFO)
    logger.info(f"   📊 [DEPOIS DE ORDENAR] Frases com cartela:")
    for i, cp in enumerate(cartela_phrases):
        prev_end = cartela_phrases[i-1]['end'] if i > 0 else 0
        gap = cp['start'] - prev_end
        gap_info = f"gap={gap:.3f}s" if i > 0 else "PRIMEIRO"
        logger.info(f"      #{i}: Phrase {cp['index']}: start={cp['start']:.3f}s, end={cp['end']:.3f}s ({gap_info})")
    
    for phrase in cartela_phrases:
        if current_plate is None:
            # Primeira frase do grupo
            current_plate = {
                'start': phrase['start'],
                'end': phrase['end'],
                'phrase_indices': [phrase['index']]
            }
            logger.debug(f"   🟢 Iniciando placa com phrase {phrase['index']}: start={phrase['start']:.3f}s")
        else:
            # Verificar gap com a frase anterior
            gap = phrase['start'] - current_plate['end']
            
            if gap <= gap_threshold_s:
                # Gap pequeno - estender placa atual
                logger.debug(f"   🔗 Phrase {phrase['index']}: gap={gap:.3f}s <= threshold ({gap_threshold_s}s) → ESTENDENDO placa")
                current_plate['end'] = max(current_plate['end'], phrase['end'])
                current_plate['phrase_indices'].append(phrase['index'])
            else:
                # Gap grande - finalizar placa atual e iniciar nova
                logger.info(f"   🆕 Phrase {phrase['index']}: gap={gap:.3f}s > threshold ({gap_threshold_s}s) → NOVA PLACA")
                virtual_plates.append(current_plate)
                current_plate = {
                    'start': phrase['start'],
                    'end': phrase['end'],
                    'phrase_indices': [phrase['index']]
                }
    
    # Adicionar última placa
    if current_plate:
        virtual_plates.append(current_plate)
    
    # Formatar como placas (sem padding para cartelas - não precisam de corte físico)
    result = []
    is_last_plate_index = len(virtual_plates) - 1
    
    for i, plate in enumerate(virtual_plates):
        # Garantir que não ultrapasse a duração do vídeo
        start = max(0, plate['start'])
        end = min(video_duration, plate['end'])
        
        # 🆕 v1.4.0: Se é a ÚLTIMA placa e ainda há tempo até o fim do vídeo,
        # estender até o final do vídeo para evitar corte abrupto da cartela
        is_last_plate = (i == is_last_plate_index)
        gap_to_end = video_duration - end
        
        if is_last_plate and gap_to_end > 0 and gap_to_end < 1.0:  # Gap menor que 1s
            logger.info(f"   🔧 [EXTEND_TO_END] Última placa cartela: estendendo {end:.2f}s → {video_duration:.2f}s (gap={gap_to_end:.2f}s)")
            end = video_duration
        
        duration = end - start
        
        virtual_plate = {
            "index": i,
            "audio_offset": round(start, 3),        # Tempo virtual (início)
            "duration": round(duration, 3),
            "phrase_indices": plate['phrase_indices'],
            "is_virtual": True,  # Marca como placa virtual
            "has_cartela": True,
            "has_matting": False
        }
        result.append(virtual_plate)
        
        logger.info(f"   [Placa Virtual Cartela {i}] {start:.2f}s → {end:.2f}s | duração: {duration:.2f}s | frases: {plate['phrase_indices']}")
    
    logger.info(f"✅ [VIRTUAL CARTELA PLATES] {len(result)} placas virtuais de cartela criadas")
    
    return result


def get_cartela_segments(
    speech_segments: List[Dict],
    phrase_groups: List[Dict],
    video_duration: float,
    gap_threshold_ms: int = DEFAULT_VIRTUAL_GAP_THRESHOLD_MS
) -> Dict[str, Any]:
    """
    🆕 v1.3.0: Obtém segmentos para CARTELAS.
    
    Decide automaticamente entre:
    - Usar placas tectônicas reais (se disponíveis e mais de uma)
    - Criar placas virtuais (se não houver speech_segments OU apenas 1 grande)
    
    Diferente do matting, cartelas NÃO precisam de corte físico.
    
    Args:
        speech_segments: Lista de placas tectônicas (pode estar vazia)
        phrase_groups: Lista de frases com features
        video_duration: Duração do vídeo em segundos
        gap_threshold_ms: Gap para merge de placas virtuais
    
    Returns:
        {
            "mode": "tectonic" | "virtual" | "all_plates",
            "plates": [...],           # Placas para criar cartelas
            "stats": {...}
        }
    """
    logger.info(f"🎴 [CARTELA SEGMENTS] Preparando segmentos para cartelas...")
    
    # Verificar se alguma frase tem cartela
    cartela_phrases = [
        p for p in phrase_groups 
        if p.get('use_cartela', False) or p.get('cartela_info') is not None
    ]
    
    if not cartela_phrases:
        logger.info(f"   ⚠️ Nenhuma frase com cartela - pulando")
        return {
            "mode": "none",
            "plates": [],
            "stats": {"total_phrases_with_cartela": 0}
        }
    
    logger.info(f"   📊 {len(cartela_phrases)}/{len(phrase_groups)} frases com cartela")
    
    # Verificar se é modo "vídeo inteiro" (todas as frases têm cartela)
    all_have_cartela = len(cartela_phrases) == len(phrase_groups)
    
    # Decidir modo baseado em:
    # 1. Se não há speech_segments → modo virtual
    # 2. Se todas as frases têm cartela → usar todas as placas tectônicas
    # 3. Se algumas frases têm cartela → modo virtual (para agrupar corretamente)
    
    if not speech_segments:
        # Sem placas tectônicas → criar virtuais
        logger.info(f"   🔄 MODO VIRTUAL: Sem speech_segments, criando placas virtuais")
        
        virtual_plates = create_virtual_cartela_plates(
            phrase_groups=phrase_groups,
            video_duration=video_duration,
            gap_threshold_ms=gap_threshold_ms
        )
        
        return {
            "mode": "virtual",
            "plates": virtual_plates,
            "stats": {
                "total_phrases_with_cartela": len(cartela_phrases),
                "virtual_plates_created": len(virtual_plates)
            }
        }
    
    elif all_have_cartela:
        # Todas as frases têm cartela → usar TODAS as placas tectônicas
        logger.info(f"   🌍 MODO VÍDEO INTEIRO: Todas as frases têm cartela")
        logger.info(f"   → Usando TODAS as {len(speech_segments)} placas tectônicas")
        
        # Converter speech_segments para formato de placa
        all_plates = []
        for seg in speech_segments:
            plate = dict(seg)
            plate['has_cartela'] = True
            plate['has_matting'] = False
            all_plates.append(plate)
        
        # Merge de placas adjacentes para evitar flicker
        if len(all_plates) > 1:
            all_plates = _merge_adjacent_plates(
                all_plates,
                gap_threshold_ms,
                feature_name='cartela_all'
            )
        
        return {
            "mode": "all_plates",
            "plates": all_plates,
            "stats": {
                "total_phrases_with_cartela": len(cartela_phrases),
                "tectonic_plates_used": len(all_plates)
            }
        }
    
    else:
        # Algumas frases têm cartela → criar placas virtuais para agrupar corretamente
        # Isso evita flicker de gaps pequenos entre frases do mesmo grupo
        logger.info(f"   🔄 MODO VIRTUAL PARCIAL: {len(cartela_phrases)}/{len(phrase_groups)} frases com cartela")
        logger.info(f"   → Agrupando frases consecutivas com cartela")
        
        virtual_plates = create_virtual_cartela_plates(
            phrase_groups=phrase_groups,
            video_duration=video_duration,
            gap_threshold_ms=gap_threshold_ms
        )
        
        return {
            "mode": "virtual",
            "plates": virtual_plates,
            "stats": {
                "total_phrases_with_cartela": len(cartela_phrases),
                "virtual_plates_created": len(virtual_plates)
            }
        }
