"""
📝 Script Generator — Gera roteiro pré-populado a partir dos LLM Directors.

Combina dados do Transcript Director (voice/fala), Vision Director (b-roll/visual),
Asset Triage (classificação/formato), e VideoClipper (EDL/ordem) para criar um
documento de roteiro V2 (ScriptRow[]) automaticamente.

O roteiro gerado é salvo em content_documents e aberto no editor via SSE.

Fluxo:
  1. Lê resultados dos directors de project_config
  2. Monta ScriptRow[] combinando voice + visual
  3. Salva como content_document
  4. Envia mensagem SSE para abrir editor no frontend

Autor: Vinicius + Claude
Data: 09/Fev/2026
"""

import logging
import json
import uuid
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Labels para formatos de vídeo
FORMAT_LABELS = {
    'talking_head_solo': 'Talking Head Solo',
    'multi_interview': 'Entrevista Múltipla',
    'narration_broll': 'Narração + B-Roll',
    'humor_dialogue': 'Diálogo / Humor',
    'tutorial': 'Tutorial',
    'mixed': 'Conteúdo Misto',
    'unknown': 'Formato não determinado',
}


def generate_script_from_directors(
    project_id: str,
    conversation_id: str,
    triage_result: dict,
    transcript_result: Optional[dict] = None,
    vision_result: Optional[dict] = None,
    edl_result: Optional[dict] = None,
) -> Optional[str]:
    """
    Gera roteiro V2 (ScriptRow[]) a partir dos dados dos directors.

    Args:
        project_id: ID do projeto
        conversation_id: ID da conversa
        triage_result: Resultado do Asset Triage Director
        transcript_result: Resultado do Transcript Director (pode ser None)
        vision_result: Resultado do Vision Director (pode ser None)
        edl_result: Resultado do VideoClipper EDL (pode ser None)

    Returns:
        document_id do roteiro criado, ou None se falhar
    """
    t0 = time.time()

    try:
        # ─── 1. Extrair dados de cada director ───────────────
        format_detected = triage_result.get('format_detected', 'unknown')
        routing = triage_result.get('routing', {})
        triage_assets = {
            a.get('asset_id', ''): a
            for a in triage_result.get('assets', [])
        }

        # Contagens para o header
        vision_count = len(routing.get('vision_analysis', []))
        pipeline_count = len(routing.get('pipeline_ready', []))

        # ─── 2. Buscar transcrições ORIGINAIS do banco ───────
        # Nunca confiar no texto via LLM — LLMs truncam/resumem
        speech_asset_ids = [
            a.get('asset_id', '') for a in triage_result.get('assets', [])
            if a.get('classification') in ('talking_head', 'audio_narration')
        ]
        original_transcriptions = _fetch_original_transcriptions(speech_asset_ids)

        # ─── 2b. Indexar decisões de retakes ──────────────────
        retake_index = _build_retake_index(transcript_result)

        # ─── 2c. Indexar retakes intra-arquivo (segmentos) ───
        intra_index = _build_intra_retakes_index(transcript_result)

        # ─── 3. Montar ScriptRows ────────────────────────────
        rows = []

        if edl_result and edl_result.get('edit_sequence'):
            # MELHOR CASO: Temos EDL do VideoClipper
            rows = _build_rows_from_edl(
                edl_result, transcript_result, vision_result,
                triage_assets, original_transcriptions,
                retake_index, intra_index,
            )
        elif transcript_result and vision_result:
            # CASO 2: Transcript + Vision (sem EDL)
            rows = _build_rows_from_analysis(
                transcript_result, vision_result,
                triage_assets, original_transcriptions,
                retake_index, intra_index,
            )
        elif transcript_result:
            # CASO 3: Só Transcript
            rows = _build_rows_from_transcript_only(
                transcript_result, triage_assets,
                original_transcriptions, retake_index, intra_index,
            )
        else:
            # CASO 4: Só triage (mínimo)
            rows = _build_rows_from_triage_only(triage_result)

        if not rows:
            logger.warning(
                f"⚠️ [SCRIPT-GEN] Nenhuma row gerada para {project_id[:8]}"
            )
            return None

        # ─── 3.title. Inserir título no topo do roteiro ──────
        title_row = _generate_title_row(project_id, conversation_id, transcript_result)
        if title_row:
            # Título fica como order=0, antes de todos os outros
            title_row['order'] = 0
            rows.insert(0, title_row)
            # Reordenar as demais rows
            for i, row in enumerate(rows):
                if i > 0:
                    row['order'] = i
            logger.info(
                f"🏷️ [SCRIPT-GEN] Título inserido: "
                f"\"{title_row.get('titleLine1', '')[:50]}\""
            )

        # ─── 3a. Garantir segmentos para todos speech rows ───
        # Busca word timestamps e cria segmento único para rows sem segmentos.
        # Isso permite word-level diff no editor (correções amarelas, deleções vermelhas).
        try:
            word_timestamps = _fetch_word_timestamps(speech_asset_ids)
            if word_timestamps:
                rows = _ensure_segments_for_speech_rows(rows, word_timestamps)
        except Exception as seg_err:
            logger.warning(
                f"⚠️ [SCRIPT-GEN] Erro ao garantir segmentos: {seg_err}"
            )

        # ─── 3. Montar document content_data ─────────────────
        format_label = FORMAT_LABELS.get(format_detected, format_detected)

        # Determinar maturity level
        maturity = 'classified'
        sources = ['triage']
        if transcript_result:
            maturity = 'analyzed'
            sources.append('transcript')
        if vision_result:
            maturity = 'analyzed'
            sources.append('vision')
        if edl_result:
            maturity = 'mounted'
            sources.append('clipper')

        # Voice e Visuals como strings V1 (retrocompatibilidade)
        voice_text = '\n\n'.join(r.get('voice', '') for r in rows if r.get('voice'))
        visual_text = '\n\n'.join(r.get('visual', '') for r in rows if r.get('visual'))

        content_data = {
            'format': 'v2',
            'twoColumns': False,
            'title': f'Roteiro — {format_label}',
            'voice': voice_text,
            'visuals': visual_text,
            'rows': rows,
            'metadata': {
                'storytelling_mode': _detect_storytelling_mode(triage_result),
                'format_detected': format_detected,
                'format_label': format_label,
                'maturity': maturity,
                'generated_from': sources,
                'generated_at': datetime.utcnow().isoformat(),
                'pipeline_ready_count': pipeline_count,
                'vision_analysis_count': vision_count,
                'total_assets': len(triage_assets),
                'is_auto_generated': True,
                'phase': 1,
            },
        }

        # ─── 4. Salvar como content_document ─────────────────
        document_id = _save_document(
            project_id=project_id,
            conversation_id=conversation_id,
            title=f'Roteiro — {format_label}',
            content_data=content_data,
        )

        if not document_id:
            return None

        elapsed = time.time() - t0

        logger.info(
            f"📝 [SCRIPT-GEN] Roteiro gerado em {elapsed:.1f}s | "
            f"{len(rows)} rows | maturity={maturity} | "
            f"sources={sources} | doc={document_id[:8]}"
        )

        # ─── 5. NÃO enviar mensagem individual ─────────────────
        # A mensagem consolidada é enviada pelo video_clipper_trigger
        # com show_script + open_editor + document_id para abrir o editor.
        # Apenas emitir SSE de progresso (sem criar mensagem no banco).
        try:
            from app.routes.chat_sse import emit_chat_event
            emit_chat_event(conversation_id, 'script_progress', {
                'type': 'script_generated',
                'document_id': document_id,
                'rows_count': len(rows),
                'maturity': maturity,
            })
        except Exception:
            pass

        return document_id

    except Exception as e:
        logger.error(
            f"❌ [SCRIPT-GEN] Erro ao gerar roteiro: {e}",
            exc_info=True,
        )
        return None


# ═══════════════════════════════════════════════════════════════
# ROW BUILDERS
# ═══════════════════════════════════════════════════════════════


def _build_rows_from_edl(
    edl_result: dict,
    transcript_result: Optional[dict],
    vision_result: Optional[dict],
    triage_assets: dict,
    original_transcriptions: Optional[Dict[str, str]] = None,
    retake_index: Optional[Dict[str, dict]] = None,
    intra_index: Optional[Dict[str, List[dict]]] = None,
) -> List[dict]:
    """
    FASE 1: Monta rows separados — speech primeiro, b-roll depois.

    Não pareamos b-roll com speech (isso será Fase 2).
    Cada speech asset = 1 row com texto ORIGINAL (do AssemblyAI).
    Cada b-roll asset = 1 row com descrição visual.
    Takes removidos aparecem com isRetakeRemoved=true (para strikethrough).
    """
    from app.services.script_row_factory import (
        create_speech_row, create_broll_row,
        attach_intra_retakes, mark_as_retake_removed, index_vision_results,
    )

    rows = []
    order_counter = 1
    edit_sequence = edl_result.get('edit_sequence', [])
    orig = original_transcriptions or {}
    retakes = retake_index or {}
    intra = intra_index or {}

    vision_videos = index_vision_results(vision_result)

    # ─── BLOCO 1: Speech mantidos (transcrição ORIGINAL) ───
    edl_speech_ids = set()
    for cut in edit_sequence:
        cut_type = cut.get('type', '')
        asset_id = cut.get('asset_id', '')

        if cut_type in ('talking_head', 'audio_narration'):
            edl_speech_ids.add(asset_id)
            triage_info = triage_assets.get(asset_id, {})
            voice_text = orig.get(asset_id, '') or cut.get('text', '') or ''

            row_data = create_speech_row(
                asset_id=asset_id,
                voice_text=voice_text,
                order=order_counter,
                filename=triage_info.get('filename', ''),
                speaker=cut.get('speaker', ''),
                source_type=cut_type,
                notes=cut.get('editorial_purpose', ''),
                editorial_purpose=cut.get('editorial_purpose', ''),
            )
            attach_intra_retakes(row_data, asset_id, intra)
            rows.append(row_data)
            order_counter += 1

    # ─── BLOCO 1b: Retakes removidos (strikethrough) ────────
    for aid, info in retakes.items():
        if info.get('is_removed') and aid not in edl_speech_ids:
            triage_info = triage_assets.get(aid, {})
            filename = triage_info.get('filename', '')
            voice_text = orig.get(aid, '') or f'[Take removido — {filename}]'

            row_data = create_speech_row(
                asset_id=aid,
                voice_text=voice_text,
                order=order_counter,
                filename=filename,
                source_type=triage_info.get('classification', 'talking_head'),
                notes=info.get('reason', ''),
            )
            mark_as_retake_removed(row_data, reason=info.get('reason', ''), kept_asset_id=info.get('kept_asset_id', ''))
            rows.append(row_data)
            order_counter += 1

    # ─── BLOCO 2: B-Roll (descrição de cada upload) ─────────
    broll_asset_ids_seen = set()

    for cut in edit_sequence:
        if cut.get('type') == 'b_roll':
            aid = cut.get('asset_id', '')
            if aid and aid not in broll_asset_ids_seen:
                broll_asset_ids_seen.add(aid)
                triage_info = triage_assets.get(aid, {})
                filename = triage_info.get('filename', '')
                visual_desc = _format_broll_description(vision_videos[aid], {}) if aid in vision_videos else f'🎬 {filename or "B-Roll"}'

                rows.append(create_broll_row(
                    asset_id=aid, visual_desc=visual_desc,
                    order=order_counter, filename=filename,
                ))
                order_counter += 1

    # B-rolls do vision que não estão no EDL
    for aid, v in vision_videos.items():
        if aid not in broll_asset_ids_seen:
            triage_info = triage_assets.get(aid, {})
            rows.append(create_broll_row(
                asset_id=aid,
                visual_desc=_format_broll_description(v, {}),
                order=order_counter,
                filename=triage_info.get('filename', ''),
            ))
            order_counter += 1

    return rows


def _build_rows_from_analysis(
    transcript_result: dict,
    vision_result: dict,
    triage_assets: dict,
    original_transcriptions: Optional[Dict[str, str]] = None,
    retake_index: Optional[Dict[str, dict]] = None,
    intra_index: Optional[Dict[str, List[dict]]] = None,
) -> List[dict]:
    """Caso 2: Transcript + Vision (sem EDL)."""
    from app.services.script_row_factory import (
        create_speech_row, create_broll_row,
        attach_intra_retakes, mark_as_retake_removed, index_vision_results,
    )

    rows = []
    order_counter = 1
    orig = original_transcriptions or {}
    retakes = retake_index or {}
    intra = intra_index or {}

    vision_videos = index_vision_results(vision_result)

    # Usar ordem sugerida pelo Transcript Director
    per_asset = transcript_result.get('per_asset_analysis', [])
    suggested_order = transcript_result.get(
        'order_analysis', {}
    ).get('suggested_order', [])

    if suggested_order:
        asset_map = {pa.get('asset_id', ''): pa for pa in per_asset}
        ordered = [asset_map[aid] for aid in suggested_order if aid in asset_map]
        remaining = [pa for pa in per_asset if pa.get('asset_id', '') not in suggested_order]
        per_asset = ordered + remaining

    # ─── BLOCO 1: Speech — mantidos primeiro, removidos depois ──
    kept_rows = []
    removed_rows = []

    for pa in per_asset:
        asset_id = pa.get('asset_id', '')
        speaker = pa.get('speaker_name', '')
        is_removed = pa.get('is_retake_duplicate') and not pa.get('keep', True)

        voice_text = orig.get(asset_id, '')
        if not voice_text:
            key_phrases = pa.get('key_phrases', [])
            voice_text = '. '.join(key_phrases[:3]) if key_phrases else f'[Fala de {speaker}]'

        triage_info = triage_assets.get(asset_id, {})
        retake_info = retakes.get(asset_id, {})

        row = create_speech_row(
            asset_id=asset_id,
            voice_text=voice_text,
            order=0,  # definido abaixo
            filename=triage_info.get('filename', ''),
            speaker=speaker,
            notes=', '.join(pa.get('topics', [])),
        )
        attach_intra_retakes(row, asset_id, intra)

        if is_removed:
            mark_as_retake_removed(
                row,
                reason=retake_info.get('reason', pa.get('retake_note', '')),
                kept_asset_id=retake_info.get('kept_asset_id', ''),
            )
            removed_rows.append(row)
        else:
            kept_rows.append(row)

    # Mantidos primeiro, removidos depois
    for row in kept_rows:
        row['order'] = order_counter
        rows.append(row)
        order_counter += 1

    for row in removed_rows:
        row['order'] = order_counter
        rows.append(row)
        order_counter += 1

    # ─── BLOCO 2: B-Roll (descrição separada) ───────────
    for v in vision_videos.values():
        aid = v.get('asset_id', '')
        triage_info = triage_assets.get(aid, {})
        rows.append(create_broll_row(
            asset_id=aid,
            visual_desc=_format_broll_description(v, {}),
            order=order_counter,
            filename=triage_info.get('filename', ''),
        ))
        order_counter += 1

    return rows


def _build_rows_from_transcript_only(
    transcript_result: dict,
    triage_assets: dict,
    original_transcriptions: Optional[Dict[str, str]] = None,
    retake_index: Optional[Dict[str, dict]] = None,
    intra_index: Optional[Dict[str, List[dict]]] = None,
) -> List[dict]:
    """Caso 3: Só transcript, sem vision. Uma row por asset (inteiro)."""
    from app.services.script_row_factory import (
        create_speech_row, attach_intra_retakes, mark_as_retake_removed,
    )

    rows = []
    order_counter = 1
    orig = original_transcriptions or {}
    retakes = retake_index or {}
    intra = intra_index or {}

    per_asset = transcript_result.get('per_asset_analysis', [])

    kept_rows = []
    removed_rows = []

    for pa in per_asset:
        asset_id = pa.get('asset_id', '')
        speaker = pa.get('speaker_name', '')
        is_removed = pa.get('is_retake_duplicate') and not pa.get('keep', True)

        voice_text = orig.get(asset_id, '')
        if not voice_text:
            key_phrases = pa.get('key_phrases', [])
            voice_text = '. '.join(key_phrases[:3]) if key_phrases else f'[Fala de {speaker}]'

        triage_info = triage_assets.get(asset_id, {})
        retake_info = retakes.get(asset_id, {})

        row = create_speech_row(
            asset_id=asset_id,
            voice_text=voice_text,
            order=0,
            filename=triage_info.get('filename', ''),
            speaker=speaker,
            notes=', '.join(pa.get('topics', [])),
        )
        attach_intra_retakes(row, asset_id, intra)

        if is_removed:
            mark_as_retake_removed(
                row,
                reason=retake_info.get('reason', pa.get('retake_note', '')),
                kept_asset_id=retake_info.get('kept_asset_id', ''),
            )
            removed_rows.append(row)
        else:
            kept_rows.append(row)

    # Mantidos primeiro, removidos depois
    for row in kept_rows:
        row['order'] = order_counter
        rows.append(row)
        order_counter += 1

    for row in removed_rows:
        row['order'] = order_counter
        rows.append(row)
        order_counter += 1

    return rows


def _build_rows_from_triage_only(triage_result: dict) -> List[dict]:
    """Caso mínimo: só triage (classificação básica). Fase 1 layout."""
    from app.services.script_row_factory import create_speech_row, create_broll_row

    rows = []
    order_counter = 1

    # Bloco 1: Speech
    for a in triage_result.get('assets', []):
        cls = a.get('classification', '')
        filename = a.get('filename', '?')
        if cls in ('talking_head', 'audio_narration'):
            rows.append(create_speech_row(
                asset_id=a.get('asset_id', ''),
                voice_text=f'[{filename}] — Aguardando transcrição',
                order=order_counter,
                filename=filename,
                source_type=cls,
                notes=a.get('notes', ''),
            ))
            order_counter += 1

    # Bloco 2: B-Roll
    for a in triage_result.get('assets', []):
        cls = a.get('classification', '')
        filename = a.get('filename', '?')
        if cls in ('b_roll', 'screen_capture', 'image_static'):
            rows.append(create_broll_row(
                asset_id=a.get('asset_id', ''),
                visual_desc=f'{filename} — Aguardando análise visual',
                order=order_counter,
                filename=filename,
                source_type=cls,
                notes=a.get('notes', ''),
            ))
            order_counter += 1

    return rows


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════


def _build_retake_index(transcript_result: Optional[dict]) -> Dict[str, dict]:
    """
    Indexa decisões de retakes do Transcript Director.

    Returns:
        Dict mapeando asset_id → {
            'is_removed': bool,
            'kept_asset_id': str,      # qual take foi preferido
            'reason': str,             # motivo da remoção
            'group_asset_ids': list,   # todos os takes do grupo
        }
    """
    if not transcript_result:
        return {}

    index = {}
    retakes = transcript_result.get('retakes_refined', [])

    for group in retakes:
        keep_id = group.get('keep_asset_id', '')
        remove_ids = group.get('remove_asset_ids', [])
        reason = group.get('reason', '')
        group_ids = group.get('group_asset_ids', [])

        # Marcar o take mantido
        if keep_id:
            index[keep_id] = {
                'is_removed': False,
                'kept_asset_id': keep_id,
                'reason': '',
                'group_asset_ids': group_ids,
            }

        # Marcar takes removidos
        for rid in remove_ids:
            index[rid] = {
                'is_removed': True,
                'kept_asset_id': keep_id,
                'reason': reason,
                'group_asset_ids': group_ids,
            }

    return index


def _build_intra_retakes_index(
    transcript_result: Optional[dict],
) -> Dict[str, List[dict]]:
    """
    Indexa segmentos intra-arquivo do IntraRetakeResolver.

    🔧 09/Fev/2026: Refatorado para padrão híbrido.
    Os segmentos agora vêm do IntraRetakeResolver (determinístico)
    e já contêm: id, start_s, end_s, text, status, removedBy, removedReason.

    Returns:
        Dict mapeando asset_id → lista de segments prontos para o frontend
    """
    if not transcript_result:
        return {}

    index = {}
    intra = transcript_result.get('intra_retakes', [])

    for item in intra:
        aid = item.get('asset_id', '')
        segments = item.get('segments', [])
        if aid and segments:
            # Segmentos já vêm prontos do resolver — apenas garantir campos obrigatórios
            index[aid] = [
                {
                    'id': seg.get('id', f'seg-{uuid.uuid4().hex[:8]}'),
                    'start_s': seg.get('start_s', 0),
                    'end_s': seg.get('end_s', 0),
                    'text': seg.get('text', ''),
                    'status': seg.get('status', 'keep'),
                    'removedBy': seg.get('removedBy') or ('llm' if seg.get('status') == 'removed' else None),
                    'removedReason': seg.get('removedReason', seg.get('reason', '')),
                }
                for seg in segments
            ]

    return index


def _fetch_original_transcriptions(asset_ids: List[str]) -> Dict[str, str]:
    """
    Busca as transcrições ORIGINAIS do AssemblyAI direto do banco.

    Nunca confiar no texto que vem via LLM (Transcript Director ou VideoClipper)
    porque LLMs truncam, resumem ou reformulam o texto.

    Returns:
        Dict mapeando asset_id → texto original completo da transcrição
    """
    if not asset_ids:
        return {}

    transcriptions = {}
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Buscar em lote
        placeholders = ','.join(['%s'] * len(asset_ids))
        cursor.execute(f"""
            SELECT id,
                   metadata->'transcription_result'->>'transcript' as transcript
            FROM project_assets
            WHERE id IN ({placeholders})
        """, tuple(asset_ids))

        for row in cursor.fetchall():
            aid = row[0]
            txt = row[1] or ''
            if txt.strip():
                transcriptions[aid] = txt.strip()

        cursor.close()
        conn.close()
        logger.info(
            f"📜 [SCRIPT-GEN] Transcrições originais: "
            f"{len(transcriptions)}/{len(asset_ids)} assets"
        )
    except Exception as e:
        logger.warning(f"⚠️ [SCRIPT-GEN] Erro ao buscar transcrições: {e}")

    return transcriptions


def _fetch_word_timestamps(asset_ids: List[str]) -> Dict[str, List[dict]]:
    """
    Busca word timestamps do AssemblyAI direto do banco.

    Returns:
        Dict mapeando asset_id → words[] (lista de {text, start, end})
    """
    if not asset_ids:
        return {}

    words_map = {}
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(asset_ids))
        cursor.execute(f"""
            SELECT id,
                   metadata->'transcription_result'->'words' as words
            FROM project_assets
            WHERE id IN ({placeholders})
        """, tuple(asset_ids))

        for row in cursor.fetchall():
            aid = row[0]
            words_raw = row[1]
            if words_raw:
                if isinstance(words_raw, str):
                    import json as _json
                    words_raw = _json.loads(words_raw)
                if isinstance(words_raw, list) and len(words_raw) > 0:
                    words_map[aid] = words_raw

        cursor.close()
        conn.close()
        logger.info(
            f"📜 [SCRIPT-GEN] Word timestamps: "
            f"{len(words_map)}/{len(asset_ids)} assets"
        )
    except Exception as e:
        logger.warning(f"⚠️ [SCRIPT-GEN] Erro ao buscar word timestamps: {e}")

    return words_map


def _ensure_segments_for_speech_rows(
    rows: List[dict],
    word_timestamps: Dict[str, List[dict]],
) -> List[dict]:
    """
    Para cada speech row SEM segmentos, cria um segmento único
    cobrindo toda a transcrição, usando word timestamps.

    Isso garante que o editor tenha a capacidade de fazer
    word-level diff (correções amarelas, deleções vermelhas riscadas)
    em TODOS os rows de fala, não só nos que têm intra-retakes.
    """
    from app.services.script_row_factory import create_single_segment_from_words

    for row in rows:
        if row.get('type') != 'speech':
            continue
        if row.get('segments'):
            continue  # Já tem segmentos (intra-retakes resolvidos)
        if row.get('isRetakeRemoved'):
            continue  # Row removido, não precisa de segmentos

        # Buscar asset_id (campo gerado pela factory)
        asset_id = row.get('sourceAssetId', '')
        if not asset_id:
            continue

        words = word_timestamps.get(asset_id, [])
        if not words:
            continue

        segments = create_single_segment_from_words(asset_id, words)
        if segments:
            row['segments'] = segments
            row['hasIntraRetakes'] = False
            logger.debug(
                f"🔧 [SCRIPT-GEN] Segmento único criado para row "
                f"{row.get('id', '?')[:12]} (asset {asset_id[:8]})"
            )

    return rows


def _format_timestamp(in_ms: int, out_ms: int) -> str:
    """Formata timestamps ms → 'M:SS - M:SS'."""
    if not in_ms and not out_ms:
        return ''

    def ms_to_str(ms):
        total_s = ms / 1000
        minutes = int(total_s // 60)
        seconds = total_s % 60
        return f"{minutes}:{seconds:04.1f}"

    return f"{ms_to_str(in_ms)} - {ms_to_str(out_ms)}"


def _format_broll_description(vision_data: dict, placement: dict) -> str:
    """Formata descrição user-friendly de um b-roll."""
    parts = []

    # Resumo visual
    summary = vision_data.get('visual_summary', '')
    if summary:
        parts.append(f"🎬 {summary[:120]}")
    else:
        parts.append('🎬 B-Roll')

    # Timestamps do melhor segmento
    best = vision_data.get('best_usable_segment', {})
    if best and best.get('in_ms') is not None:
        in_s = best['in_ms'] / 1000
        out_s = best['out_ms'] / 1000
        parts.append(f"Melhor trecho: {in_s:.1f}s - {out_s:.1f}s")

    # Propósito (do placement do EDL)
    purpose = placement.get('purpose', '') or placement.get('editorial_purpose', '')
    if purpose:
        parts.append(f"Propósito: {purpose}")

    # Cores dominantes
    colors = vision_data.get('dominant_colors', [])
    if colors:
        parts.append(f"Cores: {', '.join(colors[:3])}")

    return '\n'.join(parts)


def _detect_storytelling_mode(triage_result: dict) -> str:
    """Detecta storytelling_mode a partir do formato da triagem."""
    format_detected = triage_result.get('format_detected', 'unknown')

    mode_map = {
        'talking_head_solo': 'talking_head',
        'multi_interview': 'talking_head',
        'narration_broll': 'narration',
        'humor_dialogue': 'talking_head',
        'tutorial': 'talking_head',
        'mixed': 'talking_head',
    }

    return mode_map.get(format_detected, 'talking_head')


def _generate_title_row(
    project_id: str,
    conversation_id: str,
    transcript_result: Optional[dict],
) -> Optional[dict]:
    """
    Gera um row de título para o roteiro usando o Title Director.

    Prioridade:
    1. Se title_director_result já existe em project_config, usa ele
    2. Se transcript_result disponível, gera título síncrono
    3. Se nenhum, retorna None (sem título)
    """
    try:
        from app.services.script_row_factory import create_title_row

        # 1. Verificar se já tem título gerado
        existing_title = _get_existing_title_result(project_id)
        if existing_title and existing_title.get('status') == 'success':
            logger.info(
                f"🏷️ [SCRIPT-GEN] Usando título existente: "
                f"\"{existing_title.get('title', {}).get('line_1', '')[:50]}\""
            )
            return create_title_row(existing_title)

        # 2. Gerar título síncrono se temos transcript_result
        if transcript_result:
            from app.services.title_director_trigger import generate_title_sync
            result = generate_title_sync(project_id, conversation_id)
            if result and result.get('status') == 'success':
                return create_title_row(result)

        # 3. Sem dados para gerar título
        logger.info(
            f"🏷️ [SCRIPT-GEN] Sem dados para gerar título para {project_id[:8]}"
        )
        return None

    except Exception as e:
        logger.warning(
            f"⚠️ [SCRIPT-GEN] Erro ao gerar título: {e}"
        )
        return None


def _get_existing_title_result(project_id: str) -> Optional[dict]:
    """Busca title_director_result do project_config."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT project_config->'title_director_result'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row or not row[0]:
            return None

        data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        return data
    except Exception as e:
        logger.warning(
            f"⚠️ [SCRIPT-GEN] Erro ao ler title_director_result: {e}"
        )
        return None


def _get_project_owner(project_id: str) -> Optional[str]:
    """Busca o user_id (owner) do projeto."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id FROM projects WHERE project_id = %s LIMIT 1",
            (project_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return str(row[0])
        return None
    except Exception as e:
        logger.warning(f"⚠️ [SCRIPT-GEN] Erro ao buscar owner: {e}")
        return None


def _save_document(
    project_id: str,
    conversation_id: str,
    title: str,
    content_data: dict,
) -> Optional[str]:
    """Salva roteiro como content_document no banco."""
    try:
        from app.db import get_db_connection

        # Buscar user_id do projeto para o campo created_by
        user_id = _get_project_owner(project_id)
        if not user_id:
            logger.warning(
                f"⚠️ [SCRIPT-GEN] Não encontrou owner para project {project_id[:8]}, "
                "documento será salvo sem created_by"
            )

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO content_documents
                (title, document_type, project_id, conversation_id,
                 content_data, created_by)
            VALUES (%s, 'script', %s, %s, %s, %s)
            RETURNING document_id
        """, (
            title,
            project_id,
            conversation_id,
            json.dumps(content_data, ensure_ascii=False),
            user_id,
        ))

        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        if row:
            doc_id = str(row[0])
            logger.info(
                f"💾 [SCRIPT-GEN] Documento salvo: {doc_id[:8]} | "
                f"created_by={user_id[:8] if user_id else 'None'}"
            )
            return doc_id

        return None
    except Exception as e:
        logger.error(f"❌ [SCRIPT-GEN] Erro ao salvar documento: {e}")
        return None


def _send_script_ready_message(
    conversation_id: str,
    document_id: str,
    content_data: dict,
    rows_count: int,
) -> None:
    """Envia mensagem SSE para abrir o editor com o roteiro."""
    try:
        from app.db import get_db_connection
        msg_id = str(uuid.uuid4())
        conn = get_db_connection()
        cursor = conn.cursor()

        metadata = content_data.get('metadata', {})
        format_label = metadata.get('format_label', 'Roteiro')
        maturity = metadata.get('maturity', 'classified')
        sources = metadata.get('generated_from', [])

        maturity_labels = {
            'classified': 'Classificado',
            'analyzed': 'Analisado',
            'mounted': 'Montado',
        }

        content = (
            f"**Roteiro Gerado Automaticamente** — {format_label}\n\n"
            f"📝 {rows_count} linhas | "
            f"Maturidade: {maturity_labels.get(maturity, maturity)} | "
            f"Fontes: {', '.join(sources)}\n\n"
            f"_Clique para abrir no Editor de Roteiros e revisar._"
        )

        component_props = {
            'show_script': True,
            'document_id': document_id,
            'open_editor': True,
            'scriptData': content_data,
            'rows_count': rows_count,
            'maturity': maturity,
            'format_detected': metadata.get('format_detected'),
            'is_auto_generated': True,
        }

        cursor.execute("""
            INSERT INTO chatbot_messages
                (id, message_id, conversation_id, sender, content,
                 component_type, component_props, created_at)
            VALUES (%s, %s, %s, 'bot', %s, %s, %s, NOW())
        """, (
            msg_id, msg_id, conversation_id, content,
            'script_ready',
            json.dumps(component_props, ensure_ascii=False),
        ))
        conn.commit()
        cursor.close()
        conn.close()

        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': msg_id,
            'sender': 'bot',
            'content': content,
            'component_type': 'script_ready',
            'component_props': component_props,
        })

        logger.info(
            f"📡 [SCRIPT-GEN] Mensagem script_ready enviada: {msg_id[:8]}..."
        )
    except Exception as e:
        logger.error(
            f"❌ [SCRIPT-GEN] Erro ao enviar mensagem: {e}"
        )
