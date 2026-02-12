"""
👁️ Vision Analysis Trigger — Dispara análise visual assíncrona.

Arquitetura Híbrida v3:
  Modal CPU (v-motion-analyzer): RAFT → motion_data + frames
  LLM API (GPT-4o-mini):        frames + motion → análise semântica

Quando o usuário ativa "Análise Visual" no chatbot:
  1. Busca TODOS os vídeos do projeto (project_assets)
  2. Para cada vídeo (em paralelo):
     a. Gera URL temporária do B2
     b. Chama Modal CPU (RAFT + frames)
     c. Chama LLM API (análise semântica)
  3. Combina resultados
  4. Salva em project_config.vision_analysis_result
  5. Envia mensagem no chat via SSE

Autor: Claude + Vinicius
Data: 08/Fev/2026
"""

import logging
import threading
import json
import uuid
import time
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# Máximo de vídeos processados em paralelo
# Reduzido de 5→3 para evitar cold start overload no Modal (Starter plan)
# O retry com backoff no _call_motion_analyzer compensa a concorrência menor
MAX_PARALLEL_VIDEOS = 3


def trigger_vision_analysis_async(
    project_id: str,
    conversation_id: str,
    batch_id: str = None,
) -> None:
    """Dispara análise visual em background thread."""
    thread = threading.Thread(
        target=_run_vision_analysis,
        args=(project_id, conversation_id, batch_id),
        daemon=True,
        name=f"vision-analysis-{project_id[:8]}",
    )
    thread.start()
    logger.info(
        f"👁️ [VISION-TRIGGER] Background thread iniciada: "
        f"project={project_id[:8]}... conv={conversation_id[:8]}... "
        f"batch={batch_id[:8] if batch_id else 'none'}"
    )


def _run_vision_analysis(
    project_id: str,
    conversation_id: str,
    batch_id: str = None,
) -> None:
    """Executa a análise visual completa (roda em background thread)."""
    t0 = time.time()

    try:
        # ─── 0. Resolver batch_id se não foi passado ───
        if not batch_id:
            batch_id = _resolve_batch_id(conversation_id)
            if batch_id:
                logger.info(f"👁️ [VISION-TRIGGER] batch_id resolvido: {batch_id[:8]}")

        # ─── 1. Buscar vídeos do projeto ─────────────────────
        # Se houver resultado de triagem com routing, analisar
        # APENAS os vídeos recomendados (b_roll, etc.).
        # Senão, analisar todos.
        all_videos = _get_project_videos(project_id)
        if not all_videos:
            _send_error_message(
                conversation_id,
                "Não encontrei vídeos no projeto para analisar. "
                "Faça upload de um vídeo primeiro."
            )
            return

        # Filtrar por routing da triagem (se existir)
        triage_routing_ids = _get_triage_routing_ids(project_id)
        if triage_routing_ids:
            videos = [v for v in all_videos if v['id'] in triage_routing_ids]
            if not videos:
                # Fallback: se nenhum match, analisar todos
                videos = all_videos
                logger.warning(
                    f"⚠️ [VISION-TRIGGER] Routing IDs não bateram com assets, "
                    f"analisando todos os {len(all_videos)} vídeos"
                )
            else:
                logger.info(
                    f"👁️ [VISION-TRIGGER] Triagem recomendou {len(videos)}/{len(all_videos)} "
                    f"vídeos para análise visual"
                )
        else:
            videos = all_videos

        logger.info(f"👁️ [VISION-TRIGGER] {len(videos)} vídeo(s) para analisar")

        # ─── 2. Gerar URLs do B2 ─────────────────────────────
        video_urls = []
        for video in videos:
            url = _generate_download_url(video['bucket'], video['file_path'])
            if url:
                video_urls.append({**video, 'url': url})
                logger.info(f"👁️ [VISION-TRIGGER] URL: {video['file_path'][:60]}...")
            else:
                logger.warning(f"⚠️ [VISION-TRIGGER] Falha URL: {video['file_path']}")

        if not video_urls:
            _send_error_message(conversation_id, "Erro ao gerar URLs dos vídeos.")
            return

        # ─── 3. Typing indicator + SSE para visualizer ────────
        n = len(video_urls)
        _send_typing_message(
            conversation_id,
            f"Analisando {n} vídeo{'s' if n > 1 else ''} com IA visual..."
        )
        # 🆕 v4.7.1: Emitir upload_step para o visualizer mostrar vision analysis
        try:
            from app.routes.chat_sse import emit_chat_event
            emit_chat_event(conversation_id, 'upload_step_start', {
                'step': 'vision_analysis',
                'message': f"Análise visual: {n} vídeo{'s' if n > 1 else ''}...",
                'metadata': {
                    'total_videos': n,
                    'max_parallel': MAX_PARALLEL_VIDEOS,
                },
            })
        except Exception:
            pass
        # 🐛 FIX v4.7.2: Persistir step no banco via update_batch_final_step
        if batch_id:
            try:
                from app.routes.visualizer_persistence import update_batch_final_step
                update_batch_final_step(
                    conversation_id, batch_id, 'vision_analysis', 'active',
                    f"Análise visual: {n} vídeo{'s' if n > 1 else ''}..."
                )
            except Exception as viz_err:
                logger.warning(f"⚠️ [VISION-TRIGGER] Erro ao atualizar visualizer: {viz_err}")

        # ─── 4. Analisar vídeos (paralelo) ───────────────────
        from app.video_orchestrator.services.visual_director_service import (
            get_visual_director_service,
        )
        service = get_visual_director_service()

        # Resolver idioma de resposta para o projeto
        try:
            from app.services.asset_triage_service import get_project_locale
            response_language = get_project_locale(project_id)
        except Exception:
            response_language = 'Portuguese (pt-BR)'

        results = []
        if len(video_urls) == 1:
            # Caso simples: 1 vídeo
            result = service.analyze(
                video_url=video_urls[0]['url'],
                options={"num_frames": 8},
                response_language=response_language,
            )
            result['asset_id'] = video_urls[0]['id']
            result['file_path'] = video_urls[0]['file_path']
            results.append(result)
        else:
            # Múltiplos vídeos: paralelo
            with ThreadPoolExecutor(max_workers=MAX_PARALLEL_VIDEOS) as executor:
                futures = {}
                for v in video_urls:
                    future = executor.submit(
                        service.analyze,
                        video_url=v['url'],
                        options={"num_frames": 8},
                        response_language=response_language,
                    )
                    futures[future] = v

                completed_count = 0
                for future in as_completed(futures):
                    v = futures[future]
                    try:
                        result = future.result()
                        result['asset_id'] = v['id']
                        result['file_path'] = v['file_path']
                        results.append(result)
                        completed_count += 1
                    except Exception as e:
                        logger.error(f"❌ [VISION-TRIGGER] Falha vídeo {v['id']}: {e}")
                        results.append({
                            'status': 'error',
                            'error': str(e),
                            'asset_id': v['id'],
                            'file_path': v['file_path'],
                        })
                        completed_count += 1
                    # 🆕 v4.7.1: Emitir progresso por vídeo
                    try:
                        from app.routes.chat_sse import emit_chat_event
                        file_name = v.get('file_path', '').split('/')[-1][:30]
                        emit_chat_event(conversation_id, 'upload_step_progress', {
                            'step': 'vision_analysis',
                            'current': completed_count,
                            'total': n,
                            'message': f"Vídeo {completed_count}/{n} analisado: {file_name}",
                            'asset_id': v['id'],
                        })
                    except Exception:
                        pass

        elapsed = time.time() - t0

        # ─── 5. Verificar resultados ──────────────────────────
        successful = [r for r in results if r.get('status') == 'success']
        failed = [r for r in results if r.get('status') != 'success']

        if not successful:
            errors = "; ".join(r.get('error', '?') for r in failed)
            _send_error_message(
                conversation_id,
                f"A análise visual falhou para todos os vídeos: {errors}"
            )
            return

        # ─── 6. Persistir resultados ──────────────────────────
        if len(successful) == 1:
            _persist_analysis_result(project_id, successful[0])
        else:
            combined = {
                "videos": successful,
                "total_videos": len(video_urls),
                "successful": len(successful),
                "failed": len(failed),
            }
            _persist_analysis_result(project_id, combined)

        # ─── 6b. Registrar custos ────────────────────────────
        try:
            from app.services.ai_cost_tracker import log_ai_usage
            for r in successful:
                # Motion analysis (Modal CPU)
                motion_ms = r.get('motion_time_ms', 0)
                if motion_ms > 0:
                    log_ai_usage(
                        service_type="vision_motion",
                        provider="modal",
                        model=r.get('engine', 'raft_small'),
                        project_id=project_id,
                        conversation_id=conversation_id,
                        duration_ms=motion_ms,
                        input_units=r.get('frames_count', 0),
                        metadata={
                            'asset_id': r.get('asset_id'),
                            'engine': r.get('engine'),
                        },
                    )
                # LLM analysis (GPT-4o-mini)
                llm_ms = r.get('llm_time_ms', 0)
                tokens_in = r.get('tokens_in', 0)
                tokens_out = r.get('tokens_out', 0)
                if tokens_in > 0:
                    log_ai_usage(
                        service_type="vision_llm",
                        provider="openai",
                        model=r.get('model', 'gpt-4o-mini'),
                        project_id=project_id,
                        conversation_id=conversation_id,
                        tokens_in=tokens_in,
                        tokens_out=tokens_out,
                        duration_ms=llm_ms,
                        input_units=r.get('frames_count', 0),
                        metadata={
                            'asset_id': r.get('asset_id'),
                            'content_type': r.get('content_type'),
                        },
                    )
        except Exception as cost_err:
            logger.warning(f"⚠️ [VISION-TRIGGER] Cost tracking: {cost_err}")

        # ─── 7. SSE: vision analysis completa ─────────────────
        # Os resultados ficam salvos em project_config.
        # A mensagem consolidada será enviada pelo VideoClipper/ScriptGenerator.
        try:
            from app.routes.chat_sse import emit_chat_event
            emit_chat_event(conversation_id, 'analysis_progress', {
                'type': 'vision_complete',
                'videos_analyzed': len(successful),
                'elapsed_s': round(elapsed, 1),
            })
            # 🆕 v4.7.1: Emitir upload_step_complete para o visualizer
            emit_chat_event(conversation_id, 'upload_step_complete', {
                'step': 'vision_analysis',
                'message': f"Análise visual concluída ({elapsed:.0f}s)",
                'metadata': {
                    'videos_analyzed': len(successful),
                    'videos_failed': len(failed),
                    'total_videos': len(video_urls),
                    'elapsed_s': round(elapsed, 1),
                    'engines': list(set(r.get('engine', '?') for r in successful)),
                    'models': list(set(r.get('model', '?') for r in successful)),
                    'total_motion_ms': sum(r.get('motion_time_ms', 0) for r in successful),
                    'total_llm_ms': sum(r.get('llm_time_ms', 0) for r in successful),
                    'total_tokens_in': sum(r.get('tokens_in', 0) for r in successful),
                    'total_tokens_out': sum(r.get('tokens_out', 0) for r in successful),
                },
            })
        except Exception:
            pass
        # 🐛 FIX v4.7.2: Persistir step no banco
        if batch_id:
            try:
                from app.routes.visualizer_persistence import update_batch_final_step
                update_batch_final_step(
                    conversation_id, batch_id, 'vision_analysis', 'complete',
                    f"Análise visual concluída ({elapsed:.0f}s)"
                )
            except Exception as viz_err:
                logger.warning(f"⚠️ [VISION-TRIGGER] Erro ao completar visualizer: {viz_err}")

        for r in successful:
            logger.info(
                f"✅ [VISION-TRIGGER] Vídeo {r.get('asset_id', '?')[:8]}... | "
                f"content_type={r.get('content_type')} | "
                f"engine={r.get('engine')} | "
                f"motion={r.get('motion_time_ms', 0)}ms + "
                f"llm={r.get('llm_time_ms', 0)}ms"
            )

        logger.info(
            f"✅ [VISION-TRIGGER] Total: {elapsed:.1f}s | "
            f"{len(successful)}/{len(video_urls)} vídeos OK"
        )

        # ─── 8. Verificar se VideoClipper pode ser disparado ────
        # Se o Transcript Director também já completou, gerar EDL
        try:
            from app.services.video_clipper_trigger import check_and_trigger_clipper
            check_and_trigger_clipper(project_id, conversation_id)
        except Exception as clipper_err:
            logger.warning(
                f"⚠️ [VISION-TRIGGER] Clipper check: {clipper_err}"
            )

    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"❌ [VISION-TRIGGER] Falha após {elapsed:.1f}s: {e}", exc_info=True)
        _send_error_message(conversation_id, "Ocorreu um erro durante a análise visual.")


# ═══════════════════════════════════════════════════════════════
# HELPER — RESOLVER BATCH_ID
# ═══════════════════════════════════════════════════════════════


def _resolve_batch_id(conversation_id: str) -> Optional[str]:
    """
    Resolve o batch_id mais recente para uma conversa.
    Busca no upload_visualizer persistido no chatbot_messages.
    """
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT component_props->>'batch_id'
            FROM chatbot_messages
            WHERE conversation_id = %s
            AND component_type = 'upload_visualizer'
            ORDER BY created_at DESC
            LIMIT 1
        """, (conversation_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0]:
            return row[0]
        return None
    except Exception as e:
        logger.warning(f"⚠️ [VISION-TRIGGER] Erro ao resolver batch_id: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════


def _get_project_videos(project_id: str) -> List[dict]:
    """Busca TODOS os vídeos do projeto."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, file_path, bucket, metadata
            FROM project_assets
            WHERE project_id = %s AND asset_type = 'video'
            ORDER BY created_at DESC
        """, (project_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return [{
            'id': str(row[0]),
            'file_path': row[1],
            'bucket': row[2],
            'metadata': row[3] if row[3] else {},
        } for row in rows]
    except Exception as e:
        logger.error(f"❌ [VISION-TRIGGER] Erro ao buscar vídeos: {e}")
        return []


def _get_triage_routing_ids(project_id: str) -> List[str]:
    """
    Busca os asset IDs recomendados para análise visual pela triagem.

    Lê project_config.asset_triage_result.routing.vision_analysis
    e retorna a lista de asset_ids que o triage recomendou para RAFT.
    Retorna lista vazia se não houver triagem ou routing.
    """
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT project_config->'asset_triage_result'->'routing'->'vision_analysis'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row or not row[0]:
            return []

        import json as json_mod
        vision_ids = row[0] if isinstance(row[0], list) else json_mod.loads(row[0])

        if isinstance(vision_ids, list) and len(vision_ids) > 0:
            logger.info(
                f"👁️ [VISION-TRIGGER] Triage routing: "
                f"{len(vision_ids)} asset(s) recomendados para análise visual"
            )
            return [str(vid) for vid in vision_ids]

        return []
    except Exception as e:
        logger.warning(f"⚠️ [VISION-TRIGGER] Erro ao ler triage routing: {e}")
        return []


def _generate_download_url(bucket_name: str, file_path: str) -> Optional[str]:
    """Gera URL temporária do B2 (1 hora)."""
    try:
        from app.routes.upload import generate_temp_download_url_internal
        return generate_temp_download_url_internal(
            bucket_name=bucket_name,
            file_path=file_path,
            duration_seconds=3600,
        )
    except Exception as e:
        logger.error(f"❌ [VISION-TRIGGER] Erro URL: {e}")
        return None


def _persist_analysis_result(project_id: str, result: dict) -> None:
    """Salva resultado em project_config.vision_analysis_result."""
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        result_json = json.dumps(result, ensure_ascii=False)
        cursor.execute("""
            UPDATE projects
            SET project_config = jsonb_set(
                COALESCE(project_config, '{}'::jsonb),
                '{vision_analysis_result}',
                %s::jsonb
            )
            WHERE project_id = %s
        """, (result_json, project_id))
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"💾 [VISION-TRIGGER] Resultado salvo")
    except Exception as e:
        logger.error(f"❌ [VISION-TRIGGER] Erro ao persistir: {e}")


def _format_analysis_message(results: list, elapsed: float) -> str:
    """Formata resultado(s) como mensagem legível no chat."""
    type_labels = {
        'talking_head': 'Vídeo com Fala',
        'b_roll': 'B-Roll / Cobertura',
        'product_shot': 'Produto / Packshot',
        'screen_capture': 'Captura de Tela',
        'mixed': 'Conteúdo Misto',
        'other': 'Outro',
    }

    lines = [f"**Análise Visual Concluída** ({elapsed:.0f}s) — {len(results)} vídeo(s)", ""]

    for i, r in enumerate(results):
        if len(results) > 1:
            lines.append(f"### Vídeo {i + 1}")

        content_type = r.get('content_type', 'desconhecido')
        type_label = type_labels.get(content_type, content_type)
        lines.append(f"**Tipo:** {type_label}")

        summary = r.get('visual_summary', '')
        if summary:
            lines.append(f"**Resumo:** {summary}")

        # Transições de segmento (novo formato)
        transitions = r.get('segment_transitions', [])
        if transitions:
            lines.append(f"**Segmentos:** {len(transitions)}")
            for t in transitions:
                start_s = t.get('start_ms', 0) / 1000
                end_s = t.get('end_ms', 0) / 1000
                motion = t.get('motion_type', '?')
                change = t.get('framing_change', '')
                visual = t.get('visual_change', '')[:100]
                lines.append(f"  [{start_s:.1f}s → {end_s:.1f}s] {motion} — {change or visual}")

        # Best usable segment
        best = r.get('best_usable_segment', {})
        if best and best.get('in_ms') is not None:
            in_s = best['in_ms'] / 1000
            out_s = best['out_ms'] / 1000
            rationale = best.get('rationale', '')[:120]
            lines.append(f"**Melhor Segmento:** [{in_s:.1f}s → {out_s:.1f}s] — {rationale}")

        quality = r.get('quality_notes', '')
        if quality:
            lines.append(f"**Qualidade:** {quality[:150]}")

        colors = r.get('dominant_colors', [])
        if colors:
            lines.append(f"**Cores:** {', '.join(colors[:5])}")

        # Métricas
        engine = r.get('engine', '?')
        motion_ms = r.get('motion_time_ms', 0)
        llm_ms = r.get('llm_time_ms', 0)
        model = r.get('model', '?')
        frames = r.get('frames_count', 0)
        tokens_in = r.get('tokens_in', 0)
        tokens_out = r.get('tokens_out', 0)

        lines.append("")
        lines.append(
            f"_Motion: {engine} ({motion_ms}ms) | "
            f"LLM: {model} ({llm_ms}ms) | "
            f"Frames: {frames} | "
            f"Tokens: {tokens_in}→{tokens_out}_"
        )

        if len(results) > 1:
            lines.append("")

    return "\n".join(lines)


def _send_analysis_message(conversation_id: str, content: str, analysis_result: dict) -> None:
    """Salva mensagem + emite SSE."""
    try:
        from app.db import get_db_connection
        msg_id = str(uuid.uuid4())
        conn = get_db_connection()
        cursor = conn.cursor()

        component_props = {
            'vision_analysis': True,
            'content_type': analysis_result.get('content_type'),
            'frames_count': analysis_result.get('frames_count', 0),
            'processing_time_ms': analysis_result.get('processing_time_ms', 0),
        }

        cursor.execute("""
            INSERT INTO chatbot_messages
                (id, message_id, conversation_id, sender, content,
                 component_type, component_props, created_at)
            VALUES (%s, %s, %s, 'bot', %s, %s, %s, NOW())
        """, (msg_id, msg_id, conversation_id, content,
              'vision_analysis_result', json.dumps(component_props)))
        conn.commit()
        cursor.close()
        conn.close()

        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'new_message', {
            'message_id': msg_id,
            'sender': 'bot',
            'content': content,
            'component_type': 'vision_analysis_result',
            'component_props': component_props,
        })

        logger.info(f"📡 [VISION-TRIGGER] Mensagem enviada: {msg_id[:8]}...")
    except Exception as e:
        logger.error(f"❌ [VISION-TRIGGER] Erro ao enviar mensagem: {e}")


def _send_error_message(conversation_id: str, content: str) -> None:
    _send_analysis_message(conversation_id, content, {})


def _send_typing_message(conversation_id: str, message: str = "") -> None:
    try:
        from app.routes.chat_sse import emit_chat_event
        emit_chat_event(conversation_id, 'typing', {
            'sender': 'bot',
            'message': message or 'Analisando vídeo com IA visual...',
        })
    except Exception as e:
        logger.warning(f"⚠️ [VISION-TRIGGER] Erro typing: {e}")
