"""
Step 01b: Corte de retakes intra-arquivo.

Step CONDICIONAL — só executa quando o Transcript Director detectou
retakes repetidos DENTRO de um mesmo vídeo (intra_retakes).

Fonte de dados: project_config.transcript_analysis_result.intra_retakes[]
Cada entry tem asset_id e segments[] com timestamps e status (keep/removed).

O que faz:
  1. Busca intra_retakes do project_config
  2. Para cada vídeo com retakes: usa segments[status=keep] como speech_periods
  3. Chama v-services /ffmpeg/silence_cut para cortar os trechos removed
  4. Atualiza state.videos[] com URLs dos vídeos limpos (retake_cut_url)

Quando NÃO executa (skip):
  - Se intra_retakes está vazio ou não existe (maioria dos casos)
  - Se todos os segments são 'keep' (nada para cortar)
"""

import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from ._base import *

MAX_CONCURRENT_RETAKE = int(os.environ.get('PIPELINE_MAX_CONCURRENT_CUT', '3'))


@register_step(
    name="apply_retake_cuts",
    description="Corta retakes intra-arquivo detectados pelo Transcript Director",
    category="preprocessing",
    depends_on=["normalize"],
    produces=[],
    optional=True,
    estimated_duration_s=20,
    cost_category="cpu",
    retryable=True,
    max_retries=2,
    timeout_s=180,
    sse_step_name="RETAKE_CUT",
    tool_schema={
        "description": "Corta repetições detectadas dentro de um mesmo vídeo",
        "input": {},
        "output": {"videos_cut": "int", "videos_skipped": "int"}
    }
)
def apply_retake_cuts_step(state: PipelineState, params: dict) -> PipelineState:
    # ─── 1. Buscar intra_retakes do project_config ───
    intra_retakes = _get_intra_retakes(state.project_id)

    if not intra_retakes:
        logger.info("⏭️ [RETAKE_CUT] Sem intra_retakes — skip")
        return state

    # ─── 2. Filtrar apenas vídeos que realmente têm trechos removed ───
    videos_to_cut = {}
    for ir in intra_retakes:
        asset_id = ir.get('asset_id', '')
        segments = ir.get('segments', [])

        # Verificar se há pelo menos um segment removed
        has_removed = any(s.get('status') == 'removed' for s in segments)
        if not has_removed:
            continue

        # Extrair speech_periods (segments com status=keep)
        speech_periods = []
        for seg in segments:
            if seg.get('status') == 'keep':
                speech_periods.append({
                    'start': seg.get('start_s', 0),
                    'end': seg.get('end_s', 0),
                    'duration': seg.get('end_s', 0) - seg.get('start_s', 0),
                })

        if speech_periods:
            videos_to_cut[asset_id] = speech_periods

    if not videos_to_cut:
        logger.info("⏭️ [RETAKE_CUT] Nenhum vídeo com trechos removed — skip")
        return state

    logger.info(
        f"✂️ [RETAKE_CUT] {len(videos_to_cut)} vídeo(s) com retakes para cortar"
    )

    # ─── 3. Mapear asset_id → video dict ───
    videos = state.videos or []
    vid_map = {}
    for v in videos:
        vid_id = v.get('asset_id') or v.get('upload_id')
        if vid_id:
            vid_map[vid_id] = v

    from ..services.silence_service import SilenceService
    service = SilenceService()

    # ─── 4. Cortar retakes em paralelo ───
    updated_videos = [dict(v) for v in videos]  # Cópia
    cuts_applied = 0
    errors = []

    items = []
    for asset_id, speech_periods in videos_to_cut.items():
        video_dict = vid_map.get(asset_id)
        if not video_dict:
            logger.warning(f"⚠️ [RETAKE_CUT] Asset {asset_id[:8]} não encontrado em state.videos")
            continue

        url = video_dict.get('normalized_url') or video_dict.get('url')
        if not url:
            logger.warning(f"⚠️ [RETAKE_CUT] Asset {asset_id[:8]} sem URL")
            continue

        items.append((asset_id, video_dict, url, speech_periods))

    if not items:
        logger.info("⏭️ [RETAKE_CUT] Nenhum vídeo válido para cortar — skip")
        return state

    with ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_RETAKE, len(items))) as executor:
        futures = {}
        for asset_id, video_dict, url, speech_periods in items:
            future = executor.submit(
                _cut_retake_video,
                service, url, asset_id, state.job_id, speech_periods,
            )
            futures[future] = asset_id

        for future in as_completed(futures):
            asset_id = futures[future]
            try:
                result = future.result()
                if result.get('error'):
                    errors.append(f"{asset_id[:8]}: {result['error']}")
                    logger.warning(f"⚠️ [RETAKE_CUT] {asset_id[:8]}: {result['error']}")
                    continue

                # Atualizar video dict com URL do vídeo limpo
                output_url = result.get('concatenated_url') or result.get('output_url')
                if output_url:
                    # Encontrar e atualizar na lista
                    for v in updated_videos:
                        vid_id = v.get('asset_id') or v.get('upload_id')
                        if vid_id == asset_id:
                            v['retake_cut_url'] = output_url
                            v['retake_cut_applied'] = True
                            cuts_applied += 1
                            logger.info(
                                f"✅ [RETAKE_CUT] {asset_id[:8]}: retakes cortados → "
                                f"{output_url[:60]}..."
                            )
                            break
            except Exception as e:
                errors.append(f"{asset_id[:8]}: {e}")
                logger.error(f"❌ [RETAKE_CUT] {asset_id[:8]}: {e}")

    logger.info(
        f"✅ [RETAKE_CUT] Concluído: {cuts_applied} corte(s), "
        f"{len(errors)} erro(s), "
        f"{len(videos) - cuts_applied - len(errors)} sem retakes"
    )

    return state.with_updates(
        videos=updated_videos,
    )


def _cut_retake_video(service, url, asset_id, job_id, speech_periods):
    """Corta retakes de um único vídeo usando speech_periods (keep segments)."""
    return service.cut_silence_hybrid(
        input_file=url,
        output_prefix=f"retake_{job_id[:8]}_{asset_id[:8]}",
        clips=speech_periods,
        clips_type='speech_periods',
        fast_copy=False,             # 🔧 v4.2: explícito — cortes precisos com re-encoding
        optimize_keyframes=False,    # 🔧 v4.2: desnecessário com re-encoding
    )


def _get_intra_retakes(project_id: str) -> list:
    """
    Busca intra_retakes do project_config.transcript_analysis_result.

    Retorna lista de {asset_id, segments[]} ou [] se não houver.
    """
    if not project_id:
        return []

    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT project_config->'transcript_analysis_result'->'intra_retakes'
            FROM projects
            WHERE project_id = %s
        """, (project_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row or not row[0]:
            return []

        data = row[0]
        if isinstance(data, str):
            data = json.loads(data)

        if isinstance(data, list):
            return data

        return []
    except Exception as e:
        logger.warning(f"⚠️ [RETAKE_CUT] Erro ao buscar intra_retakes: {e}")
        return []
