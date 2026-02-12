"""
Step 01: Normalização de áudio/vídeo.

v4.0: Multi-arquivo — normaliza cada vídeo e armazena URLs por vídeo
em state.videos[].normalized_url.

O NormalizeService do v-services já aceita urls[] e processa em paralelo
(ThreadPoolExecutor internamente). Retorna results[] com um item por vídeo.
"""

from ._base import *


@register_step(
    name="normalize",
    description="Normaliza áudio e vídeo (volume, FPS, resolução)",
    category="preprocessing",
    depends_on=["load_template"],
    produces=["normalized_video_url", "normalization_stats", "base_normalized_url"],
    estimated_duration_s=30,
    cost_category="cpu",
    retryable=True,
    max_retries=2,
    timeout_s=300,
    sse_step_name="NORMALIZE",
    tool_schema={
        "description": "Normaliza volume de áudio e formato de vídeo",
        "input": {"target_fps": "int?", "target_resolution": "str?"},
        "output": {"url": "str", "stats": "dict"}
    }
)
def normalize_step(state: PipelineState, params: dict) -> PipelineState:
    from ..services.normalize_service import NormalizeService

    videos = state.videos or []
    urls = [v.get('url') for v in videos if v.get('url')]
    if not urls:
        raise ValueError("Nenhuma URL de vídeo para normalizar")

    service = NormalizeService()
    result = service.normalize(
        urls=urls,
        target_fps=params.get('target_fps', 30),
        target_resolution=params.get('target_resolution'),
    )

    if result.get('error'):
        raise Exception(f"Normalização falhou: {result['error']}")

    # ─── v4.1: Mapear resultados POR VÍDEO ───
    # v-services retorna results[] onde cada item usa chaves dinâmicas:
    #   input_url_XX: URL original
    #   url_XX: URL do vídeo processado (normalizado)
    #   output_file_XX: URL cloud (Backblaze)
    #   shared_path_XX: caminho shared no container
    #   order: índice 1-based
    #   volume_analysis: {...}
    results_list = result.get('results', [])

    if results_list and len(results_list) > 0:
        # Ordenar por 'order' para garantir correspondência com state.videos[]
        sorted_results = sorted(results_list, key=lambda r: r.get('order', 0))

        # Helper: extrair URL normalizada e URL original de um result com chaves dinâmicas
        def _extract_urls(r):
            """Extrai output_url e input_url de chaves dinâmicas (url_01, input_url_01, etc.)"""
            norm_url = None
            orig_url = None
            for key, val in r.items():
                if key.startswith('url_') and not key.startswith('input_url_') and isinstance(val, str):
                    norm_url = val
                elif key.startswith('input_url_') and isinstance(val, str):
                    orig_url = val
            # Fallback: chaves fixas (caso v-services mude o formato)
            if not norm_url:
                norm_url = r.get('output_url', '') or r.get('url', '')
            if not orig_url:
                orig_url = r.get('original_url', '') or r.get('input_url', '')
            return norm_url, orig_url

        updated_videos = []
        first_normalized_url = None
        first_stats = None

        for idx, video in enumerate(videos):
            v = dict(video)  # Cópia para não mutar

            if idx < len(sorted_results):
                r = sorted_results[idx]
                normalized_url, _ = _extract_urls(r)
                v['normalized_url'] = normalized_url
                v['normalization_stats'] = r.get('volume_analysis', {})

                if first_normalized_url is None and normalized_url:
                    first_normalized_url = normalized_url
                    first_stats = r.get('volume_analysis', {})

                if normalized_url:
                    logger.info(
                        f"✅ [NORMALIZE] Vídeo {idx + 1}/{len(videos)}: "
                        f"{normalized_url[:80]}..."
                    )
                else:
                    logger.warning(f"⚠️ [NORMALIZE] Vídeo {idx + 1}: sem URL normalizada")
                    logger.warning(f"   Keys no resultado: {list(r.keys())}")
            else:
                logger.warning(
                    f"⚠️ [NORMALIZE] Vídeo {idx + 1}: sem resultado (results={len(sorted_results)})"
                )

            updated_videos.append(v)

        logger.info(
            f"✅ [NORMALIZE] {len(sorted_results)} vídeo(s) normalizado(s) | "
            f"first_url={'OK' if first_normalized_url else 'MISSING'}"
        )

        return state.with_updates(
            videos=updated_videos,
            normalized_video_url=first_normalized_url,
            base_normalized_url=first_normalized_url,
            normalization_stats=first_stats or {},
            original_video_url=urls[0] if len(urls) == 1 else state.original_video_url,
        )
    else:
        # Fallback: resposta sem results[] (formato antigo ou single-file)
        output_url = result.get('output_url') or result.get('url')
        stats = result.get('stats') or result.get('analysis', {})

        # Enriquecer o primeiro vídeo
        if videos:
            updated_videos = [dict(v) for v in videos]
            if output_url:
                updated_videos[0]['normalized_url'] = output_url
                updated_videos[0]['normalization_stats'] = stats
        else:
            updated_videos = state.videos

        if output_url:
            logger.info(f"✅ [NORMALIZE] URL: {output_url[:80]}...")
        else:
            logger.warning("⚠️ [NORMALIZE] Sem URL no resultado")
            logger.warning(f"   Keys na resposta: {list(result.keys())}")

        return state.with_updates(
            videos=updated_videos,
            normalized_video_url=output_url,
            base_normalized_url=output_url,
            normalization_stats=stats,
            original_video_url=urls[0] if len(urls) == 1 else state.original_video_url,
        )
