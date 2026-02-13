"""
Render Pod Client — chama v-render-pod no Modal via SDK (.remote())
====================================================================
v1.0.0 (Fev 2026):
- Sem web endpoints (zero quota consumida)
- Sem timeout HTTP (execucao pode durar horas)
- Chamadas assincronas via .spawn() para fire-and-forget
- Chamadas sincronas via .remote() para aguardar resultado

Requisitos:
- modal>=1.2.0 no requirements
- MODAL_TOKEN_ID e MODAL_TOKEN_SECRET no .env (ou ~/.modal.toml configurado)
"""

import os
import time
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Nome do app e funcoes no Modal
MODAL_APP_NAME = "v-render-pod"
MODAL_ENV = os.environ.get("MODAL_ENVIRONMENT", "main")


def _get_function(func_name: str):
    """
    Obtem referencia a uma funcao do v-render-pod no Modal.
    Usa modal.Function.from_name() para lookup pelo nome.
    """
    import modal
    return modal.Function.from_name(MODAL_APP_NAME, func_name, environment_name=MODAL_ENV)


class RenderPodClient:
    """
    Cliente para o v-render-pod no Modal.
    Todas as chamadas sao feitas via SDK (sem HTTP, sem web endpoint).
    """

    def __init__(self):
        self.enabled = os.environ.get("RENDER_POD_ENABLED", "false").lower() == "true"
        logger.info(f"[RenderPod] Client inicializado (enabled={self.enabled})")

    def is_enabled(self) -> bool:
        return self.enabled

    # ──────────────────────────────────────────────
    # PREP (Phase 1) — FFmpeg
    # ──────────────────────────────────────────────
    def prep(
        self,
        job_id: str,
        tasks: List[Dict[str, Any]],
        user_id: str = None,
        project_path: str = None,
    ) -> Dict[str, Any]:
        """
        Chama prep() no Modal de forma SINCRONA (aguarda resultado).
        Retorna dict com status, resultados e metricas.
        """
        start = time.time()
        logger.info(f"[RenderPod] prep.remote() job={job_id}, tasks={len(tasks)}")

        try:
            fn = _get_function("prep")
            result = fn.remote(
                job_id=job_id,
                tasks=tasks,
                user_id=user_id,
                project_path=project_path,
            )
            elapsed = time.time() - start
            logger.info(f"[RenderPod] prep concluido em {elapsed:.1f}s")
            return result

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"[RenderPod] prep ERRO apos {elapsed:.1f}s: {e}")
            return {"status": "error", "job_id": job_id, "error": str(e)}

    def prep_async(
        self,
        job_id: str,
        tasks: List[Dict[str, Any]],
        user_id: str = None,
        project_path: str = None,
    ):
        """
        Chama prep() no Modal de forma ASSINCRONA (fire-and-forget).
        Retorna um FunctionCall que pode ser polled depois.
        """
        logger.info(f"[RenderPod] prep.spawn() job={job_id}")
        fn = _get_function("prep")
        return fn.spawn(
            job_id=job_id,
            tasks=tasks,
            user_id=user_id,
            project_path=project_path,
        )

    # ──────────────────────────────────────────────
    # RENDER (Phase 2) — Playwright + MoviePy
    # ──────────────────────────────────────────────
    def render(
        self,
        job_id: str,
        scenes: List[Dict[str, Any]],
        project_settings: Dict[str, Any] = None,
        user_id: str = None,
        project_path: str = None,
        upload_to_b2: bool = True,
    ) -> Dict[str, Any]:
        """
        Chama render() no Modal de forma SINCRONA.
        """
        start = time.time()
        logger.info(f"[RenderPod] render.remote() job={job_id}, scenes={len(scenes)}")

        try:
            fn = _get_function("render")
            result = fn.remote(
                job_id=job_id,
                scenes=scenes,
                project_settings=project_settings,
                user_id=user_id,
                project_path=project_path,
                upload_to_b2=upload_to_b2,
            )
            elapsed = time.time() - start
            logger.info(f"[RenderPod] render concluido em {elapsed:.1f}s")
            return result

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"[RenderPod] render ERRO apos {elapsed:.1f}s: {e}")
            return {"status": "error", "job_id": job_id, "error": str(e)}

    def render_async(
        self,
        job_id: str,
        scenes: List[Dict[str, Any]],
        project_settings: Dict[str, Any] = None,
        user_id: str = None,
        project_path: str = None,
        upload_to_b2: bool = True,
    ):
        """
        Chama render() no Modal de forma ASSINCRONA.
        """
        logger.info(f"[RenderPod] render.spawn() job={job_id}")
        fn = _get_function("render")
        return fn.spawn(
            job_id=job_id,
            scenes=scenes,
            project_settings=project_settings,
            user_id=user_id,
            project_path=project_path,
            upload_to_b2=upload_to_b2,
        )

    # ──────────────────────────────────────────────
    # FULL PIPELINE (auto mode)
    # ──────────────────────────────────────────────
    def full_pipeline(
        self,
        job_id: str,
        prep_tasks: List[Dict[str, Any]],
        scenes: List[Dict[str, Any]],
        project_settings: Dict[str, Any] = None,
        user_id: str = None,
        project_path: str = None,
        upload_to_b2: bool = True,
    ) -> Dict[str, Any]:
        """
        Chama full_pipeline() no Modal de forma SINCRONA.
        Executa Phase 1 + Phase 2 sem pausa.
        """
        start = time.time()
        logger.info(f"[RenderPod] full_pipeline.remote() job={job_id}")

        try:
            fn = _get_function("full_pipeline")
            result = fn.remote(
                job_id=job_id,
                prep_tasks=prep_tasks,
                scenes=scenes,
                project_settings=project_settings,
                user_id=user_id,
                project_path=project_path,
                upload_to_b2=upload_to_b2,
            )
            elapsed = time.time() - start
            logger.info(f"[RenderPod] full_pipeline concluido em {elapsed:.1f}s")
            return result

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"[RenderPod] full_pipeline ERRO apos {elapsed:.1f}s: {e}")
            return {"status": "error", "job_id": job_id, "error": str(e)}

    def full_pipeline_async(
        self,
        job_id: str,
        prep_tasks: List[Dict[str, Any]],
        scenes: List[Dict[str, Any]],
        project_settings: Dict[str, Any] = None,
        user_id: str = None,
        project_path: str = None,
        upload_to_b2: bool = True,
    ):
        """
        Chama full_pipeline() no Modal de forma ASSINCRONA.
        """
        logger.info(f"[RenderPod] full_pipeline.spawn() job={job_id}")
        fn = _get_function("full_pipeline")
        return fn.spawn(
            job_id=job_id,
            prep_tasks=prep_tasks,
            scenes=scenes,
            project_settings=project_settings,
            user_id=user_id,
            project_path=project_path,
            upload_to_b2=upload_to_b2,
        )


# ──────────────────────────────────────────────
# SINGLETON
# ──────────────────────────────────────────────
_client: Optional[RenderPodClient] = None


def get_render_pod_client() -> RenderPodClient:
    """Retorna instancia singleton do RenderPodClient."""
    global _client
    if _client is None:
        _client = RenderPodClient()
    return _client
