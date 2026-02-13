"""
Auto Runner - Modo automático (sem LLM, zero tokens).

Equivalente ao comportamento atual do orchestrator.
Executa listas pré-definidas de steps para cada modo de disparo.

v4.0: Pipeline multi-arquivo, STM-aware.
  - s01b (apply_retake_cuts) adicionado após normalize
  - s02 (concat) mantido mas faz skip automático para talking_head
"""

import logging
from typing import Dict, List, Optional

from .models import PipelineState
from .pipeline_engine import PipelineEngine

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Presets de Steps
# ═══════════════════════════════════════════════════════════════

# Step 0: Setup (sempre roda primeiro)
SETUP_STEPS = ['load_template']

# Fase 1: Pré-processamento (transcrição, fraseamento, classificação)
PHASE_1_STEPS = [
    'load_template',
    'normalize',
    'apply_retake_cuts',     # 4.0: corta retakes intra-arquivo (condicional)
    'concat',                # v4.0: skip automático para talking_head
    'analyze',
    'detect_silence',
    'silence_cut',
    'concat_plates',         # 🆕 v4.1: concatena placas tectônicas na ordem narrativa
    'transcribe',
    'merge_transcriptions',
    'fraseamento',
    'classify',
]

# Fase 2: Renderização (PNGs, shadows, animações, render)
PHASE_2_STEPS = [
    'load_template',       # Recarregar template (pode ter mudado)
    'classify',            # Reclassificar se necessário
    'video_clipper',       # 🆕 v4.4: async, fires ASAP e roda em paralelo com rendering
    'generate_pngs',
    'add_shadows',
    'apply_animations',
    'calculate_positions',
    'generate_backgrounds',
    'motion_graphics',
    'matting',
    'cartelas',
    'subtitle_pipeline',
    'title_generation',    # 🆕 v4.6: Gera PNG do título (Title Director)
    'render',              # await_async=["video_clipper"] — espera resultado antes de enviar
]

# Pipeline completo (Fase 1 + Fase 2)
ALL_STEPS = [
    'load_template',
    'normalize',
    'apply_retake_cuts',     # v4.0
    'concat',                # v4.0: skip automático para talking_head
    'analyze',
    'detect_silence',
    'silence_cut',
    'concat_plates',         # 🆕 v4.1: concatena placas tectônicas
    'transcribe',
    'video_clipper',         # 🆕 v4.4: async, fires após transcribe
    'merge_transcriptions',
    'fraseamento',
    'classify',
    'generate_pngs',
    'add_shadows',
    'apply_animations',
    'calculate_positions',
    'generate_backgrounds',
    'motion_graphics',
    'matting',
    'cartelas',
    'subtitle_pipeline',
    'title_generation',      # 🆕 v4.6: Gera PNG do título (Title Director)
    'render',                # await_async=["video_clipper"]
]

# Pipeline completo COM análise visual (feature premium / LLM Director)
ALL_STEPS_WITH_VISUAL = [
    'load_template',
    'normalize',
    'apply_retake_cuts',     # v4.0
    'concat',                # v4.0: skip automático para talking_head
    'analyze',
    'detect_silence',
    'silence_cut',
    'concat_plates',         # 🆕 v4.1: concatena placas tectônicas
    'visual_analysis',      # Análise visual (após normalize, paralelo com transcribe)
    'transcribe',
    'video_clipper',         # 🆕 v4.4: async, fires após transcribe
    'merge_transcriptions',
    'fraseamento',
    'classify',
    'generate_pngs',
    'add_shadows',
    'apply_animations',
    'calculate_positions',
    'generate_backgrounds',
    'motion_graphics',
    'matting',
    'cartelas',
    'subtitle_pipeline',
    'title_generation',      # 🆕 v4.6: Gera PNG do título (Title Director)
    'render',                # await_async=["video_clipper"]
]


# 🆕 STM Text Video: Pipeline sem vídeo (texto → timestamps virtuais → render)
TEXT_VIDEO_STEPS = [
    'load_template',           # 1. Carrega template visual (base/fallback)
    'format_script',           # 2. ★ Parseia tags visuais → clean_text + scene_overrides
    'generate_timestamps',     # 3. ★ clean_text → transcription_words (virtual)
    'fraseamento',             # 4. Agrupa palavras em frases
    'classify',                # 5. Classifica style_type (STM-aware: sem cartela/matting)
    'generate_pngs',           # 6. Gera PNGs dos textos
    'add_shadows',             # 7. Sombras (controlado pelo template)
    'apply_animations',        # 8. Animações de entrada/saída
    'calculate_positions',     # 9. Posicionamento na tela
    'generate_backgrounds',    # 10. Background por cena (usa scene_overrides)
    'cartelas',                # 11. Cartelas (usa scene_overrides, fallback template)
    'subtitle_pipeline',       # 12. Payload para v-editor
    'title_generation',        # 13. Título do vídeo
    'render',                  # 14. Renderização final
]


# 🆕 STM Motion Graphics: Pipeline sem vídeo (roteiro → LLM visual layout → PNGs → vídeo)
MOTION_GRAPHICS_STEPS = [
    'load_template',            # 1. Carrega template (palette, fonts, mood)
    'format_script',            # 2. Parseia tags visuais → clean_text + scene_overrides
    'generate_timestamps',      # 3. clean_text → transcription_words (virtual TTS)
    'fraseamento',              # 4. Agrupa palavras em frases (para timing)
    'generate_visual_layout',   # 5. ★ LLM gera HTML/CSS → Playwright → PNGs em camadas
    'subtitle_pipeline',        # 6. Payload para v-editor
    'title_generation',         # 7. Título do vídeo
    'render',                   # 8. Renderização final (v-editor-python composita)
]


class AutoRunner:
    """
    Executa pipeline com lista fixa de steps (sem LLM).
    
    Suporta os 3 modos de disparo:
    1. run_full(): Pipeline completo (Fase 1 + Fase 2)
    2. run_phase1_only(): Só Fase 1 → AWAITING_REVIEW
    3. run_phase2(): Fase 2 (após revisão do usuário)
    """

    def __init__(self, engine: PipelineEngine):
        self.engine = engine

    def run_full(self, job_id: str,
                 state: PipelineState = None,
                 include_visual: bool = False) -> PipelineState:
        """
        Pipeline completo (Fase 1 + Fase 2).
        
        Args:
            job_id: ID do job
            state: Estado inicial
            include_visual: Se True, inclui análise visual (feature premium)
        
        Equivalente ao antigo _execute_pipeline() sem phase_1_only.
        """
        steps = ALL_STEPS_WITH_VISUAL if include_visual else ALL_STEPS
        label = "COMPLETO+VISUAL" if include_visual else "COMPLETO"
        logger.info(f"🎬 [AUTO] Pipeline {label} para {job_id[:8]}...")
        return self.engine.run(job_id, steps, initial_state=state)

    def run_phase1_only(self, job_id: str,
                        state: PipelineState = None) -> PipelineState:
        """
        Só Fase 1 (para depois o usuário revisar).
        
        Para após 'classify', job fica em AWAITING_REVIEW.
        Equivalente ao antigo _execute_pipeline() com phase_1_only=True.
        """
        logger.info(f"🎬 [AUTO] Pipeline FASE 1 para {job_id[:8]}...")
        return self.engine.run(
            job_id, PHASE_1_STEPS,
            initial_state=state,
            stop_after='classify'
        )

    def run_phase2(self, job_id: str,
                   steps: List[str] = None,
                   state: PipelineState = None) -> PipelineState:
        """
        Fase 2 (após revisão do usuário).
        
        Aceita lista customizada de steps (o endpoint /continue pode
        enviar steps específicos).
        
        Equivalente ao antigo continue_pipeline().
        """
        steps_to_run = steps or PHASE_2_STEPS
        logger.info(f"🎬 [AUTO] Pipeline FASE 2 para {job_id[:8]}...")
        logger.info(f"   Steps: {steps_to_run}")
        return self.engine.run(job_id, steps_to_run, initial_state=state)

    def run_text_video(self, job_id: str,
                       state: PipelineState = None) -> PipelineState:
        """
        Pipeline Text Video (sem vídeo de entrada).
        
        Usa TEXT_VIDEO_STEPS: format_script → generate_timestamps → fraseamento
        → classify (style only) → render.
        """
        logger.info(f"🎬 [AUTO] Pipeline TEXT_VIDEO para {job_id[:8]}...")
        # Garantir que storytelling_mode está setado no state
        if state and state.storytelling_mode != "text_video":
            state = state.with_updates(storytelling_mode="text_video")
        return self.engine.run(job_id, TEXT_VIDEO_STEPS, initial_state=state)

    def run_motion_graphics(self, job_id: str,
                            state: PipelineState = None) -> PipelineState:
        """
        Pipeline Motion Graphics (roteiro → LLM visual layout → PNGs → vídeo).

        Usa MOTION_GRAPHICS_STEPS: format_script → generate_timestamps → fraseamento
        → generate_visual_layout (LLM + Playwright) → subtitle_pipeline → render.
        """
        logger.info(f"🎨 [AUTO] Pipeline MOTION_GRAPHICS para {job_id[:8]}...")
        if state and state.storytelling_mode != "motion_graphics":
            state = state.with_updates(storytelling_mode="motion_graphics")
        return self.engine.run(job_id, MOTION_GRAPHICS_STEPS, initial_state=state)

    def run_custom(self, job_id: str,
                   steps: List[str],
                   state: PipelineState = None,
                   stop_after: str = None) -> PipelineState:
        """
        Executa lista customizada de steps.
        
        Usado quando o caller precisa de controle total
        (ex: re-render parcial, debug de um step específico).
        """
        logger.info(f"🎬 [AUTO] Pipeline CUSTOM para {job_id[:8]}...")
        logger.info(f"   Steps: {steps}")
        return self.engine.run(
            job_id, steps,
            initial_state=state,
            stop_after=stop_after
        )
