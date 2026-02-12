"""
Modelos de dados do Pipeline Engine.

PipelineState: estado centralizado que flui entre steps.
StepResult: resultado de execução de um step (para LLM Director inspecionar).
"""

import logging
from copy import deepcopy
from dataclasses import dataclass, field, fields, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineState:
    """
    Estado centralizado e imutável do pipeline.
    
    Cada step recebe uma instância e retorna uma nova (via with_updates).
    O StateManager persiste no PostgreSQL após cada step.
    """

    # ─── Identificação ───
    job_id: str = ""
    project_id: str = ""
    user_id: str = ""
    conversation_id: Optional[str] = None
    template_id: Optional[str] = None

    # ─── Configuração (imutável após criação) ───
    videos: List[Dict] = field(default_factory=list)
    options: Dict = field(default_factory=dict)
    webhook_url: Optional[str] = None

    # ─── Template (carregado uma vez pelo step load_template) ───
    template_config: Optional[Dict] = None
    text_styles: Optional[Dict] = None
    enabled_types: List[str] = field(default_factory=lambda: ['default'])
    video_width: int = 1080
    video_height: int = 1920

    # ─── 🆕 v4.5.0: Resolução do upload e aspect ratio do projeto ───
    # upload_width/height: resolução real do vídeo principal (detectada no upload)
    # target_aspect_ratio: "9:16" | "16:9" | "1:1" (escolha do usuário ou auto-detect)
    # Regra: video_width/video_height = f(upload_resolution, target_aspect_ratio)
    upload_width: int = 0
    upload_height: int = 0
    target_aspect_ratio: str = ""  # "9:16", "16:9", "1:1", ou "" (auto)

    # ─── URLs de Vídeo ───
    original_video_url: Optional[str] = None
    normalized_video_url: Optional[str] = None
    concatenated_video_url: Optional[str] = None
    phase1_video_url: Optional[str] = None
    phase1_audio_url: Optional[str] = None
    phase1_video_concatenated_url: Optional[str] = None
    phase2_video_url: Optional[str] = None
    output_video_url: Optional[str] = None
    matted_video_url: Optional[str] = None
    base_normalized_url: Optional[str] = None
    matting_artifacts_url: Optional[str] = None

    # ─── Resultados de Processamento ───
    normalization_stats: Optional[Dict] = None
    silence_detection: Optional[Dict] = None
    cut_timestamps: Optional[List[Dict]] = None
    speech_segments: Optional[List[Dict]] = None
    untranscribed_segments: Optional[Dict] = None
    transcription_text: Optional[str] = None
    transcription_words: Optional[List[Dict]] = None
    phrase_groups: Optional[List[Dict]] = None
    png_results: Optional[Dict] = None
    shadow_results: Optional[Dict] = None
    animation_results: Optional[List[Dict]] = None
    positioning_results: Optional[Dict] = None
    background_results: Optional[Dict] = None
    motion_graphics_plan: Optional[List[Dict]] = None
    motion_graphics_rendered: Optional[List[Dict]] = None
    matting_segments: Optional[List[Dict]] = None
    foreground_segments: Optional[List[Dict]] = None
    matting_config_hash: Optional[str] = None
    cartela_results: Optional[Dict] = None
    subtitle_payload: Optional[Dict] = None
    tectonic_plates: Optional[Dict] = None

    # ─── Visual Analysis (do Visual Director - Level 1) ───
    visual_analysis: Optional[Dict] = None
    shot_list: Optional[List[Dict]] = None
    edit_decision_list: Optional[List[Dict]] = None
    content_type_detected: Optional[str] = None

    # ─── Video Clipper (b-roll overlay track) ───
    video_clipper_track: Optional[List[Dict]] = None

    # ─── Title Director (título overlay track) ───
    title_track: Optional[List[Dict]] = None
    title_overrides: Optional[Dict] = None  # Overrides para replay (texto, cor, fonte, posição)

    # ─── 🆕 Text Video STM ───
    storytelling_mode: str = "talking_head"   # "talking_head" | "text_video"
    clean_text: Optional[str] = None          # Texto limpo (sem tags visuais), produzido pelo format_script
    scene_overrides: Optional[List[Dict]] = None  # Override visual por cena [{cartela:{}, background:{}}]

    # ─── Metadata ───
    total_duration_ms: Optional[int] = None
    phase1_source: Optional[str] = None
    phase1_metadata: Optional[Dict] = None
    error_message: Optional[str] = None

    # ─── Tracking ───
    completed_steps: List[str] = field(default_factory=list)
    skipped_steps: List[str] = field(default_factory=list)
    failed_step: Optional[str] = None
    step_timings: Dict[str, Dict] = field(default_factory=dict)
    # formato: {"step_name": {"started_at": "ISO", "duration_ms": 123, "attempt": 1}}

    # ─── Versão e compatibilidade ───
    engine_version: str = "3.0.0"
    created_at: Optional[str] = None

    def with_updates(self, **kwargs) -> 'PipelineState':
        """Retorna nova instância com campos atualizados (imutabilidade)."""
        data = self.to_dict()
        data.update(kwargs)
        return PipelineState.from_dict(data)

    def to_dict(self) -> Dict:
        """Serializa para JSON (para persistência e debug)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'PipelineState':
        """Deserializa de JSON. Ignora campos desconhecidos (forward-compat)."""
        valid_fields = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)

    @classmethod
    def from_job(cls, job) -> 'PipelineState':
        """
        Cria PipelineState a partir de um VideoJob existente.
        Usado na transição do sistema legado para o engine novo.
        """
        # Inferir completed_steps dos steps do job
        completed = []
        if hasattr(job, 'steps') and job.steps:
            for s in job.steps:
                status = s.status if hasattr(s, 'status') else s.get('status', '')
                name = s.name if hasattr(s, 'name') else s.get('name', '')
                status_val = status.value if hasattr(status, 'value') else str(status)
                if status_val == 'completed' and name:
                    completed.append(name)

        return cls(
            job_id=job.job_id or "",
            project_id=job.project_id or "",
            user_id=job.user_id or "",
            conversation_id=getattr(job, 'conversation_id', None),
            template_id=getattr(job, 'template_id', None),
            videos=job.videos or [],
            options=job.options or {},
            webhook_url=getattr(job, 'webhook_url', None),
            original_video_url=getattr(job, 'original_video_url', None),
            normalized_video_url=getattr(job, 'base_normalized_url', None),
            phase1_video_url=getattr(job, 'phase1_video_url', None),
            phase1_audio_url=getattr(job, 'phase1_audio_url', None),
            phase1_video_concatenated_url=getattr(job, 'phase1_video_concatenated_url', None),
            phase2_video_url=getattr(job, 'phase2_video_url', None),
            output_video_url=getattr(job, 'output_video_url', None),
            matted_video_url=getattr(job, 'matted_video_url', None),
            base_normalized_url=getattr(job, 'base_normalized_url', None),
            matting_artifacts_url=getattr(job, 'matting_artifacts_url', None),
            normalization_stats=getattr(job, 'normalization_stats', None),
            cut_timestamps=getattr(job, 'cut_timestamps', None),
            speech_segments=getattr(job, 'speech_segments', None),
            untranscribed_segments=getattr(job, 'untranscribed_segments', None),
            transcription_text=getattr(job, 'transcription_text', None),
            transcription_words=getattr(job, 'transcription_words', None),
            phrase_groups=getattr(job, 'phrase_groups', None),
            png_results=getattr(job, 'png_results', None),
            shadow_results=getattr(job, 'shadow_results', None),
            matting_segments=getattr(job, 'matting_segments', None),
            foreground_segments=getattr(job, 'foreground_segments', None),
            matting_config_hash=getattr(job, 'matting_config_hash', None),
            total_duration_ms=getattr(job, 'total_duration_ms', None),
            phase1_source=getattr(job, 'phase1_source', None),
            phase1_metadata=getattr(job, 'phase1_metadata', None),
            error_message=getattr(job, 'error_message', None),
            completed_steps=completed,
            created_at=getattr(job, 'created_at', None),
        )

    def get_video_url_for_processing(self) -> Optional[str]:
        """Retorna a melhor URL de vídeo disponível para o step atual."""
        return (
            self.phase1_video_url
            or self.concatenated_video_url
            or self.normalized_video_url
            or self.original_video_url
            or (self.videos[0].get('url') if self.videos else None)
        )

    def get_audio_url_for_transcription(self) -> Optional[str]:
        """Retorna a melhor URL de áudio para transcrição."""
        return (
            self.phase1_audio_url
            or self.phase1_video_concatenated_url
            or self.get_video_url_for_processing()
        )

    def summary(self) -> Dict:
        """Resumo compacto do state (para logs e LLM Director)."""
        return {
            'job_id': self.job_id,
            'template_id': self.template_id,
            'storytelling_mode': self.storytelling_mode,
            'completed_steps': self.completed_steps,
            'failed_step': self.failed_step,
            'phrase_count': len(self.phrase_groups) if self.phrase_groups else 0,
            'has_template': self.template_config is not None,
            'has_pngs': self.png_results is not None,
            'has_transcription': self.transcription_text is not None,
            'has_visual_analysis': self.visual_analysis is not None,
            'has_video_clipper_track': self.video_clipper_track is not None,
            'video_clipper_brolls': len(self.video_clipper_track) if self.video_clipper_track else 0,
            'has_title_track': self.title_track is not None,
            'has_title_overrides': self.title_overrides is not None,
            'has_scene_overrides': self.scene_overrides is not None,
            'scene_count': len(self.scene_overrides) if self.scene_overrides else 0,
            'content_type': self.content_type_detected,
            'shot_count': len(self.shot_list) if self.shot_list else 0,
            'duration_ms': self.total_duration_ms,
            'video_dimensions': f"{self.video_width}x{self.video_height}",
            'phase1_source': self.phase1_source,
        }


@dataclass
class StepResult:
    """
    Resultado da execução de um step.
    Retornado por PipelineEngine.run_step() para o LLM Director inspecionar.
    """
    step_name: str
    success: bool
    duration_ms: int = 0
    error: Optional[str] = None
    state_summary: Optional[Dict] = None
    metadata: Optional[Dict] = None

    def to_dict(self) -> Dict:
        return {
            'step_name': self.step_name,
            'success': self.success,
            'duration_ms': self.duration_ms,
            'error': self.error,
            'state_summary': self.state_summary,
            'metadata': self.metadata,
        }
