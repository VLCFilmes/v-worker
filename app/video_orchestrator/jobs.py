"""
🎬 Job Manager - Gerenciamento de Jobs de Processamento de Vídeo

Responsabilidades:
- Criar e rastrear jobs de processamento
- Atualizar status de cada step
- Persistir no PostgreSQL
- Suporte a Redis/RQ para filas (opcional)
- 🆕 v2.9.250: Emitir eventos SSE para PipelineVisualizer
"""

import uuid
import json
import logging
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict

logger = logging.getLogger(__name__)

# 🆕 v2.9.250: Import opcional do SSE (pode não estar disponível em todos os contextos)
try:
    from app.routes.sse_stream import emit_job_event
    SSE_AVAILABLE = True
except ImportError:
    SSE_AVAILABLE = False
    logger.warning("⚠️ SSE não disponível - eventos não serão emitidos")


class JobStatus(str, Enum):
    """Status possíveis de um job"""
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    AWAITING_REVIEW = "awaiting_review"  # 🆕 Pipeline 2 Fases - aguardando revisão do usuário
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    """Status possíveis de um step"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class Phase1Source(str, Enum):
    """
    🆕 v2.9.182: Origem do vídeo da Fase 1
    
    Identifica como o vídeo base foi gerado:
    - NORMALIZED: 1 upload, sem silence cut → apenas normalizado
    - CONCATENATED: N uploads, sem silence cut → concatenados
    - TECTONIC: Com silence cut → placas tectônicas (speech_segments)
    """
    NORMALIZED = "normalized"       # 1 vídeo, sem silence cut
    CONCATENATED = "concatenated"   # N vídeos, sem silence cut  
    TECTONIC = "tectonic"           # Com silence cut (gera placas)


@dataclass
class ProcessingStep:
    """Representa um step do processamento"""
    name: str
    status: StepStatus = StepStatus.PENDING
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_ms: Optional[int] = None
    result: Optional[Dict] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "result": self.result,
            "error": self.error
        }


@dataclass
class VideoJob:
    """Representa um job de processamento de vídeo"""
    job_id: str
    conversation_id: str
    project_id: str
    user_id: str
    status: JobStatus = JobStatus.PENDING
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    
    # Configuração do job
    videos: List[Dict] = field(default_factory=list)
    options: Dict = field(default_factory=dict)
    webhook_url: Optional[str] = None
    
    # Steps do processamento
    steps: List[ProcessingStep] = field(default_factory=list)
    current_step: int = 0
    
    # Template usado no processamento
    template_id: Optional[str] = None
    
    # Resultados
    output_video_url: Optional[str] = None            # URL final (compatibilidade)
    phase1_video_url: Optional[str] = None            # 🆕 URL do vídeo cortado (Fase 1)
    phase2_video_url: Optional[str] = None            # 🆕 URL do vídeo renderizado (Fase 2)
    transcription_text: Optional[str] = None
    transcription_words: Optional[List[Dict]] = None  # Word-level timestamps
    phrase_groups: Optional[List[Dict]] = None        # Frases agrupadas
    png_results: Optional[Dict] = None                # Resultados da geração de PNGs
    shadow_results: Optional[Dict] = None             # Resultados das sombras
    matted_video_url: Optional[str] = None            # 🆕 URL do vídeo com matting (foreground)
    matting_segments: Optional[List[Dict]] = None     # 🆕 Segmentos de tempo para matting
    foreground_segments: Optional[List[Dict]] = None  # 🆕 v2.1.0: Array de foregrounds separados (Opção B - sem concat)
    base_normalized_url: Optional[str] = None         # 🆕 URL do vídeo base normalizado (do v-matting)
    normalization_stats: Optional[Dict] = None        # 🆕 Estatísticas de normalização (FPS, resolução)
    cut_timestamps: Optional[List[Dict]] = None       # 🆕 v2.5.0: Pontos de corte (silence removal)
    # 🆕 v2.5.1: Trechos sem transcrição (início e fim do vídeo)
    untranscribed_segments: Optional[Dict] = None     # {"start": {duration_ms, has_cut}, "end": {duration_ms, has_cut}}
    # 🆕 v2.9.0: Hybrid silence cut - segmentos de vídeo separados
    speech_segments: Optional[List[Dict]] = None      # [{url, shared_path, original_start, original_end, audio_offset, duration}]
    phase1_audio_url: Optional[str] = None            # URL do áudio/vídeo concatenado (para transcrição)
    # 🆕 v2.9.47: Vídeo 360p concatenado (duração exata para transcrição)
    phase1_video_concatenated_url: Optional[str] = None  # Vídeo de baixa res. para transcrição
    # 🆕 v2.9.2: URL do vídeo original (antes do corte) para player seek-based
    original_video_url: Optional[str] = None          # Player usa este vídeo com seek entre timestamps
    total_duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    
    # 🆕 v2.9.182: Rastreabilidade da Fase 1
    phase1_source: Optional[str] = None               # "normalized", "concatenated", "tectonic"
    phase1_metadata: Optional[Dict] = None            # {input_videos, silence_cut_enabled, duration_ms, ...}
    
    # 🆕 v2.10.6: Cache de Matting (invalidação inteligente)
    matting_artifacts_url: Optional[str] = None       # URL do cache de matting (foreground_segments)
    matting_config_hash: Optional[str] = None         # Hash MD5 da configuração de person_overlay
    
    def to_dict(self) -> Dict:
        return {
            "job_id": self.job_id,
            "conversation_id": self.conversation_id,
            "project_id": self.project_id,
            "user_id": self.user_id,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "videos": self.videos,
            "options": self.options,
            "webhook_url": self.webhook_url,
            "steps": [s.to_dict() for s in self.steps],
            "current_step": self.current_step,
            "output_video_url": self.output_video_url,
            "phase1_video_url": self.phase1_video_url,    # 🆕 URL Fase 1
            "phase2_video_url": self.phase2_video_url,    # 🆕 URL Fase 2
            "transcription_text": self.transcription_text,
            "transcription_words": self.transcription_words,
            "phrase_groups": self.phrase_groups,
            "png_results": self.png_results,
            "shadow_results": self.shadow_results,
            "matted_video_url": self.matted_video_url,           # 🆕 Matting
            "matting_segments": self.matting_segments,           # 🆕 Matting
            "foreground_segments": self.foreground_segments,     # 🆕 v2.1.0: Foregrounds separados
            "base_normalized_url": self.base_normalized_url,     # 🆕 Normalização
            "normalization_stats": self.normalization_stats,     # 🆕 Normalização
            "cut_timestamps": self.cut_timestamps,               # 🆕 v2.5.0: Pontos de corte
            "untranscribed_segments": self.untranscribed_segments,  # 🆕 v2.5.1: Trechos sem transcrição
            "speech_segments": self.speech_segments,              # 🆕 v2.9.0: Segmentos de vídeo separados
            "phase1_audio_url": self.phase1_audio_url,            # 🆕 v2.9.0: Áudio/vídeo concatenado (transcrição)
            "phase1_video_concatenated_url": self.phase1_video_concatenated_url,  # 🆕 v2.9.47: Vídeo 360p
            "original_video_url": self.original_video_url,        # 🆕 v2.9.2: Vídeo original (sem cortes)
            "total_duration_ms": self.total_duration_ms,
            "error_message": self.error_message,
            "phase1_source": self.phase1_source,                    # 🆕 v2.9.182: Origem (normalized/concatenated/tectonic)
            "phase1_metadata": self.phase1_metadata,                # 🆕 v2.9.182: Metadados da Fase 1
            "matting_artifacts_url": self.matting_artifacts_url,    # 🆕 v2.10.6: Cache de matting
            "matting_config_hash": self.matting_config_hash,        # 🆕 v2.10.6: Hash da configuração de person_overlay
            "progress_percent": self._calculate_progress()
        }
    
    def _calculate_progress(self) -> int:
        """Calcula percentual de progresso baseado nos steps"""
        if not self.steps:
            return 0
        
        completed = sum(1 for s in self.steps if s.status in [StepStatus.COMPLETED, StepStatus.SKIPPED])
        return int((completed / len(self.steps)) * 100)


class JobManager:
    """
    Gerenciador de Jobs de Processamento de Vídeo
    
    Persiste jobs no PostgreSQL e opcionalmente usa Redis para filas.
    """
    
    def __init__(self, db_connection_func=None):
        """
        Args:
            db_connection_func: Função para obter conexão com banco
        """
        self.db_connection_func = db_connection_func
        self._jobs_cache: Dict[str, VideoJob] = {}
        
        # Tentar inicializar Redis
        self._redis_available = False
        try:
            from redis import Redis
            from rq import Queue
            import os
            
            self._redis = Redis(
                host=os.environ.get('REDIS_HOST', 'localhost'),
                port=int(os.environ.get('REDIS_PORT', 6379)),
                password=os.environ.get('REDIS_PASSWORD', None),
                decode_responses=True
            )
            self._queue = Queue('video_orchestrator', connection=self._redis)
            self._redis_available = True
            logger.info("✅ Redis disponível para filas")
        except Exception as e:
            logger.warning(f"⚠️ Redis não disponível: {e}")
            self._redis = None
            self._queue = None
    
    def create_job(
        self,
        conversation_id: Optional[str],
        project_id: Optional[str],
        user_id: str,
        videos: Optional[List[Dict]] = None,
        options: Optional[Dict] = None,
        webhook_url: Optional[str] = None,
        template_id: Optional[str] = None,
        text: Optional[str] = None,
        phrases: Optional[List[Dict]] = None
    ) -> VideoJob:
        """
        Cria um novo job de processamento
        
        Args:
            conversation_id: ID da conversa (opcional para Generator V3)
            project_id: ID do projeto (opcional para Generator V3)
            user_id: ID do usuário
            videos: Lista de vídeos com url e order (opcional para text_video)
            options: Opções de processamento
            webhook_url: URL para callback
            template_id: ID do template (opcional)
            text: Texto para processar (para text_video)
            phrases: Frases já processadas (para Generator V3)
            
        Returns:
            VideoJob criado
        """
        job_id = str(uuid.uuid4())
        videos = videos or []
        options = options or {}
        
        # 🔍 DEBUG: Verificar se editor_worker_id está sendo recebido
        logger.info(f"[CREATE_JOB] 🎬 editor_worker_id: {options.get('editor_worker_id', 'NOT_SET')}")
        logger.info(f"[CREATE_JOB] 🎭 worker_override: {options.get('worker_override', 'NOT_SET')}")
        
        # Validar que temos conversation_id e project_id reais
        # O BFF do Generator V3 cria esses IDs antes de chamar o pipeline
        if not conversation_id:
            logger.warning(f"⚠️ conversation_id não fornecido - job não será persistido no banco")
        if not project_id:
            logger.warning(f"⚠️ project_id não fornecido - job não será persistido no banco")
        
        # Definir steps baseado nas opções e se tem vídeos
        steps = self._create_steps(options, len(videos), has_text=bool(text), has_phrases=bool(phrases))
        
        job = VideoJob(
            job_id=job_id,
            conversation_id=conversation_id,
            project_id=project_id,
            user_id=user_id,
            videos=videos,
            options=options,
            webhook_url=webhook_url,
            steps=steps,
            template_id=template_id
        )
        
        # Armazenar texto/frases no job se fornecidos
        if text:
            job.transcription_text = text
        if phrases:
            job.phrase_groups = phrases
        
        # Salvar no cache
        self._jobs_cache[job_id] = job
        
        # Salvar no banco
        self._persist_job(job)
        
        logger.info(f"✅ Job criado: {job_id} para projeto {project_id} (videos={len(videos)}, text={bool(text)}, phrases={bool(phrases)})")
        return job
    
    def _create_steps(
        self, 
        options: Dict, 
        video_count: int,
        has_text: bool = False,
        has_phrases: bool = False
    ) -> List[ProcessingStep]:
        """
        Cria lista de steps baseado nas opções
        
        Args:
            options: Opções de processamento
            video_count: Quantidade de vídeos
            has_text: Se foi fornecido texto (pula transcrição)
            has_phrases: Se foram fornecidas frases prontas (pula phrase_grouping)
        """
        # ═══ Motion Graphics: pipeline dedicado ═══
        storytelling_mode = options.get('storytelling_mode', '')
        if storytelling_mode == 'motion_graphics':
            mg_steps = [
                'load_template', 'format_script', 'generate_timestamps',
                'fraseamento', 'generate_visual_layout', 'subtitle_pipeline',
                'title_generation', 'render'
            ]
            logger.info(f"📋 Steps MOTION_GRAPHICS: {mg_steps}")
            return [ProcessingStep(name=s) for s in mg_steps]
        
        steps = []
        skip_video_processing = options.get('skip_video_processing', False) or video_count == 0
        phase_1_only = options.get('phase_1_only', False)  # 🆕 Pipeline 2 Fases
        
        # ═══════════════════════════════════════════════════════════════
        # FASE 1: STEPS DE VÍDEO (só se tiver vídeos)
        # ═══════════════════════════════════════════════════════════════
        if not skip_video_processing:
            # Normalização
            if options.get('normalize_audio', True):
                steps.append(ProcessingStep(name="normalize"))
            
            # Concatenação (se múltiplos vídeos)
            if video_count > 1:
                steps.append(ProcessingStep(name="concat"))
            
            # Detecção e corte de silêncio
            if options.get('silence_cut', True):
                steps.append(ProcessingStep(name="detect_silence"))
                steps.append(ProcessingStep(name="silence_cut"))
            
            # Merge de transcrições (reutiliza transcrições do upload)
            # NÃO chama AssemblyAI novamente - usa TranscriptionMergeService
            if options.get('transcribe', True) and not has_text:
                steps.append(ProcessingStep(name="merge_transcriptions"))
        
        # ═══════════════════════════════════════════════════════════════
        # FASE 1: FRASEAMENTO + CLASSIFICAÇÃO
        # 🆕 v2.5.0: Classify movido para Fase 1 para que o revisor
        #            receba as frases já classificadas pela LLM
        # ═══════════════════════════════════════════════════════════════
        
        # Fraseamento (só se não tiver frases prontas)
        if not has_phrases and options.get('phrase_grouping', True):
            steps.append(ProcessingStep(name="phrase_grouping"))
        
        # 🆕 Classificação de frases ANTES da revisão (para preencher estrelas, cartelas, etc.)
        steps.append(ProcessingStep(name="classify"))
        
        # 🆕 Se phase_1_only=true, parar aqui (sem steps da Fase 2)
        if phase_1_only:
            logger.info(f"📋 Steps FASE 1: {[s.name for s in steps]} (phase_1_only=True)")
            return steps
        
        # ═══════════════════════════════════════════════════════════════
        # FASE 2: STEPS DE RENDERIZAÇÃO (só se phase_1_only=false)
        # ═══════════════════════════════════════════════════════════════
        
        # Geração de PNGs
        steps.append(ProcessingStep(name="generate_pngs"))
        
        # 🚫 Adição de sombras - DESATIVADO (29/Jan/2026)
        # O sistema de sombras foi desativado pois:
        # 1. Não está funcionando corretamente (assets 404)
        # 2. Templates não usam sombras por padrão
        # 3. Adiciona tempo de processamento desnecessário
        # steps.append(ProcessingStep(name="add_shadows"))
        
        # Posicionamento
        steps.append(ProcessingStep(name="positioning"))
        
        # Backgrounds
        steps.append(ProcessingStep(name="generate_backgrounds"))
        
        # Animações
        steps.append(ProcessingStep(name="apply_animations"))
        
        # Render final
        steps.append(ProcessingStep(name="render"))
        
        logger.info(f"📋 Steps criados: {[s.name for s in steps]} (skip_video={skip_video_processing}, has_text={has_text}, has_phrases={has_phrases})")
        
        return steps
    
    def get_job(self, job_id: str, force_reload: bool = False) -> Optional[VideoJob]:
        """Busca job por ID
        
        Args:
            job_id: ID do job
            force_reload: Se True, ignora cache e busca do banco
            
        Returns:
            VideoJob ou None se não encontrado
        """
        # Se force_reload ou não está no cache, buscar do banco
        if force_reload or job_id not in self._jobs_cache:
            job = self._load_job_from_db(job_id)
            if job:
                self._jobs_cache[job_id] = job
            return job
        
        return self._jobs_cache[job_id]
    
    def invalidate_cache(self, job_id: str):
        """Remove job do cache para forçar reload do banco"""
        if job_id in self._jobs_cache:
            del self._jobs_cache[job_id]
            logger.debug(f"🗑️ Cache invalidado para job {job_id}")
    
    def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        error_message: Optional[str] = None
    ) -> Optional[VideoJob]:
        """Atualiza status do job"""
        job = self.get_job(job_id)
        if not job:
            return None
        
        job.status = status
        
        if status == JobStatus.PROCESSING and not job.started_at:
            job.started_at = datetime.now(timezone.utc).isoformat()
        
        if status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            job.completed_at = datetime.now(timezone.utc).isoformat()
            # 🔧 FIX: Só calcular tempo de processamento se NÃO tiver duração de vídeo definida
            # A duração do vídeo (total_duration_ms) é definida após silence_cut
            # e NÃO deve ser sobrescrita com tempo de processamento
            if job.started_at and not job.total_duration_ms:
                start = datetime.fromisoformat(job.started_at.replace('Z', '+00:00'))
                end = datetime.fromisoformat(job.completed_at.replace('Z', '+00:00'))
                # Fallback: se não tiver duração do vídeo, usar tempo de processamento
                job.total_duration_ms = int((end - start).total_seconds() * 1000)
        
        if error_message:
            job.error_message = error_message
        
        # 🆕 v2.9.250: Emitir evento SSE para mudança de status do job
        if SSE_AVAILABLE:
            try:
                if status == JobStatus.COMPLETED:
                    emit_job_event(job_id, "job_complete", {
                        "status": "completed",
                        "output_video_url": job.output_video_url,
                        "total_duration_ms": job.total_duration_ms,
                    })
                elif status == JobStatus.FAILED:
                    emit_job_event(job_id, "job_error", {
                        "status": "failed",
                        "error": error_message,
                    })
                elif status == JobStatus.PROCESSING:
                    emit_job_event(job_id, "job_start", {
                        "status": "processing",
                        "total_steps": len(job.steps),
                    })
                elif status == JobStatus.AWAITING_REVIEW:
                    emit_job_event(job_id, "job_awaiting_review", {
                        "status": "awaiting_review",
                        "message": "Fase 1 completa. Aguardando revisão.",
                    })
            except Exception as sse_error:
                logger.warning(f"⚠️ [SSE] Erro ao emitir evento de job: {sse_error}")
        
        self._persist_job(job)
        return job
    
    def add_steps(
        self,
        job_id: str,
        step_names: List[str]
    ) -> Optional[VideoJob]:
        """
        🆕 v2.5.10: Adiciona novos steps ao job (para Fase 2).
        
        Quando phase_1_only=True é usado, os steps da Fase 2 não são criados
        inicialmente. Este método permite adicioná-los quando continue_pipeline
        é chamado, garantindo que as métricas de tempo sejam capturadas.
        
        Args:
            job_id: ID do job
            step_names: Lista de nomes de steps a adicionar
            
        Returns:
            Job atualizado ou None se não encontrado
        """
        job = self.get_job(job_id)
        if not job:
            return None
        
        # Verificar quais steps já existem
        existing_names = {s.name for s in job.steps}
        
        # Adicionar apenas steps que não existem
        added = []
        for name in step_names:
            if name not in existing_names:
                job.steps.append(ProcessingStep(name=name))
                added.append(name)
        
        if added:
            logger.info(f"🆕 [ADD_STEPS] Job {job_id[:8]}...: adicionados {len(added)} steps: {added}")
            self._persist_job(job)
        
        return job
    
    def update_step(
        self,
        job_id: str,
        step_name: str,
        status: StepStatus,
        result: Optional[Dict] = None,
        error: Optional[str] = None
    ) -> Optional[VideoJob]:
        """Atualiza status de um step"""
        job = self.get_job(job_id)
        if not job:
            return None
        
        for step in job.steps:
            if step.name == step_name:
                now = datetime.now(timezone.utc).isoformat()
                
                # 🆕 v2.9.84: Sempre resetar timestamps quando step inicia
                # Isso evita durações absurdas quando job é re-executado
                if status == StepStatus.PROCESSING:
                    step.started_at = now  # Sempre atualizar para agora
                    step.completed_at = None  # Limpar completed anterior
                    step.duration_ms = None  # Limpar duration anterior
                
                step.status = status
                
                if status in [StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED]:
                    step.completed_at = now
                    if step.started_at:
                        start = datetime.fromisoformat(step.started_at.replace('Z', '+00:00'))
                        end = datetime.fromisoformat(step.completed_at.replace('Z', '+00:00'))
                        duration_ms = int((end - start).total_seconds() * 1000)
                        
                        # 🆕 v2.9.84: Validar duração - valores > 1h são provavelmente erro
                        # (um step de vídeo não deveria levar mais que 1 hora)
                        MAX_SANE_DURATION_MS = 3600000  # 1 hora
                        if duration_ms > MAX_SANE_DURATION_MS:
                            logger.warning(f"⚠️ [DURATION] Step '{step_name}' tem duração absurda: {duration_ms}ms ({duration_ms/1000:.0f}s). Definindo como None.")
                            step.duration_ms = None  # Não mostrar valor errado
                        else:
                            step.duration_ms = duration_ms
                
                if result:
                    step.result = result
                
                if error:
                    step.error = error
                
                # 🆕 v2.9.250: Emitir evento SSE
                # 🔧 v2.9.251: Converter step_name para UPPERCASE (frontend espera DETECT_SILENCE, não detect_silence)
                if SSE_AVAILABLE:
                    try:
                        event_type = self._get_sse_event_type(status)
                        step_id = step_name.upper()  # Frontend espera UPPERCASE
                        event_data = {
                            "step": step_id,
                            "step_id": step_id,  # Alias para compatibilidade
                            "status": status.value,
                            "current_step": None,  # Será atualizado abaixo
                            "total_steps": len(job.steps),
                            "duration_ms": step.duration_ms,
                        }
                        if error:
                            event_data["error"] = error
                        if result:
                            event_data["result"] = result
                        
                        emit_job_event(job_id, event_type, event_data)
                        logger.debug(f"📡 [SSE] Evento emitido: {event_type} para step {step_id}")
                    except Exception as sse_error:
                        logger.warning(f"⚠️ [SSE] Erro ao emitir evento: {sse_error}")
                
                break
        
        # Atualizar current_step
        for i, step in enumerate(job.steps):
            if step.status in [StepStatus.PENDING, StepStatus.PROCESSING]:
                job.current_step = i
                break
        else:
            job.current_step = len(job.steps)
        
        self._persist_job(job)
        return job
    
    def _get_sse_event_type(self, status: StepStatus) -> str:
        """Mapeia StepStatus para tipo de evento SSE."""
        mapping = {
            StepStatus.PROCESSING: "step_start",
            StepStatus.COMPLETED: "step_complete",
            StepStatus.FAILED: "step_error",
            StepStatus.SKIPPED: "step_skipped",
            StepStatus.PENDING: "step_pending",
        }
        return mapping.get(status, "step_update")
    
    def set_output(
        self,
        job_id: str,
        output_video_url: Optional[str] = None,
        phase1_video_url: Optional[str] = None,  # 🆕 URL Fase 1
        phase2_video_url: Optional[str] = None,  # 🆕 URL Fase 2
        transcription_text: Optional[str] = None,
        transcription_words: Optional[List[Dict]] = None,
        phrase_groups: Optional[List[Dict]] = None,
        png_results: Optional[Dict] = None,
        shadow_results: Optional[Dict] = None,
        matted_video_url: Optional[str] = None,       # 🆕 URL do vídeo matted
        matting_segments: Optional[List[Dict]] = None,  # 🆕 Segmentos de matting
        foreground_segments: Optional[List[Dict]] = None,  # 🆕 v2.1.0: Foregrounds separados (sem concat)
        base_normalized_url: Optional[str] = None,    # 🆕 URL do vídeo base normalizado
        normalization_stats: Optional[Dict] = None,   # 🆕 Estatísticas de normalização
        cut_timestamps: Optional[List[Dict]] = None,   # 🆕 v2.5.0: Pontos de corte (silence removal)
        untranscribed_segments: Optional[Dict] = None,  # 🆕 v2.5.1: Trechos sem transcrição
        speech_segments: Optional[List[Dict]] = None,   # 🆕 v2.9.0: Segmentos de vídeo separados
        phase1_audio_url: Optional[str] = None,         # 🆕 v2.9.0: Áudio/Vídeo concatenado para transcrição
        phase1_video_concatenated_url: Optional[str] = None,  # 🆕 v2.9.47: Vídeo 360p concatenado
        original_video_url: Optional[str] = None,       # 🆕 v2.9.2: URL do vídeo original (antes do corte)
        phase1_source: Optional[str] = None,            # 🆕 v2.9.182: Origem (normalized/concatenated/tectonic)
        phase1_metadata: Optional[Dict] = None,         # 🆕 v2.9.182: Metadados da Fase 1
        matting_config_hash: Optional[str] = None,      # 🆕 v2.10.7: Hash da configuração do person_overlay
        matting_artifacts_url: Optional[str] = None     # 🆕 v2.10.7: URL dos artifacts do matting
    ) -> Optional[VideoJob]:
        """Define outputs do job"""
        job = self.get_job(job_id)
        if not job:
            return None
        
        if output_video_url:
            job.output_video_url = output_video_url
        
        # 🆕 URLs separadas para Fase 1 e Fase 2
        if phase1_video_url:
            job.phase1_video_url = phase1_video_url
            logger.info(f"💾 [set_output] phase1_video_url salva: {phase1_video_url[:60]}...")
        
        if phase2_video_url:
            job.phase2_video_url = phase2_video_url
            logger.info(f"💾 [set_output] phase2_video_url salva: {phase2_video_url[:60]}...")
        
        if transcription_text:
            job.transcription_text = transcription_text
        
        if transcription_words:
            job.transcription_words = transcription_words
        
        if phrase_groups:
            job.phrase_groups = phrase_groups
        
        if png_results:
            job.png_results = png_results
        
        if shadow_results:
            job.shadow_results = shadow_results
        
        # 🆕 Campos de matting
        if matted_video_url:
            job.matted_video_url = matted_video_url
            logger.info(f"💾 [set_output] matted_video_url salva: {matted_video_url[:60]}...")
        
        if matting_segments:
            job.matting_segments = matting_segments
            logger.info(f"💾 [set_output] matting_segments: {len(matting_segments)} segmentos")
        
        # 🆕 v2.1.0: Foregrounds separados (Opção B - sem concatenação)
        if foreground_segments:
            job.foreground_segments = foreground_segments
            logger.info(f"💾 [set_output] foreground_segments: {len(foreground_segments)} foregrounds separados (sem concat)")
        
        # 🆕 Campos de normalização (v-matting retorna junto)
        if base_normalized_url:
            job.base_normalized_url = base_normalized_url
            logger.info(f"💾 [set_output] base_normalized_url salva: {base_normalized_url[:60]}...")
        
        if normalization_stats:
            job.normalization_stats = normalization_stats
            logger.info(f"💾 [set_output] normalization_stats: {normalization_stats}")
        
        # 🆕 v2.5.0: Pontos de corte (silence removal)
        if cut_timestamps:
            job.cut_timestamps = cut_timestamps
            logger.info(f"💾 [set_output] cut_timestamps: {len(cut_timestamps)} pontos de corte")
        
        # 🆕 v2.5.1: Trechos sem transcrição (início e fim do vídeo)
        if untranscribed_segments:
            job.untranscribed_segments = untranscribed_segments
            logger.info(f"💾 [set_output] untranscribed_segments: start={untranscribed_segments.get('start', {}).get('duration_ms', 0)}ms, end={untranscribed_segments.get('end', {}).get('duration_ms', 0)}ms")
        
        # 🆕 v2.9.0: Hybrid silence cut - segmentos de vídeo separados
        if speech_segments:
            job.speech_segments = speech_segments
            logger.info(f"💾 [set_output] speech_segments: {len(speech_segments)} segmentos de vídeo separados")
        
        if phase1_audio_url:
            job.phase1_audio_url = phase1_audio_url
            logger.info(f"💾 [set_output] phase1_audio_url salva: {phase1_audio_url[:60]}...")
        
        # 🆕 v2.9.47: Vídeo 360p concatenado (para transcrição com duração exata)
        if phase1_video_concatenated_url:
            job.phase1_video_concatenated_url = phase1_video_concatenated_url
            logger.info(f"💾 [set_output] phase1_video_concatenated_url salva: {phase1_video_concatenated_url[:60]}...")
        
        # 🆕 v2.9.2: URL do vídeo original (antes do corte de silêncios)
        if original_video_url:
            job.original_video_url = original_video_url
            logger.info(f"💾 [set_output] original_video_url salva: {original_video_url[:60]}...")
        
        # 🆕 v2.9.182: Rastreabilidade da Fase 1
        if phase1_source:
            job.phase1_source = phase1_source
            logger.info(f"💾 [set_output] phase1_source: {phase1_source}")
        
        if phase1_metadata:
            job.phase1_metadata = phase1_metadata
            logger.info(f"💾 [set_output] phase1_metadata: {list(phase1_metadata.keys())}")
        
        # 🆕 v2.10.7: Cache invalidation para matting
        if matting_config_hash is not None:
            job.matting_config_hash = matting_config_hash
            logger.info(f"💾 [set_output] matting_config_hash: {matting_config_hash[:16] if matting_config_hash else None}...")
        
        if matting_artifacts_url is not None:
            job.matting_artifacts_url = matting_artifacts_url
            logger.info(f"💾 [set_output] matting_artifacts_url: {matting_artifacts_url[:60] if matting_artifacts_url else None}...")
        
        self._persist_job(job)
        # Atualizar cache com job atualizado
        self._jobs_cache[job_id] = job
        return job
    
    def set_b2_url(
        self,
        job_id: str,
        b2_url: str
    ) -> Optional[VideoJob]:
        """
        Atualiza a URL do B2 do job.
        
        Chamado após o upload para o Backblaze B2 ser concluído.
        """
        job = self.get_job(job_id)
        if not job:
            return None
        
        # Substituir URL temporária pela URL do B2
        job.output_video_url = b2_url
        
        self._persist_job(job)
        logger.info(f"✅ [JobManager] B2 URL atualizada para job {job_id}")
        return job
    
    def _persist_job(self, job: VideoJob):
        """Persiste job no banco de dados"""
        if not self.db_connection_func:
            logger.warning("⚠️ Sem conexão com banco, job apenas em memória")
            return
        
        try:
            from psycopg2.extras import Json
            
            conn = self.db_connection_func()
            cursor = conn.cursor()
            
            # 🔍 DEBUG: Verificar o que está sendo persistido
            if job.options:
                import inspect
                caller = inspect.stack()[1].function if len(inspect.stack()) > 1 else "unknown"
                logger.info(f"[PERSIST_JOB] {job.job_id[:8]}... - editor_worker_id: {job.options.get('editor_worker_id', 'NOT_SET')} (caller: {caller})")
                logger.info(f"[PERSIST_JOB] {job.job_id[:8]}... - worker_override: {job.options.get('worker_override', 'NOT_SET')}")
            
            # Upsert na tabela video_processing_jobs
            # 🆕 v2.5.0: Inclui cut_timestamps para mostrar linhas de corte no revisor
            # 🆕 v2.5.1: Inclui untranscribed_segments para mostrar trechos sem transcrição
            cursor.execute("""
                INSERT INTO video_processing_jobs (
                    job_id, conversation_id, project_id, user_id,
                    status, created_at, started_at, completed_at,
                    videos, options, webhook_url, steps, current_step,
                    output_video_url, phase1_video_url, phase2_video_url,
                    transcription_text, transcription_words,
                    phrase_groups, png_results, shadow_results, total_duration_ms, error_message,
                    matted_video_url, matting_segments,
                    foreground_segments, base_normalized_url, normalization_stats, cut_timestamps, untranscribed_segments,
                    speech_segments, phase1_audio_url, original_video_url,
                    phase1_source, phase1_metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (job_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    options = EXCLUDED.options,
                    steps = EXCLUDED.steps,
                    current_step = EXCLUDED.current_step,
                    output_video_url = EXCLUDED.output_video_url,
                    phase1_video_url = COALESCE(EXCLUDED.phase1_video_url, video_processing_jobs.phase1_video_url),
                    phase2_video_url = COALESCE(EXCLUDED.phase2_video_url, video_processing_jobs.phase2_video_url),
                    transcription_text = EXCLUDED.transcription_text,
                    transcription_words = EXCLUDED.transcription_words,
                    phrase_groups = EXCLUDED.phrase_groups,
                    png_results = EXCLUDED.png_results,
                    shadow_results = EXCLUDED.shadow_results,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    error_message = EXCLUDED.error_message,
                    matted_video_url = COALESCE(EXCLUDED.matted_video_url, video_processing_jobs.matted_video_url),
                    matting_segments = COALESCE(EXCLUDED.matting_segments, video_processing_jobs.matting_segments),
                    foreground_segments = COALESCE(EXCLUDED.foreground_segments, video_processing_jobs.foreground_segments),
                    base_normalized_url = COALESCE(EXCLUDED.base_normalized_url, video_processing_jobs.base_normalized_url),
                    normalization_stats = COALESCE(EXCLUDED.normalization_stats, video_processing_jobs.normalization_stats),
                    cut_timestamps = COALESCE(EXCLUDED.cut_timestamps, video_processing_jobs.cut_timestamps),
                    untranscribed_segments = COALESCE(EXCLUDED.untranscribed_segments, video_processing_jobs.untranscribed_segments),
                    speech_segments = COALESCE(EXCLUDED.speech_segments, video_processing_jobs.speech_segments),
                    phase1_audio_url = COALESCE(EXCLUDED.phase1_audio_url, video_processing_jobs.phase1_audio_url),
                    original_video_url = COALESCE(EXCLUDED.original_video_url, video_processing_jobs.original_video_url),
                    phase1_source = COALESCE(EXCLUDED.phase1_source, video_processing_jobs.phase1_source),
                    phase1_metadata = COALESCE(EXCLUDED.phase1_metadata, video_processing_jobs.phase1_metadata),
                    updated_at = NOW()
            """, (
                job.job_id,
                job.conversation_id,
                job.project_id,
                job.user_id,
                job.status.value,
                job.created_at,
                job.started_at,
                job.completed_at,
                Json(job.videos),
                Json(job.options),
                job.webhook_url,
                Json([s.to_dict() for s in job.steps]),
                job.current_step,
                job.output_video_url,
                job.phase1_video_url,
                job.phase2_video_url,
                job.transcription_text,
                Json(job.transcription_words) if job.transcription_words else None,
                Json(job.phrase_groups) if job.phrase_groups else None,
                Json(job.png_results) if job.png_results else None,
                Json(job.shadow_results) if job.shadow_results else None,
                job.total_duration_ms,
                job.error_message,
                job.matted_video_url,
                Json(job.matting_segments) if job.matting_segments else None,
                Json(job.foreground_segments) if job.foreground_segments else None,
                job.base_normalized_url,
                Json(job.normalization_stats) if job.normalization_stats else None,
                Json(job.cut_timestamps) if job.cut_timestamps else None,
                Json(job.untranscribed_segments) if job.untranscribed_segments else None,
                Json(job.speech_segments) if job.speech_segments else None,
                job.phase1_audio_url,
                job.original_video_url,  # 🆕 v2.9.2: URL do vídeo original para player seek-based
                job.phase1_source,       # 🆕 v2.9.182: Origem (normalized/concatenated/tectonic)
                Json(job.phase1_metadata) if job.phase1_metadata else None  # 🆕 v2.9.182: Metadados
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # 🔍 DEBUG: Confirmar persistência
            logger.debug(f"✅ [PERSIST] Job {job.job_id} persistido no banco (status={job.status.value})")
            
        except Exception as e:
            logger.error(f"❌ Erro ao persistir job {job.job_id}: {e}")
    
    def _load_job_from_db(self, job_id: str) -> Optional[VideoJob]:
        """Carrega job do banco de dados"""
        if not self.db_connection_func:
            return None
        
        try:
            from psycopg2.extras import RealDictCursor
            
            conn = self.db_connection_func()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT * FROM video_processing_jobs WHERE job_id = %s
            """, (job_id,))
            
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not row:
                return None
            
            # Reconstruir steps
            steps = [
                ProcessingStep(
                    name=s['name'],
                    status=StepStatus(s['status']),
                    started_at=s.get('started_at'),
                    completed_at=s.get('completed_at'),
                    duration_ms=s.get('duration_ms'),
                    result=s.get('result'),
                    error=s.get('error')
                )
                for s in (row['steps'] or [])
            ]
            
            job = VideoJob(
                job_id=row['job_id'],
                conversation_id=row['conversation_id'],
                project_id=row['project_id'],
                user_id=row['user_id'],
                status=JobStatus(row['status']),
                created_at=row['created_at'].isoformat() if row['created_at'] else None,
                started_at=row['started_at'].isoformat() if row['started_at'] else None,
                completed_at=row['completed_at'].isoformat() if row['completed_at'] else None,
                videos=row['videos'] or [],
                options=row['options'] or {},
                webhook_url=row['webhook_url'],
                steps=steps,
                current_step=row['current_step'] or 0,
                output_video_url=row['output_video_url'],
                phase1_video_url=row.get('phase1_video_url'),  # 🆕
                phase2_video_url=row.get('phase2_video_url'),  # 🆕
                transcription_text=row['transcription_text'],
                transcription_words=row.get('transcription_words'),
                phrase_groups=row.get('phrase_groups'),
                png_results=row.get('png_results'),
                shadow_results=row.get('shadow_results'),
                total_duration_ms=row['total_duration_ms'],
                error_message=row['error_message'],
                matted_video_url=row.get('matted_video_url'),           # 🆕 Matting
                matting_segments=row.get('matting_segments'),           # 🆕 Matting
                foreground_segments=row.get('foreground_segments'),     # 🆕 v2.1.0: Foregrounds separados
                base_normalized_url=row.get('base_normalized_url'),     # 🆕 Normalização
                normalization_stats=row.get('normalization_stats'),     # 🆕 Normalização
                cut_timestamps=row.get('cut_timestamps'),               # 🆕 v2.5.0: Pontos de corte
                untranscribed_segments=row.get('untranscribed_segments'),  # 🆕 v2.5.1: Trechos sem transcrição
                speech_segments=row.get('speech_segments'),             # 🆕 v2.9.0: Segmentos de vídeo separados
                phase1_audio_url=row.get('phase1_audio_url'),           # 🆕 v2.9.0: Áudio concatenado
                original_video_url=row.get('original_video_url'),       # 🆕 v2.9.2: Vídeo original para player
                phase1_source=row.get('phase1_source'),                 # 🆕 v2.9.182: Origem
                phase1_metadata=row.get('phase1_metadata')              # 🆕 v2.9.182: Metadados
            )
            
            # Cachear
            self._jobs_cache[job_id] = job
            return job
            
        except Exception as e:
            logger.error(f"❌ Erro ao carregar job {job_id}: {e}")
            return None


# ═══════════════════════════════════════════════════════════════
# Singleton — substituindo orchestrator.job_manager
# ═══════════════════════════════════════════════════════════════

_job_manager = None


def get_job_manager() -> JobManager:
    """
    Retorna instância singleton do JobManager.
    
    Uso:
        from app.video_orchestrator.jobs import get_job_manager
        jm = get_job_manager()
        job = jm.get_job(job_id)
    """
    global _job_manager
    if _job_manager is None:
        from app.supabase_client import get_direct_db_connection
        _job_manager = JobManager(db_connection_func=get_direct_db_connection)
    return _job_manager

