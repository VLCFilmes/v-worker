"""
🎬 Video Orchestrator Endpoints - API REST

Endpoints para iniciar e monitorar processamento de vídeo.
Inclui SSE (Server-Sent Events) para progresso em tempo real.

Dependências:
- JobManager (get_job_manager) para CRUD de jobs
- queue.py para enfileiramento Redis
- EngineBridge para execução do pipeline
"""

import logging
import json
import uuid
from flask import Blueprint, request, jsonify, Response
from typing import Dict, Any

from .jobs import JobStatus, get_job_manager

logger = logging.getLogger(__name__)

# Blueprint
video_orchestrator_bp = Blueprint('video_orchestrator', __name__)


# 🗑️ REMOVIDO v3.3.0 (05/Fev/2026)
# Endpoint /video/process DEPRECADO e removido
# Era usado pelo Generator V2 (via BFF /api/pipeline/start)
# Use: POST /api/projects/{id}/start-processing (Generator V3)
# Motivo: Generator V2 descontinuado, endpoint duplicava lógica de start-processing


@video_orchestrator_bp.route('/video/job/<job_id>', methods=['GET'])
def get_job_status(job_id: str):
    """
    GET /api/video/job/{job_id}
    
    Retorna status atual de um job de processamento.
    
    Response:
    {
        "job_id": "uuid",
        "status": "processing",
        "conversation_id": "uuid",
        "project_id": "uuid",
        "created_at": "2025-11-30T...",
        "started_at": "2025-11-30T...",
        "steps": [
            {"name": "normalize", "status": "completed", "duration_ms": 15000},
            {"name": "concat", "status": "processing"},
            ...
        ],
        "current_step": 1,
        "progress_percent": 40,
        "output_video_url": null,
        "transcription_text": null
    }
    """
    try:
        jm = get_job_manager()
        job = jm.get_job(job_id, force_reload=True)
        
        if not job:
            return jsonify({"error": "Job não encontrado"}), 404
        
        # 🔍 DEBUG: Log do status retornado pelo banco
        logger.info(f"📊 [GET_STATUS] Job {job_id}: status={job.status.value}, phrase_groups={len(job.phrase_groups) if job.phrase_groups else 0}")
        
        # 🆕 v2.9.0: Debug speech_segments
        speech_seg_count = len(job.speech_segments) if job.speech_segments else 0
        logger.info(f"📊 [GET_STATUS] Job {job_id}: speech_segments={speech_seg_count}, phase1_audio_url={'✅' if job.phase1_audio_url else '❌'}")
        
        # 🐛 DEBUG: Verificar qual URL está sendo retornada
        logger.info(f"🔍 [DEBUG] Verificando condição completed: status={job.status}, type={type(job.status)}, value={job.status.value if hasattr(job.status, 'value') else 'N/A'}")
        logger.info(f"🔍 [DEBUG] JobStatus.COMPLETED={JobStatus.COMPLETED}, type={type(JobStatus.COMPLETED)}")
        logger.info(f"🔍 [DEBUG] Comparação: {job.status == JobStatus.COMPLETED}")
        
        if job.status == JobStatus.COMPLETED:
            logger.info(f"✅ [DEBUG] Entrou na condição COMPLETED!")
            output_preview = job.output_video_url[:80] + '...' if job.output_video_url else 'None'
            phase2_preview = job.phase2_video_url[:80] + '...' if job.phase2_video_url else 'None'
            logger.info(f"📊 [GET_STATUS] URLs do job completed:")
            logger.info(f"   • output_video_url: {output_preview}")
            logger.info(f"   • phase2_video_url: {phase2_preview}")
            logger.info(f"   • Contém 'normalized': {bool(job.output_video_url and 'normalized' in job.output_video_url)}")
            logger.info(f"   • Contém 'renders': {bool(job.output_video_url and 'renders' in job.output_video_url)}")
        else:
            logger.info(f"❌ [DEBUG] NÃO entrou na condição COMPLETED (status={job.status})")
        
        # Calcular progresso
        total_steps = len(job.steps)
        completed_steps = sum(1 for s in job.steps if s.status.value in ['completed', 'skipped'])
        progress_percent = int((completed_steps / total_steps) * 100) if total_steps > 0 else 0
        
        # 🆕 Pipeline 2 Fases: Incluir dados completos se estiver em AWAITING_REVIEW
        is_awaiting_review = job.status == JobStatus.AWAITING_REVIEW
        
        response_data = {
            "job_id": job.job_id,
            "status": job.status.value,
            "conversation_id": job.conversation_id,
            "project_id": job.project_id,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "steps": [s.to_dict() for s in job.steps],
            "current_step": job.current_step,
            "progress_percent": progress_percent,
            "output_video_url": job.output_video_url,
            "phase1_video_url": job.phase1_video_url,  # 🆕 URL Fase 1 (vídeo cortado)
            "phase2_video_url": job.phase2_video_url,  # 🆕 URL Fase 2 (vídeo renderizado)
            "transcription_text": job.transcription_text[:500] if job.transcription_text else None,
            "transcription_words_count": len(job.transcription_words) if job.transcription_words else 0,
            "phrase_groups_count": len(job.phrase_groups) if job.phrase_groups else 0,
            "total_duration_ms": job.total_duration_ms,
            "error_message": job.error_message,
            "is_awaiting_review": is_awaiting_review,
            # 🆕 v2.9.183: Rastreabilidade da Fase 1 (alinhamento frontend/backend)
            "phase1_source": job.phase1_source,  # "normalized", "concatenated", "tectonic"
            "phase1_metadata": job.phase1_metadata
        }
        
        # 🆕 v2.9.123: SEMPRE incluir phrase_groups quando disponível
        # Permite revisão da classificação mesmo após Pipeline Automático
        # O frontend pode exibir o card de revisão em qualquer status
        has_phrase_data = job.phrase_groups and len(job.phrase_groups) > 0
        
        if has_phrase_data:
            response_data["phrase_groups"] = job.phrase_groups
            response_data["transcription_words"] = job.transcription_words
            # 🆕 v2.9.0: Dados do modo HYBRID
            response_data["speech_segments"] = job.speech_segments
            response_data["phase1_audio_url"] = job.phase1_audio_url
            response_data["cut_timestamps"] = job.cut_timestamps
            response_data["untranscribed_segments"] = job.untranscribed_segments
            # 🆕 v2.9.2: URL do vídeo original para player seek-based
            response_data["original_video_url"] = job.original_video_url
            # 🆕 v2.9.123: Flag para indicar que revisão está disponível
            response_data["can_review_classification"] = True
        
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@video_orchestrator_bp.route('/projects/<project_id>/jobs', methods=['GET'])
def get_project_jobs(project_id: str):
    """
    GET /api/projects/{project_id}/jobs
    
    🆕 v2.10.10 (04/Fev/2025): Buscar jobs de um projeto
    
    Retorna lista de jobs de um projeto ordenados por created_at DESC.
    Usado pelo Generator V3 para recuperar estado após refresh.
    
    Query Parameters:
        - limit: Número máximo de jobs a retornar (default: 10, max: 50)
        - status: Filtrar por status (optional)
    
    Response:
    {
        "project_id": "uuid",
        "jobs": [
            {
                "id": "uuid",
                "status": "completed",
                "phase1_video_url": "...",
                "phase2_video_url": "...",
                "created_at": "2025-02-04T...",
                "steps": [...]
            },
            ...
        ],
        "total": 15
    }
    """
    try:
        limit = min(int(request.args.get('limit', 10)), 50)
        status_filter = request.args.get('status')
        
        from app.supabase_client import get_direct_db_connection
        
        # Buscar jobs do projeto usando SQL direto
        conn = get_direct_db_connection()
        try:
            cursor = conn.cursor()
            
            # Query com filtro opcional de status
            query = """
                SELECT 
                    job_id, status, conversation_id, project_id,
                    created_at, started_at, completed_at,
                    output_video_url, phase1_video_url, phase2_video_url,
                    steps, transcription_text
                FROM video_processing_jobs
                WHERE project_id = %s
            """
            params = [project_id]
            
            if status_filter:
                query += " AND status = %s"
                params.append(status_filter)
            
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            jobs = []
            for row in rows:
                job_dict = {
                    "id": row[0],
                    "status": row[1],
                    "conversation_id": row[2],
                    "project_id": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                    "started_at": row[5].isoformat() if row[5] else None,
                    "completed_at": row[6].isoformat() if row[6] else None,
                    "output_video_url": row[7],
                    "phase1_video_url": row[8],
                    "phase2_video_url": row[9],
                    "steps": row[10],  # JSON
                    "transcription_text": row[11]
                }
                jobs.append(job_dict)
            
            # Count total
            cursor.execute(
                "SELECT COUNT(*) FROM video_processing_jobs WHERE project_id = %s",
                [project_id]
            )
            total = cursor.fetchone()[0]
        finally:
            conn.close()
        
        logger.info(f"[GET /projects/{project_id}/jobs] Retornando {len(jobs)} de {total} jobs")
        
        return jsonify({
            "project_id": project_id,
            "jobs": jobs,
            "total": total,
            "limit": limit
        }), 200
    
    except Exception as e:
        logger.error(f"Error getting project jobs: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@video_orchestrator_bp.route('/video/job/<job_id>/results', methods=['GET'])
def get_job_results(job_id: str):
    """
    GET /api/video/job/{job_id}/results
    
    Retorna resultados completos do job incluindo transcription_words e phrase_groups.
    Usar este endpoint após job completado para obter dados de renderização.
    
    Response:
    {
        "job_id": "uuid",
        "status": "completed",
        "output_video_url": "...",
        "transcription_text": "...",
        "transcription_words": [
            {"text": "Olá", "start": 0.0, "end": 0.5, "confidence": 0.98},
            ...
        ],
        "phrase_groups": [
            {
                "phrase_index": 0,
                "text": "Olá mundo",
                "words": [...],
                "phrase_type": "double",
                "emphasis_level": "normal",
                ...
            },
            ...
        ]
    }
    """
    try:
        jm = get_job_manager()
        job = jm.get_job(job_id, force_reload=True)
        
        if not job:
            return jsonify({"error": "Job não encontrado"}), 404
        
        # 🆕 v2.9.123: Permitir ver results em mais status para revisão pós-pipeline
        # COMPLETED: Job finalizado
        # AWAITING_REVIEW: Fase 1 concluída, aguardando aprovação
        # RENDERING: Renderização em andamento (permite ver classificação enquanto renderiza)
        valid_statuses = [JobStatus.COMPLETED, JobStatus.AWAITING_REVIEW, JobStatus.RENDERING]
        
        # 🆕 v2.9.123: Se tem phrase_groups, permitir acesso mesmo em outros status
        has_results = job.phrase_groups and len(job.phrase_groups) > 0
        
        if job.status not in valid_statuses and not has_results:
            return jsonify({
                "error": "Job ainda não tem resultados disponíveis",
                "status": job.status.value,
                "hint": "Aguarde o job completar ou entrar em revisão (phase_1_only)"
            }), 400
        
        # 🆕 v2.9.170: SEMPRE retornar dados para o revisor de transcrição
        # O revisor precisa funcionar tanto em AWAITING_REVIEW quanto COMPLETED (pipeline automático)
        
        # Determinar qual vídeo usar para revisão:
        # 1. speech_segments (modo híbrido) - preferido
        # 2. phase1_video_url - vídeo cortado
        # 3. original_video_url - vídeo original
        # 4. output_video_url (fallback - pode ser vídeo renderizado)
        review_video_url = (
            job.phase1_video_url or 
            job.original_video_url or 
            job.output_video_url
        )
        
        return jsonify({
            "job_id": job.job_id,
            "status": job.status.value,
            "output_video_url": job.output_video_url,
            "phase2_video_url": job.phase2_video_url,  # 🔧 v3.7.0: Frontend precisa deste campo para Fase 2
            "transcription_text": job.transcription_text,
            "transcription_words": job.transcription_words,
            "phrase_groups": job.phrase_groups,
            "total_duration_ms": job.total_duration_ms,
            "completed_at": job.completed_at,
            # 🆕 v2.9.170: SEMPRE retornar dados para o revisor
            "is_awaiting_review": job.status == JobStatus.AWAITING_REVIEW,
            "phase_1_video_url": job.phase1_video_url,  # 🔧 Sempre retornar se disponível
            "original_video_url": job.original_video_url,  # 🆕 Para player seek-based
            "speech_segments": job.speech_segments,  # 🆕 Para modo híbrido
            "review_video_url": review_video_url,  # 🆕 URL recomendada para revisor
            "can_review": True  # 🆕 Flag indicando que revisão está disponível
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar resultados do job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@video_orchestrator_bp.route('/video/job/<job_id>/phrases', methods=['PUT'])
def update_job_phrases(job_id: str):
    """
    PUT /api/video/job/{job_id}/phrases
    
    Atualiza as phrase_groups de um job (após revisão do usuário).
    Usado para salvar alterações feitas no TranscriptionReviewEditor:
    - Texto corrigido
    - style_type alterado (estrelas)
    - Merge/split de frases
    
    Request Body:
    {
        "phrase_groups": [
            {
                "phrase_index": 0,
                "text": "Texto editado",
                "style_type": "emphasis",
                ...
            }
        ]
    }
    
    Response:
    {
        "job_id": "uuid",
        "updated": true,
        "phrase_count": 15
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'phrase_groups' not in data:
            return jsonify({"error": "phrase_groups é obrigatório"}), 400
        
        phrase_groups = data['phrase_groups']
        
        if not isinstance(phrase_groups, list):
            return jsonify({"error": "phrase_groups deve ser uma lista"}), 400
        
        # 🔧 v3.2.12: Mapear campos do frontend para o backend
        # Frontend usa: use_cartela
        # Backend usa: render_as_title_card
        for phrase in phrase_groups:
            if 'use_cartela' in phrase:
                phrase['render_as_title_card'] = phrase['use_cartela']
        
        jm = get_job_manager()
        job = jm.get_job(job_id)
        
        if not job:
            return jsonify({"error": "Job não encontrado"}), 404
        
        # Atualizar phrase_groups no job
        updated_job = jm.set_output(
            job_id,
            phrase_groups=phrase_groups
        )
        
        if not updated_job:
            return jsonify({"error": "Falha ao atualizar job"}), 500
        
        logger.info(f"✅ Phrase groups atualizadas para job {job_id}: {len(phrase_groups)} frases")
        
        return jsonify({
            "job_id": job_id,
            "updated": True,
            "phrase_count": len(phrase_groups)
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar phrases do job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@video_orchestrator_bp.route('/video/job/<job_id>/continue', methods=['POST'])
def continue_job_pipeline(job_id: str):
    """
    POST /api/video/job/{job_id}/continue
    
    🆕 v3.2.16: SEMPRE CRIA NOVO JOB_ID PARA FASE 2!
    
    Arquitetura: 1 RENDER = 1 JOB_ID
    - Fase 1 → job A
    - Fase 2 (primeira) → job B (NOVO!)
    - Re-render Fase 2 → job C (NOVO!)
    
    Continua o pipeline criando SEMPRE um novo job para a Fase 2.
    Isso garante rastreabilidade total: cada execução tem seu ID único.
    
    Dispara os próximos steps:
    - classify (classificação LLM, se não feita manualmente)
    - generate_pngs (gerar PNGs das legendas)
    - add_shadows (adicionar sombras)
    - apply_animations (aplicar metadados de animação - Step 11)
    - positioning (posicionamento X,Y no canvas)
    - render (enviar para v-editor)
    
    Request Body (opcional):
    {
        "steps": ["generate_pngs", "add_shadows", "apply_animations", "generate_backgrounds", "positioning", "render"],
        "template_id": "uuid"  // template a usar (opcional, usa do projeto se não informado)
    }
    
    Response:
    {
        "job_id": "uuid",  // SEMPRE NOVO (diferente do job da Fase 1)
        "is_rerender": boolean,  // true se já existia phase2_video_url
        "original_job_id": "uuid",  // Job da Fase 1 (referência)
        "status": "processing",
        "steps_queued": ["generate_pngs", "add_shadows", "apply_animations", "generate_backgrounds", "positioning", "render"]
    }
    """
    try:
        jm = get_job_manager()
        original_job = jm.get_job(job_id, force_reload=True)
        
        if not original_job:
            return jsonify({"error": "Job não encontrado"}), 404
        
        # Verificar se tem phrase_groups (pré-requisito)
        phrase_groups = original_job.phrase_groups or []
        logger.info(f"📋 Job {job_id} tem {len(phrase_groups)} phrase_groups")
        if not phrase_groups:
            return jsonify({
                "error": "Job não tem phrase_groups. Execute a transcrição primeiro."
            }), 400
        
        # Arquitetura: 1 RENDER = 1 JOB_ID (consistência total)
        # Fase 1 → job A  |  Fase 2 → job B (NOVO)  |  Re-render → job C (NOVO)
        is_rerender = bool(original_job.phase2_video_url)
        new_job_id = str(uuid.uuid4())
        
        logger.info(f"🆕 [FASE 2] {'Re-render' if is_rerender else 'Primeira renderização'}: "
                     f"criando job {new_job_id[:8]}... (Fase 1: {job_id[:8]}...)")
        
        from app.video_orchestrator.jobs import VideoJob
        
        # Criar novo job copiando dados da Fase 1
        new_job = VideoJob(
            job_id=new_job_id,
            conversation_id=original_job.conversation_id,
            project_id=original_job.project_id,
            user_id=original_job.user_id,
            status=JobStatus.PROCESSING,
            phase1_video_url=original_job.phase1_video_url,
            phase1_audio_url=original_job.phase1_audio_url,
            phase1_source=original_job.phase1_source,
            phase1_metadata=original_job.phase1_metadata,
            phase1_video_concatenated_url=original_job.phase1_video_concatenated_url,
            original_video_url=original_job.original_video_url,
            transcription_text=original_job.transcription_text,
            transcription_words=original_job.transcription_words,
            phrase_groups=original_job.phrase_groups,
            speech_segments=original_job.speech_segments,
            cut_timestamps=original_job.cut_timestamps,
            total_duration_ms=original_job.total_duration_ms,
            untranscribed_segments=original_job.untranscribed_segments,
            template_id=original_job.template_id,
            phase2_video_url=None,
            output_video_url=None,
            options={
                **(original_job.options or {}),
                'phase1_job_id': job_id
            },
            steps=[],
            current_step=0
        )
        
        # Salvar novo job
        jm._jobs_cache[new_job_id] = new_job
        jm._persist_job(new_job)
        
        logger.info(f"✅ [FASE 2] Novo job criado: {new_job_id[:8]}... "
                     f"(projeto: {original_job.project_id[:8]}...)")
        
        # Overrides opcionais do frontend (o engine decide quais steps rodar)
        data = request.get_json() or {}
        template_id = data.get('template_id')
        worker_override = data.get('worker_override')
        editor_worker_id = data.get('editor_worker_id')
        
        # Salvar overrides no job.options para o bridge ler
        continue_params = {}
        if template_id:
            continue_params['template_id'] = template_id
        if worker_override:
            continue_params['worker_override'] = worker_override
        if editor_worker_id:
            continue_params['editor_worker_id'] = editor_worker_id
        
        if continue_params:
            new_job.options = {**(new_job.options or {}), '_continue_params': continue_params}
            jm._jobs_cache[new_job_id] = new_job
            jm._persist_job(new_job)
        
        jm.update_job_status(new_job_id, JobStatus.PROCESSING)
        
        logger.info(f"🔄 Continuando pipeline para job {new_job_id[:8]}...")
        
        # Enfileirar no Redis para o worker processar
        from .queue import enqueue_continue_job
        enqueued = enqueue_continue_job(new_job_id)
        
        if not enqueued:
            # Fallback: executar localmente se Redis indisponível
            logger.warning(f"⚠️ Redis indisponível, executando localmente")
            from concurrent.futures import ThreadPoolExecutor
            from app.video_orchestrator.engine.bridge import get_engine_bridge
            executor = ThreadPoolExecutor(max_workers=1)
            bridge = get_engine_bridge()
            executor.submit(bridge.continue_pipeline, new_job_id)
        
        return jsonify({
            "job_id": new_job_id,
            "is_rerender": is_rerender,
            "original_job_id": job_id if is_rerender else None,
            "status": "processing",
            "message": "Pipeline Fase 2 iniciado"
        }), 202
        
    except Exception as e:
        logger.error(f"❌ Erro ao continuar pipeline do job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


# 🗑️ REMOVIDO v2.10.9 (04/Fev/2025)
# Endpoint /continue-phase2 DEPRECADO e removido
# Motivo: Conflitava com /continue (v3.2.16) - comportamento de job_id diferente
# Use: POST /api/video/job/{id}/continue (SEMPRE cria novo job_id)
# Histórico: Este endpoint mantinha o MESMO job_id, causando confusão e bugs


@video_orchestrator_bp.route('/video/job/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id: str):
    """
    POST /api/video/job/{job_id}/cancel
    
    Cancela um job de processamento (se ainda estiver em andamento).
    
    Response:
    {
        "job_id": "uuid",
        "status": "cancelled",
        "message": "Job cancelado com sucesso"
    }
    """
    try:
        jm = get_job_manager()
        job = jm.get_job(job_id)
        
        if not job:
            return jsonify({"error": "Job não encontrado"}), 404
        
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            return jsonify({
                "error": f"Job já finalizado com status: {job.status.value}"
            }), 400
        
        # Atualizar status
        jm.update_job_status(job_id, JobStatus.CANCELLED)
        
        logger.info(f"🛑 Job cancelado: {job_id}")
        
        return jsonify({
            "job_id": job_id,
            "status": "cancelled",
            "message": "Job cancelado com sucesso"
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erro ao cancelar job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@video_orchestrator_bp.route('/video/health', methods=['GET'])
def health_check():
    """
    GET /api/video/health
    
    Verifica saúde do orquestrador e serviços dependentes.
    
    Response:
    {
        "status": "healthy",
        "services": {
            "normalize": true,
            "ffmpeg": true,
            "whisper": true
        }
    }
    """
    try:
        import os
        from .services.normalize_service import NormalizeService
        from .services.concat_service import ConcatService
        from .services.fraseamento_service import FraseamentoService
        
        # Verificar qual serviço de transcrição está ativo
        use_assembly = os.environ.get('USE_ASSEMBLY', 'true').lower() == 'true'
        if use_assembly:
            from .services.assembly_service import AssemblyService
            transcription_service = "assemblyai"
            transcription_healthy = AssemblyService().health_check()
        else:
            from .services.transcription_service import TranscriptionService
            transcription_service = "whisper"
            transcription_healthy = TranscriptionService().health_check()
        
        services = {
            "normalize": NormalizeService().health_check(),
            "ffmpeg": ConcatService().health_check(),
            "transcription": transcription_healthy,
            "transcription_provider": transcription_service,
            "fraseamento": FraseamentoService().health_check()
        }
        
        # Não considerar provider no all_healthy
        core_services = {k: v for k, v in services.items() if k != "transcription_provider"}
        all_healthy = all(core_services.values())
        
        return jsonify({
            "status": "healthy" if all_healthy else "degraded",
            "services": services
        }), 200 if all_healthy else 503
        
    except Exception as e:
        logger.error(f"❌ Erro no health check: {e}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 503


# =============================================================================
# 🔴 SSE Helpers
# =============================================================================

# 🗑️ REMOVIDO v3.3.0 (05/Fev/2026)
# Endpoint /video/job/<job_id>/stream-legacy DEPRECADO e removido
# Usava polling local e NÃO recebia eventos do video-worker
# Use: GET /api/video/job/{job_id}/stream (sse_stream.py com Redis pub/sub)


def _sse_event(event_type: str, data: dict) -> str:
    """Formata um evento SSE corretamente"""
    json_data = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {event_type}\ndata: {json_data}\n\n"


def _now_iso() -> str:
    """Retorna timestamp atual em ISO format"""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════════════
# TIMESTAMP GENERATION ENDPOINT
# ═══════════════════════════════════════════════════════════════════════════

@video_orchestrator_bp.route('/text/generate-timestamps', methods=['POST'])
def generate_timestamps():
    """
    POST /api/text/generate-timestamps
    
    Gera timestamps artificiais para texto.
    Usado pelo Generator V3 e chatbot quando não há transcrição.
    
    Request Body:
    {
        "text": "Descubra como transformar sua presença digital...",
        "speed": "normal",  // very_slow, slow, normal, fast, very_fast
        "max_words_per_phrase": 4,
        "min_words_per_phrase": 2
    }
    
    Response:
    {
        "status": "success",
        "phrases": [
            {
                "id": "phrase_0",
                "text": "Descubra como",
                "start": 0,
                "end": 0.8,
                "words": [
                    {"text": "Descubra", "start": 0, "end": 0.4},
                    {"text": "como", "start": 0.4, "end": 0.8}
                ]
            }
        ],
        "total_duration_ms": 5000,
        "total_duration_seconds": 5.0,
        "word_count": 15,
        "phrase_count": 4
    }
    """
    try:
        from .services.timestamp_generator_service import TimestampGeneratorService
        
        data = request.get_json() or {}
        
        text = data.get('text', '').strip()
        if not text:
            return jsonify({
                "error": "Campo 'text' é obrigatório"
            }), 400
        
        speed = data.get('speed', 'normal')
        max_words = data.get('max_words_per_phrase', 4)
        min_words = data.get('min_words_per_phrase', 2)
        
        service = TimestampGeneratorService(speed=speed)
        result = service.generate_timestamps(
            text=text,
            max_words_per_phrase=max_words,
            min_words_per_phrase=min_words
        )
        
        return jsonify({
            "status": "success",
            **result
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao gerar timestamps: {e}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


@video_orchestrator_bp.route('/text/add-timestamps-to-phrases', methods=['POST'])
def add_timestamps_to_phrases():
    """
    POST /api/text/add-timestamps-to-phrases
    
    Adiciona timestamps a frases já agrupadas.
    Útil quando o Generator V3 já agrupou mas não tem timestamps.
    
    Request Body:
    {
        "phrases": [
            {"id": "phrase_0", "text": "Descubra como"},
            {"id": "phrase_1", "text": "transformar sua presença digital"}
        ],
        "speed": "normal"
    }
    
    Response:
    {
        "status": "success",
        "phrases": [
            {
                "id": "phrase_0",
                "text": "Descubra como",
                "start": 0,
                "end": 0.8,
                "words": [...]
            }
        ],
        "total_duration_ms": 5000
    }
    """
    try:
        from .services.timestamp_generator_service import TimestampGeneratorService
        
        data = request.get_json() or {}
        
        phrases = data.get('phrases', [])
        if not phrases:
            return jsonify({
                "error": "Campo 'phrases' é obrigatório"
            }), 400
        
        speed = data.get('speed', 'normal')
        
        service = TimestampGeneratorService(speed=speed)
        result = service.generate_for_phrases(phrases)
        
        # Calcular duração total
        total_duration_ms = 0
        if result:
            total_duration_ms = int(result[-1].get('end', 0) * 1000)
        
        return jsonify({
            "status": "success",
            "phrases": result,
            "total_duration_ms": total_duration_ms,
            "phrase_count": len(result)
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar timestamps: {e}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


# ═══════════════════════════════════════════════════════════════
# 🆕 v3.10.0: Pipeline Replay — Re-execução parcial
# ═══════════════════════════════════════════════════════════════

@video_orchestrator_bp.route('/video/job/<job_id>/replay-from/<step_name>', methods=['POST'])
def replay_from_step(job_id: str, step_name: str):
    """
    POST /api/video/job/{job_id}/replay-from/{step_name}
    
    🆕 v3.10.0: Re-executa pipeline a partir de um step específico.
    
    Fluxo:
    1. Carrega checkpoint do step anterior ao target
    2. Aplica modifications no state
    3. Cria novo job (rastreabilidade: 1 replay = 1 job_id)
    4. Enfileira para o worker executar do target até o render
    
    Request Body:
    {
        "modifications": {                   // Modificações no state (dot-notation)
            "text_styles.default.fill_color": "#0000FF",
            "text_styles.emphasis.font_size": 48
        },
        "run_until": "render"               // Opcional: parar após step X (default: executar todos)
    }
    
    Response (202 Accepted):
    {
        "success": true,
        "new_job_id": "uuid",
        "original_job_id": "uuid",
        "replaying_from": "generate_pngs",
        "steps_to_run": ["generate_pngs", "add_shadows", ...],
        "modifications_applied": 2,
        "estimated_time_seconds": 35
    }
    """
    try:
        from .engine.replay import (
            prepare_replay,
            estimate_replay_time,
            get_steps_from,
            get_previous_step,
        )
        
        data = request.get_json() or {}
        modifications = data.get('modifications', {})
        run_until = data.get('run_until')
        
        # 1. Preparar replay (reconstruct + modify + calc steps)
        modified_state, steps_to_run, error = prepare_replay(
            job_id=job_id,
            target_step=step_name,
            modifications=modifications,
        )
        
        if error:
            return jsonify({
                "success": False,
                "error": error
            }), 400
        
        # 2. Aplicar run_until se fornecido
        if run_until and run_until in steps_to_run:
            cut_idx = steps_to_run.index(run_until) + 1
            steps_to_run = steps_to_run[:cut_idx]
        
        # 3. Criar novo job (1 replay = 1 job_id)
        new_job_id = str(uuid.uuid4())
        
        logger.info(f"🔄 [REPLAY] Criando job {new_job_id[:8]}... "
                     f"(replay de {job_id[:8]}... a partir de '{step_name}')")
        
        from app.video_orchestrator.jobs import VideoJob
        jm = get_job_manager()
        
        # Buscar job original para metadata
        original_job = jm.get_job(job_id, force_reload=True)
        if not original_job:
            return jsonify({"error": f"Job original {job_id} não encontrado"}), 404
        
        # 4. Criar novo VideoJob com dados do state reconstruído
        new_job = VideoJob(
            job_id=new_job_id,
            conversation_id=original_job.conversation_id,
            project_id=original_job.project_id,
            user_id=original_job.user_id,
            status=JobStatus.PROCESSING,
            phase1_video_url=modified_state.phase1_video_url,
            phase1_audio_url=modified_state.phase1_audio_url,
            phase1_source=modified_state.phase1_source,
            phase1_metadata=modified_state.phase1_metadata,
            phase1_video_concatenated_url=modified_state.phase1_video_concatenated_url,
            original_video_url=modified_state.original_video_url,
            transcription_text=modified_state.transcription_text,
            transcription_words=modified_state.transcription_words,
            phrase_groups=modified_state.phrase_groups,
            speech_segments=modified_state.speech_segments,
            cut_timestamps=modified_state.cut_timestamps,
            total_duration_ms=modified_state.total_duration_ms,
            untranscribed_segments=modified_state.untranscribed_segments,
            template_id=modified_state.template_id,
            phase2_video_url=None,
            output_video_url=None,
            options={
                **(original_job.options or {}),
                '_replay_params': {
                    'original_job_id': job_id,
                    'target_step': step_name,
                    'steps_to_run': steps_to_run,
                    'modifications': modifications,
                }
            },
            steps=[],
            current_step=0,
        )
        
        # 5. Persistir novo job
        jm._jobs_cache[new_job_id] = new_job
        jm._persist_job(new_job)
        
        # 6. Salvar PipelineState modificado no banco (para o bridge carregar)
        from app.video_orchestrator.engine.state_manager import StateManager
        from app.supabase_client import get_direct_db_connection
        sm = StateManager(db_connection_func=get_direct_db_connection)
        
        # Atualizar job_id e incluir _replay_params no state
        # (bridge.replay_pipeline lê params de state.options)
        replay_options = {
            **(modified_state.options or {}),
            '_replay_params': {
                'original_job_id': job_id,
                'target_step': step_name,
                'steps_to_run': steps_to_run,
                'modifications': modifications,
            }
        }
        replay_state = modified_state.with_updates(
            job_id=new_job_id,
            options=replay_options,
        )
        sm.save(new_job_id, replay_state, step_name=f"replay_init_{step_name}")
        
        # 6b. Salvar checkpoint do step anterior ao target no debug_logger
        # Isso permite replays encadeados (replay de um replay)
        # reconstruct_state_until(target) busca checkpoint de get_previous_step(target)
        previous_step = get_previous_step(step_name)
        if previous_step:
            from .debug_logger import get_debug_logger
            debug = get_debug_logger()
            debug.log_checkpoint(
                job_id=new_job_id,
                step_name=previous_step,
                state_dict=replay_state.to_dict(),
                duration_ms=0,
                attempt=1,
            )
            logger.info(f"💾 [REPLAY] Checkpoint '{previous_step}' salvo para replay encadeado")
        
        jm.update_job_status(new_job_id, JobStatus.PROCESSING)
        
        # 7. Enfileirar no Redis
        from .queue import enqueue_replay_job
        enqueued = enqueue_replay_job(new_job_id)
        
        if not enqueued:
            # Fallback: executar localmente se Redis indisponível
            logger.warning(f"⚠️ Redis indisponível, executando replay localmente")
            from concurrent.futures import ThreadPoolExecutor
            from app.video_orchestrator.engine.bridge import get_engine_bridge
            executor = ThreadPoolExecutor(max_workers=1)
            bridge = get_engine_bridge()
            executor.submit(bridge.replay_pipeline, new_job_id)
        
        # 8. Resposta
        estimated_time = estimate_replay_time(step_name)
        
        logger.info(f"✅ [REPLAY] Job {new_job_id[:8]}... criado e enfileirado | "
                     f"steps={steps_to_run} | ~{estimated_time}s")
        
        return jsonify({
            "success": True,
            "new_job_id": new_job_id,
            "original_job_id": job_id,
            "replaying_from": step_name,
            "steps_to_run": steps_to_run,
            "modifications_applied": len(modifications),
            "estimated_time_seconds": estimated_time,
            "status": "processing",
            "message": f"Pipeline Replay iniciado a partir de '{step_name}'"
        }), 202
        
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar replay para job {job_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════
# 🆕 v3.10.0: Pipeline Checkpoints (listagem/inspeção)
# ═══════════════════════════════════════════════════════════════

@video_orchestrator_bp.route('/video/job/<job_id>/checkpoints', methods=['GET'])
def get_job_checkpoints(job_id: str):
    """
    GET /api/video/job/{job_id}/checkpoints
    
    🆕 v3.10.0: Lista checkpoints salvos de um job.
    
    Cada checkpoint contém o PipelineState completo salvo após
    a execução de um step. Usado pelo LLM Director para:
    1. Descobrir quais steps foram executados e têm dados salvos
    2. Decidir de qual step fazer replay
    3. Inspecionar o estado em qualquer ponto do pipeline
    
    Query Parameters:
        - step: Filtrar por step específico (opcional)
        - include_payload: Se "true", inclui payload completo (default: false)
    
    Response:
    {
        "job_id": "uuid",
        "checkpoints": [
            {
                "step_name": "normalize",
                "created_at": "2026-02-06T...",
                "duration_ms": 1500,
                "completed_steps": ["load_template", "normalize"],
                "has_payload": true,
                "payload_size": 45000
            },
            ...
        ],
        "total_steps": 19,
        "checkpoint_count": 19
    }
    """
    try:
        from .debug_logger import get_debug_logger
        
        debug = get_debug_logger()
        step_filter = request.args.get('step')
        include_payload = request.args.get('include_payload', 'false').lower() == 'true'
        include_parent = request.args.get('include_parent', 'false').lower() == 'true'
        
        # Listar checkpoints
        checkpoints = debug.get_checkpoints(job_id)
        
        if not checkpoints:
            return jsonify({
                "job_id": job_id,
                "checkpoints": [],
                "total_steps": 0,
                "checkpoint_count": 0,
                "message": "Nenhum checkpoint encontrado. Job pode não ter sido executado pelo Engine v3.10+"
            }), 200
        
        # 🆕 v4.4.2: Buscar checkpoints do job pai (se replay) para steps não presentes
        # Isso garante que o Sandbox Director tenha acesso a TODOS os checkpoints,
        # mesmo para steps que não foram re-executados no replay (ex: detect_silence)
        root_job_id = None
        if include_parent:
            root_job_id = _find_root_job_id(job_id)
            if root_job_id and root_job_id != job_id:
                current_steps = {cp['step_name'] for cp in checkpoints}
                parent_checkpoints = debug.get_checkpoints(root_job_id)
                if parent_checkpoints:
                    for pcp in parent_checkpoints:
                        if pcp['step_name'] not in current_steps:
                            pcp['source_job_id'] = root_job_id
                            pcp['from_parent'] = True
                            checkpoints.append(pcp)
                    logger.info(
                        f"📋 [CHECKPOINTS] Merged {len(parent_checkpoints)} checkpoints do "
                        f"job pai {root_job_id[:8]} (steps adicionados: "
                        f"{[p['step_name'] for p in parent_checkpoints if p['step_name'] not in current_steps]})"
                    )
        
        # Filtrar por step se solicitado
        if step_filter:
            checkpoints = [cp for cp in checkpoints if cp['step_name'] == step_filter]
        
        # Incluir payload completo se solicitado
        if include_payload and checkpoints:
            for cp in checkpoints:
                source = cp.get('source_job_id', job_id)
                payload = debug.get_step_checkpoint(source, cp['step_name'])
                cp['payload'] = payload
        
        response = {
            "job_id": job_id,
            "checkpoints": checkpoints,
            "total_steps": len(checkpoints),
            "checkpoint_count": len(checkpoints),
        }
        if root_job_id and root_job_id != job_id:
            response["root_job_id"] = root_job_id
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar checkpoints do job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@video_orchestrator_bp.route('/video/job/<job_id>/checkpoints/<step_name>', methods=['GET'])
def get_step_checkpoint(job_id: str, step_name: str):
    """
    GET /api/video/job/{job_id}/checkpoints/{step_name}
    
    🆕 v3.10.0: Retorna o payload completo de um checkpoint específico.
    
    Usado pelo LLM Director para inspecionar o estado exato de um step
    antes de decidir quais campos modificar para replay.
    
    Response:
    {
        "job_id": "uuid",
        "step_name": "generate_pngs",
        "found": true,
        "state": {
            "job_id": "...",
            "phrase_groups": [...],
            "png_results": {...},
            "completed_steps": [...],
            ...
        }
    }
    """
    try:
        from .debug_logger import get_debug_logger
        
        debug = get_debug_logger()
        include_parent = request.args.get('include_parent', 'false').lower() == 'true'
        
        payload = debug.get_step_checkpoint(job_id, step_name)
        source_job_id = job_id
        
        # 🆕 v4.4.2: Se não encontrou e include_parent, buscar no job raiz
        if payload is None and include_parent:
            root_job_id = _find_root_job_id(job_id)
            if root_job_id and root_job_id != job_id:
                payload = debug.get_step_checkpoint(root_job_id, step_name)
                if payload:
                    source_job_id = root_job_id
                    logger.info(
                        f"📋 [CHECKPOINTS] Step '{step_name}' encontrado no job pai "
                        f"{root_job_id[:8]} (não presente no replay {job_id[:8]})"
                    )
        
        if payload is None:
            return jsonify({
                "job_id": job_id,
                "step_name": step_name,
                "found": False,
                "message": f"Checkpoint não encontrado para step '{step_name}'"
            }), 404
        
        response = {
            "job_id": job_id,
            "step_name": step_name,
            "found": True,
            "state": payload,
        }
        if source_job_id != job_id:
            response["source_job_id"] = source_job_id
            response["from_parent"] = True
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar checkpoint {step_name} do job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════
# 🆕 v3.4.0: Pipeline Engine v3 - Debug & Status Endpoints
# ═══════════════════════════════════════════════════════════════

@video_orchestrator_bp.route('/video/engine/status', methods=['GET'])
def engine_status():
    """
    GET /api/video/engine/status
    
    Retorna status do Pipeline Engine v3: feature flag, steps registrados,
    e informações de debug para suporte.
    
    Response:
    {
        "engine_v3_enabled": true/false,
        "steps_registered": ["load_template", "normalize", ...],
        "step_count": 19,
        "categories": {"setup": 1, "preprocessing": 8, "rendering": 9, "output": 1},
        "tools_available": 22,
        "version": "3.0.0"
    }
    """
    try:
        from app.video_orchestrator.engine.step_registry import StepRegistry

        steps = StepRegistry.all()
        step_names = [s.name for s in steps]
        
        # Contar categorias
        categories = {}
        for s in steps:
            cat = s.category or 'default'
            categories[cat] = categories.get(cat, 0) + 1

        # Tools para LLM Director
        tools = StepRegistry.get_tools_for_director()

        return jsonify({
            "engine_enabled": True,
            "steps_registered": step_names,
            "step_count": len(steps),
            "categories": categories,
            "tools_available": len(tools),
            "version": "3.0.0",
        })

    except Exception as e:
        logger.error(f"❌ Erro no engine status: {e}")
        return jsonify({"error": str(e)}), 500


@video_orchestrator_bp.route('/video/engine/job/<job_id>', methods=['GET'])
def engine_job_state(job_id: str):
    """
    GET /api/video/engine/job/{job_id}
    
    Retorna o PipelineState de um job (se existir no Engine v3).
    Útil para debug e suporte.
    
    Response:
    {
        "found": true,
        "state": {
            "job_id": "...",
            "completed_steps": [...],
            "failed_step": null,
            "step_timings": {...},
            ...
        },
        "summary": {...}
    }
    """
    try:
        from app.video_orchestrator.engine.state_manager import StateManager

        sm = StateManager()
        state = sm.load(job_id)

        if not state:
            return jsonify({
                "found": False,
                "message": f"Nenhum PipelineState v3 para job {job_id}. "
                           f"Pode estar usando o orchestrator legado."
            }), 404

        return jsonify({
            "found": True,
            "summary": state.summary(),
            "completed_steps": state.completed_steps,
            "skipped_steps": state.skipped_steps,
            "failed_step": state.failed_step,
            "step_timings": state.step_timings,
            "error_message": state.error_message,
            "engine_version": state.engine_version,
            "created_at": state.created_at,
        })

    except Exception as e:
        logger.error(f"❌ Erro ao buscar engine state para {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════
# 🆕 v4.4.2: Helper para encontrar root job (cadeia de replays)
# ═══════════════════════════════════════════════════════════════

def _find_root_job_id(job_id: str, max_depth: int = 5) -> str:
    """
    Percorre a cadeia de replays para encontrar o job raiz (que tem todos os checkpoints).
    
    Replay jobs têm pipeline_state.options._replay_params.original_job_id apontando
    para o job anterior. Seguimos essa cadeia até encontrar o job original.
    
    Args:
        job_id: ID do job atual
        max_depth: Profundidade máxima para evitar loops infinitos
    
    Returns:
        ID do job raiz, ou o próprio job_id se não for um replay
    """
    try:
        from app.db import get_db_connection
        
        current_id = job_id
        for _ in range(max_depth):
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT pipeline_state->'options'->'_replay_params'->>'original_job_id'
                FROM video_processing_jobs
                WHERE job_id = %s
            """, (current_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if not row or not row[0]:
                # Não é replay ou não tem parent → este é o root
                return current_id
            
            parent_id = row[0]
            if parent_id == current_id:
                # Safety: evitar loop
                return current_id
            current_id = parent_id
        
        return current_id
        
    except Exception as e:
        logger.warning(f"⚠️ [CHECKPOINTS] Erro ao buscar root job para {job_id}: {e}")
        return job_id

