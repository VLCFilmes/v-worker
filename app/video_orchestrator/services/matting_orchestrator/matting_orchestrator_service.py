"""
Matting Orchestrator Service (v3.6.0 — Modal-only)

Serviço dedicado para orquestração completa do processo de matting (background removal).

Responsabilidades:
- Decidir modo de operação (TECTONIC, VIRTUAL, HYBRID)
- Preparar matting_segments
- Processar clips via Modal.com (v-matting-modal)
- Aplicar configurações do template (edge refinement, border effects)
- Queue routing inteligente entre workers Modal

Modos Suportados:
- TECTONIC: Usar placas do silence_cut (speech_segments disponíveis)
- VIRTUAL: Criar placas virtuais agrupando frases (sem speech_segments)
- HYBRID: Processar múltiplos clips separadamente
"""

import os
import logging
import time
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class MattingOrchestratorService:
    """
    Orquestra o processo completo de matting (background removal) via Modal.com.
    """
    
    def __init__(self, job_manager):
        """
        Inicializa o Matting Orchestrator Service.
        
        Args:
            job_manager: Instância de JobManager
        """
        self.job_manager = job_manager
        logger.info("🎭 [MATTING ORCHESTRATOR] Serviço inicializado")
    
    def execute_matting(
        self,
        job_id: str,
        phrase_groups: list,
        job,
        template_config: dict,
        speech_segments: list = None
    ) -> dict:
        """
        Executa todo o fluxo de matting via Modal.com.
        
        Args:
            job_id: ID do job
            phrase_groups: Lista de frases com timestamps
            job: Objeto VideoJob
            template_config: Configurações do template
            speech_segments: Clips do silence_cut (opcional, usado no modo TECTONIC)
        
        Returns:
            {
                'status': 'success' | 'error',
                'time': float,  # Tempo de execução em segundos
                'mode': 'tectonic' | 'virtual' | 'hybrid' | 'legacy',
                'clips_processed': int,
                'error': str (se status == 'error')
            }
        """
        _start = time.time()
        phrases_with_matting = [p for p in phrase_groups if p.get('person_overlay_enabled', False)]
        
        logger.info(f"🎭 [MATTING ORCHESTRATOR] Executando matting para job {job_id}")
        logger.info(f"   Frases com matting: {len(phrases_with_matting)}")
        
        try:
            # Obter job_current atualizado
            use_hybrid_cut = job.options.get('use_hybrid_silence_cut', True) if job.options else True
            job_current = self.job_manager.get_job(job_id)
            
            # Usar speech_segments passados como parâmetro, ou buscar do job
            if speech_segments is None:
                speech_segments = getattr(job_current, 'speech_segments', None) if job_current else None
            
            # ═══════════════════════════════════════════════════════════════
            # 🆕 v2.9.46: MATTING UNIFICADO - Decide automaticamente entre:
            # 
            # 1. MODO TECTÔNICO (speech_segments disponíveis):
            #    - Temos speech_segments (clips de vídeo já cortados)
            #    - V-matting usa esses clips INTEIROS
            #    - Placas já possuem URLs próprias
            #
            # 2. MODO VIRTUAL (SEM speech_segments - corte de silêncio desabilitado):
            #    - Não há placas tectônicas reais
            #    - Cria "placas virtuais" baseadas nas frases com matting
            #    - Agrupa frases consecutivas (gap < 500ms)
            #    - CORTA o vídeo original nos timestamps das placas virtuais
            #    - Envia apenas os trechos necessários ao v-matting
            # ═══════════════════════════════════════════════════════════════
            
            from app.video_orchestrator.services.tectonic_plates_service import get_matting_segments_for_phase2
            
            # Obter vídeo original/concatenado e sua duração
            original_video_url = getattr(job_current, 'original_video_url', None) if job_current else None
            original_video_duration = job.options.get('video_duration', 0) if job.options else 0  # Em segundos
            
            # Se não temos original_video_url, usar phase1_video_url
            if not original_video_url:
                original_video_url = job.phase1_video_url or job.output_video_url
                logger.info(f"   📹 Usando phase1_video_url como vídeo base: {original_video_url[:60] if original_video_url else 'None'}...")
            
            # 🆕 v2.9.51: Múltiplas fontes para duração
            # Prioridade:
            # 1. job.options['video_duration'] - definido manualmente
            # 2. job.total_duration_ms - salvo pelo normalize (NO_SILENCE_CUT) ou silence_cut
            # 3. Somar durações das placas tectônicas (se existirem)
            if not original_video_duration:
                # Tentar total_duration_ms do job
                if job_current and job_current.total_duration_ms:
                    original_video_duration = job_current.total_duration_ms / 1000.0
                    logger.info(f"   ⏱️ Duração do job.total_duration_ms: {original_video_duration:.2f}s")
                elif speech_segments:
                    # Somar durações das placas tectônicas
                    original_video_duration = sum(s.get('duration', 0) for s in speech_segments)
                    logger.info(f"   ⏱️ Duração calculada das placas: {original_video_duration:.2f}s")
                else:
                    # 🆕 Último fallback: calcular da última frase
                    if phrase_groups:
                        last_phrase = phrase_groups[-1]
                        last_end = last_phrase.get('end_time') or last_phrase.get('end', 0)
                        # Se end_time > 1000, provavelmente está em ms
                        if last_end > 1000:
                            original_video_duration = last_end / 1000.0
                        else:
                            original_video_duration = last_end
                        logger.info(f"   ⏱️ Duração estimada da última frase: {original_video_duration:.2f}s")
            
            # Chamar função unificada para obter matting segments
            matting_info = get_matting_segments_for_phase2(
                speech_segments=speech_segments if speech_segments else [],
                phrase_groups=phrase_groups,
                original_video_url=original_video_url,
                original_video_duration=original_video_duration,
                gap_threshold_ms=500  # Gap para merge de placas virtuais
            )
            
            matting_mode = matting_info['mode']
            matting_plates = matting_info['plates']
            matting_needs_cutting = matting_info['needs_cutting']
            
            logger.info(f"🎭 [MATTING ORCHESTRATOR {matting_mode.upper()}] Modo: {matting_mode}")
            logger.info(f"   📊 Stats: {matting_info['stats']}")
            
            if matting_mode == 'none':
                logger.info(f"   ℹ️ Modo 'none' - nenhuma frase com matting para processar")
                return {
                    'status': 'success',
                    'time': time.time() - _start,
                    'mode': 'none',
                    'clips_processed': 0
                }
            
            # Preparar matting_segments baseado no modo
            matting_segments = self._prepare_matting_segments(
                matting_mode=matting_mode,
                matting_plates=matting_plates,
                original_video_url=original_video_url
            )
            
            if not matting_segments:
                logger.warning("   ⚠️ Nenhum matting_segment preparado")
                return {
                    'status': 'success',
                    'time': time.time() - _start,
                    'mode': matting_mode,
                    'clips_processed': 0
                }
            
            # Detectar modo de processamento (HYBRID vs outros)
            is_hybrid_matting = any(seg.get('_hybrid_mode', False) for seg in matting_segments)
            
            if is_hybrid_matting:
                # Modo HYBRID: Processar clips separados
                result = self._execute_hybrid_matting(
                    job_id=job_id,
                    job=job,
                    matting_segments=matting_segments,
                    template_config=template_config
                )
            else:
                # Modo VIRTUAL ou LEGACY: Processar com vídeo único
                result = self._execute_virtual_or_legacy_matting(
                    job_id=job_id,
                    job=job,
                    matting_segments=matting_segments,
                    original_video_url=original_video_url,
                    template_config=template_config
                )
            
            elapsed = time.time() - _start
            logger.info(f"✅ [MATTING ORCHESTRATOR] Concluído em {elapsed:.2f}s")
            
            return {
                'status': result.get('status', 'success'),
                'time': elapsed,
                'mode': matting_mode,
                'clips_processed': result.get('clips_processed', 0)
            }
        
        except Exception as e:
            logger.error(f"❌ [MATTING ORCHESTRATOR] Erro: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'error': str(e),
                'time': time.time() - _start
            }
    
    def _prepare_matting_segments(
        self,
        matting_mode: str,
        matting_plates: list,
        original_video_url: str = None
    ) -> list:
        """
        Prepara matting_segments baseado no modo de operação.
        
        Args:
            matting_mode: 'tectonic', 'virtual', 'hybrid', ou 'legacy'
            matting_plates: Lista de placas do tectonic_plates_service
            original_video_url: URL do vídeo original (para modo VIRTUAL)
        
        Returns:
            Lista de matting_segments prontos para processamento
        """
        matting_segments = []
        
        if matting_mode == 'tectonic':
            # ═══════════════════════════════════════════════════════════
            # MODO TECTÔNICO: Usar placas do silence_cut
            # ═══════════════════════════════════════════════════════════
            logger.info(f"🌍 [TECTONIC PLATES] {len(matting_plates)} placas para matting")
            
            # Converter para formato esperado pelo matting_service
            for plate in matting_plates:
                matting_segments.append({
                    "start": plate['original_start'],
                    "end": plate['original_end'],
                    "video_start_from": 0.0,  # Usar placa inteira
                    "duration": plate['duration'],
                    "text": f"Placa {plate.get('index', 0)}",
                    "phrase_indices": plate.get('phrase_indices', []),
                    # Metadados extras para o modo híbrido
                    "_hybrid_mode": True,
                    "_clip_url": plate.get('url'),
                    "_clip_shared_path": plate.get('shared_path'),
                    "_audio_offset": plate.get('audio_offset', 0)
                })
            
            for i, seg in enumerate(matting_segments):
                logger.info(f"   📍 Clip {i}: {seg['start']:.2f}s - {seg['end']:.2f}s ({seg['duration']:.1f}s)")
        
        elif matting_mode == 'virtual':
            # ═══════════════════════════════════════════════════════════
            # 🆕 v2.9.46: MODO VIRTUAL - Envia vídeo original + timestamps
            # 
            # O v-matting já aceita matting_segments para cortar trechos
            # específicos, então não precisamos cortar antes!
            # Vantagem: evita uploads intermediários de clips cortados
            # ═══════════════════════════════════════════════════════════
            logger.info(f"🎭 [VIRTUAL PLATES] {len(matting_plates)} placas virtuais")
            logger.info(f"   📹 Vídeo original: {original_video_url[:60] if original_video_url else 'None'}...")
            
            if original_video_url:
                # Converter placas virtuais para formato de matting_segments
                # O v-matting vai cortar e processar cada trecho separadamente
                for plate in matting_plates:
                    matting_segments.append({
                        "start": plate['original_start'],
                        "end": plate['original_end'],
                        "video_start_from": 0.0,
                        "duration": plate['duration'],
                        "text": f"Placa Virtual {plate.get('index', 0)}",
                        "phrase_indices": plate.get('phrase_indices', []),
                        # Metadados para modo virtual
                        "_hybrid_mode": False,  # NÃO é híbrido - usamos o vídeo original
                        "_virtual_mode": True,  # Marca como placa virtual
                        "_video_url": original_video_url,  # URL do vídeo completo
                        "_audio_offset": plate['original_start']  # Offset = tempo no vídeo original
                    })
                    logger.info(f"   📍 Placa {plate['index']}: {plate['original_start']:.2f}s → {plate['original_end']:.2f}s ({plate['duration']:.2f}s)")
                
                logger.info(f"   ✅ {len(matting_segments)} placas virtuais preparadas para v-matting")
            else:
                logger.warning(f"   ⚠️ Modo virtual requer vídeo original, mas não temos URL!")
        
        return matting_segments
    
    def _execute_hybrid_matting(
        self,
        job_id: str,
        job,
        matting_segments: list,
        template_config: dict
    ) -> dict:
        """
        Executa matting em modo HYBRID (placas tectônicas).
        
        Os clips do silence_cut são as "placas tectônicas" - NUNCA cortamos
        elas novamente. Cada clip é processado INTEIRO pelo v-matting.
        
        Args:
            job_id: ID do job
            job: Objeto VideoJob
            matting_segments: Segments com _hybrid_mode=True
            template_config: Configurações do template
        
        Returns:
            {
                'status': 'success' | 'error',
                'clips_processed': int
            }
        """
        logger.info(f"🔀 [MATTING HYBRID v2.9.37] Modo PLACAS TECTÔNICAS ativado!")
        logger.info(f"   Os clips JÁ estão cortados pelo silence_cut - NÃO vamos cortar novamente!")
        
        # Coletar clips únicos para processar
        clips_to_process = []
        for seg in matting_segments:
            clip_url = seg.get('_clip_url')
            if clip_url and clip_url not in [c['url'] for c in clips_to_process]:
                clips_to_process.append({
                    'url': clip_url,
                    'shared_path': seg.get('_clip_shared_path'),
                    'duration': seg.get('duration', 0),
                    'original_start': seg.get('start', 0),
                    'original_end': seg.get('end', 0),
                    'segment_index': seg.get('segment_index', len(clips_to_process)),
                    'audio_offset': seg.get('audio_offset', 0),
                    '_audio_offset': seg.get('_audio_offset', 0),
                    'phrase_indices': seg.get('phrase_indices', [])
                })
        
        logger.info(f"   📹 {len(clips_to_process)} clips (placas tectônicas) para processar")
        
        if not clips_to_process:
            return {'status': 'success', 'clips_processed': 0}
        
        # Preparar configurações do template
        from app.utils.b2_paths import generate_project_path
        project_path = None
        if job.user_id and job.project_id and job.conversation_id:
            project_path = generate_project_path(
                user_id=job.user_id,
                project_id=job.project_id,
                conversation_id=job.conversation_id
            )
        
        # Extrair configurações do template
        template_configs = self._extract_template_configs(template_config)
        
        logger.info(f"   📐 Template: FPS={template_configs['fps']}, Resolution={template_configs['resolution']}")
        logger.info(f"   📊 Downsample: {template_configs['downsample_ratio']}")
        
        # 🆕 v3.8.2: Sempre usar alpha_only (luma matte) para v-editor-python
        # WebM VP9 alpha era para v-editor-remotion (legado, removido)
        # Luma matte gera _alpha.mp4 (grayscale mask) + _base.mp4 (RGB video)
        import os
        use_alpha_only = os.environ.get('MODAL_OUTPUT_FORMAT', 'alpha_only') == 'alpha_only'
        if not use_alpha_only:
            # Fallback: checar editor_worker_id para compatibilidade
            editor_worker_id = job.options.get('editor_worker_id') if job.options else None
            use_alpha_only = editor_worker_id == 'python'
        logger.info(f"   🎭 [OUTPUT_FORMAT] use_alpha_only={use_alpha_only} (env={os.environ.get('MODAL_OUTPUT_FORMAT', 'N/A')})")
        
        # Queue Router - Roteamento inteligente entre workers Modal
        from app.video_orchestrator.services.queue_router_service import get_queue_router
        from app.video_orchestrator.pipeline_events import emit_step_start, emit_step_complete, emit_step_error
        
        total_matting_duration = sum(clip.get('duration', 0) for clip in clips_to_process)
        
        emit_step_start(job_id, 'MATTING', metadata={
            'segments': len(clips_to_process),
            'total_duration': round(total_matting_duration, 2)
        })
        num_segments = len(clips_to_process)
        
        queue_router = get_queue_router()
        worker_override = job.options.get('worker_override') if job.options else None
        current_template_id = template_config.get('template_id') if template_config else None
        if not current_template_id:
            current_template_id = getattr(job, 'template_id', None)
        routing_decision = queue_router.route(
            job_id=job_id,
            video_duration=total_matting_duration,
            segments=num_segments,
            template_id=current_template_id,
            applies_to='matting',
            worker_override=worker_override
        )
        
        selected_worker = routing_decision['worker_id']
        
        logger.info(f"   🚦 [QUEUE_ROUTER] Worker selecionado: {selected_worker}")
        logger.info(f"      Regra: {routing_decision.get('rule_name', 'N/A')}")
        logger.info(f"      Razão: {routing_decision.get('reason', 'N/A')}")
        
        # Processar via Modal
        result = self._process_via_modal(
            clips_to_process=clips_to_process,
            job_id=job_id,
            job=job,
            project_path=project_path,
            template_configs=template_configs,
            use_alpha_only=use_alpha_only,
            selected_worker=selected_worker
        )
        
        # Salvar resultados no job
        if result['foreground_segments']:
            logger.info(f"✅ [MATTING HYBRID] {len(result['foreground_segments'])} foregrounds gerados!")
            self.job_manager.set_output(
                job_id,
                matted_video_url=None,
                matting_segments=matting_segments,
                foreground_segments=result['foreground_segments']
            )
            # Emitir evento de sucesso
            emit_step_complete(job_id, 'MATTING', metadata={'segments': len(result['foreground_segments'])})
        else:
            logger.warning("⚠️ [MATTING HYBRID] Nenhum foreground gerado!")
            emit_step_error(job_id, 'MATTING', 'No foreground segments generated')
        
        return {
            'status': 'success' if result['foreground_segments'] else 'error',
            'clips_processed': result['clips_processed']
        }
    
    def _execute_virtual_or_legacy_matting(
        self,
        job_id: str,
        job,
        matting_segments: list,
        original_video_url: str,
        template_config: dict
    ) -> dict:
        """
        Executa matting em modo VIRTUAL ou LEGACY.
        
        MODO VIRTUAL:
        1. Pré-corta segmentos no v-services (FFmpeg)
        2. Recebe clips cortados
        3. Processa como HYBRID (cada clip inteiro)
        
        Args:
            job_id: ID do job
            job: Objeto VideoJob
            matting_segments: Segments com _virtual_mode=True ou LEGACY
            original_video_url: URL do vídeo original
            template_config: Configurações do template
        
        Returns:
            {
                'status': 'success' | 'error',
                'clips_processed': int
            }
        """
        logger.info(f"🎭 [MATTING VIRTUAL v2.9.55] Modo PLACAS VIRTUAIS ativado!")
        logger.info(f"   PRÉ-CORTE será feito no v-services (FFmpeg moderno)")
        
        # Obter URL do vídeo original dos segmentos
        virtual_video_url = original_video_url
        if not virtual_video_url:
            for seg in matting_segments:
                if seg.get('_video_url'):
                    virtual_video_url = seg['_video_url']
                    break
        
        if not virtual_video_url:
            logger.error("   ❌ Modo virtual requer vídeo original, mas não temos URL!")
            return {
                'status': 'error',
                'error': 'No video URL for virtual mode',
                'clips_processed': 0
            }
        
        logger.info(f"   📹 Vídeo original: {virtual_video_url[:80] if len(virtual_video_url) > 80 else virtual_video_url}...")
        logger.info(f"   ✂️ Enviando {len(matting_segments)} segmentos para pré-corte no v-services...")
        
        # Preparar segmentos para corte
        segments_to_cut = []
        for seg in matting_segments:
            segments_to_cut.append({
                "start": seg.get('start', 0),
                "end": seg.get('end', 0),
                "index": seg.get('segment_index', len(segments_to_cut)),
                "original_start": seg.get('start', 0),
                "original_end": seg.get('end', 0),
                "phrase_indices": seg.get('phrase_indices', []),
                "_audio_offset": seg.get('_audio_offset', seg.get('start', 0))
            })
        
        # Chamar v-services para pré-cortar
        import os
        import requests
        v_services_url = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')
        
        try:
            logger.info(f"   🔗 Chamando {v_services_url}/ffmpeg/cut_segments...")
            cut_response = requests.post(
                f"{v_services_url}/ffmpeg/cut_segments",
                json={
                    "video_url": virtual_video_url,
                    "segments": segments_to_cut,
                    "job_id": job_id,
                    "output_prefix": "virtual_plate",
                    "quality": 23,
                    "preset": "fast"
                },
                timeout=300  # 5 minutos
            )
            
            if cut_response.status_code != 200:
                logger.error(f"   ❌ Erro ao cortar segmentos: HTTP {cut_response.status_code}")
                logger.error(f"      {cut_response.text[:200]}")
                return {
                    'status': 'error',
                    'error': f'Failed to cut segments: HTTP {cut_response.status_code}',
                    'clips_processed': 0
                }
            
            cut_result = cut_response.json()
            pre_cut_segments = cut_result.get('segments', [])
            
            if not pre_cut_segments:
                logger.error(f"   ❌ Nenhum segmento cortado retornado!")
                return {
                    'status': 'error',
                    'error': 'No segments returned from cut service',
                    'clips_processed': 0
                }
            
            logger.info(f"   ✅ {len(pre_cut_segments)} segmentos pré-cortados!")
            
            # Converter para formato de clips_to_process (como no modo tectônico)
            clips_to_process = []
            for seg in pre_cut_segments:
                clips_to_process.append({
                    'url': seg['url'],
                    'shared_path': seg.get('shared_path'),
                    'duration': seg.get('actual_duration', seg.get('requested_duration', 0)),
                    'original_start': seg.get('original_start', seg.get('start', 0)),
                    'original_end': seg.get('original_end', seg.get('end', 0)),
                    'segment_index': seg.get('index', 0),
                    'audio_offset': seg.get('_audio_offset', seg.get('start', 0)),
                    '_audio_offset': seg.get('_audio_offset', seg.get('start', 0)),
                    'phrase_indices': seg.get('phrase_indices', [])
                })
            
            logger.info(f"   🎭 Processando {len(clips_to_process)} clips no v-matting...")
            
            # Preparar configurações do template
            from app.utils.b2_paths import generate_project_path
            project_path = None
            if job.user_id and job.project_id and job.conversation_id:
                project_path = generate_project_path(
                    user_id=job.user_id,
                    project_id=job.project_id,
                    conversation_id=job.conversation_id
                )
            
            # Extrair configurações do template
            template_configs = self._extract_template_configs(template_config)
            
            logger.info(f"   📐 Template: FPS={template_configs['fps']}, Resolution={template_configs['resolution']}")
            
            # 🆕 v3.8.2: Sempre usar alpha_only (luma matte) para v-editor-python
            import os
            use_alpha_only = os.environ.get('MODAL_OUTPUT_FORMAT', 'alpha_only') == 'alpha_only'
            if not use_alpha_only:
                editor_worker_id = job.options.get('editor_worker_id') if job.options else None
                use_alpha_only = editor_worker_id == 'python'
            logger.info(f"   🎭 [OUTPUT_FORMAT] use_alpha_only={use_alpha_only}")
            
            # Queue Router para selecionar worker Modal
            from app.video_orchestrator.services.queue_router_service import get_queue_router
            from app.video_orchestrator.pipeline_events import emit_step_start, emit_step_complete, emit_step_error
            
            total_matting_duration = sum(clip.get('duration', 0) for clip in clips_to_process)
            num_segments = len(clips_to_process)
            
            emit_step_start(job_id, 'MATTING', metadata={
                'segments': num_segments,
                'total_duration': round(total_matting_duration, 2)
            })
            
            queue_router = get_queue_router()
            worker_override = job.options.get('worker_override') if job.options else None
            current_template_id = template_config.get('template_id') if template_config else None
            if not current_template_id:
                current_template_id = getattr(job, 'template_id', None)
            
            routing_decision = queue_router.route(
                job_id=job_id,
                video_duration=total_matting_duration,
                segments=num_segments,
                template_id=current_template_id,
                applies_to='matting',
                worker_override=worker_override
            )
            
            selected_worker = routing_decision['worker_id']
            
            logger.info(f"   🚦 [QUEUE_ROUTER] Worker selecionado: {selected_worker}")
            logger.info(f"      Regra: {routing_decision.get('rule_name', 'N/A')}")
            logger.info(f"      Razão: {routing_decision.get('reason', 'N/A')}")
            
            # Processar via Modal
            result = self._process_via_modal(
                clips_to_process=clips_to_process,
                job_id=job_id,
                job=job,
                project_path=project_path,
                template_configs=template_configs,
                use_alpha_only=use_alpha_only,
                selected_worker=selected_worker
            )
            
            # Salvar resultados no job
            if result['foreground_segments']:
                logger.info(f"✅ [MATTING VIRTUAL] {len(result['foreground_segments'])} foregrounds gerados!")
                self.job_manager.set_output(
                    job_id,
                    matted_video_url=None,
                    matting_segments=matting_segments,
                    foreground_segments=result['foreground_segments']
                )
                emit_step_complete(job_id, 'MATTING', metadata={'segments': len(result['foreground_segments'])})
            else:
                logger.warning("⚠️ [MATTING VIRTUAL] Nenhum foreground gerado!")
                emit_step_error(job_id, 'MATTING', 'No foreground segments generated')
            
            return {
                'status': 'success' if result['foreground_segments'] else 'error',
                'clips_processed': result['clips_processed']
            }
        
        except requests.exceptions.Timeout:
            logger.error(f"   ❌ Timeout ao cortar segmentos (>5min)")
            emit_step_error(job_id, 'MATTING', 'Timeout cutting segments')
            return {
                'status': 'error',
                'error': 'Timeout cutting segments',
                'clips_processed': 0
            }
        except Exception as e:
            logger.error(f"   ❌ Erro ao processar modo virtual: {e}")
            import traceback
            logger.error(traceback.format_exc())
            emit_step_error(job_id, 'MATTING', str(e))
            return {
                'status': 'error',
                'error': str(e),
                'clips_processed': 0
            }
    
    def _extract_template_configs(self, template_config: dict) -> dict:
        """
        Extrai configurações relevantes do template.
        
        Args:
            template_config: Template configuration dict
        
        Returns:
            {
                'fps': int,
                'resolution': str | None,
                'downsample_ratio': float,
                'edge_refinement': dict | None,
                'border_effect': dict | None
            }
        """
        result = {
            'fps': 30,
            'resolution': None,
            'downsample_ratio': 0.25,
            'edge_refinement': None,
            'border_effect': None
        }
        
        if not template_config:
            return result
        
        # Extrair FPS e resolução
        vs = template_config.get('project-settings', {}).get('video_settings', {})
        fps_raw = vs.get('fps')
        result['fps'] = fps_raw.get('value', 30) if isinstance(fps_raw, dict) else (fps_raw or 30)
        
        w_raw = vs.get('width')
        h_raw = vs.get('height')
        w = w_raw.get('value') if isinstance(w_raw, dict) else w_raw
        h = h_raw.get('value') if isinstance(h_raw, dict) else h_raw
        if w and h:
            result['resolution'] = f"{int(w)}x{int(h)}"
        
        # Extrair configurações de matting
        matting_config = template_config.get('matting', {})
        downsample_raw = matting_config.get('processing', {}).get('downsample_ratio')
        result['downsample_ratio'] = downsample_raw.get('value', 0.25) if isinstance(downsample_raw, dict) else (downsample_raw or 0.25)
        
        # Edge Refinement
        edge_config = matting_config.get('edge_refinement', {})
        edge_enabled = edge_config.get('enabled', {}).get('value', False) if isinstance(edge_config.get('enabled'), dict) else bool(edge_config.get('enabled'))
        if edge_enabled:
            result['edge_refinement'] = {
                'enabled': edge_enabled,
                'erode_pixels': edge_config.get('erode_pixels', {}).get('value', 2) if isinstance(edge_config.get('erode_pixels'), dict) else (edge_config.get('erode_pixels') or 2),
                'feather_pixels': edge_config.get('feather_pixels', {}).get('value', 1) if isinstance(edge_config.get('feather_pixels'), dict) else (edge_config.get('feather_pixels') or 1),
                'threshold': edge_config.get('threshold', {}).get('value', 0.5) if isinstance(edge_config.get('threshold'), dict) else (edge_config.get('threshold') or 0.5)
            }
        
        # Border Effect
        border_config = matting_config.get('border_effect', {})
        border_enabled = border_config.get('enabled', {}).get('value', False) if isinstance(border_config.get('enabled'), dict) else bool(border_config.get('enabled'))
        if border_enabled:
            result['border_effect'] = {
                'enabled': border_enabled,
                'type': border_config.get('type', {}).get('value', 'solid') if isinstance(border_config.get('type'), dict) else (border_config.get('type') or 'solid'),
                'thickness_px': border_config.get('thickness_px', {}).get('value', 3) if isinstance(border_config.get('thickness_px'), dict) else (border_config.get('thickness_px') or 3),
                'color': border_config.get('color', {}).get('value', '255,215,0') if isinstance(border_config.get('color'), dict) else (border_config.get('color') or '255,215,0')
            }
        
        return result
    
    def _process_via_modal(
        self,
        clips_to_process: list,
        job_id: str,
        job,
        project_path: str,
        template_configs: dict,
        use_alpha_only: bool,
        selected_worker: str
    ) -> dict:
        """
        Processa clips via Modal.com em PARALELO.
        
        v4.2: Usa ThreadPoolExecutor para enviar múltiplos clips simultaneamente.
        
        Args:
            clips_to_process: Lista de clips
            job_id: ID do job
            job: Objeto VideoJob
            project_path: Caminho do projeto no B2
            template_configs: Configurações do template
            use_alpha_only: Se deve gerar alpha_only
            selected_worker: Worker Modal selecionado
        
        Returns:
            {
                'foreground_segments': list,
                'clips_processed': int
            }
        """
        import os
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from app.video_orchestrator.services.modal_matting_service import ModalMattingService
        
        MAX_CONCURRENT_MATTING = int(os.environ.get('PIPELINE_MAX_CONCURRENT_MATTING', '4'))
        
        # Criar instância com worker específico
        modal_service = ModalMattingService(worker_id=selected_worker)
        num_clips = len(clips_to_process)
        max_workers = min(MAX_CONCURRENT_MATTING, num_clips)
        logger.info(f"   🚀 [MODAL] Usando Modal.com (worker: {selected_worker})")
        logger.info(f"   🔀 [MODAL] Processamento PARALELO: {num_clips} clips, max_concurrent={max_workers}")
        
        foreground_segments = []
        clips_processed = 0
        
        def _process_single_clip(clip_idx, clip):
            """Processa um único clip via Modal (thread-safe)."""
            logger.info(f"   📹 [{clip_idx+1}/{num_clips}] Processando clip via Modal...")
            logger.info(f"      URL: {clip['url'][:80] if len(clip['url']) > 80 else clip['url']}...")
            logger.info(f"      Duração: {clip.get('duration', 0):.2f}s")
            
            result = modal_service.process_segment(
                video_url=clip['url'],
                job_id=job_id,
                segment_index=clip.get('segment_index', clip_idx),
                user_id=job.user_id,
                project_path=project_path,
                output_format='alpha_only' if use_alpha_only else 'webm',
                original_start=clip.get('original_start', 0),
                original_end=clip.get('original_end', 0),
                audio_offset=clip.get('_audio_offset', 0),
                duration=clip.get('duration', 0),
                skip_merge=use_alpha_only
            )
            return clip_idx, clip, result
        
        # ─── Executar em paralelo ───
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process_single_clip, idx, clip): idx
                for idx, clip in enumerate(clips_to_process)
            }
            
            for future in as_completed(futures):
                clip_idx = futures[future]
                try:
                    clip_idx, clip, result = future.result()
                    
                    if result.get('foreground_url'):
                        original_start_ms = int(clip.get('original_start', 0) * 1000) if isinstance(clip.get('original_start', 0), float) else clip.get('original_start', 0)
                        original_end_ms = int(clip.get('original_end', 0) * 1000) if isinstance(clip.get('original_end', 0), float) else clip.get('original_end', 0)
                        
                        foreground_segment = {
                            'foreground_url': result['foreground_url'],
                            'segment_index': clip.get('segment_index', clip_idx),
                            'original_start': clip.get('original_start', 0),
                            'original_end': clip.get('original_end', 0),
                            'audio_offset': clip.get('_audio_offset', 0),
                            'duration': clip.get('duration', 0),
                            'phrase_indices': clip.get('phrase_indices', []),
                            'id': f"person_overlay_{clip_idx}",
                            'zIndex': 600,
                            'start_time': original_start_ms,
                            'end_time': original_end_ms,
                            'position': {
                                'x': 0,
                                'y': 0,
                                'width': "100%",
                                'height': "100%"
                            }
                        }
                        
                        if use_alpha_only and result.get('base_video_url'):
                            foreground_segment['base_video_url'] = result['base_video_url']
                            foreground_segment['mask_url'] = result['foreground_url']
                            foreground_segment['original_video_url'] = result['base_video_url']
                            foreground_segment['src'] = result['foreground_url']
                            logger.info(f"      🎭 Luma Matte: mask_url + original_video_url, zIndex=600, timing={original_start_ms}-{original_end_ms}ms")
                        
                        foreground_segments.append(foreground_segment)
                        clips_processed += 1
                        logger.info(f"      ✅ [{clip_idx+1}/{num_clips}] Foreground gerado: {result['foreground_url'][:80]}...")
                    else:
                        logger.warning(f"      ⚠️ [{clip_idx+1}/{num_clips}] Nenhum foreground gerado")
                
                except Exception as e:
                    logger.error(f"      ❌ [{clip_idx+1}/{num_clips}] Erro ao processar clip via Modal: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
        
        # Ordenar por segment_index para manter ordem correta
        foreground_segments.sort(key=lambda s: s.get('segment_index', 0))
        
        logger.info(f"   ✅ [MODAL] {clips_processed}/{num_clips} clips processados (paralelo, max={max_workers})")
        
        return {
            'foreground_segments': foreground_segments,
            'clips_processed': clips_processed
        }
    
