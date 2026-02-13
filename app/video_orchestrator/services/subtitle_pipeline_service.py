"""
🎬 Subtitle Pipeline Service - Orquestra Steps 11-15

Este serviço coordena todo o fluxo de posicionamento e renderização:
1. Positioning (Step 11): Calcular X,Y de cada palavra
2. Global Position (Step 13): Aplicar posição global do template
3. Payload Builder (Step 14): Montar payload para Remotion
4. Render (Step 15): Enviar para v-editor

Fluxo:
png_results → positioning → payload_builder → render_service → v-editor
"""

import os
import json
import logging
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# 🆕 Importar debug_logger para salvar payloads no banco
try:
    from app.video_orchestrator.debug_logger import debug_logger
except ImportError:
    debug_logger = None
    logger.warning("⚠️ debug_logger não disponível - logs de auditoria desabilitados")

# 🆕 Importar HighlightLayoutService para reposicionamento dinâmico
try:
    from .highlight_layout_service import HighlightLayoutService
    _highlight_layout_service = HighlightLayoutService(debug_logger=debug_logger)
except ImportError:
    _highlight_layout_service = None
    logger.warning("⚠️ HighlightLayoutService não disponível")

# URL do V-Services
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')


class SubtitlePipelineService:
    """
    Serviço de pipeline de legendas.
    
    Orquestra os steps 11-15 do Video Orchestrator.
    """
    
    def __init__(self, v_services_url: str = None):
        self.base_url = v_services_url or V_SERVICES_URL
        self.positioning_endpoint = f"{self.base_url}/positioning/calculate"
        self.payload_endpoint = f"{self.base_url}/payload/build"
        logger.info(f"🎬 Subtitle Pipeline inicializado: {self.base_url}")
    
    def execute_pipeline(
        self,
        job_id: str,
        png_results: Dict[str, Any],
        video_url: str,
        duration_ms: int,
        template_config: Dict[str, Any],
        user_id: str = None,
        project_id: str = None,
        matting_data: Dict[str, Any] = None,  # 🆕 Dados de matting
        speech_segments: List[Dict[str, Any]] = None  # 🆕 v2.9.0: Clips do hybrid cut
    ) -> Dict[str, Any]:
        """
        Executa o pipeline completo de posicionamento e preparação.
        
        Args:
            job_id: ID do job
            png_results: Resultado do PngGeneratorService (frases com PNGs)
            video_url: URL do vídeo cortado
            duration_ms: Duração do vídeo em ms
            template_config: Configuração do template (positioning, text-alignment, etc)
            user_id: ID do usuário
            project_id: ID do projeto
            matting_data: 🆕 Dados de matting (foreground_url, segments)
            speech_segments: 🆕 v2.9.0: Clips do hybrid silence cut
            
        Returns:
            {
                "status": "success",
                "sentences": [...],  # Sentences posicionadas
                "payload": {...}     # Payload pronto para v-editor
            }
        """
        logger.info(f"🎬 Iniciando pipeline de legendas para job {job_id}")
        if speech_segments:
            logger.info(f"   🎬 Speech Segments (hybrid cut): {len(speech_segments)} clips")
        if matting_data and matting_data.get('foreground_url'):
            logger.info(f"   👤 Matting: {len(matting_data.get('segments', []))} segmentos")
        
        # 📝 DEBUG: Salvar input do subtitle_pipeline
        if debug_logger:
            debug_logger.log_step(job_id, "subtitle_pipeline", "input", {
                "video_url": video_url,
                "duration_ms": duration_ms,
                "has_png_results": png_results is not None,
                "png_sentences_count": len(png_results.get("sentences", [])) if png_results else 0,
                "backgrounds_count": len(png_results.get("backgrounds", [])) if png_results else 0,
                "template_config_keys": list(template_config.keys()) if template_config else [],
                "base_layer": template_config.get("base_layer", template_config.get("base-layer")),
                "user_id": user_id,
                "project_id": project_id
            })
        
        try:
            # Step 11: Positioning
            logger.info("📍 [Step 11] Calculando posições...")
            positioning_result = self._execute_positioning(
                png_results=png_results,
                template_config=template_config
            )
            
            if "error" in positioning_result:
                return {
                    "status": "error",
                    "step": "positioning",
                    "error": positioning_result["error"]
                }
            
            sentences = positioning_result.get("sentences", [])
            logger.info(f"✅ [Step 11] Posicionadas {len(sentences)} sentenças")
            
            # Step 11.5: Highlight Layout (controlado por template: layout_spacing.enable_dynamic_highlight_layout)
            logger.info(f"📐 [Step 11.5] Verificando Highlight Layout Service...")
            logger.info(f"   • Service disponível: {_highlight_layout_service is not None}")
            
            if _highlight_layout_service:
                # Extrair configuração do template
                layout_spacing = template_config.get("layout-spacing", template_config.get("layout_spacing", {}))
                logger.info(f"   • layout_spacing keys: {list(layout_spacing.keys()) if layout_spacing else 'VAZIO'}")
                
                template_hl_enabled = layout_spacing.get("enable_dynamic_highlight_layout", {})
                logger.info(f"   • enable_dynamic_highlight_layout raw: {template_hl_enabled}")
                
                # Pode ser dict com 'value' ou boolean direto
                if isinstance(template_hl_enabled, dict):
                    template_hl_enabled = template_hl_enabled.get("value", False)
                
                logger.info(f"   • template_hl_enabled final: {template_hl_enabled}")
                
                if _highlight_layout_service.should_process(template_hl_enabled):
                    logger.info("📐 [Step 11.5] Aplicando layout dinâmico para highlights maiores...")
                    
                    # Extrair canvas do template
                    # 🔧 FIX: Tentar múltiplas variações de chave e estrutura
                    project_settings = template_config.get("project_settings", template_config.get("project-settings", {}))
                    video_settings = project_settings.get("video_settings", {})
                    
                    # 🔧 FIX: Valores podem ser dict com 'value' ou diretos
                    width_val = video_settings.get("width", {})
                    height_val = video_settings.get("height", {})
                    canvas = {
                        "width": width_val.get("value", width_val) if isinstance(width_val, dict) else (width_val or 1080),
                        "height": height_val.get("value", height_val) if isinstance(height_val, dict) else (height_val or 1920)
                    }
                    
                    logger.info(f"   • Canvas: {canvas}")
                    
                    hl_result = _highlight_layout_service.process_sentences(
                        sentences=sentences,
                        canvas=canvas,
                        job_id=job_id,
                        template_enabled=template_hl_enabled
                    )
                    
                    if hl_result.get("stats", {}).get("error"):
                        logger.warning(f"⚠️ [Step 11.5] Erro, usando sentences originais: {hl_result['stats']['error']}")
                    else:
                        sentences = hl_result.get("sentences", sentences)
                        stats = hl_result.get("stats", {})
                        if stats.get("sentences_with_larger_highlights", 0) > 0:
                            logger.info(f"✅ [Step 11.5] Highlight Layout aplicado:")
                            logger.info(f"   • Sentences afetadas: {stats.get('sentences_with_larger_highlights', 0)}")
                            logger.info(f"   • Layers expandidas: {stats.get('original_layers')} → {stats.get('expanded_layers')}")
                        else:
                            logger.info("✅ [Step 11.5] Nenhum highlight maior detectado - sem alterações")
                else:
                    logger.debug("📐 [Step 11.5] Highlight Layout desabilitado para este template")
            
            # Extrair backgrounds gerados (se existirem)
            backgrounds = png_results.get('backgrounds', [])
            if backgrounds:
                logger.info(f"🎨 Encontrados {len(backgrounds)} backgrounds PNG gerados")
            
            # 🆕 Extrair motion graphics renderizados pelo Manim (se existirem)
            motion_graphics = png_results.get('motion_graphics', [])
            if motion_graphics:
                logger.info(f"🎬 Encontrados {len(motion_graphics)} motion graphics do Manim")
            
            # 🆕 v2.9.47: Criar cartelas baseadas nas PLACAS TECTÔNICAS, não nas frases
            # 🆕 v2.9.48: Agora também suporta "placas virtuais" quando não há speech_segments
            # Isso garante que as cartelas tenham a mesma duração dos segmentos de vídeo
            # E agrupa frases consecutivas para evitar flicker
            cartela_backgrounds = self._create_cartelas_from_tectonic_plates(
                sentences=sentences,
                speech_segments=speech_segments,
                video_duration_ms=duration_ms
            )
            if cartela_backgrounds:
                mode_str = "PLACAS TECTÔNICAS" if speech_segments else "PLACAS VIRTUAIS"
                logger.info(f"🎬 [v2.9.48] Criadas {len(cartela_backgrounds)} cartelas baseadas em {mode_str}")
                backgrounds = backgrounds + cartela_backgrounds
            
            # Step 14: Payload Builder
            logger.info("📦 [Step 14] Construindo payload...")
            payload_result = self._execute_payload_builder(
                sentences=sentences,
                video_url=video_url,
                duration_ms=duration_ms,
                template_config=template_config,
                backgrounds=backgrounds,
                matting_data=matting_data,
                speech_segments=speech_segments,  # 🆕 v2.9.0
                motion_graphics=motion_graphics   # 🆕 Motion graphics do Manim
            )
            
            if "error" in payload_result:
                return {
                    "status": "error",
                    "step": "payload_builder",
                    "error": payload_result["error"]
                }
            
            payload = payload_result.get("payload", {})
            logger.info(f"✅ [Step 14] Payload construído: {len(payload.get('tracks', {}).get('subtitles', []))} subtitles")
            
            # Log backgrounds incluídos
            bg_track = payload.get('tracks', {}).get('backgrounds', [])
            if bg_track:
                logger.info(f"   • Backgrounds no payload: {len(bg_track)}")
            
            # 🆕 Adicionar motion graphics ao payload (se existirem)
            if motion_graphics:
                if 'tracks' not in payload:
                    payload['tracks'] = {}
                payload['tracks']['motion_graphics'] = motion_graphics
                logger.info(f"   • Motion graphics no payload: {len(motion_graphics)}")
                # 📋 LOG DETALHADO: o que vai no payload final para v-editor-python
                for idx, mg in enumerate(motion_graphics):
                    logger.info(f"   📦 [PAYLOAD→EDITOR MG #{idx}]:")
                    logger.info(f"      id={mg.get('id')}, type={mg.get('type')}")
                    logger.info(f"      src={str(mg.get('src', 'N/A'))[:80]}")
                    logger.info(f"      x={mg.get('x')}, y={mg.get('y')}, position={mg.get('position')}")
                    logger.info(f"      start_time={mg.get('start_time')}, duration={mg.get('duration')}")
                    logger.info(f"      width={mg.get('width')}, height={mg.get('height')}")
                    logger.info(f"      zIndex={mg.get('zIndex')}")
            
            result = {
                "status": "success",
                "sentences": sentences,
                "payload": payload,
                "metadata": {
                    "job_id": job_id,
                    "total_sentences": len(sentences),
                    "total_subtitles": len(payload.get("tracks", {}).get("subtitles", [])),
                    "total_highlights": len(payload.get("tracks", {}).get("highlights", [])),
                    "duration_ms": duration_ms,
                    "processing_date": datetime.utcnow().isoformat() + "Z"
                }
            }
            
            # 📝 DEBUG: Salvar output do subtitle_pipeline
            if debug_logger:
                tracks = payload.get("tracks", {})
                debug_logger.log_step(job_id, "subtitle_pipeline", "output", {
                    "status": "success",
                    "total_sentences": len(sentences),
                    "total_subtitles": len(tracks.get("subtitles", [])),
                    "total_highlights": len(tracks.get("highlights", [])),
                    "total_word_bgs": len(tracks.get("word_bgs", [])),
                    "total_phrase_bgs": len(tracks.get("phrase_bgs", [])),
                    "render_settings": payload.get("render_settings"),
                    "duration_in_frames": payload.get("project_settings", {}).get("video_settings", {}).get("duration_in_frames"),
                    "base_layer": payload.get("base_layer")
                })
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Erro no pipeline: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _execute_positioning(
        self,
        png_results: Dict[str, Any],
        template_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Executa o serviço de posicionamento."""
        
        # =========================================================================
        # 🐛 FIX: Reutilizar positioned_sentences se já existirem!
        # Isso garante que texto e backgrounds usem as MESMAS posições.
        # O positioned_sentences vem do PositioningService.calculate_positions()
        # que foi chamado ANTES do BackgroundGeneratorService.
        # =========================================================================
        positioned_sentences = png_results.get('positioned_sentences')
        if positioned_sentences:
            logger.info(f"✅ [POSITIONING] Reutilizando {len(positioned_sentences)} sentences já posicionadas")
            return {
                "status": "success",
                "sentences": positioned_sentences
            }
        
        # Se não tem positioned_sentences, calcular (fallback)
        logger.warning("⚠️ [POSITIONING] Sem positioned_sentences - calculando do zero")
        
        # Extrair configurações do template
        positioning = template_config.get("positioning", {})
        text_alignment = template_config.get("text-alignment", {})
        project_settings = template_config.get("project-settings", {})
        video_settings = project_settings.get("video_settings", {})
        
        # Montar payload para o positioning service
        payload = {
            "png_results": png_results,
            "canvas": {
                "width": self._get_value(video_settings, "width", 720),
                "height": self._get_value(video_settings, "height", 1280),
                "padding": self._get_value(video_settings, "padding", 20)
            },
            "layout_settings": {
                "spacing": {
                    "height_percent": 35,
                    "multiplier": 1.0
                },
                "max_line_width_percent": 80,
                "line_spacing_px": 20
            },
            "positioning": {
                "group_position_x": self._get_value(positioning, "group_position_x", 0.5),
                "group_position_y": self._get_value(positioning, "group_position_y", 0.5),
                "group_anchor_x": self._get_value(positioning, "group_anchor_x", 0.5),
                "group_anchor_y": self._get_value(positioning, "group_anchor_y", 0.5),
                "group_scale": self._get_value(positioning, "group_scale", 1.0)
            },
            "text_alignment": {
                "default": self._get_value(text_alignment, "default_text_align", "center"),
                "emphasis": self._get_value(text_alignment, "emphasis", "center"),
                "letter_effect": self._get_value(text_alignment, "letter_effect", "center")
            }
        }
        
        try:
            response = requests.post(
                self.positioning_endpoint,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                return {"error": f"Positioning retornou {response.status_code}: {response.text[:200]}"}
            
            return response.json()
            
        except requests.Timeout:
            return {"error": "Timeout no positioning service"}
        except requests.RequestException as e:
            return {"error": f"Erro de conexão: {str(e)}"}
    
    def _execute_payload_builder(
        self,
        sentences: List[Dict[str, Any]],
        video_url: str,
        duration_ms: int,
        template_config: Dict[str, Any],
        backgrounds: List[Dict[str, Any]] = None,
        matting_data: Dict[str, Any] = None,  # 🆕 Dados de matting
        speech_segments: List[Dict[str, Any]] = None,  # 🆕 v2.9.0: Clips do hybrid cut
        motion_graphics: List[Dict[str, Any]] = None   # 🆕 Motion graphics do Manim
    ) -> Dict[str, Any]:
        """
        Executa o serviço de construção de payload.
        
        Args:
            sentences: Sentenças posicionadas
            video_url: URL do vídeo
            duration_ms: Duração do vídeo em ms
            template_config: Configuração do template
            backgrounds: Lista de backgrounds PNG gerados (opcional)
            matting_data: 🆕 Dados de matting (foreground_url, segments)
            speech_segments: 🆕 v2.9.0: Clips do hybrid silence cut
            motion_graphics: 🆕 Motion graphics renderizados pelo Manim
        """
        backgrounds = backgrounds or []
        matting_data = matting_data or {}
        speech_segments = speech_segments or []
        motion_graphics = motion_graphics or []
        
        # Extrair configurações do template
        project_settings = template_config.get("project-settings", {})
        video_settings = project_settings.get("video_settings", {})
        animation_config = template_config.get("animation-config", {})
        multi_text_styling = template_config.get("multi-text-styling", {})
        z_index_config = template_config.get("z-index-hierarchy", {})
        # 🔧 FIX: Preferir base_layer (underscore) pois _apply_implication_rules
        # atualiza base_type nessa chave. "base-layer" (dash) pode estar desatualizada.
        base_layer = template_config.get("base_layer", template_config.get("base-layer", {}))
        
        # Configurações de animação
        subtitles_anim = animation_config.get("subtitles", {})
        stagger_config = animation_config.get("stagger", {})
        backgrounds_anim = animation_config.get("backgrounds", {})
        
        # 🐛 FIX: Verificar se animações per-style estão habilitadas
        # Se _text_styles existe, verificar se ALGUMA tem animation.enabled = true
        # 🐛 FIX: Usar 'or {}' para tratar valores None explícitos nos estilos
        text_styles = template_config.get("_text_styles") or {}
        any_animation_enabled = False
        if text_styles:
            for style_name, style_config in text_styles.items():
                # 🐛 FIX: style_config pode ser None se estilo não configurado
                anim = (style_config or {}).get("animation") or {}
                # Suporta enabled como bool ou dict {value: bool}
                enabled_val = anim.get("enabled", False)
                if isinstance(enabled_val, dict):
                    enabled_val = enabled_val.get("value", False)
                if enabled_val:
                    any_animation_enabled = True
                    break
            logger.info(f"🎬 [PAYLOAD_BUILDER] Animações per-style: any_enabled={any_animation_enabled}")
        
        # Se animações per-style existem, usar esse flag; senão usar config global
        animations_enabled = any_animation_enabled if text_styles else self._get_value(subtitles_anim, "enabled", False)
        
        # Configurações de backgrounds
        word_bgs = multi_text_styling.get("word_bgs", {})
        full_screen_bgs = multi_text_styling.get("full_screen_bgs", {})
        
        # 🔗 Layout spacing (para highlight gap merge)
        layout_spacing = template_config.get("layout-spacing", template_config.get("layout_spacing", {}))
        
        payload = {
            "sentences": sentences,
            "video_url": video_url,
            "duration_ms": duration_ms,
            "canvas": {
                "width": self._get_value(video_settings, "width", 720),
                "height": self._get_value(video_settings, "height", 1280)
            },
            "fps": self._get_value(video_settings, "fps", 30),
            "animation_config": {
                # 🐛 FIX: Usar flag calculado de animações per-style
                "enabled": animations_enabled,
                "type_in": self._get_value(subtitles_anim, "type_in", "none") if not animations_enabled else self._get_value(subtitles_anim, "type_in", "fade-in"),
                "type_out": self._get_value(subtitles_anim, "type_out", "none") if not animations_enabled else self._get_value(subtitles_anim, "type_out", "fade-out"),
                "duration_in_frames": self._get_value(subtitles_anim, "duration_in_frames", 0) if not animations_enabled else self._get_value(subtitles_anim, "duration_in_frames", 15),
                "duration_out_frames": self._get_value(subtitles_anim, "duration_out_frames", 0) if not animations_enabled else self._get_value(subtitles_anim, "duration_out_frames", 15),
                "easing": self._get_value(subtitles_anim, "easing", "linear"),
                "exit_animation_enabled": animations_enabled and self._get_value(subtitles_anim, "exit_animation_enabled", False)
            },
            "stagger_config": {
                "enabled": animations_enabled and self._get_value(stagger_config, "enabled", False),
                "delay_per_element_ms": self._get_value(stagger_config, "delay_per_element_ms", 50),
                "direction": self._get_value(stagger_config, "direction", "left_to_right")
            },
            "opacity_config": {
                # 🐛 FIX: Desabilitar fade de opacidade se animações estão desabilitadas
                "enabled": animations_enabled,
                "fade_in_duration_ms": 0 if not animations_enabled else 200,
                "fade_out_duration_ms": 0 if not animations_enabled else 150
            },
            "style_config": {
                # 🔗 Highlight gap merge: evita "piscadas" entre highlights próximos
                # Funciona independente do layout dinâmico
                "highlight_gap_merge_threshold_ms": self._get_value(
                    layout_spacing.get("highlight_gap_merge_threshold_ms", {}), 
                    "value", 
                    100  # Default: 100ms
                ) if isinstance(layout_spacing.get("highlight_gap_merge_threshold_ms"), dict) else layout_spacing.get("highlight_gap_merge_threshold_ms", 100),
                "word_bgs": {
                    "enabled": self._get_value(word_bgs, "enabled", False),
                    "solid_color_rgba": self._get_value(word_bgs, "solid_color_rgba", "0,0,0,0.5"),
                    "border_radius_value": self._get_value(word_bgs, "border_radius_value", 8),
                    "border_radius_unit": self._get_value(word_bgs, "border_radius_unit", "px"),
                    "animation": {
                        "type_in": self._get_value(backgrounds_anim, "type_in", "fade-in"),
                        "type_out": self._get_value(backgrounds_anim, "type_out", "fade-out"),
                        "duration_in_frames": self._get_value(backgrounds_anim, "duration_in_frames", 10),
                        "duration_out_frames": self._get_value(backgrounds_anim, "duration_out_frames", 10)
                    }
                },
                "full_screen_bgs": {
                    "enabled": self._get_value(full_screen_bgs, "enabled", True),
                    "solid_color_rgba": self._get_value(full_screen_bgs, "solid_color_rgba", "0,0,0,0.7"),
                    "opacity": self._get_value(full_screen_bgs, "opacity", 0.7),
                    "animation": {
                        "type_in": self._get_value(backgrounds_anim, "type_in", "fade-in"),
                        "type_out": self._get_value(backgrounds_anim, "type_out", "fade-out"),
                        "duration_in_frames": self._get_value(backgrounds_anim, "duration_in_frames", 10),
                        "duration_out_frames": self._get_value(backgrounds_anim, "duration_out_frames", 10)
                    }
                }
            },
            "z_index_hierarchy": self._extract_z_index_hierarchy(z_index_config),
            # Backgrounds PNG gerados (nova arquitetura)
            "generated_backgrounds": self._prepare_backgrounds_for_payload(backgrounds),
            # Base layer para text_video (solid/gradient)
            "base_layer": base_layer,
            # 🆕 Dados de matting (person overlay)
            "matting_data": matting_data,
            # 🆕 v2.9.0: Clips do hybrid silence cut
            "speech_segments": speech_segments
        }
        
        try:
            logger.info(f"📡 Chamando payload_builder: {self.payload_endpoint}")
            if backgrounds:
                logger.info(f"   - generated_backgrounds: {len(backgrounds)} imagens")
            if speech_segments:
                logger.info(f"   - 🎬 speech_segments (hybrid cut): {len(speech_segments)} clips")
            if matting_data.get('foreground_url'):
                logger.info(f"   - matting_data: {len(matting_data.get('segments', []))} segmentos")
            logger.info(f"   - sentences: {len(sentences)}")
            logger.info(f"   - video_url: {video_url[:60] if video_url else 'None'}...")
            logger.info(f"   - duration_ms: {duration_ms}")
            
            response = requests.post(
                self.payload_endpoint,
                json=payload,
                timeout=60
            )
            
            logger.info(f"📥 Resposta do payload_builder: status={response.status_code}")
            
            if response.status_code != 200:
                error_msg = f"Payload builder retornou {response.status_code}: {response.text[:500]}"
                logger.error(f"❌ {error_msg}")
                return {"error": error_msg}
            
            result = response.json()
            logger.info(f"✅ Payload recebido: {list(result.keys()) if isinstance(result, dict) else type(result)}")
            
            # Debug: verificar se video_url está no payload retornado
            if isinstance(result, dict) and 'payload' in result:
                payload_data = result['payload']
                logger.info(f"   - video_url no payload: {payload_data.get('video_url', 'AUSENTE')[:60] if payload_data.get('video_url') else 'AUSENTE'}...")
                
                # 🆕 Adicionar motion_graphics ao payload (se existirem)
                # Os motion graphics são renderizados pelo Manim e precisam ser adicionados como track
                if motion_graphics:
                    if 'tracks' not in payload_data:
                        payload_data['tracks'] = {}
                    payload_data['tracks']['motion_graphics'] = motion_graphics
                    logger.info(f"   - 🎬 motion_graphics adicionados ao payload: {len(motion_graphics)} items")
            
            return result
            
        except requests.Timeout:
            logger.error("❌ Timeout no payload builder service (60s)")
            return {"error": "Timeout no payload builder service"}
        except requests.RequestException as e:
            logger.error(f"❌ Erro de conexão com payload builder: {e}")
            return {"error": f"Erro de conexão: {str(e)}"}
    
    def _get_value(self, obj: Any, key: str, default: Any = None) -> Any:
        """
        Extrai valor de objeto, suportando formato {value: x}.
        """
        if not obj or not isinstance(obj, dict):
            return default
        
        val = obj.get(key)
        
        if val is None:
            return default
        
        # Se for objeto com .value, extrair
        if isinstance(val, dict) and 'value' in val:
            return val['value']
        
        return val
    
    def _extract_z_index_hierarchy(self, z_index_config: Dict[str, Any]) -> Dict[str, int]:
        """
        Extrai hierarquia de z-index do template config.
        
        Mapeia nomes do z-index-hierarchy.json para nomes usados no payload_builder.
        """
        z_layers = z_index_config.get("z_index_layers", {})
        
        # Mapeamento: nome no JSON -> nome no payload_builder
        return {
            "bg_full_screen": self._get_value(z_layers, "fullscreen_overlay_min", 500),
            "phrase_bgs": self._get_value(z_layers, "phrase_bg_min", 1200),
            "word_bgs": self._get_value(z_layers, "word_bg_min", 1500),
            "subtitles_min": self._get_value(z_layers, "subtitles_min", 2000),
            "highlights_min": self._get_value(z_layers, "highlights_min", 3000)
        }
    
    def _extract_cartelas_from_sentences(self, sentences: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extrai cartela_info das sentences e converte para formato de background.
        
        As cartelas são adicionadas pelo cartela_service como sentence['cartela_info'],
        contendo o caminho do PNG gerado (para solid/gradient) ou URL de vídeo (para asset_video).
        
        Args:
            sentences: Lista de sentences com possíveis cartela_info
        
        Returns:
            Lista de backgrounds no formato esperado pelo payload_builder
        """
        cartela_backgrounds = []
        
        for idx, sentence in enumerate(sentences):
            cartela_info = sentence.get('cartela_info')
            if not cartela_info:
                continue
            
            # Extrair timings da sentence
            # 🔧 FIX v2.9.30: Fallback para start_time/end_time se group_* não existir
            # 🔧 v2.9.44: As frases usam tempo VIRTUAL (transcrição do áudio concatenado)
            start_time = sentence.get('group_start_time') or sentence.get('start_time', 0)
            end_time = sentence.get('group_end_time') or sentence.get('end_time', 0)
            
            # 🐛 FIX: Conversão robusta de segundos para ms
            # Usar o MAIOR valor para determinar a unidade (evita confundir 8ms com 8s)
            max_time = max(start_time, end_time)
            if max_time > 0 and max_time < 100:
                # Se o maior valor é < 100, ambos estão em segundos
                start_time = int(start_time * 1000)
                end_time = int(end_time * 1000)
            # Se max_time >= 100, já está em ms, não converte
            
            # 🔒 Garantir que start < end
            if start_time > end_time:
                logger.warning(f"⚠️ Cartela {idx}: start_time ({start_time}) > end_time ({end_time}), invertendo")
                start_time, end_time = end_time, start_time
            
            cartela_type = cartela_info.get('type', 'solid')
            
            # Construir background no formato esperado
            bg = {
                "type": "fullscreen",  # Cartelas são sempre fullscreen
                "sentence_index": idx,
                "dimensions": {
                    "width": cartela_info.get('width', 1080),
                    "height": cartela_info.get('height', 1920)
                },
                "position": {
                    "x": 0,
                    "y": 0
                },
                "timing": {
                    "start_ms": start_time,
                    "end_ms": end_time
                },
                # Metadados da cartela
                "cartela_type": cartela_type,
                "cartela_preset_id": cartela_info.get('preset_id', ''),
                "layout": cartela_info.get('layout', 'fullscreen')
            }
            
            # 🎬 Diferenciar entre PNG (solid/gradient) e Vídeo (asset_video)
            if cartela_type == 'asset_video':
                # Cartela de vídeo - usar video_url
                video_url = cartela_info.get('video_url')
                if video_url:
                    bg["video_url"] = video_url
                    bg["is_video"] = True
                    bg["loop"] = cartela_info.get('loop', True)
                    bg["opacity"] = cartela_info.get('opacity', 1.0)
                    # 🎬 FIX LOOP: Passar duração do vídeo para loop correto
                    bg["video_duration"] = cartela_info.get('video_duration', 10.0)  # Default 10s
                    bg["video_duration_frames"] = cartela_info.get('video_duration_frames', 300)  # 10s @ 30fps
                    logger.info(f"🎬 Cartela VÍDEO extraída: sentence {idx}, url={video_url[:60]}..., duration={bg['video_duration']}s")
                else:
                    logger.warning(f"⚠️ Cartela vídeo sentence {idx}: video_url não encontrada")
                    continue  # Pular esta cartela
            else:
                # Cartela de imagem (solid/gradient) - usar png_path
                bg["path"] = cartela_info.get('png_path', cartela_info.get('relative_path', ''))
                bg["is_video"] = False
            
            cartela_backgrounds.append(bg)
            logger.debug(f"🎬 Cartela extraída: sentence {idx}, type={cartela_type}, timing={start_time}-{end_time}ms")
        
        return cartela_backgrounds
    
    def _create_cartelas_from_tectonic_plates(
        self,
        sentences: List[Dict[str, Any]],
        speech_segments: List[Dict[str, Any]],
        video_duration_ms: int = None
    ) -> List[Dict[str, Any]]:
        """
        🆕 v2.9.47: Cria cartelas baseadas nas PLACAS TECTÔNICAS, não nas frases.
        🆕 v2.9.48: Agora suporta "placas virtuais" quando não há speech_segments.
        
        Isso garante que as cartelas tenham EXATAMENTE a mesma duração e posição
        dos segmentos de vídeo que serão montados no v-editor.
        
        Lógica:
        1. Se há speech_segments:
           a. Se TODAS as sentences têm cartela → criar cartela para TODAS as placas
           b. Caso contrário → usar get_cartela_segments para agrupar corretamente
        2. Se NÃO há speech_segments:
           → Criar "placas virtuais" agrupando frases consecutivas com cartela
           → Isso evita flicker de gaps pequenos entre cartelas individuais
        
        Args:
            sentences: Lista de sentences com possíveis cartela_info
            speech_segments: Lista de placas tectônicas (do hybrid silence cut) - pode ser vazia
            video_duration_ms: Duração do vídeo em ms (necessário para placas virtuais)
        
        Returns:
            Lista de backgrounds baseados nos timestamps das placas
        """
        from app.video_orchestrator.services.tectonic_plates_service import get_cartela_segments
        
        # Verificar se existem sentences com cartela
        sentences_with_cartela = [s for s in sentences if s.get('cartela_info')]
        
        if not sentences_with_cartela:
            logger.info(f"   ⚠️ Nenhuma sentence com cartela - pulando")
            return []
        
        # Calcular duração do vídeo em segundos (para placas virtuais)
        if video_duration_ms:
            video_duration_s = video_duration_ms / 1000.0
        elif speech_segments:
            # Estimar da última placa tectônica
            last_seg = max(speech_segments, key=lambda s: s.get('audio_offset', 0) + s.get('duration', 0))
            video_duration_s = last_seg.get('audio_offset', 0) + last_seg.get('duration', 0)
        else:
            # Estimar da última sentence
            last_sent = max(sentences, key=lambda s: s.get('end_time', s.get('group_end_time', 0)))
            video_duration_s = last_sent.get('end_time', last_sent.get('group_end_time', 30000)) / 1000.0
            if video_duration_s < 1:  # Provavelmente já em segundos
                video_duration_s = last_sent.get('end_time', last_sent.get('group_end_time', 30))
        
        logger.info(f"🎴 [CARTELAS] {len(sentences_with_cartela)}/{len(sentences)} sentences têm cartela")
        logger.info(f"   📊 Speech segments: {len(speech_segments) if speech_segments else 0}")
        logger.info(f"   📏 Duração estimada: {video_duration_s:.2f}s")
        
        # Preparar phrase_groups no formato esperado pelo tectonic_plates_service
        phrase_groups = []
        for i, s in enumerate(sentences):
            phrase_groups.append({
                'phrase_index': i,
                'start_time': s.get('start_time', s.get('group_start_time', 0)),
                'end_time': s.get('end_time', s.get('group_end_time', 0)),
                'use_cartela': s.get('cartela_info') is not None
            })
        
        # Usar get_cartela_segments que decide automaticamente entre:
        # - Modo "all_plates" (todas as placas tectônicas)
        # - Modo "tectonic" (apenas placas com frases com cartela)
        # - Modo "virtual" (placas virtuais agrupando frases)
        cartela_result = get_cartela_segments(
            speech_segments=speech_segments or [],
            phrase_groups=phrase_groups,
            video_duration=video_duration_s,
            gap_threshold_ms=500  # Gap de 500ms para merge (conforme solicitado pelo usuário)
        )
        
        mode = cartela_result.get('mode', 'none')
        plates_to_use = cartela_result.get('plates', [])
        
        if mode == 'none' or not plates_to_use:
            logger.info(f"   ⚠️ Nenhuma placa de cartela gerada (mode={mode})")
            return []
        
        logger.info(f"   🎴 MODO: {mode.upper()} - {len(plates_to_use)} placas para cartela")
        
        # Obter cartela_info de referência (usar a primeira sentence com cartela)
        reference_cartela_info = sentences_with_cartela[0].get('cartela_info', {})
        cartela_type = reference_cartela_info.get('type', 'solid')
        
        # Criar backgrounds para cada placa
        cartela_backgrounds = []
        
        for plate in plates_to_use:
            plate_index = plate.get('index', 0)
            is_virtual = plate.get('is_virtual', False)
            
            # USAR audio_offset e duration (tempo virtual) - isso é o que o v-editor usa
            audio_offset = plate.get('audio_offset', 0)
            duration = plate.get('duration', 0)
            
            # Converter para ms
            start_ms = int(audio_offset * 1000)
            end_ms = int((audio_offset + duration) * 1000)
            
            # Validar timestamps
            if start_ms >= end_ms:
                logger.warning(f"   ⚠️ Placa {plate_index}: timing inválido ({start_ms}ms >= {end_ms}ms), pulando")
                continue
            
            bg = {
                "type": "fullscreen",
                "plate_index": plate_index,
                "dimensions": {
                    "width": reference_cartela_info.get('width', 1080),
                    "height": reference_cartela_info.get('height', 1920)
                },
                "position": {
                    "x": 0,
                    "y": 0
                },
                "timing": {
                    "start_ms": start_ms,
                    "end_ms": end_ms
                },
                "cartela_type": cartela_type,
                "cartela_preset_id": reference_cartela_info.get('preset_id', ''),
                "layout": reference_cartela_info.get('layout', 'fullscreen'),
                "_from_tectonic_plate": not is_virtual,
                "_from_virtual_plate": is_virtual  # Flag para debug
            }
            
            # Diferenciar entre PNG e Vídeo
            if cartela_type == 'asset_video':
                video_url = reference_cartela_info.get('video_url')
                if video_url:
                    bg["video_url"] = video_url
                    bg["is_video"] = True
                    bg["loop"] = reference_cartela_info.get('loop', True)
                    bg["opacity"] = reference_cartela_info.get('opacity', 1.0)
                    bg["video_duration"] = reference_cartela_info.get('video_duration', 10.0)
                    bg["video_duration_frames"] = reference_cartela_info.get('video_duration_frames', 300)
                else:
                    logger.warning(f"⚠️ Cartela vídeo placa {plate_index}: video_url não encontrada")
                    continue
            else:
                bg["path"] = reference_cartela_info.get('png_path', reference_cartela_info.get('relative_path', ''))
                bg["is_video"] = False
            
            cartela_backgrounds.append(bg)
            plate_type = "virtual" if is_virtual else "tectônica"
            logger.debug(f"   🎴 Placa {plate_type} {plate_index}: {start_ms}ms → {end_ms}ms (duração: {end_ms - start_ms}ms)")
        
        logger.info(f"   ✅ {len(cartela_backgrounds)} cartelas criadas ({mode})")
        
        return cartela_backgrounds
    
    def _prepare_backgrounds_for_payload(self, backgrounds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepara os backgrounds PNG gerados para o payload do V-Editor.
        
        Cada background contém:
        - path: Caminho do arquivo PNG (EFS compartilhado)
        - type: "word", "phrase", ou "fullscreen"
        - sentence_index: Índice da sentença associada
        - word_index: Índice da palavra (para word backgrounds)
        - animation_config: Configuração de animação independente
        
        Returns:
            Lista de backgrounds formatados para o V-Editor
        """
        if not backgrounds:
            return []
        
        formatted_bgs = []
        
        for bg in backgrounds:
            # 🐛 FIX: Ler de estruturas aninhadas geradas pelo background_generator_service
            # O service gera: dimensions: {width, height}, position: {x, y}, timing: {start_ms, end_ms}
            dimensions = bg.get("dimensions", {})
            position = bg.get("position", {})
            timing = bg.get("timing", {})
            
            formatted_bg = {
                "path": bg.get("path", ""),
                "type": bg.get("type", "phrase"),
                "sentence_index": bg.get("sentence_index", 0),
                "word_index": bg.get("word_index"),  # None para phrase/fullscreen
                "dimensions": {
                    "width": dimensions.get("width", 0) or bg.get("width", 0),
                    "height": dimensions.get("height", 0) or bg.get("height", 0)
                },
                "position": {
                    "x": position.get("x", 0),
                    "y": position.get("y", 0)
                },
                "timing": {
                    "start_ms": timing.get("start_ms", 0),
                    "end_ms": timing.get("end_ms", 0)
                }
            }
            
            # 🎬 FIX: Repassar campos de vídeo para cartelas de vídeo
            if bg.get("is_video"):
                formatted_bg["video_url"] = bg.get("video_url", "")
                formatted_bg["is_video"] = True
                formatted_bg["loop"] = bg.get("loop", True)
                formatted_bg["opacity"] = bg.get("opacity", 1.0)
                formatted_bg["cartela_type"] = bg.get("cartela_type", "asset_video")
                # 🎬 FIX LOOP: Passar duração do vídeo para loop correto
                formatted_bg["video_duration"] = bg.get("video_duration", 10.0)  # Default 10s
                formatted_bg["video_duration_frames"] = bg.get("video_duration_frames", 300)  # 10s @ 30fps
            
            # Incluir animação se configurada
            anim_config = bg.get("animation_config", {})
            if anim_config:
                formatted_bg["animation"] = {
                    "entry": anim_config.get("entry", {}),
                    "exit": anim_config.get("exit", {}),
                    "offset_ms": anim_config.get("offset_ms", 0),
                    "computed_start_time": anim_config.get("computed_start_time"),
                    "computed_end_time": anim_config.get("computed_end_time")
                }
            
            formatted_bgs.append(formatted_bg)
        
        logger.info(f"   • Backgrounds preparados: {len(formatted_bgs)} (word: {sum(1 for b in formatted_bgs if b['type']=='word')}, phrase: {sum(1 for b in formatted_bgs if b['type']=='phrase')}, fullscreen: {sum(1 for b in formatted_bgs if b['type']=='fullscreen')})")
        
        return formatted_bgs
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica status dos serviços."""
        try:
            # Verificar positioning
            pos_response = requests.get(
                f"{self.base_url}/positioning/health",
                timeout=5
            )
            
            # Verificar payload builder
            payload_response = requests.get(
                f"{self.base_url}/payload/health",
                timeout=5
            )
            
            return {
                "available": pos_response.status_code == 200 and payload_response.status_code == 200,
                "services": {
                    "positioning": pos_response.status_code == 200,
                    "payload_builder": payload_response.status_code == 200
                },
                "base_url": self.base_url
            }
        except Exception as e:
            return {
                "available": False,
                "error": str(e)
            }


# Singleton instance
_pipeline_service = None


def get_subtitle_pipeline_service() -> SubtitlePipelineService:
    """Retorna instância singleton do SubtitlePipelineService."""
    global _pipeline_service
    if _pipeline_service is None:
        _pipeline_service = SubtitlePipelineService()
    return _pipeline_service

