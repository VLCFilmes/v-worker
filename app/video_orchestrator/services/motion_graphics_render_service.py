"""
🎨 Motion Graphics Render Service

Serviço para renderizar motion graphics via v-services/manim.
Os arquivos são compartilhados via Docker volume (vinicius-ai-shared)
montado em /app/shared em ambos v-services e v-editor-python.

Usa o endpoint /render-and-position que renderiza E calcula posição
baseado no text_layout (positioned_sentences).

🔧 v3.2.0: Buffer de timing — MG inicia 2.5s ANTES da palavra aparecer.
A animação precisa de tempo para "crescer" (GrowFromCenter ~0.5s).
Sem buffer, o frame 0 é invisível e a palavra já está sendo falada.
"""

import os
import json
import requests
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# 🔧 v3.2.0: Buffer de timing para motion graphics
# A animação começa X segundos ANTES da palavra aparecer no vídeo.
# Isso garante que quando a palavra é falada, a seta/destaque já está visível.
MG_TIMING_BUFFER_SECONDS = 2.5


class MotionGraphicsRenderService:
    """
    Serviço para renderização de motion graphics usando Manim.
    
    Responsável por:
    - Renderizar motion graphics do plano do Director via v-services
    - Calcular posição via layout_service do v-services
    - Retornar paths locais dos .mp4 no volume compartilhado (/app/shared/manim/)
    
    Os arquivos renderizados ficam em /app/shared/manim/output/ ou /app/shared/manim/cache/
    que é acessível tanto por v-services (RW) quanto por v-editor-python (RO)
    via Docker volume 'vinicius-ai-shared'.
    """
    
    # Dimensões default por tipo de template (quando Director não especifica)
    TYPE_DEFAULTS = {
        'arrow_pointing': {'width': 200, 'height': 300},
        'straight_arrow': {'width': 200, 'height': 200},
        'curved_arrow': {'width': 300, 'height': 200},
        'oval_highlight': {'width': 400, 'height': 200},
        'rectangle_highlight': {'width': 400, 'height': 200},
        'underline': {'width': 400, 'height': 60},
        'bracket_highlight': {'width': 400, 'height': 200},
        'circle_attention': {'width': 300, 'height': 300},
    }
    
    def __init__(self):
        self.base_url = os.getenv('V_SERVICES_URL', 'http://v-services:5000')
        self.render_endpoint = f"{self.base_url}/motion-graphics/render"
        self.render_and_position_endpoint = f"{self.base_url}/motion-graphics/render-and-position"
        self.timeout = int(os.getenv('MANIM_RENDER_TIMEOUT', '60'))
        logger.info(f"🎨 MotionGraphicsRenderService inicializado")
        logger.info(f"   URL: {self.base_url}")
        logger.info(f"   Modo: shared volume (sem B2)")
        logger.info(f"   Endpoints: /render, /render-and-position")
    
    def render_motion_graphics(
        self,
        motion_graphics_plan: List[Dict],
        job_id: str = None,
        project_id: str = None,
        user_id: str = None,
        text_layout: Optional[List[Dict]] = None,
        canvas: Optional[Dict] = None
    ) -> Dict:
        """
        Renderiza todos os motion graphics do plano.
        
        Se text_layout estiver disponível, usa /render-and-position para obter
        posição calculada pelo layout_service do v-services.
        
        Se não, faz render individual via /render com posição do orchestrator.
        
        Args:
            motion_graphics_plan: Lista de motion graphics do plano do Director
            job_id: ID do job (para logging)
            project_id: ID do projeto (para logging)
            user_id: ID do usuário (para logging)
            text_layout: positioned_sentences do pipeline (para layout_service)
            canvas: Dimensões do canvas, ex: {"width": 1080, "height": 1920}
        
        Returns:
            {
                "status": "success",
                "motion_graphics": [
                    {
                        "id": "mg_001",
                        "src": "/app/shared/manim/output/mg_xxx.mp4",
                        "x": 400, "y": 300,
                        "start_time": 2.5,
                        "width": 300, "height": 80,
                        ...
                    }
                ],
                "total": 3
            }
        """
        logger.info(f"🎨 [MANIM] ═══════════════════════════════════════════")
        logger.info(f"🎨 [MANIM] Renderizando {len(motion_graphics_plan)} motion graphics...")
        logger.info(f"🎨 [MANIM] job_id={job_id}")
        logger.info(f"🎨 [MANIM] text_layout disponível: {bool(text_layout)} ({len(text_layout) if text_layout else 0} itens)")
        logger.info(f"🎨 [MANIM] canvas: {canvas}")
        
        # Se temos text_layout, usar endpoint combinado
        if text_layout:
            return self._render_and_position(
                motion_graphics_plan=motion_graphics_plan,
                text_layout=text_layout,
                canvas=canvas or {"width": 1080, "height": 1920},
                job_id=job_id
            )
        else:
            # Fallback: render individual sem posicionamento do layout_service
            return self._render_individual(
                motion_graphics_plan=motion_graphics_plan,
                job_id=job_id
            )
    
    def _render_and_position(
        self,
        motion_graphics_plan: List[Dict],
        text_layout: List[Dict],
        canvas: Dict,
        job_id: str = None
    ) -> Dict:
        """
        Usa o endpoint /render-and-position que renderiza + calcula posição.
        """
        logger.info(f"🎨 [MANIM] Usando /render-and-position (com layout_service)")
        
        # Converter plano do Director → formato esperado pelo endpoint
        mg_definitions = []
        for mg in motion_graphics_plan:
            mg_config = mg.get('config', {})
            mg_type = mg.get('type', '')
            mg_timing = mg.get('timing', {})
            
            # Adicionar defaults de dimensão se não presentes no config
            defaults = self.TYPE_DEFAULTS.get(mg_type, {'width': 300, 'height': 200})
            if 'width' not in mg_config:
                mg_config['width'] = defaults['width']
            if 'height' not in mg_config:
                mg_config['height'] = defaults['height']
            
            # Adicionar duration do timing ao config se não presente
            if 'duration' not in mg_config and 'duration' in mg_timing:
                mg_config['duration'] = mg_timing['duration']
            
            mg_def = {
                "template": mg_type,
                "target_word": mg.get('target_word', ''),
                "config": mg_config,
                "timing": mg_timing
            }
            mg_definitions.append(mg_def)
            
            logger.info(f"   📋 [PLAN→RENDER] {mg.get('id', '?')}: template={mg_type}")
            logger.info(f"      target_word='{mg.get('target_word', '')}'")
            logger.info(f"      config={mg_config}")
            logger.info(f"      timing={mg_timing}")
        
        # Payload para /render-and-position
        payload = {
            "motion_graphics": mg_definitions,
            "text_layout": text_layout,
            "canvas": canvas
        }
        
        logger.info(f"🎨 [MANIM] Enviando para /render-and-position:")
        logger.info(f"   {len(mg_definitions)} MGs, {len(text_layout)} text_layout items")
        logger.info(f"   Canvas: {canvas}")
        
        try:
            response = requests.post(
                self.render_and_position_endpoint,
                json=payload,
                timeout=self.timeout
            )
            
            logger.info(f"🎨 [MANIM] Response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                
                logger.info(f"🎨 [MANIM] Response body (resumo): status={result.get('status')}, "
                          f"total={result.get('total')}, render_time={result.get('total_render_time')}s")
                
                if result.get('status') == 'success':
                    positioned_mgs = result.get('motion_graphics', [])
                    
                    # Enriquecer com dados do plano original (timing, etc)
                    enriched_mgs = []
                    for idx, pos_mg in enumerate(positioned_mgs):
                        # Encontrar plano original correspondente
                        original_plan = motion_graphics_plan[idx] if idx < len(motion_graphics_plan) else {}
                        original_timing = original_plan.get('timing', {})
                        
                        # Extrair timing
                        raw_start_time = (
                            original_timing.get('start_time') or 
                            original_timing.get('start') or 
                            0
                        )
                        duration = original_timing.get('duration', 0)
                        
                        # 🔧 v3.2.0: Buffer de timing — iniciar MG ANTES da palavra
                        # A animação precisa de tempo para "crescer" antes de ser visível.
                        # Sem buffer, a seta aparece no frame 0 (invisível) e a palavra já sumiu.
                        start_time = max(0, raw_start_time - MG_TIMING_BUFFER_SECONDS)
                        logger.info(f"      ⏱️ [TIMING BUFFER] raw={raw_start_time}s → buffered={start_time}s "
                                  f"(buffer={MG_TIMING_BUFFER_SECONDS}s)")
                        
                        # Extrair posição calculada pelo layout_service
                        position = pos_mg.get('position', {})
                        mg_x = position.get('x', 0)
                        mg_y = position.get('y', 0)
                        
                        # 🔧 v3.1.0: z-index com piso mínimo de 2200 (acima de subtitles 2000)
                        raw_z = pos_mg.get('z_index', 2200 + idx)
                        safe_z = max(raw_z, 2200) + idx  # garante faixa correta
                        
                        enriched = {
                            'id': pos_mg.get('id', original_plan.get('id', f'mg_{idx:03d}')),
                            'type': pos_mg.get('type', original_plan.get('type')),
                            'target_word': pos_mg.get('target', original_plan.get('target_word', '')),
                            # Campos que v-editor-python lê diretamente:
                            'start_time': start_time,
                            'duration': duration,
                            'x': mg_x,
                            'y': mg_y,
                            'position': {'x': mg_x, 'y': mg_y},
                            # Dimensões
                            'width': pos_mg.get('dimensions', {}).get('width', 0),
                            'height': pos_mg.get('dimensions', {}).get('height', 0),
                            'dimensions': pos_mg.get('dimensions', {}),
                            # Arquivo (path no shared volume)
                            'local_path': pos_mg.get('asset_path', ''),
                            'src': pos_mg.get('asset_path', ''),
                            'url': pos_mg.get('asset_path', ''),
                            'video_url': pos_mg.get('asset_path', ''),
                            # 🔧 v3.1.0: Z-index com piso seguro
                            'zIndex': safe_z,
                            # Metadata
                            'timing': original_timing,
                            'config': original_plan.get('config', {}),
                            'justification': original_plan.get('justification', ''),
                            'render_info': pos_mg.get('render_info', {}),
                        }
                        
                        enriched_mgs.append(enriched)
                        
                        logger.info(f"   ✅ [ENRICHED MG #{idx}] id={enriched['id']}")
                        logger.info(f"      type={enriched['type']}, target='{enriched['target_word']}'")
                        logger.info(f"      position=({mg_x}, {mg_y}) [CENTRO], zIndex={safe_z} (raw={raw_z})")
                        logger.info(f"      start_time={start_time}s, duration={duration}s")
                        logger.info(f"      src={enriched['src'][:80] if enriched['src'] else 'N/A'}")
                        logger.info(f"      dimensions={enriched['dimensions']}")
                    
                    return {
                        "status": "success",
                        "motion_graphics": enriched_mgs,
                        "failed": [],
                        "total_success": len(enriched_mgs),
                        "total_failed": 0,
                        "total": len(motion_graphics_plan),
                        "total_render_time": result.get('total_render_time', 0),
                        "method": "render-and-position"
                    }
                else:
                    error_msg = result.get('error', 'Unknown error')
                    logger.error(f"❌ [MANIM] /render-and-position falhou: {error_msg}")
                    # Fallback para render individual
                    logger.warning(f"⚠️ [MANIM] Tentando fallback para /render individual...")
                    return self._render_individual(
                        motion_graphics_plan=motion_graphics_plan,
                        job_id=job_id
                    )
            else:
                logger.error(f"❌ [MANIM] HTTP {response.status_code} em /render-and-position")
                logger.error(f"   Response: {response.text[:300]}")
                # Fallback para render individual
                logger.warning(f"⚠️ [MANIM] Tentando fallback para /render individual...")
                return self._render_individual(
                    motion_graphics_plan=motion_graphics_plan,
                    job_id=job_id
                )
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ [MANIM] Timeout em /render-and-position (>{self.timeout}s)")
            return self._render_individual(
                motion_graphics_plan=motion_graphics_plan,
                job_id=job_id
            )
        except Exception as e:
            logger.error(f"❌ [MANIM] Erro em /render-and-position: {e}", exc_info=True)
            return self._render_individual(
                motion_graphics_plan=motion_graphics_plan,
                job_id=job_id
            )
    
    def _render_individual(
        self,
        motion_graphics_plan: List[Dict],
        job_id: str = None
    ) -> Dict:
        """
        Fallback: renderiza cada MG individualmente via /render.
        Posição vem do orchestrator (target_position calculado no step 11.6.5).
        """
        logger.info(f"🎨 [MANIM] Usando /render individual (sem layout_service)")
        
        rendered_mgs = []
        failed_mgs = []
        
        for idx, mg in enumerate(motion_graphics_plan):
            mg_id = mg.get('id', 'unknown')
            mg_type = mg.get('type')
            mg_config = mg.get('config', {})
            mg_timing = mg.get('timing', {})
            mg_target_word = mg.get('target_word', '')
            mg_target_position = mg.get('target_position', {})
            mg_justification = mg.get('justification', '')
            
            try:
                logger.info(f"🎨 [MANIM] Renderizando {mg_id} ({mg_type})...")
                logger.info(f"   📋 [PLAN INPUT] target_word='{mg_target_word}'")
                logger.info(f"   📋 [PLAN INPUT] timing={mg_timing}")
                logger.info(f"   📋 [PLAN INPUT] config={mg_config}")
                logger.info(f"   📋 [PLAN INPUT] target_position={mg_target_position}")
                
                # Adicionar defaults de dimensão se não presentes no config
                enriched_config = {**mg_config}
                defaults = self.TYPE_DEFAULTS.get(mg_type, {'width': 300, 'height': 200})
                if 'width' not in enriched_config:
                    enriched_config['width'] = defaults['width']
                if 'height' not in enriched_config:
                    enriched_config['height'] = defaults['height']
                
                # Adicionar duration do timing ao config
                if 'duration' not in enriched_config and 'duration' in mg_timing:
                    enriched_config['duration'] = mg_timing['duration']
                
                # Incluir target_position no config para que o template Manim possa posicionar
                if mg_target_position:
                    enriched_config['target_position'] = mg_target_position
                
                logger.info(f"   📋 [ENRICHED CONFIG] {enriched_config}")
                
                # Payload para v-services/manim
                payload = {
                    "template": mg_type,
                    "config": enriched_config
                }
                
                # Chamar v-services/manim
                response = requests.post(
                    self.render_endpoint,
                    json=payload,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    logger.info(f"   📋 [RENDER RESPONSE] {json.dumps(result, default=str)[:300]}")
                    
                    if result.get('status') == 'success':
                        # output_path já é o path no volume compartilhado (/app/shared/manim/...)
                        output_path = result.get('output_path', '')
                        dimensions = result.get('dimensions', {})
                        
                        if not output_path:
                            logger.error(f"❌ [MANIM] {mg_id}: output_path vazio!")
                            failed_mgs.append({"id": mg_id, "error": "output_path vazio"})
                            continue
                        
                        # Extrair timing do plano do Director
                        raw_start_time = (
                            mg_timing.get('start_time') or 
                            mg_timing.get('start') or 
                            0
                        )
                        duration = mg_timing.get('duration', 0)
                        
                        # 🔧 v3.2.0: Buffer de timing — iniciar MG ANTES da palavra
                        start_time = max(0, raw_start_time - MG_TIMING_BUFFER_SECONDS)
                        logger.info(f"   ⏱️ [TIMING BUFFER] raw={raw_start_time}s → buffered={start_time}s")
                        
                        # Extrair posição calculada pelo orchestrator (step 11.6.5)
                        mg_x = mg_target_position.get('x', 0)
                        mg_y = mg_target_position.get('y', 0)
                        
                        # Montar resultado — com timing e posição propagados
                        render_data = {
                            'id': mg_id,
                            'type': mg_type,
                            'target_word': mg_target_word,
                            'timing': mg_timing,
                            # Campos que v-editor-python lê diretamente:
                            'start_time': start_time,
                            'duration': duration,
                            'x': mg_x,
                            'y': mg_y,
                            'position': {'x': mg_x, 'y': mg_y},
                            # Dimensões do render
                            'width': dimensions.get('width'),
                            'height': dimensions.get('height'),
                            'dimensions': dimensions,
                            # Arquivo
                            'local_path': output_path,
                            'src': output_path,
                            'url': output_path,
                            'video_url': output_path,
                            # 🔧 v3.1.0: z-index na faixa correta (acima de subtitles 2000)
                            'zIndex': 2200 + idx,
                            # Metadata
                            'config': mg_config,
                            'justification': mg_justification,
                            'cache_hit': result.get('cache_hit', False),
                            'render_time': result.get('render_time', 0)
                        }
                        
                        rendered_mgs.append(render_data)
                        logger.info(f"✅ [MANIM] {mg_id} renderizado:")
                        logger.info(f"   src={output_path}")
                        logger.info(f"   start_time={start_time}s, duration={duration}s")
                        logger.info(f"   position=({mg_x}, {mg_y})")
                        logger.info(f"   dimensions={dimensions}")
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        logger.error(f"❌ [MANIM] {mg_id} falhou: {error_msg}")
                        failed_mgs.append({
                            "id": mg_id,
                            "error": error_msg
                        })
                else:
                    logger.error(f"❌ [MANIM] {mg_id} HTTP Error {response.status_code}")
                    logger.error(f"   Response: {response.text[:200]}")
                    failed_mgs.append({
                        "id": mg_id,
                        "error": f"HTTP {response.status_code}"
                    })
            
            except requests.exceptions.Timeout:
                logger.error(f"❌ [MANIM] {mg_id} timeout (>{self.timeout}s)")
                failed_mgs.append({
                    "id": mg_id,
                    "error": "timeout"
                })
            except Exception as e:
                logger.error(f"❌ [MANIM] {mg_id} erro: {e}", exc_info=True)
                failed_mgs.append({
                    "id": mg_id,
                    "error": str(e)
                })
        
        # Resultado final
        total_success = len(rendered_mgs)
        total_failed = len(failed_mgs)
        
        if total_failed > 0:
            logger.warning(f"⚠️ [MANIM] {total_failed} motion graphics falharam")
        
        logger.info(f"✅ [MANIM] Renderização completa: {total_success} success, {total_failed} failed")
        
        return {
            "status": "success" if total_success > 0 else "error",
            "motion_graphics": rendered_mgs,
            "failed": failed_mgs,
            "total_success": total_success,
            "total_failed": total_failed,
            "total": len(motion_graphics_plan),
            "method": "render-individual"
        }
    
    def health_check(self) -> bool:
        """
        Verifica se v-services/manim está disponível.
        """
        try:
            response = requests.get(
                f"{self.base_url}/motion-graphics/health",
                timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"⚠️ v-services/manim health check failed: {e}")
            return False


# Singleton instance
_service_instance = None


def get_motion_graphics_render_service() -> MotionGraphicsRenderService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = MotionGraphicsRenderService()
    return _service_instance
