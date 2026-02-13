# 🎬 CARTELA SERVICE CLIENT
# Cliente HTTP para o v-services cartela endpoint
# Gera fundos visuais (cartelas) para frases individuais

import os
import requests
import logging
import random
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURATION
# ==========================================
V_SERVICES_BASE_URL = os.environ.get('V_SERVICES_URL', 'https://services.vinicius.ai')
DB_URL = os.environ.get('DB_REMOTE_URL')


class CartelaService:
    """
    Cliente para o Cartela Service do v-services.
    
    Gera cartelas (fundos visuais) para frases baseado nas configurações
    de cada estilo de texto (ts_*_cartela).
    """
    
    def __init__(self):
        self.base_url = f"{V_SERVICES_BASE_URL}/cartela"
        self._video_selector_state = {}  # Rastreia vídeos usados por collection
        logger.info(f"🎬 CartelaService inicializado: {self.base_url}")
    
    def _get_collection_assets(self, collection_id: str) -> List[Dict]:
        """
        Busca os assets de uma collection do banco de dados.
        
        Args:
            collection_id: ID da collection
            
        Returns:
            Lista de assets com url, id, aspect_ratio, etc.
        """
        if not DB_URL:
            logger.error("❌ DB_REMOTE_URL não configurada")
            return []
        
        try:
            conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT assets 
                FROM asset_collections 
                WHERE id = %s AND is_active = true
            """, (collection_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result and result.get('assets'):
                assets = result['assets']
                # Assets pode ser JSON string ou dict
                if isinstance(assets, str):
                    import json
                    assets = json.loads(assets)
                logger.info(f"🎬 Collection {collection_id}: {len(assets)} assets encontrados")
                return assets
            else:
                logger.warning(f"⚠️ Collection {collection_id} não encontrada ou sem assets")
                return []
                
        except Exception as e:
            logger.error(f"❌ Erro ao buscar collection {collection_id}: {e}")
            return []
    
    def _select_video_from_collection(
        self, 
        collection_id: str, 
        target_aspect: str = "9:16"
    ) -> Optional[Dict]:
        """
        Seleciona um vídeo aleatório da collection, sem repetir até todos serem usados.
        
        Args:
            collection_id: ID da collection
            target_aspect: Aspect ratio desejado ("9:16" ou "16:9")
            
        Returns:
            Asset selecionado com url, ou None
        """
        assets = self._get_collection_assets(collection_id)
        if not assets:
            return None
        
        # Filtrar apenas vídeos
        videos = [a for a in assets if a.get('asset_type') == 'video' or a.get('type') == 'video']
        
        # Se não houver vídeos marcados, tentar usar todos (assumindo que são vídeos)
        if not videos:
            videos = [a for a in assets if a.get('url', '').endswith(('.mp4', '.webm', '.mov'))]
        
        if not videos:
            logger.warning(f"⚠️ Collection {collection_id}: Nenhum vídeo encontrado")
            return None
        
        # Filtrar por aspect ratio se disponível
        matching = []
        for v in videos:
            aspect = v.get('aspect_ratio', '')
            # Aceitar variações de nomenclatura
            if target_aspect == "9:16":
                if aspect in ["9:16", "9x16", "vertical", "9/16", ""] or not aspect:
                    matching.append(v)
            elif target_aspect == "16:9":
                if aspect in ["16:9", "16x9", "horizontal", "16/9", ""] or not aspect:
                    matching.append(v)
            else:
                matching.append(v)
        
        # Se nenhum match, usar todos os vídeos
        if not matching:
            logger.info(f"🎬 Nenhum vídeo com aspect {target_aspect}, usando todos")
            matching = videos
        
        # Gerenciar estado de vídeos usados (para não repetir)
        if collection_id not in self._video_selector_state:
            self._video_selector_state[collection_id] = set()
        
        used = self._video_selector_state[collection_id]
        available = [v for v in matching if v.get('id') not in used and v.get('url') not in used]
        
        # Se todos foram usados, resetar
        if not available:
            logger.info(f"🔄 Todos os vídeos da collection foram usados, resetando...")
            self._video_selector_state[collection_id] = set()
            available = matching
        
        # Selecionar aleatoriamente
        selected = random.choice(available)
        
        # Marcar como usado
        self._video_selector_state[collection_id].add(selected.get('id', selected.get('url')))
        
        logger.info(f"🎲 Vídeo selecionado: {selected.get('url', 'N/A')[:50]}...")
        return selected
    
    def generate_cartelas(
        self,
        sentences: List[Dict],
        template_config: Dict,
        canvas_width: int,
        canvas_height: int,
        job_id: str
    ) -> Dict[str, Any]:
        """
        Gera cartelas para as frases que precisam.
        
        Suporta duas fontes de configuração:
        1. Template: _text_styles.*.cartela_config (presets visuais)
        2. Roteiro: sentence.cartela_override (tags [CARTELA: cor] do script)
        
        Se o roteiro define cartela mas o template não tem preset, gera um
        preset solid sintético baseado na cor do roteiro.
        
        Args:
            sentences: Lista de frases com style_type e cartela_preset_id
            template_config: Config do template com _text_styles
            canvas_width: Largura do canvas
            canvas_height: Altura do canvas
            job_id: ID do job
        
        Returns:
            Dict com cartelas geradas e stats
        """
        # Verificar se há cartelas configuradas
        # 🐛 FIX: Usar 'or {}' para tratar valores None explícitos
        text_styles = template_config.get('_text_styles') or {}
        
        # Coletar presets de cartela ativos (do template)
        active_presets = []
        for style_name, style_config in text_styles.items():
            # 🐛 FIX: style_config pode ser None se estilo não configurado
            cartela_config = (style_config or {}).get('cartela_config')
            if cartela_config and cartela_config.get('enabled'):
                preset = {
                    'id': style_name,  # Usar nome do estilo como ID
                    'type': cartela_config.get('type', 'solid'),
                    'layout': cartela_config.get('layout', 'fullscreen'),
                }
                
                if preset['type'] == 'solid':
                    preset['solid_config'] = cartela_config.get('solid_config', {})
                elif preset['type'] == 'gradient':
                    preset['gradient_config'] = cartela_config.get('gradient_config', {})
                elif preset['type'] == 'asset_video':
                    # 🔧 FIX: Campo correto é 'asset_video_config', não 'asset_config'
                    preset['asset_video_config'] = cartela_config.get('asset_video_config', {})
                
                active_presets.append(preset)
                logger.info(f"🎬 Preset de cartela ativo: {style_name} ({preset['type']})")
        
        # 🆕 Se o template não tem cartela mas o roteiro define [CARTELA: ...],
        # criar presets sintéticos a partir dos overrides do roteiro
        if not active_presets:
            script_presets = self._build_script_cartela_presets(sentences)
            if script_presets:
                active_presets = script_presets
                logger.info(f"🎬 {len(script_presets)} preset(s) de cartela criados a partir do roteiro")
        
        if not active_presets:
            logger.info("🎬 Nenhuma cartela configurada (template ou roteiro) - pulando")
            return {
                'status': 'skipped',
                'reason': 'Nenhuma cartela configurada',
                'generated_cartelas': [],
                'stats': {'total': 0}
            }
        
        # Chamar v-services
        endpoint = f"{self.base_url}/generate"
        payload = {
            'job_id': job_id,
            'canvas_width': canvas_width,
            'canvas_height': canvas_height,
            'presets': active_presets
        }
        
        try:
            logger.info(f"🎬 Gerando {len(active_presets)} cartelas...")
            response = requests.post(endpoint, json=payload, timeout=120)
            response.raise_for_status()
            result = response.json()
            
            cartelas = result.get('cartelas', {})
            logger.info(f"✅ Cartelas geradas: {len(cartelas)}")
            
            # Mapear cartelas para frases
            # 🎬 IMPORTANTE: Para cartelas de vídeo, NÃO selecionar o vídeo aqui!
            # A seleção será feita em assign_cartelas_to_phrases para cada frase individualmente
            generated = []
            for preset_id, cartela_info in cartelas.items():
                # Para cartelas de vídeo, apenas validar que tem collection_id
                if cartela_info.get('type') == 'asset_video':
                    collection_id = cartela_info.get('collection_id')
                    if not collection_id:
                        logger.warning(f"⚠️ Cartela vídeo '{preset_id}': collection_id não informado")
                        continue
                    # Guardar dimensões do canvas para uso posterior
                    cartela_info['canvas_width'] = canvas_width
                    cartela_info['canvas_height'] = canvas_height
                    logger.info(f"🎬 Cartela vídeo '{preset_id}': collection_id={collection_id} (seleção por frase)")
                
                generated.append({
                    'style_key': preset_id,
                    'cartela': cartela_info
                })
            
            return {
                'status': 'success',
                'generated_cartelas': generated,
                'stats': {
                    'total': len(generated),
                    'presets': list(cartelas.keys())
                }
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro ao gerar cartelas: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'generated_cartelas': [],
                'stats': {'total': 0}
            }
    
    def _build_script_cartela_presets(self, sentences: List[Dict]) -> List[Dict]:
        """
        🆕 Cria presets de cartela sintéticos a partir de overrides do roteiro.
        
        Quando o template não tem cartela configurada mas o roteiro tem tags
        [CARTELA: cor], criamos presets solid com as cores do roteiro.
        
        Args:
            sentences: Lista de frases com possíveis cartela_override
            
        Returns:
            Lista de presets sintéticos (ou vazio se não houver overrides)
        """
        # Coletar cores únicas dos overrides do roteiro
        colors_seen = {}
        for sentence in sentences:
            override = sentence.get('cartela_override', {})
            if override.get('enabled'):
                color = override.get('color', '#000000')
                opacity = override.get('opacity', 0.85)
                if color not in colors_seen:
                    colors_seen[color] = opacity
        
        if not colors_seen:
            return []
        
        presets = []
        for idx, (color, opacity) in enumerate(colors_seen.items()):
            preset_id = f"script_cartela_{idx}"
            preset = {
                'id': preset_id,
                'type': 'solid',
                'layout': 'fullscreen',
                'solid_config': {
                    'color': {'value': color},
                    'opacity': {'value': opacity},
                },
                # 🆕 Marcar como originado do roteiro
                '_from_script': True,
                '_script_color': color,
            }
            presets.append(preset)
            logger.info(f"🎬 Preset sintético do roteiro: {preset_id} (solid, cor={color}, opacidade={opacity})")
        
        return presets
    
    def assign_cartelas_to_phrases(
        self,
        sentences: List[Dict],
        generated_cartelas: List[Dict],
        phrase_classification: Dict
    ) -> List[Dict]:
        """
        Atribui cartelas às frases baseado na classificação.
        
        A cartela usada é determinada pelo style_type da frase (default/emphasis/letter_effect).
        Só é aplicada se use_cartela=True na classificação.
        
        🎬 IMPORTANTE: Para cartelas de vídeo, cada frase recebe um vídeo DIFERENTE
        da collection, selecionado aleatoriamente sem repetição até todos serem usados.
        
        Args:
            sentences: Lista de frases
            generated_cartelas: Cartelas geradas (por estilo)
            phrase_classification: Classificação de frases do template
        
        Returns:
            Sentences com cartela_info adicionado
        """
        # Criar mapa de cartelas por estilo (style_key = 'default' | 'emphasis' | 'letter_effect')
        cartela_map = {}
        for item in generated_cartelas:
            cartela_map[item['style_key']] = item['cartela']
        
        logger.info(f"🎬 Cartelas disponíveis: {list(cartela_map.keys())}")
        
        # Pegar classificação de frases (fallback se não existir nas sentences)
        classified = phrase_classification.get('classified_phrases', {})
        if isinstance(classified, dict):
            classified = classified.get('value', [])
        
        # Atribuir cartelas
        for i, sentence in enumerate(sentences):
            # 🆕 PRIORIDADE 1: Olhar diretamente na própria sentence (vem do merge com processedPhrases)
            use_cartela = sentence.get('use_cartela', None)
            style_type = sentence.get('style_type', 'default')
            cartela_override = sentence.get('cartela_override', {})
            
            # 🆕 PRIORIDADE 2: Fallback para phrase_classification do template
            if use_cartela is None:
                phrase_class = next(
                    (p for p in classified if p.get('phrase_index') == i),
                    None
                )
                if phrase_class:
                    use_cartela = phrase_class.get('use_cartela', False)
                    style_type = phrase_class.get('style_type', 'default')
                else:
                    use_cartela = False
            
            if not use_cartela:
                logger.debug(f"🎬 Frase {i}: sem cartela (use_cartela=False)")
                continue
            
            # 🆕 Resolver cartela: template preset OU preset sintético do roteiro
            base_cartela = None
            
            # Tentar match pelo style_type no cartela_map (presets do template)
            if style_type in cartela_map:
                base_cartela = cartela_map[style_type]
            
            # 🆕 Se não encontrou por style_type, tentar presets sintéticos do roteiro
            if not base_cartela and cartela_override.get('enabled'):
                script_color = cartela_override.get('color', '#000000')
                # Procurar preset sintético que corresponde à cor do override
                for key, cartela in cartela_map.items():
                    if cartela.get('_from_script') and cartela.get('_script_color') == script_color:
                        base_cartela = cartela
                        break
                # Se não encontrou preset correspondente, usar o primeiro preset disponível
                if not base_cartela and cartela_map:
                    base_cartela = next(iter(cartela_map.values()))
            
            if not base_cartela:
                logger.warning(f"⚠️ Frase {i}: use_cartela=True mas nenhum preset disponível "
                             f"(style='{style_type}', override={bool(cartela_override)})")
                continue
            
            # 🎬 Para cartelas de vídeo, selecionar um vídeo DIFERENTE para cada frase
            if base_cartela.get('type') == 'asset_video':
                collection_id = base_cartela.get('collection_id')
                canvas_width = base_cartela.get('canvas_width', 1080)
                canvas_height = base_cartela.get('canvas_height', 1920)
                
                if collection_id:
                    # Determinar aspect ratio
                    target_aspect = "9:16" if canvas_height > canvas_width else "16:9"
                    
                    # Selecionar vídeo único para esta frase
                    selected_video = self._select_video_from_collection(
                        collection_id=collection_id,
                        target_aspect=target_aspect
                    )
                    
                    if selected_video:
                        # Criar cópia da cartela com vídeo específico para esta frase
                        phrase_cartela = base_cartela.copy()
                        phrase_cartela['video_url'] = selected_video.get('url')
                        phrase_cartela['video_id'] = selected_video.get('id')
                        phrase_cartela['width'] = canvas_width
                        phrase_cartela['height'] = canvas_height
                        
                        # Incluir duração do vídeo para loop correto
                        video_duration = selected_video.get('duration')
                        if video_duration is None:
                            duration_ms = selected_video.get('duration_ms')
                            if duration_ms:
                                video_duration = duration_ms / 1000.0
                            else:
                                video_duration = 10.0  # Default
                        
                        phrase_cartela['video_duration'] = video_duration
                        phrase_cartela['video_duration_frames'] = int(video_duration * 30)
                        
                        sentence['cartela_info'] = phrase_cartela
                        logger.info(f"🎬 Frase {i}: vídeo selecionado '{selected_video.get('url', 'N/A')[:50]}...' (duration={video_duration}s)")
                    else:
                        logger.warning(f"⚠️ Frase {i}: Nenhum vídeo disponível na collection")
                else:
                    logger.warning(f"⚠️ Frase {i}: collection_id não configurado")
            else:
                # Cartelas não-vídeo (solid, gradient, etc.) - usar preset do template
                phrase_cartela = base_cartela.copy()
                
                # 🆕 Aplicar overrides do roteiro sobre o preset do template
                if cartela_override.get('enabled'):
                    phrase_cartela = self._apply_script_override(phrase_cartela, cartela_override, i)
                
                sentence['cartela_info'] = phrase_cartela
                source = "roteiro" if cartela_override.get('enabled') else f"template/{style_type}"
                logger.info(f"🎬 Frase {i}: cartela atribuída (source={source})")
        
        return sentences
    
    def _apply_script_override(
        self,
        cartela: Dict[str, Any],
        override: Dict[str, Any],
        sentence_idx: int
    ) -> Dict[str, Any]:
        """
        🆕 Aplica overrides do roteiro ([CARTELA: cor]) sobre um preset de cartela.
        
        O roteiro pode especificar:
        - color: Sobrescreve a cor (solid_config.color ou gradient_config.color_start)
        - opacity: Sobrescreve a opacidade
        - type: Pode mudar de solid para gradient etc. (futuro)
        
        Args:
            cartela: Preset de cartela (do template ou sintético)
            override: Overrides do roteiro {enabled, color, opacity?, type?}
            sentence_idx: Índice da frase (para logging)
            
        Returns:
            Cartela com overrides aplicados
        """
        result = dict(cartela)
        
        override_color = override.get('color')
        override_opacity = override.get('opacity')
        
        if override_color:
            cartela_type = result.get('type', 'solid')
            
            if cartela_type == 'solid':
                # Sobrescrever cor do solid
                if 'solid_config' not in result:
                    result['solid_config'] = {}
                result['solid_config']['color'] = {'value': override_color}
                logger.info(f"   📝 Frase {sentence_idx}: cor sobrescrita pelo roteiro → {override_color}")
                
            elif cartela_type == 'gradient':
                # Sobrescrever cor primária do gradiente
                if 'gradient_config' not in result:
                    result['gradient_config'] = {}
                result['gradient_config']['color_start'] = {'value': override_color}
                logger.info(f"   📝 Frase {sentence_idx}: cor gradiente sobrescrita pelo roteiro → {override_color}")
        
        if override_opacity is not None:
            if 'solid_config' in result:
                result['solid_config']['opacity'] = {'value': override_opacity}
            elif 'gradient_config' in result:
                result['gradient_config']['opacity'] = {'value': override_opacity}
            logger.info(f"   📝 Frase {sentence_idx}: opacidade sobrescrita pelo roteiro → {override_opacity}")
        
        return result

