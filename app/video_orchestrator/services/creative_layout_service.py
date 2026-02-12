"""
Creative Layout Service Client
==============================

Cliente para o serviço de Creative Layout do v-services.
Aplica variação de tamanhos e shifts de linha antes do PNG e Positioning.

Step: 10.5 (após classify, antes de generate_pngs)
"""

import logging
import requests
import os
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# URL do v-services (configurável via environment)
V_SERVICES_URL = os.environ.get('V_SERVICES_URL', 'http://v-services:5000')


class CreativeLayoutService:
    """
    Cliente para o serviço de Creative Layout.
    Aplica variação de tamanhos por classe gramatical e shifts de linha.
    """
    
    def __init__(self, debug_logger=None):
        self.base_url = V_SERVICES_URL
        self.endpoint = f"{self.base_url}/creative-layout/process"
        self.debug_logger = debug_logger
    
    def process(
        self,
        sentences: List[Dict[str, Any]],
        creative_layout_config: Dict[str, Any],
        job_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Processa as sentences aplicando creative layout.
        
        Args:
            sentences: Lista de sentences com words
            creative_layout_config: Configuração do creative layout do template
            job_id: ID do job para logging
            
        Returns:
            sentences com creative_layout info em cada palavra
        """
        # Verificar se feature está habilitada
        if not creative_layout_config.get('enabled', False):
            logger.info(f"[Job {job_id}] CreativeLayout: Feature desabilitada, pulando...")
            return sentences
        
        logger.info(f"[Job {job_id}] CreativeLayout: Processando {len(sentences)} sentences")
        
        # Preparar payload
        payload = {
            'sentences': sentences,
            'config': creative_layout_config
        }
        
        # Log input se debug_logger disponível
        if self.debug_logger and job_id:
            try:
                self.debug_logger.log_step(job_id, 'creative_layout', 'input', payload)
            except Exception as e:
                logger.warning(f"[Job {job_id}] Erro ao logar input: {e}")
        
        try:
            response = requests.post(
                self.endpoint,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                logger.error(f"[Job {job_id}] CreativeLayout: Erro HTTP {response.status_code}: {response.text}")
                return sentences
            
            result = response.json()
            
            if not result.get('success', False):
                logger.error(f"[Job {job_id}] CreativeLayout: Erro: {result.get('error', 'Unknown')}")
                return sentences
            
            processed_sentences = result.get('sentences', sentences)
            
            # Log output se debug_logger disponível
            if self.debug_logger and job_id:
                try:
                    self.debug_logger.log_step(job_id, 'creative_layout', 'output', {'sentences': processed_sentences})
                except Exception as e:
                    logger.warning(f"[Job {job_id}] Erro ao logar output: {e}")
            
            # Contar palavras processadas
            processed_count = sum(
                1 for s in processed_sentences 
                for w in s.get('words', []) 
                if 'creative_layout' in w
            )
            
            logger.info(f"[Job {job_id}] CreativeLayout: ✅ {processed_count} palavras com layout criativo")
            
            return processed_sentences
            
        except requests.exceptions.Timeout:
            logger.error(f"[Job {job_id}] CreativeLayout: Timeout ao chamar v-services")
            return sentences
        except requests.exceptions.ConnectionError:
            logger.error(f"[Job {job_id}] CreativeLayout: Erro de conexão com v-services")
            return sentences
        except Exception as e:
            logger.exception(f"[Job {job_id}] CreativeLayout: Erro inesperado: {e}")
            return sentences
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica se o serviço está saudável."""
        try:
            response = requests.get(
                f"{self.base_url}/creative-layout/health",
                timeout=5
            )
            return response.json() if response.status_code == 200 else {'status': 'unhealthy'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}


def extract_creative_layout_config(template_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrai a configuração de creative layout do template.
    
    Args:
        template_config: Configuração completa do template
        
    Returns:
        Configuração do creative layout
    """
    creative_layout = template_config.get('creative_layout', template_config.get('creative-layout', {}))
    
    # Se não existe, retornar config desabilitada
    if not creative_layout:
        return {'enabled': False}
    
    # Função auxiliar para extrair valores
    def get_value(obj, default):
        if obj is None:
            return default
        if isinstance(obj, dict) and 'value' in obj:
            return obj.get('value', default)
        return obj
    
    # Extrair configurações com valores padrão
    size_variation_raw = creative_layout.get('size_variation', {})
    line_shift_raw = creative_layout.get('line_shift', {})
    shift_x_raw = line_shift_raw.get('shift_x', {})
    shift_y_raw = line_shift_raw.get('shift_y', {})
    
    config = {
        'enabled': get_value(creative_layout.get('enabled'), False),
        'apply_to_styles': get_value(creative_layout.get('apply_to_styles'), ['default', 'emphasis']),
        'size_variation': {
            'enabled': get_value(size_variation_raw.get('enabled'), True),
            'highlight_classes': get_value(
                size_variation_raw.get('highlight_classes'), 
                ['nouns', 'adjectives', 'proper_names']
            ),
            'level2_reduction_percent': get_value(
                size_variation_raw.get('level2_reduction_percent'), 
                20
            ),
            'level3_reduction_percent': get_value(
                size_variation_raw.get('level3_reduction_percent'), 
                10
            )
        },
        'line_shift': {
            'shift_x': {
                'enabled': get_value(shift_x_raw.get('enabled'), True),
                'intensity_percent': get_value(shift_x_raw.get('intensity_percent'), 5),
                'pattern': get_value(shift_x_raw.get('pattern'), 'alternate')
            },
            'shift_y': {
                'enabled': get_value(shift_y_raw.get('enabled'), False),
                'intensity_percent': get_value(shift_y_raw.get('intensity_percent'), 2),
                'pattern': get_value(shift_y_raw.get('pattern'), 'cascade_down')
            },
            'single_line_behavior': get_value(
                line_shift_raw.get('single_line_behavior'), 
                'center'
            )
        }
    }
    
    return config

