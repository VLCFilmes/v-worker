"""
Base imports e helpers para steps.
Cada step importa daqui para evitar repetição.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from ..engine.models import PipelineState
from ..engine.step_registry import register_step

logger = logging.getLogger(__name__)


def extract_value(raw):
    """Extrai valor de formato {value: x} ou valor direto (padrão do schema)."""
    if isinstance(raw, dict):
        return raw.get('value')
    return raw


def get_env(key: str, default: str = None) -> Optional[str]:
    """Busca variável de ambiente."""
    return os.environ.get(key, default)
