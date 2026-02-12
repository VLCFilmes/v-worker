"""
Pipeline Engine v3 - Motor declarativo de steps.

Arquitetura modular:
- PipelineEngine: executa steps em ordem, persiste estado
- StateManager: estado centralizado no PostgreSQL
- StepRegistry: steps declarativos via decorator
- AutoRunner: modo automático (sem LLM, zero tokens)
- Replay: re-execução parcial do pipeline (v3.10.0)
"""

from .models import PipelineState, StepResult
from .step_registry import StepRegistry, register_step
from .state_manager import StateManager
from .pipeline_engine import PipelineEngine
from .auto_runner import AutoRunner
from .events import EngineEvents
from .bridge import EngineBridge, get_engine_bridge
from .replay import (
    prepare_replay,
    reconstruct_state_until,
    apply_modifications,
    get_steps_from,
    estimate_replay_time,
    validate_modifications,
)

__all__ = [
    'PipelineState',
    'StepResult',
    'StepRegistry',
    'register_step',
    'StateManager',
    'PipelineEngine',
    'AutoRunner',
    'EngineEvents',
    'EngineBridge',
    'get_engine_bridge',
    # v3.10.0: Replay
    'prepare_replay',
    'reconstruct_state_until',
    'apply_modifications',
    'get_steps_from',
    'estimate_replay_time',
    'validate_modifications',
]
