"""
Step Registry - Registro global de steps com auto-discovery.

Steps se registram via decorator @register_step.
O registry resolve dependências (topological sort) e fornece
metadata para o LLM Director (tool schemas).
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class StepDefinition:
    """Metadados de um step registrado."""
    name: str
    fn: Callable
    description: str = ""
    category: str = "default"           # preprocessing, rendering, creative, setup
    depends_on: List[str] = field(default_factory=list)
    produces: List[str] = field(default_factory=list)
    optional: bool = False              # Se True, falha não interrompe pipeline
    estimated_duration_s: int = 10
    cost_category: str = "free"         # free, cpu, gpu, llm
    retryable: bool = True
    max_retries: int = 2
    timeout_s: int = 300                # 5 min default
    tool_schema: Optional[Dict] = None  # Schema para LLM Director tools
    # Mapeamento para nome de step SSE (display_config.py)
    sse_step_name: Optional[str] = None  # Ex: "PNG_GEN", "TRANSCRIBE"
    # 🆕 v4.3.0: Async Subflows (Fire-and-Wait)
    async_mode: bool = False            # Se True, dispara em background (thread)
    await_async: List[str] = field(default_factory=list)  # Steps async a esperar antes de rodar


class StepRegistry:
    """
    Registro global de steps. Thread-safe (imutável após import).
    
    Uso:
        @register_step(name="classify", depends_on=["fraseamento"], ...)
        def classify_step(state, params):
            ...
    """
    _steps: Dict[str, StepDefinition] = {}
    _initialized: bool = False

    @classmethod
    def register(cls, **kwargs):
        """Decorator para registrar um step."""
        def decorator(fn):
            step_def = StepDefinition(fn=fn, **kwargs)
            cls._steps[step_def.name] = step_def
            logger.debug(f"📦 Step registrado: {step_def.name} (category={step_def.category})")
            return fn
        return decorator

    @classmethod
    def get(cls, name: str) -> Optional[StepDefinition]:
        """Busca step por nome."""
        cls._ensure_initialized()
        step = cls._steps.get(name)
        if not step:
            logger.error(f"❌ Step não encontrado: {name}. Disponíveis: {list(cls._steps.keys())}")
        return step

    @classmethod
    def all(cls) -> List[StepDefinition]:
        """Retorna todos os steps registrados."""
        cls._ensure_initialized()
        return list(cls._steps.values())

    @classmethod
    def names(cls) -> List[str]:
        """Retorna nomes de todos os steps registrados."""
        cls._ensure_initialized()
        return list(cls._steps.keys())

    @classmethod
    def resolve_order(cls, requested_steps: List[str]) -> List[str]:
        """
        Resolve ordem de execução respeitando dependências (topological sort).
        
        Apenas steps explicitamente solicitados são incluídos.
        Dependências são usadas apenas para ordenação, não para inclusão automática.
        
        Args:
            requested_steps: Lista de steps que o caller quer executar
            
        Returns:
            Lista ordenada respeitando dependências
        """
        cls._ensure_initialized()

        # Filtrar apenas steps que existem no registry
        valid_steps = set()
        for name in requested_steps:
            if name in cls._steps:
                valid_steps.add(name)
            else:
                logger.warning(f"⚠️ Step '{name}' solicitado mas não registrado, ignorando")

        # Topological sort (Kahn's algorithm)
        # Construir grafo de dependências apenas para os steps solicitados
        in_degree = defaultdict(int)
        graph = defaultdict(list)

        for name in valid_steps:
            if name not in in_degree:
                in_degree[name] = 0
            step_def = cls._steps[name]
            for dep in step_def.depends_on:
                if dep in valid_steps:
                    graph[dep].append(name)
                    in_degree[name] += 1

        # Iniciar com nós sem dependências
        queue = [name for name in valid_steps if in_degree[name] == 0]
        # Ordenar para determinismo (mesma ordem sempre)
        queue.sort(key=lambda n: requested_steps.index(n) if n in requested_steps else 999)

        result = []
        while queue:
            # Pegar o primeiro (menor índice no request original)
            queue.sort(key=lambda n: requested_steps.index(n) if n in requested_steps else 999)
            node = queue.pop(0)
            result.append(node)

            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Verificar ciclos
        if len(result) != len(valid_steps):
            missing = valid_steps - set(result)
            logger.error(f"❌ Dependência circular detectada! Steps afetados: {missing}")
            # Fallback: retornar na ordem original
            return [s for s in requested_steps if s in valid_steps]

        return result

    @classmethod
    def get_tools_for_director(cls) -> List[Dict]:
        """
        Converte steps registrados em tool definitions para o LLM Director.
        
        Returns:
            Lista de tools no formato OpenAI function calling
        """
        cls._ensure_initialized()
        tools = []
        for step_def in cls._steps.values():
            if step_def.tool_schema:
                tool = {
                    "type": "function",
                    "function": {
                        "name": step_def.name,
                        "description": step_def.tool_schema.get(
                            "description", step_def.description
                        ),
                        "parameters": {
                            "type": "object",
                            "properties": step_def.tool_schema.get("input", {}),
                            "required": [],
                        }
                    }
                }
                tools.append(tool)
        return tools

    @classmethod
    def _ensure_initialized(cls):
        """Auto-discovery: importa o módulo steps/ para registrar todos os steps."""
        if not cls._initialized:
            cls._initialized = True
            try:
                # Importar o pacote steps/ que faz auto-discovery
                from .. import steps as _  # noqa: F401
                logger.info(f"📦 StepRegistry: {len(cls._steps)} steps registrados")
            except ImportError as e:
                logger.warning(f"⚠️ Auto-discovery de steps falhou: {e}")

    @classmethod
    def reset(cls):
        """Reset para testes."""
        cls._steps = {}
        cls._initialized = False


# Alias global para uso como decorator
register_step = StepRegistry.register
