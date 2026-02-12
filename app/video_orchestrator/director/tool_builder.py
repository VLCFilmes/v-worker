"""
Tool Builder - Converte steps registrados em tools OpenAI function calling.

Gera automaticamente as tool definitions a partir do StepRegistry,
para que o LLM Director possa chamar qualquer step via tool-use.

Também inclui tools auxiliares:
- get_pipeline_status: inspecionar estado atual
- skip_step: pular step opcional
"""

import logging
from typing import Dict, List, Optional

from ..engine.step_registry import StepRegistry

logger = logging.getLogger(__name__)


class ToolBuilder:
    """
    Converte StepRegistry em OpenAI-compatible tool definitions.
    
    Uso:
        tools = ToolBuilder.from_registry()
        # Passa para openai.chat.completions.create(tools=tools)
    """

    @classmethod
    def from_registry(cls, include_auxiliary: bool = True) -> List[Dict]:
        """
        Gera tool definitions a partir de todos os steps registrados.
        
        Args:
            include_auxiliary: Se True, inclui tools auxiliares (status, skip)
            
        Returns:
            Lista de tools no formato OpenAI function calling
        """
        tools = []

        # Step tools (do StepRegistry)
        step_tools = StepRegistry.get_tools_for_director()
        tools.extend(step_tools)

        # Auxiliary tools
        if include_auxiliary:
            tools.extend(cls._auxiliary_tools())

        logger.info(f"🔧 [TOOL_BUILDER] {len(tools)} tools geradas "
                     f"({len(step_tools)} steps + {len(tools) - len(step_tools)} auxiliares)")

        return tools

    @classmethod
    def _auxiliary_tools(cls) -> List[Dict]:
        """Tools auxiliares que não são steps do pipeline."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_pipeline_status",
                    "description": (
                        "Retorna o estado atual do pipeline: steps completados, "
                        "steps pendentes, erros, duração, e um resumo dos dados "
                        "disponíveis. Use para inspecionar progresso antes de "
                        "decidir o próximo step."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "skip_step",
                    "description": (
                        "Marca um step opcional como pulado. Use quando um step "
                        "não é necessário para este job específico (ex: matting "
                        "sem person_overlay, motion_graphics sem prompt)."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "step_name": {
                                "type": "string",
                                "description": "Nome do step a pular"
                            },
                            "reason": {
                                "type": "string",
                                "description": "Motivo para pular (para logs)"
                            }
                        },
                        "required": ["step_name"],
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "finish_pipeline",
                    "description": (
                        "Indica que o pipeline está completo e o Director "
                        "deve encerrar. Use após o step 'render' ter sido "
                        "executado com sucesso."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "summary": {
                                "type": "string",
                                "description": "Resumo do que foi executado"
                            }
                        },
                        "required": [],
                    }
                }
            },
        ]

    @classmethod
    def get_step_descriptions(cls) -> str:
        """
        Gera texto descritivo de todos os steps para incluir no system prompt.
        
        Returns:
            String formatada com nomes, categorias e dependências
        """
        lines = ["Steps disponíveis no pipeline:\n"]

        for step_def in StepRegistry.all():
            optional_tag = " (OPCIONAL)" if step_def.optional else ""
            cost_tag = f" [{step_def.cost_category}]" if step_def.cost_category != "free" else ""
            deps = f" → depende de: {', '.join(step_def.depends_on)}" if step_def.depends_on else ""

            lines.append(
                f"- {step_def.name}{optional_tag}{cost_tag}: "
                f"{step_def.description}{deps}"
            )

        return "\n".join(lines)
