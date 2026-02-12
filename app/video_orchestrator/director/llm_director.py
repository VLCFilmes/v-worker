"""
LLM Director - Agente IA que orquestra o pipeline via tool-use.

Em vez de uma lista fixa de steps (AutoRunner), o Director usa um LLM
para decidir quais steps executar, em que ordem, e com quais parâmetros.

Isso habilita:
- Decisões criativas (ex: "usar motion graphics mais sutis")
- Recuperação inteligente de erros (ex: "matting falhou, tentar sem")
- Fluxos customizados (ex: "só gerar PNGs com estilo emphasis")
"""

import json
import logging
import os
import time
from typing import Dict, List, Optional

from ..engine.models import PipelineState, StepResult
from ..engine.pipeline_engine import PipelineEngine
from ..engine.state_manager import StateManager
from .tool_builder import ToolBuilder

logger = logging.getLogger(__name__)

# Limites de segurança
MAX_ITERATIONS = 40
MAX_CONSECUTIVE_ERRORS = 5
DIRECTOR_TIMEOUT_S = 600  # 10 min total


class LLMDirector:
    """
    Agente LLM que usa tools para executar o pipeline.
    
    Uso:
        engine = PipelineEngine(state_manager=StateManager())
        director = LLMDirector(engine=engine)
        final_state = director.run(job_id, objective="Produza o vídeo com legendas animadas")
    """

    def __init__(
        self,
        engine: PipelineEngine,
        model: str = None,
        api_key: str = None,
    ):
        self.engine = engine
        self.model = model or os.environ.get('DIRECTOR_MODEL', 'gpt-4o-mini')
        self.api_key = api_key or os.environ.get('OPENAI_API_KEY')
        self.tools = ToolBuilder.from_registry(include_auxiliary=True)
        self.step_descriptions = ToolBuilder.get_step_descriptions()
        logger.info(f"🎬 [DIRECTOR] Inicializado (model={self.model}, tools={len(self.tools)})")

    def run(
        self,
        job_id: str,
        objective: str = None,
        initial_state: PipelineState = None,
    ) -> PipelineState:
        """
        Executa o pipeline guiado pelo LLM Director.
        
        Args:
            job_id: ID do job
            objective: Instrução/objetivo do usuário (opcional)
            initial_state: Estado inicial (se não fornecido, carrega do banco)
            
        Returns:
            PipelineState final após todas as decisões do Director
        """
        start_time = time.time()

        # Carregar ou usar state inicial
        state = initial_state or self.engine.state_manager.load(job_id)
        if not state:
            raise ValueError(f"Nenhum state encontrado para job {job_id}")

        logger.info(f"🎬 [DIRECTOR] Iniciando para job {job_id[:8]}...")
        logger.info(f"   Objective: {objective or '(padrão)'}")
        logger.info(f"   State: completed={state.completed_steps}")

        # Construir mensagens iniciais
        messages = [
            {"role": "system", "content": self._build_system_prompt(state)},
            {"role": "user", "content": objective or (
                "Produza o vídeo seguindo o pipeline padrão. "
                "Execute todos os steps necessários na ordem correta. "
                "Pule steps opcionais quando não forem necessários."
            )},
        ]

        consecutive_errors = 0

        for iteration in range(MAX_ITERATIONS):
            elapsed = time.time() - start_time
            if elapsed > DIRECTOR_TIMEOUT_S:
                logger.warning(f"⏰ [DIRECTOR] Timeout após {elapsed:.0f}s ({iteration} iterações)")
                break

            try:
                import openai
                client = openai.OpenAI(api_key=self.api_key)

                response = client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=self.tools if self.tools else None,
                    temperature=0.1,  # Baixa temperatura para decisões mais determinísticas
                )

                choice = response.choices[0]
                message = choice.message

                # LLM decidiu parar (sem tool calls)
                if choice.finish_reason == "stop" or not message.tool_calls:
                    logger.info(f"✅ [DIRECTOR] Finalizado após {iteration + 1} iterações "
                                 f"({elapsed:.1f}s)")
                    if message.content:
                        logger.info(f"   Mensagem final: {message.content[:200]}")
                    break

                # Processar tool calls
                messages.append(message)  # Adicionar assistant message com tool_calls

                for tool_call in message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_args = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}

                    logger.info(f"🔧 [DIRECTOR] Tool call: {tool_name}({tool_args})")

                    # Executar tool e obter resultado
                    tool_result = self._execute_tool(
                        job_id, tool_name, tool_args, state
                    )

                    # Atualizar state se foi um step
                    if tool_result.get('_new_state'):
                        state = tool_result.pop('_new_state')

                    # Adicionar resultado como tool response
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(tool_result, ensure_ascii=False, default=str),
                    })

                    # Resetar erro consecutivo em sucesso
                    if tool_result.get('success', True):
                        consecutive_errors = 0
                    else:
                        consecutive_errors += 1
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                            logger.error(f"❌ [DIRECTOR] {MAX_CONSECUTIVE_ERRORS} erros consecutivos, abortando")
                            break

            except ImportError:
                logger.error("❌ [DIRECTOR] openai não instalado. pip install openai")
                raise
            except Exception as e:
                logger.error(f"❌ [DIRECTOR] Erro na iteração {iteration}: {e}")
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    break
                # Adicionar erro como mensagem para o LLM tentar se recuperar
                messages.append({
                    "role": "user",
                    "content": f"Erro interno: {str(e)}. Tente uma abordagem diferente."
                })

        # Recarregar state final do banco (mais atualizado)
        final_state = self.engine.state_manager.load(job_id) or state
        elapsed = time.time() - start_time
        logger.info(f"🏁 [DIRECTOR] Concluído em {elapsed:.1f}s | "
                     f"Steps: {final_state.completed_steps}")

        return final_state

    def _execute_tool(
        self,
        job_id: str,
        tool_name: str,
        tool_args: dict,
        current_state: PipelineState,
    ) -> Dict:
        """
        Executa uma tool call do LLM.
        
        Diferencia entre:
        - Steps do pipeline (delegados ao PipelineEngine)
        - Tools auxiliares (get_pipeline_status, skip_step, finish_pipeline)
        """
        # ═══ Tools Auxiliares ═══
        if tool_name == "get_pipeline_status":
            return {
                "success": True,
                **current_state.summary(),
                "available_steps": [
                    s.name for s in self.engine.registry.all()
                    if s.name not in (current_state.completed_steps or [])
                ],
            }

        if tool_name == "skip_step":
            step_name = tool_args.get("step_name", "")
            reason = tool_args.get("reason", "Director decidiu pular")
            logger.info(f"⏭️ [DIRECTOR] Pulando step '{step_name}': {reason}")
            new_state = current_state.with_updates(
                skipped_steps=(current_state.skipped_steps or []) + [step_name]
            )
            self.engine.state_manager.save(job_id, new_state)
            return {
                "success": True,
                "step_skipped": step_name,
                "reason": reason,
                "_new_state": new_state,
            }

        if tool_name == "finish_pipeline":
            summary = tool_args.get("summary", "Pipeline concluído pelo Director")
            logger.info(f"🏁 [DIRECTOR] finish_pipeline: {summary}")
            return {
                "success": True,
                "finished": True,
                "summary": summary,
            }

        # ═══ Steps do Pipeline ═══
        step_def = self.engine.registry.get(tool_name)
        if not step_def:
            return {
                "success": False,
                "error": f"Step '{tool_name}' não encontrado. "
                         f"Disponíveis: {self.engine.registry.names()}",
            }

        # Verificar se já foi completado
        if tool_name in (current_state.completed_steps or []):
            return {
                "success": True,
                "already_completed": True,
                "message": f"Step '{tool_name}' já foi executado anteriormente.",
            }

        # Executar via engine
        result: StepResult = self.engine.run_step(
            job_id=job_id,
            step_name=tool_name,
            params=tool_args,
        )

        # Construir resposta para o LLM
        response = result.to_dict()

        # Se sucesso, recarregar state atualizado
        if result.success:
            new_state = self.engine.state_manager.load(job_id)
            if new_state:
                response['_new_state'] = new_state
                response['state_summary'] = new_state.summary()

        return response

    def _build_system_prompt(self, state: PipelineState) -> str:
        """Constrói o system prompt para o LLM Director."""
        return f"""Você é um diretor de vídeo AI. Seu trabalho é executar os steps do pipeline
de produção de vídeo na ordem correta, tomando decisões inteligentes.

## Estado atual do job
- Job ID: {state.job_id[:8]}...
- Template: {state.template_id or 'N/A'}
- Duração: {state.total_duration_ms or 0}ms
- Frases: {len(state.phrase_groups or [])}
- Steps completados: {state.completed_steps or []}
- Steps pulados: {state.skipped_steps or []}
- Erro: {state.error_message or 'Nenhum'}
- Vídeo Fase 1: {'Sim' if state.phase1_video_url else 'Não'}
- Transcrição: {'Sim' if state.transcription_text else 'Não'}
- Template carregado: {'Sim' if state.template_config else 'Não'}

## {self.step_descriptions}

## Regras
1. Execute os steps na ordem lógica (respeite dependências listadas acima)
2. Use get_pipeline_status para inspecionar o estado antes de decidir
3. Se um step falhar e for marcado como OPCIONAL, pule-o com skip_step
4. Se um step não-opcional falhar, tente entender o erro e adaptar
5. Quando o step 'render' for concluído com sucesso, chame finish_pipeline
6. NÃO execute steps que já foram completados (verifique completed_steps)
7. NÃO execute steps opcionais desnecessários (ex: matting sem person_overlay)
8. Sempre comece com load_template se ainda não foi executado

## Ordem típica do pipeline completo
load_template → normalize → concat → analyze → detect_silence → silence_cut →
transcribe → fraseamento → classify → generate_pngs → add_shadows →
apply_animations → calculate_positions → generate_backgrounds →
motion_graphics → matting → cartelas → subtitle_pipeline → render
"""
