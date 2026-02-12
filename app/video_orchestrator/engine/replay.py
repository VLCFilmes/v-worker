"""
🔄 Pipeline Replay — Re-execução parcial do pipeline.

v3.10.0: Permite ao LLM Director (e endpoint) re-executar o pipeline
a partir de um step específico, com modificações no estado.

v4.4.1: Corrigido merge de outputs de async steps (Fire-and-Wait) ao
reconstruir state para replay. Sem isso, replay a partir de steps que
dependem de async steps (ex: render → video_clipper, cartelas → matting)
perdia os outputs dos async steps.

Fluxo:
1. reconstruct_state_until(job_id, target_step) → PipelineState antes do step alvo
2. apply_modifications(state_dict, mods) → state_dict com campos alterados
3. get_steps_from(target_step) → lista de steps a executar
4. Engine.run(new_job_id, steps, modified_state)

Usado por:
- bridge.replay_pipeline()
- endpoint POST /api/video/job/{job_id}/replay-from/{step_name}
- Director tool: replay_from_step()
"""

import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from .auto_runner import ALL_STEPS
from .models import PipelineState

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Campos protegidos — NUNCA podem ser modificados via replay
# ═══════════════════════════════════════════════════════════════

BLOCKED_FIELDS = {
    "job_id",
    "project_id",
    "user_id",
    "conversation_id",
    "webhook_url",
    "original_video_url",
    "video_width",
    "video_height",
    "completed_steps",
    "skipped_steps",
    "failed_step",
    "step_timings",
    "error_message",
    "engine_version",
    "created_at",
}

# Campos estimados de custo por step alvo (segundos)
STEP_COST_ESTIMATES = {
    "load_template": 2,
    "normalize": 15,
    "concat": 10,
    "analyze": 5,
    "detect_silence": 5,
    "silence_cut": 10,
    "transcribe": 30,
    "video_clipper": 15,         # 🆕 v4.4: LLM async (~5-15s)
    "merge_transcriptions": 2,
    "fraseamento": 5,
    "classify": 8,
    "generate_pngs": 15,
    "add_shadows": 5,
    "apply_animations": 3,
    "calculate_positions": 3,
    "generate_backgrounds": 10,
    "motion_graphics": 45,
    "matting": 75,
    "cartelas": 5,
    "subtitle_pipeline": 10,
    "title_generation": 5,
    "render": 25,
}


def get_step_index(step_name: str) -> int:
    """Retorna o índice de um step na ordem canônica (ALL_STEPS)."""
    try:
        return ALL_STEPS.index(step_name)
    except ValueError:
        return -1


def get_previous_step(step_name: str) -> Optional[str]:
    """
    Retorna o step imediatamente anterior ao step dado.
    
    Args:
        step_name: Nome do step alvo
        
    Returns:
        Nome do step anterior, ou None se é o primeiro step
    """
    idx = get_step_index(step_name)
    if idx <= 0:
        return None
    return ALL_STEPS[idx - 1]


def get_steps_from(target_step: str) -> List[str]:
    """
    Retorna todos os steps do target até o final do pipeline.
    
    Args:
        target_step: Nome do step a partir do qual executar
        
    Returns:
        Lista ordenada de steps (do target até 'render')
        
    Raises:
        ValueError: Se o step não existe em ALL_STEPS
    """
    idx = get_step_index(target_step)
    if idx < 0:
        raise ValueError(
            f"Step '{target_step}' não encontrado. "
            f"Steps válidos: {ALL_STEPS}"
        )
    return ALL_STEPS[idx:]


def estimate_replay_time(target_step: str) -> int:
    """
    Estima o tempo total de replay (em segundos) a partir de um step.
    
    Args:
        target_step: Step a partir do qual re-executar
        
    Returns:
        Estimativa em segundos
    """
    steps = get_steps_from(target_step)
    return sum(STEP_COST_ESTIMATES.get(s, 10) for s in steps)


def validate_modifications(modifications: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Valida as modificações antes de aplicar.
    
    Regras:
    1. Campos bloqueados não podem ser modificados
    2. Paths devem ser strings não-vazias
    3. Valores não podem ser None (use "" ou 0 para "limpar")
    
    Args:
        modifications: Dict de dot-notation paths → valores
        
    Returns:
        Tuple (is_valid, error_message)
    """
    if not modifications:
        return True, ""
    
    for path, value in modifications.items():
        if not isinstance(path, str) or not path.strip():
            return False, f"Path inválido: {path!r}"
        
        # Verificar campo raiz contra blocked list
        root_field = path.split(".")[0]
        if root_field in BLOCKED_FIELDS:
            return False, (
                f"Campo protegido: '{root_field}' não pode ser modificado via replay. "
                f"Campos protegidos: {sorted(BLOCKED_FIELDS)}"
            )
    
    return True, ""


def apply_modifications(state_dict: Dict, modifications: Dict[str, Any]) -> Dict:
    """
    Aplica modificações dot-notation em um state dict.
    
    Exemplos de modificação:
        {"text_styles.default.fill_color": "#0000FF"}
        {"template_config.text_styles.emphasis.font_size": 48}
    
    Args:
        state_dict: PipelineState.to_dict() (será modificado in-place e retornado)
        modifications: Dict de paths dot-notation → novos valores
        
    Returns:
        state_dict modificado
        
    Raises:
        KeyError: Se um path intermediário não existir
        ValueError: Se um campo é protegido
    """
    if not modifications:
        return state_dict
    
    # Validar primeiro
    is_valid, error = validate_modifications(modifications)
    if not is_valid:
        raise ValueError(error)
    
    for path, value in modifications.items():
        parts = path.split(".")
        
        # Navegar até o penúltimo nível
        current = state_dict
        for i, part in enumerate(parts[:-1]):
            # Suportar acesso a arrays: "tracks.subtitles[0].position.x"
            if "[" in part:
                key, idx_str = part.split("[")
                idx = int(idx_str.rstrip("]"))
                if key not in current or not isinstance(current[key], list):
                    raise KeyError(
                        f"Path '{'.'.join(parts[:i+1])}' não é uma lista. "
                        f"Disponível: {list(current.keys()) if isinstance(current, dict) else type(current)}"
                    )
                current = current[key][idx]
            else:
                if not isinstance(current, dict):
                    raise KeyError(
                        f"Path '{'.'.join(parts[:i+1])}' — "
                        f"esperado dict, encontrado {type(current).__name__}"
                    )
                if part not in current or current[part] is None:
                    # Auto-criar dicts intermediários (para campos novos ou None)
                    current[part] = {}
                    logger.info(f"   ℹ️ Criando campo intermediário: {'.'.join(parts[:i+1])}")
                current = current[part]
        
        # Setar o valor final
        final_key = parts[-1]
        if "[" in final_key:
            key, idx_str = final_key.split("[")
            idx = int(idx_str.rstrip("]"))
            current[key][idx] = value
        else:
            old_value = current.get(final_key, "<não existia>") if isinstance(current, dict) else "<n/a>"
            current[final_key] = value
            logger.info(f"   🔧 {path}: {old_value!r} → {value!r}")
    
    return state_dict


def reconstruct_state_until(
    job_id: str,
    target_step: str
) -> Optional[PipelineState]:
    """
    Reconstrói o PipelineState como estava ANTES do target_step executar.
    
    Estratégia: carrega o checkpoint (state_after) do step imediatamente anterior.
    
    Exemplo:
        reconstruct_state_until(job_id, "generate_pngs")
        → carrega checkpoint de "classify" (step anterior)
        → retorna PipelineState após classify, ANTES de generate_pngs
    
    Args:
        job_id: ID do job original
        target_step: Nome do step a partir do qual queremos re-rodar
        
    Returns:
        PipelineState reconstruído, ou None se checkpoints insuficientes
    """
    from ..debug_logger import get_debug_logger
    debug = get_debug_logger()
    
    previous_step = get_previous_step(target_step)
    
    if previous_step is None:
        # Target é o primeiro step (load_template) — não há checkpoint anterior
        # Precisamos do estado INICIAL do job (antes de qualquer step)
        logger.info(f"🔄 [REPLAY] Target é primeiro step ({target_step}), "
                     f"carregando estado inicial do job")
        
        # Tentar carregar do banco diretamente
        from .state_manager import StateManager
        from app.supabase_client import get_direct_db_connection
        sm = StateManager(db_connection_func=get_direct_db_connection)
        state = sm.load(job_id)
        
        if state:
            # Limpar todos os completed_steps pois vamos re-rodar tudo
            state = state.with_updates(
                completed_steps=[],
                skipped_steps=[],
                failed_step=None,
                error_message=None,
                step_timings={},
            )
            return state
        
        logger.error(f"❌ [REPLAY] Não foi possível carregar estado inicial do job {job_id}")
        return None
    
    # Carregar checkpoint do step anterior
    logger.info(f"🔄 [REPLAY] Carregando checkpoint de '{previous_step}' "
                 f"(step anterior a '{target_step}')")
    
    checkpoint_dict = debug.get_step_checkpoint(job_id, previous_step)
    
    if checkpoint_dict is None:
        logger.error(f"❌ [REPLAY] Checkpoint não encontrado para '{previous_step}' "
                      f"do job {job_id[:8]}...")
        return None
    
    # Reconstruir PipelineState do checkpoint
    state = PipelineState.from_dict(checkpoint_dict)
    
    # Remover o target_step e todos os posteriores de completed_steps
    # (vamos re-executá-los)
    target_idx = get_step_index(target_step)
    steps_to_rerun = set(ALL_STEPS[target_idx:])
    
    new_completed = [s for s in state.completed_steps if s not in steps_to_rerun]
    new_skipped = [s for s in state.skipped_steps if s not in steps_to_rerun]
    
    state = state.with_updates(
        completed_steps=new_completed,
        skipped_steps=new_skipped,
        failed_step=None,
        error_message=None,
    )
    
    logger.info(f"✅ [REPLAY] State reconstruído: "
                 f"completed={new_completed}, "
                 f"steps_to_rerun={sorted(steps_to_rerun)}")
    
    # 🆕 v4.4.1: Mergear outputs de async steps que não serão re-executados
    # Sem isso, replay a partir de steps com await_async (ex: render, cartelas)
    # perde os outputs dos async steps (ex: video_clipper_track, matting_segments)
    state = _merge_async_outputs_for_replay(
        job_id, target_step, state, debug
    )
    
    return state


def _merge_async_outputs_for_replay(
    job_id: str,
    target_step: str,
    state: PipelineState,
    debug_logger
) -> PipelineState:
    """
    🆕 v4.4.1: Garante que outputs de async steps estejam presentes no state
    quando fazendo replay a partir de um step que depende deles.
    
    Problema: Steps com async_mode=True (ex: video_clipper, matting) rodam
    em thread separada. Seus outputs são mesclados no state principal pelo
    _await_async_step APENAS quando o step dependente (await_async) vai rodar.
    Isso significa que o checkpoint do step ANTERIOR ao dependente NÃO contém
    esses outputs.
    
    Exemplo:
        video_clipper fires (async) → merge_transcriptions → ... → subtitle_pipeline
        render (await_async=["video_clipper"]) → merge video_clipper_track → render
        
        Checkpoint de subtitle_pipeline NÃO tem video_clipper_track.
        Replay from render → carrega subtitle_pipeline checkpoint → sem video_clipper_track!
    
    Solução: Após carregar o checkpoint base, identifica quais async steps
    são dependências de steps que serão re-executados mas NÃO estão na lista
    de re-execução. Para esses, carrega seus checkpoints e mergeia seus
    `produces` no state reconstruído.
    
    Args:
        job_id: ID do job original
        target_step: Step a partir do qual o replay começa
        state: PipelineState reconstruído (sem async outputs)
        debug_logger: DebugLogger para buscar checkpoints
        
    Returns:
        PipelineState com async outputs mergeados (se necessário)
    """
    from .step_registry import StepRegistry
    
    steps_to_run = get_steps_from(target_step)
    steps_to_run_set = set(steps_to_run)
    
    # Coletar todos os async dependencies que NÃO serão re-executados
    # (ou seja, já rodaram no pipeline original e seus outputs precisam estar no state)
    missing_async_deps = set()
    for step_name in steps_to_run:
        step_def = StepRegistry.get(step_name)
        if step_def and step_def.await_async:
            for async_dep in step_def.await_async:
                if async_dep not in steps_to_run_set:
                    missing_async_deps.add(async_dep)
    
    if not missing_async_deps:
        return state
    
    logger.info(f"🔄 [REPLAY] Async deps fora de steps_to_run: {missing_async_deps}")
    
    state_dict = state.to_dict()
    merged_any = False
    
    for async_name in missing_async_deps:
        step_def = StepRegistry.get(async_name)
        if not step_def or not step_def.produces:
            logger.info(f"   ⏭️ '{async_name}' sem produces definidos, ignorando")
            continue
        
        # Verificar se os campos já estão no state (pode acontecer se
        # o checkpoint base já incluiu por algum outro caminho)
        needs_merge = False
        for field_name in step_def.produces:
            if state_dict.get(field_name) is None:
                needs_merge = True
                break
        
        if not needs_merge:
            logger.info(f"   ✅ '{async_name}' outputs já presentes no state")
            continue
        
        # Tentar carregar checkpoint do await merge (salvo pelo Fix 1 - v4.4.1)
        checkpoint = debug_logger.get_step_checkpoint(job_id, f"await_{async_name}")
        
        if checkpoint is None:
            # Fallback: checkpoint do próprio async step (salvo pela thread async)
            checkpoint = debug_logger.get_step_checkpoint(job_id, async_name)
        
        if checkpoint is None:
            logger.warning(
                f"   ⚠️ Checkpoint de '{async_name}' não encontrado — "
                f"outputs podem estar faltando (step opcional/não executado?)"
            )
            continue
        
        # Mergear APENAS os campos `produces` (mesma lógica do _await_async_step)
        for field_name in step_def.produces:
            value = checkpoint.get(field_name)
            if value is not None:
                state_dict[field_name] = value
                merged_any = True
                logger.info(f"   🔄 Mergeado '{field_name}' do checkpoint de '{async_name}'")
        
        # Mergear campos extras conhecidos (matted_video_url do matting)
        for extra_field in ['matted_video_url']:
            val = checkpoint.get(extra_field)
            if val is not None and state_dict.get(extra_field) is None:
                state_dict[extra_field] = val
                merged_any = True
                logger.info(f"   🔄 Mergeado extra '{extra_field}' de '{async_name}'")
    
    if merged_any:
        state = PipelineState.from_dict(state_dict)
        logger.info(f"✅ [REPLAY] State enriquecido com outputs de async steps")
    
    return state


def _sync_text_styles_to_template_config(
    state_dict: Dict,
    modifications: Dict[str, Any]
) -> None:
    """
    Sincroniza state.text_styles → state.template_config._text_styles
    quando uma modificação envolve text_styles.
    
    Problema: O pipeline tem DUAS cópias de text_styles:
      1. state.text_styles — setado em load_template, usado no state
      2. state.template_config._text_styles — injetado no template_config para
         compatibilidade com serviços (generate_pngs, cartelas, etc.)
    
    O apply_modifications opera em state.text_styles (path: "text_styles.X.Y"),
    mas o step generate_pngs lê de template_config._text_styles.
    
    Esta função copia state.text_styles → template_config._text_styles após
    cada apply_modifications, garantindo que ambas as cópias estejam sincronizadas.
    
    Também sincroniza na direção inversa (template_config._text_styles → text_styles)
    caso a modificação use o path "template_config._text_styles.X.Y".
    """
    has_text_styles_mod = any(
        path.startswith("text_styles.") for path in modifications
    )
    has_template_text_styles_mod = any(
        path.startswith("template_config._text_styles.") for path in modifications
    )
    
    template_config = state_dict.get("template_config")
    if not isinstance(template_config, dict):
        return
    
    if has_text_styles_mod:
        # Copiar state.text_styles → template_config._text_styles
        text_styles = state_dict.get("text_styles")
        if text_styles:
            template_config["_text_styles"] = deepcopy(text_styles)
            logger.info("   🔄 [SYNC] state.text_styles → template_config._text_styles")
    
    elif has_template_text_styles_mod:
        # Copiar template_config._text_styles → state.text_styles
        ts = template_config.get("_text_styles")
        if ts:
            state_dict["text_styles"] = deepcopy(ts)
            logger.info("   🔄 [SYNC] template_config._text_styles → state.text_styles")


def prepare_replay(
    job_id: str,
    target_step: str,
    modifications: Optional[Dict[str, Any]] = None
) -> Tuple[Optional[PipelineState], List[str], str]:
    """
    Prepara tudo para um replay: state reconstruído + modificações + steps.
    
    Função de conveniência que combina:
    1. reconstruct_state_until()
    2. apply_modifications()
    3. get_steps_from()
    
    Args:
        job_id: ID do job original
        target_step: Step a partir do qual re-rodar
        modifications: Modificações dot-notation a aplicar
        
    Returns:
        Tuple (modified_state, steps_to_run, error_message)
        Se error_message não for vazio, a operação falhou.
    """
    # 1. Validar step
    if get_step_index(target_step) < 0:
        return None, [], f"Step '{target_step}' não existe. Válidos: {ALL_STEPS}"
    
    # 2. Validar modifications
    if modifications:
        is_valid, error = validate_modifications(modifications)
        if not is_valid:
            return None, [], error
    
    # 3. Reconstruir state
    state = reconstruct_state_until(job_id, target_step)
    if state is None:
        return None, [], (
            f"Não foi possível reconstruir o estado do job {job_id} "
            f"até o step '{target_step}'. Verifique se o job tem checkpoints salvos "
            f"(Engine v3.10+)."
        )
    
    # 4. Aplicar modificações
    if modifications:
        try:
            state_dict = state.to_dict()
            state_dict = apply_modifications(state_dict, modifications)
            
            # 🔧 FIX 07/Feb/2026: Sincronizar state.text_styles → template_config._text_styles
            # O generate_pngs (e outros steps) lê text_styles de template_config._text_styles,
            # não de state.text_styles. Sem essa sincronização, modificações de cor/fonte/tamanho
            # são aplicadas no state mas nunca chegam ao step que gera os PNGs.
            _sync_text_styles_to_template_config(state_dict, modifications)
            
            state = PipelineState.from_dict(state_dict)
            logger.info(f"✅ [REPLAY] {len(modifications)} modificações aplicadas")
        except (KeyError, ValueError) as e:
            return None, [], f"Erro ao aplicar modificações: {e}"
    
    # 5. Calcular steps a executar
    steps_to_run = get_steps_from(target_step)
    
    return state, steps_to_run, ""
