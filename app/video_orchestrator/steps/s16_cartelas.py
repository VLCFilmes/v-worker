"""Step 16: Cartelas (fundos visuais com overlay text).

Nota: A geração de cartelas é feita no s09_generate_pngs (pré-PNG).
Este step é um placeholder para quando cartelas precisarem de
processamento adicional pós-posicionamento (ex: cartelas animadas,
cartelas com motion design).

No fluxo atual, cartelas já são processadas no s09 junto com os PNGs.
Este step existe para compatibilidade com o auto_runner e como ponto
de extensão futuro.
"""

from ._base import *

@register_step(
    name="cartelas",
    description="Processa cartelas visuais pós-posicionamento (extensão futura)",
    category="rendering",
    depends_on=["calculate_positions"],
    produces=["cartela_results"],
    optional=True,
    estimated_duration_s=5,
    cost_category="free",
    retryable=True,
    sse_step_name="CARTELAS",
    await_async=["matting"],  # 🆕 v4.3.0: Espera matting async terminar antes de prosseguir
    tool_schema={
        "description": "Processa cartelas visuais (backgrounds textuais estilizados)",
        "input": {},
        "output": {"total_cartelas": "int", "status": "str"}
    }
)
def cartelas_step(state: PipelineState, params: dict) -> PipelineState:
    # Cartelas já foram geradas no s09_generate_pngs
    # Este step é um ponto de extensão para:
    # - Cartelas animadas (futuro)
    # - Cartelas com motion design
    # - Cartelas que dependem de posicionamento final

    if state.cartela_results:
        logger.info(f"⏭️ [CARTELAS] Já processadas no step generate_pngs "
                     f"(stats: {state.cartela_results.get('stats', {})})")
        return state

    # Verificar se há cartelas nos png_results (geradas pelo s09)
    if state.png_results and state.png_results.get('cartelas'):
        cartelas = state.png_results['cartelas']
        logger.info(f"✅ [CARTELAS] {len(cartelas)} cartelas disponíveis dos PNGs")
        return state.with_updates(
            cartela_results={
                'status': 'success',
                'source': 'generate_pngs',
                'total': len(cartelas),
            }
        )

    # Verificar se template tem cartelas habilitadas
    text_styles = (state.template_config or {}).get('_text_styles') or state.text_styles or {}
    has_cartela = any(
        (style or {}).get('cartela_config', {}).get('enabled', False)
        for style in text_styles.values()
    )

    if not has_cartela:
        logger.info("⏭️ [CARTELAS] Sem cartelas habilitadas no template")
        return state

    # Se chegou aqui, cartelas estão habilitadas mas não foram geradas
    logger.warning("⚠️ [CARTELAS] Cartelas habilitadas mas não encontradas nos PNGs")
    return state
