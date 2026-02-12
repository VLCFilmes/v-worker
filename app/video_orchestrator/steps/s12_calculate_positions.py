"""Step 12: Cálculo de posições das legendas (bounding boxes)."""

from ._base import *

@register_step(
    name="calculate_positions",
    description="Calcula posições X/Y e bounding boxes de cada frase",
    category="rendering",
    depends_on=["apply_animations"],
    produces=["positioning_results", "png_results"],
    estimated_duration_s=10,
    cost_category="cpu",
    retryable=True,
    sse_step_name="POSITION",
    tool_schema={
        "description": "Calcula layout de posições das legendas no canvas",
        "input": {},
        "output": {"total_positioned": "int", "status": "str"}
    }
)
def calculate_positions_step(state: PipelineState, params: dict) -> PipelineState:
    if not state.png_results:
        raise ValueError("Sem png_results para calcular posições")
    if not state.template_config:
        raise ValueError("Sem template_config para calcular posições")

    from ..services.positioning_service import PositioningService

    service = PositioningService()
    canvas = {
        'width': state.video_width,
        'height': state.video_height,
    }

    logger.info(f"📍 [POSITIONS] Calculando posições (canvas {canvas['width']}x{canvas['height']})...")

    result = service.calculate_positions(
        png_results=state.png_results,
        template_config=state.template_config,
        canvas=canvas,
    )

    if result.get('status') not in ('success', 'partial'):
        raise Exception(f"Posicionamento falhou: {result.get('error', 'desconhecido')}")

    positioned_sentences = result.get('sentences', [])
    logger.info(f"✅ [POSITIONS] {len(positioned_sentences)} frases posicionadas")

    # Adicionar posições ao png_results para uso por steps subsequentes
    updated_png_results = state.png_results.copy()
    updated_png_results['positioned_sentences'] = positioned_sentences

    return state.with_updates(
        positioning_results=result,
        png_results=updated_png_results,
    )
