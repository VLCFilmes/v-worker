"""
Pipeline Steps - Auto-discovery.

Importa todos os módulos de steps para que os decorators @register_step
registrem automaticamente cada step no StepRegistry.
"""

# Fase 0: Setup
from . import s00_load_template

# Fase 1: Pré-processamento
from . import s01_normalize
from . import s01b_apply_retake_cuts  # 🆕 v4.0: corte de retakes intra-arquivo
from . import s02_concat
from . import s05c_concat_plates  # 🆕 v4.1: concatena placas tectônicas após silence_cut
from . import s03_analyze
from . import s04_detect_silence
from . import s05_silence_cut
from . import s06_transcribe
from . import s07_fraseamento
from . import s08_classify

# Fase 2: Renderização
from . import s09_generate_pngs
from . import s10_add_shadows
from . import s11_apply_animations
from . import s12_calculate_positions
from . import s13_generate_backgrounds
from . import s14_motion_graphics
from . import s15_matting
from . import s16_cartelas
from . import s17_subtitle_pipeline
from . import s18_render

# Opcionais / Criativos (plugáveis, ativados pelo LLM Director ou por request)
from . import s_visual_analysis
from . import s_video_clipper  # 🆕 v4.4: B-roll overlay via LLM (async_mode=True)
from . import s_title_generation  # 🆕 v4.6: Título do vídeo via Title Director

# 🆕 Text Video STM (steps exclusivos)
from . import s_format_script       # Parseia tags visuais do roteiro
from . import s_generate_timestamps  # Gera timestamps virtuais para texto puro

# 🆕 Motion Graphics STM (steps exclusivos)
from . import s_generate_visual_layout  # LLM gera HTML/CSS → Playwright → PNGs
