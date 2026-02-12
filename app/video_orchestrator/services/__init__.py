"""
🎬 Video Services - Wrappers para V-Services

Estes módulos encapsulam as chamadas HTTP para os microserviços em v-services.

Pipeline de Processamento:
1. Normalização de áudio
2. Concatenação
3. Detecção/Corte de silêncios
4. Transcrição (AssemblyAI → word timestamps)
5. Fraseamento (SpaCy → phrase groups)
6. Classificação de frases (LLM → style_type)
7. Geração de PNGs (V-Services → png_service)
8. Adição de Sombras (V-Services → add_shadow_batch)

Transcrição:
- AssemblyAI (prioritário) - Word-level timestamps nativos
- Whisper (fallback) - Se ASSEMBLY_API_KEY não configurada

Fraseamento:
- SpaCy para análise POS
- Regras configuráveis via template (enhanced-phrase-rules.json)

Classificação:
- LLM (prioritário) - GPT-4o-mini via ai_config
- Heurísticas (fallback) - Regras baseadas em tamanho/pontuação

Geração de PNGs:
- Converte frases classificadas → payload V-Services
- Suporta 3 estilos: default, emphasis, letter_effect
- letter_effect gera PNGs por letra (animação)
"""

from .normalize_service import NormalizeService
from .analyze_service import AnalyzeService
from .concat_service import ConcatService
from .silence_service import SilenceService
from .transcription_service import TranscriptionService
from .assembly_service import AssemblyAIService
from .fraseamento_service import FraseamentoService
from .phrase_classifier_service import PhraseClassifierService
from .png_generator_service import PngGeneratorService
from .shadow_service import ShadowService

__all__ = [
    'NormalizeService',
    'AnalyzeService',
    'ConcatService', 
    'SilenceService',
    'TranscriptionService',
    'AssemblyAIService',
    'FraseamentoService',
    'PhraseClassifierService',
    'PngGeneratorService',
    'ShadowService'
]

