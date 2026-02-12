"""
🏷️ Title Director Service — Geração inteligente de títulos para vídeo.

Usa GPT-4o-mini para criar títulos curtos e impactantes (estilo redes sociais)
baseado na análise de transcrição + inputs do usuário no chat.

Input:
  - transcript_analysis_result (narrativa, temas, sound bites)
  - user_input (instruções do chat, se houver)
  - storytelling_mode, format_detected, language

Output:
  - title (line_1, line_2, highlight_words)
  - alternatives (2 opções extras)
  - style_suggestion (position, timing, animation)

Custo estimado: ~$0.001-0.003 (texto puro, muito barato)

Autor: Vinicius + Claude
Data: 11/Fev/2026
"""

import os
import json
import time
import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
TITLE_LLM_MODEL = os.getenv('TITLE_LLM_MODEL', 'gpt-4o-mini')
TITLE_LLM_MAX_TOKENS = int(os.getenv('TITLE_LLM_MAX_TOKENS', '1024'))


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

TITLE_SYSTEM_PROMPT = """You are a Title Director for a video production platform.
You create short, impactful titles for social media videos (Instagram Reels, TikTok, YouTube Shorts).

Your job:
1. ANALYZE the content: understand what the video is about from the transcript analysis
2. CREATE a title: 1-2 short phrases that capture attention
3. SUGGEST highlight words: which words should be visually emphasized (rendered bigger, bolder, or in a different color)
4. PROVIDE 2 alternatives: other title options for the user to choose from

TITLE RULES:
- Line 1: The HOOK — grab attention in 2-5 words. Use power words.
- Line 2 (optional): The COMPLEMENT — complete the thought or add curiosity. Can be empty string if line 1 is self-sufficient.
- Total: Maximum 10-12 words across both lines.
- At least 1 word should be a HIGHLIGHT (the most impactful word — will be rendered in uppercase, bold, or different color).
- Match the tone of the content (professional, casual, energetic, emotional).
- The title should make people STOP scrolling and watch.

HIGHLIGHT WORD GUIDELINES:
- Pick 1-2 words that carry the most emotional/semantic weight.
- Common patterns: VERBS of action, NUMBERS, SUPERLATIVES, NEGATIVES, KEY NOUNS.
- Examples: "NINGUÉM", "3 TRUQUES", "VALE", "MUDOU", "SEGREDO", "MELHOR".
- The highlight words MUST appear exactly as written in line_1 or line_2.

POSITION AND TIMING:
- position: ALWAYS "center_top" by default. This places the title centered horizontally and at the top of the video with a safe margin (8% from top edge — avoids Instagram/TikTok status bars).
  * "center_top": top of video, safe area margin. USE THIS FOR ALL talking_head videos (person is visible below).
  * "center": vertically centered. Only for pure text/narration videos (no person on screen).
  * "bottom_third": bottom area. Rarely used for titles.
- timing_start_ms: 0 (appear immediately when video starts)
- timing_end_ms: 3000-5000 (visible for 3-5 seconds, then fades out)
- animation: "fade_in_up" is the safe default, "scale_in" for more energy

PNG STYLE (png_style):
The title is rendered as a PNG image using the same system as video subtitles.
You MUST suggest a visual style. This is the exact format our PNG generator accepts:

- fontFamily: Pango font string. Available fonts: "Poppins:style=Black", "Poppins:style=Bold", "Poppins:style=ExtraBold", "Poppins:style=SemiBold", "Poppins:style=Medium", "Poppins:style=Regular", "Poppins:style=Light". IMPORTANT: Only Poppins family is currently installed. ALWAYS use Poppins variants.
- size_line1: Font size for the main line as "X%" of video height. Titles are BIG: use "4.5%" to "6%". Default "5%".
- size_line2: Font size for the secondary line. Smaller: "3%" to "4%". Default "3.5%".
- uppercase: true (recommended for titles) or false
- padding_x: Horizontal padding in pixels. 20-40 is normal. Default 30.
- padding_y: Vertical padding in pixels. 10-20 is normal. Default 15.
- text_style: Color config for the title text:
  * Solid color: {{"render_type": "solid", "solid_color_rgb": "R,G,B"}} (e.g. "255,255,255" = white)
  * Gradient: {{"render_type": "gradient", "gradient_start_color_rgb": "R,G,B", "gradient_end_color_rgb": "R,G,B", "gradient_text_direction": "vertical_text"}}
- text_border_config: Border/stroke around text (makes it readable on any background):
  * line_join: "round" (smooth corners — always use "round")
  * border_1_inner: {{"enabled": true, "thickness_value": 5-12, "thickness_unit": "percent_font", "color_rgb": "0,0,0", "blur_radius": 0}}
  * border_2_spacing (optional outer glow): {{"enabled": true, "thickness_value": 15-25, "thickness_unit": "percent_font", "color_rgb": "0,0,0", "blur_radius": 3}}
- highlight_text_style (optional): Different color for highlight words. Same format as text_style.
  * Example: {{"render_type": "solid", "solid_color_rgb": "255,230,0"}} (yellow highlight)
- highlight_border_config (optional): Different border for highlight words. Same format as text_border_config.

DEFAULT STYLE (use this unless the content suggests otherwise):
- Black text with thick white border → readable on any background, professional.
- fontFamily="Poppins:style=Black", text black (0,0,0), border white (255,255,255) thickness 30, with phrase background.
- This is the safe default. Only change if you have a strong reason.

Common title styles:
1. DEFAULT (black + white border): fontFamily="Poppins:style=Black", black text, thick white border (30). Clean, professional. USE THIS BY DEFAULT.
2. INVERTED (white + black border): fontFamily="Poppins:style=Black", white text, thick black border (30). For dark videos.
3. GRADIENT YELLOW: fontFamily="Poppins:style=Bold", yellow-to-white gradient, thick black border. Eye-catching.
4. NEON POP: fontFamily="Poppins:style=ExtraBold", solid bright color, thick border with blur. TikTok vibe.
5. ELEGANT: fontFamily="Poppins:style=SemiBold", white text, subtle border. Professional.

NOTE: In the future, there will be a template bank where you can search for visual styles.
For now, use the default style unless the user explicitly requests a different look.

LANGUAGE: Generate titles in {language}. Keep JSON keys in English.

Output STRICT JSON with this exact structure (no extra text, no markdown):
{{
  "title": {{
    "line_1": "First line (the hook)",
    "line_2": "Second line (complement, or empty string)",
    "highlight_words": ["WORD1"],
    "full_text": "First line Second line"
  }},
  "alternatives": [
    {{
      "line_1": "Alternative 1 hook",
      "line_2": "Alternative 1 complement",
      "highlight_words": ["WORD"]
    }},
    {{
      "line_1": "Alternative 2 hook",
      "line_2": "Alternative 2 complement",
      "highlight_words": ["WORD"]
    }}
  ],
  "style_suggestion": {{
    "position": "center_top",
    "timing_start_ms": 0,
    "timing_end_ms": 4000,
    "animation": "fade_in_up",
    "emphasis_style": "uppercase_bold",
    "png_style": {{
      "fontFamily": "Poppins:style=Black",
      "size_line1": "5%",
      "size_line2": "3.5%",
      "uppercase": true,
      "padding_x": 30,
      "padding_y": 15,
      "text_style": {{
        "render_type": "solid",
        "solid_color_rgb": "0,0,0"
      }},
      "text_border_config": {{
        "line_join": "round",
        "border_1_inner": {{
          "enabled": true,
          "thickness_value": 30,
          "thickness_unit": "percent_font",
          "color_rgb": "255,255,255",
          "blur_radius": 0
        }}
      }},
      "has_phrase_background": true
    }}
  }},
  "rationale": "Brief explanation of why this title works"
}}"""


TITLE_USER_PROMPT_TEMPLATE = """Generate a title for this video.

Mode: {storytelling_mode}
Format: {format_detected}
Language: {language}

== TRANSCRIPT ANALYSIS ==
Main theme: {main_theme}
Themes: {themes}
Summary: {summary}

{sound_bites_section}

{user_input_section}

Create a compelling, scroll-stopping title. If the user provided specific instructions, follow them closely. If not, create the best title based on the transcript analysis.

Respond ONLY with valid JSON (no markdown, no extra text)."""


# ═══════════════════════════════════════════════════════════════
# SERVICE
# ═══════════════════════════════════════════════════════════════

class TitleDirectorService:
    """Serviço de geração de títulos via LLM."""

    def __init__(self, api_key: str = None, model: str = None):
        self.api_key = api_key or OPENAI_API_KEY
        self.model = model or TITLE_LLM_MODEL
        self.max_tokens = TITLE_LLM_MAX_TOKENS

    def generate(
        self,
        transcript_analysis: dict,
        storytelling_mode: str = 'talking_head',
        format_detected: str = 'unknown',
        language: str = 'Portuguese (pt-BR)',
        user_input: Optional[str] = None,
    ) -> dict:
        """
        Gera título para o vídeo baseado na análise de transcrição.

        Args:
            transcript_analysis: Resultado do Transcript Director
            storytelling_mode: Modo do vídeo (talking_head, narration, etc.)
            format_detected: Formato detectado pelo Triage
            language: Idioma para o título
            user_input: Instruções do usuário sobre o título (opcional)

        Returns:
            Dict com title, alternatives, style_suggestion, status, etc.
        """
        t0 = time.time()

        try:
            # ─── Montar contexto ──────────────────────
            narrative = transcript_analysis.get('narrative_analysis', {})
            main_theme = narrative.get('main_theme', 'Não identificado')

            themes = narrative.get('themes', [])
            themes_str = ', '.join(
                t.get('theme', '') for t in themes if t.get('theme')
            ) if themes else 'Não identificados'

            summary = transcript_analysis.get('summary', '')

            # Sound bites (top 3)
            sound_bites = transcript_analysis.get('sound_bites', [])
            if sound_bites:
                sb_lines = []
                for sb in sound_bites[:3]:
                    text = sb.get('text', '')[:100]
                    strength = sb.get('strength', 'medium')
                    sb_lines.append(f"- [{strength}] \"{text}\"")
                sound_bites_section = "== SOUND BITES (strongest phrases) ==\n" + '\n'.join(sb_lines)
            else:
                sound_bites_section = ""

            # User input
            if user_input and user_input.strip():
                user_input_section = (
                    f"== USER INSTRUCTIONS ==\n"
                    f"The user said: \"{user_input.strip()}\"\n"
                    f"Follow the user's instructions for the title."
                )
            else:
                user_input_section = (
                    "== USER INSTRUCTIONS ==\n"
                    "No specific instructions from user. Generate best title from transcript analysis."
                )

            # ─── Montar prompts ───────────────────────
            system_prompt = TITLE_SYSTEM_PROMPT.format(language=language)
            user_prompt = TITLE_USER_PROMPT_TEMPLATE.format(
                storytelling_mode=storytelling_mode,
                format_detected=format_detected,
                language=language,
                main_theme=main_theme,
                themes=themes_str,
                summary=summary[:500] if summary else 'Não disponível',
                sound_bites_section=sound_bites_section,
                user_input_section=user_input_section,
            )

            # ─── Chamar LLM ──────────────────────────
            llm_t0 = time.time()
            response = self._call_openai(system_prompt, user_prompt)
            llm_time_ms = int((time.time() - llm_t0) * 1000)

            if not response:
                return {
                    'status': 'error',
                    'error': 'LLM retornou resposta vazia',
                }

            # ─── Parse JSON ──────────────────────────
            result = self._parse_response(response)

            if not result:
                return {
                    'status': 'error',
                    'error': 'Não foi possível parsear resposta da LLM',
                    'raw_response': response[:500],
                }

            # ─── Enriquecer resultado ─────────────────
            result['status'] = 'success'
            result['model'] = self.model
            result['llm_time_ms'] = llm_time_ms
            result['processing_time_ms'] = int((time.time() - t0) * 1000)
            result['tokens_in'] = response.get('_tokens_in', 0) if isinstance(response, dict) else 0
            result['tokens_out'] = response.get('_tokens_out', 0) if isinstance(response, dict) else 0
            result['source'] = 'combined' if user_input else 'transcript_analysis'
            result['user_input_used'] = bool(user_input)

            # Garantir full_text
            title = result.get('title', {})
            if title and not title.get('full_text'):
                line_1 = title.get('line_1', '')
                line_2 = title.get('line_2', '')
                title['full_text'] = f"{line_1} {line_2}".strip()

            # Garantir language
            if title:
                title['language'] = language

            elapsed = time.time() - t0
            logger.info(
                f"🏷️ [TITLE-DIRECTOR] Título gerado em {elapsed:.1f}s | "
                f"model={self.model} | "
                f"llm={llm_time_ms}ms | "
                f"line_1=\"{title.get('line_1', '')[:50]}\" | "
                f"source={'combined' if user_input else 'transcript'}"
            )

            return result

        except Exception as e:
            elapsed = time.time() - t0
            logger.error(
                f"❌ [TITLE-DIRECTOR] Erro após {elapsed:.1f}s: {e}",
                exc_info=True,
            )
            return {
                'status': 'error',
                'error': str(e),
                'processing_time_ms': int((time.time() - t0) * 1000),
            }

    def _call_openai(self, system_prompt: str, user_prompt: str) -> dict:
        """Chama OpenAI API e retorna resposta parseada."""
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY não configurada")

        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
        }

        payload = {
            'model': self.model,
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_prompt},
            ],
            'max_tokens': self.max_tokens,
            'temperature': 0.7,  # Um pouco criativo para títulos
            'response_format': {'type': 'json_object'},
        }

        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers=headers,
            json=payload,
            timeout=30,
        )

        if response.status_code != 200:
            logger.error(
                f"❌ [TITLE-DIRECTOR] OpenAI API error: "
                f"{response.status_code} — {response.text[:200]}"
            )
            raise Exception(f"OpenAI API error: {response.status_code}")

        data = response.json()
        choice = data.get('choices', [{}])[0]
        content = choice.get('message', {}).get('content', '')

        # Extrair token counts
        usage = data.get('usage', {})
        tokens_in = usage.get('prompt_tokens', 0)
        tokens_out = usage.get('completion_tokens', 0)

        logger.info(
            f"🏷️ [TITLE-DIRECTOR] LLM response: "
            f"tokens={tokens_in}→{tokens_out} | "
            f"content_len={len(content)}"
        )

        return {
            'content': content,
            '_tokens_in': tokens_in,
            '_tokens_out': tokens_out,
        }

    def _parse_response(self, response: dict) -> Optional[dict]:
        """Parseia resposta da LLM em JSON estruturado."""
        content = response.get('content', '') if isinstance(response, dict) else str(response)

        if not content:
            return None

        try:
            # Limpar possível markdown
            text = content.strip()
            if text.startswith('```'):
                lines = text.split('\n')
                # Remove primeira e última linha (```json e ```)
                lines = [l for l in lines if not l.strip().startswith('```')]
                text = '\n'.join(lines)

            result = json.loads(text)

            # Preservar token counts
            if isinstance(response, dict):
                result['tokens_in'] = response.get('_tokens_in', 0)
                result['tokens_out'] = response.get('_tokens_out', 0)

            # Validar estrutura mínima
            if 'title' not in result:
                logger.warning(
                    f"⚠️ [TITLE-DIRECTOR] Resposta sem campo 'title': "
                    f"{text[:200]}"
                )
                return None

            title = result['title']
            if not title.get('line_1'):
                logger.warning(
                    f"⚠️ [TITLE-DIRECTOR] Título sem line_1"
                )
                return None

            return result

        except json.JSONDecodeError as e:
            logger.error(
                f"❌ [TITLE-DIRECTOR] JSON parse error: {e} | "
                f"content: {content[:200]}"
            )
            return None


# ═══════════════════════════════════════════════════════════════
# SINGLETON
# ═══════════════════════════════════════════════════════════════

_service_instance = None


def get_title_director_service() -> TitleDirectorService:
    """Retorna instância singleton do TitleDirectorService."""
    global _service_instance
    if _service_instance is None:
        _service_instance = TitleDirectorService()
    return _service_instance
