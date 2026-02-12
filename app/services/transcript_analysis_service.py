"""
🎙️ Transcript Analysis Service — Análise profunda de transcrições.

Recebe transcrições completas dos assets pipeline_ready (talking_head,
audio_narration) e usa GPT-4o-mini para:
  - Análise narrativa (temas, arco, estrutura)
  - Seleção de sound bites (melhores frases)
  - Refinamento de retakes (comparação de qualidade)
  - Verificação de ordem narrativa
  - Segmentação (trechos úteis vs pausas/hesitações)

Custo estimado: ~$0.005-0.02 (texto puro, sem imagens)
Mais barato que Vision Director por não usar frames.

Autor: Vinicius + Claude
Data: 09/Fev/2026
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
TRANSCRIPT_LLM_MODEL = os.getenv('TRANSCRIPT_LLM_MODEL', 'gpt-4o-mini')
# 🔧 09/Fev/2026: Reduzido de 8192 — prompt agora é mais simples
# (word timestamps não são mais enviados à LLM)
TRANSCRIPT_LLM_MAX_TOKENS = int(os.getenv('TRANSCRIPT_LLM_MAX_TOKENS', '4096'))


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

TRANSCRIPT_SYSTEM_PROMPT = """You are a Transcript Director for a video production platform.
You receive COMPLETE transcriptions from talking-head videos and audio narrations.
These assets have already been classified by the Asset Triage Director.

You have EXACTLY THREE jobs:

## JOB 1: RETAKE CURATION (between files)
Compare DIFFERENT assets that contain REPEATED/SIMILAR content (retakes of the same scene).
- If 2 assets say essentially the same thing → keep the BEST one, mark the other for removal.
- If 5 assets say the same thing → keep the BEST one, mark the other 4 for removal.
- Judge quality by: fluency, energy, clarity, completeness, fewer hesitations.
- ONLY mark assets as retakes if they are truly REPEATED content (same topic, same speaker, same intent).

## JOB 2: INTRA-FILE REPETITION DETECTION (within same file)
Within a SINGLE asset, the speaker may have made a mistake and restarted a phrase WITHOUT
stopping the recording. This creates repeated phrases INSIDE the same file.
Example: "Hello everyone, today I... uh, let me start over. Hello everyone, today I want to show you..."

Your job is ONLY to DETECT which phrases are repeated and which occurrence to keep.
You do NOT need to provide timestamps — a separate system handles that precisely.

For each repeated phrase detected:
- Provide the EXACT repeated text (copy from the transcription)
- Say which occurrence to KEEP (1 = first, 2 = second, etc.)
- Say which occurrence to REMOVE
- Explain WHY the kept version is better

## JOB 3: ORDER SUGGESTION
The uploads may have arrived out of order. Analyze the narrative content of each asset
and suggest the BEST logical sequence for storytelling.

## ABSOLUTE RULES — DO NOT VIOLATE:
- NEVER remove UNIQUE content. Only remove REPEATED/DUPLICATE segments.
- NEVER create, rewrite, or summarize the script. You are NOT an editor or scriptwriter.
- For JOB 1 (between files): each asset is either KEPT entirely or REMOVED entirely.
- For JOB 2 (within file): only identify the REPEATED phrase and which occurrence to remove.
- If a phrase is unique (appears only once in the file), it MUST NOT be flagged.
- When in doubt, DON'T flag it. Better to include a mistake than lose unique content.

LANGUAGE: Write ALL descriptive text fields in {response_language}.
Keep JSON keys in English; only VALUES should be in {response_language}.
Always respond in valid JSON format."""


TRANSCRIPT_USER_PROMPT_TEMPLATE = """Analyze these {num_assets} transcriptions from a video project.
Project format detected by triage: {format_detected}

{assets_description}

{retakes_section}

Your THREE tasks:
1. RETAKES BETWEEN FILES: If any assets contain repeated/duplicate content, pick the best take.
2. REPEATED PHRASES WITHIN FILES: For each asset, look for phrases that appear MORE THAN ONCE
   in the SAME file (speaker made a mistake and repeated). Just IDENTIFY the repeated phrase
   and which occurrence is better. Do NOT provide timestamps — another system handles that.
3. ORDER: Suggest the best narrative sequence for these assets.

REMEMBER: NEVER remove unique content. ONLY flag phrases that truly appear more than once.
Write all descriptive fields in {response_language}.

Respond with this exact JSON structure:
{{
  "retakes_refined": [
    {{
      "group_asset_ids": ["id1", "id2"],
      "keep_asset_id": "id2",
      "remove_asset_ids": ["id1"],
      "reason": "Why this take is better (fluency, energy, clarity)",
      "quality_comparison": {{
        "id1": {{"fluency": 0.7, "hesitations": 3, "energy": "medium", "clarity": "good"}},
        "id2": {{"fluency": 0.9, "hesitations": 0, "energy": "high", "clarity": "excellent"}}
      }}
    }}
  ],

  "intra_retakes": [
    {{
      "asset_id": "id of the asset that has repeated phrases inside it",
      "detections": [
        {{
          "repeated_text": "The EXACT phrase that appears more than once (copy from transcription)",
          "keep_occurrence": 1,
          "remove_occurrence": 2,
          "reason": "First occurrence is more fluent / Second is a false restart"
        }}
      ]
    }}
  ],

  "order_analysis": {{
    "current_order": ["id1", "id2", "id3"],
    "suggested_order": ["id1", "id3", "id2"],
    "reorder_needed": false,
    "confidence": 0.8,
    "reason": "Explanation of why this order makes more narrative sense"
  }},

  "per_asset_analysis": [
    {{
      "asset_id": "id",
      "speaker_name": "Detected speaker name or Speaker 1",
      "duration_ms": 45000,
      "word_count": 320,
      "topics": ["topic1", "topic2"],
      "key_phrases": ["most notable phrase 1", "phrase 2"],
      "is_retake_duplicate": false,
      "keep": true,
      "has_intra_retakes": false
    }}
  ],

  "sound_bites": [
    {{
      "asset_id": "id",
      "text": "The exact impactful phrase from transcription",
      "strength": "high | medium",
      "category": "emotional | factual | humorous | insight"
    }}
  ],

  "summary": "1-2 sentence summary of findings"
}}

IMPORTANT for intra_retakes:
- Only include assets that actually have phrases appearing MORE THAN ONCE.
- The "repeated_text" MUST be the EXACT phrase from the transcription (copy it verbatim).
- keep_occurrence = which occurrence number to keep (1 = first, 2 = second).
- remove_occurrence = which occurrence number to remove.
- Do NOT provide timestamps. Do NOT provide segments. Just the phrase and the decision."""


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _format_transcript_description(asset: dict, index: int, total: int) -> str:
    """Format a single asset's transcription for the LLM prompt."""
    lines = [f"[Asset {index + 1} of {total}: {asset.get('filename', 'unknown')}]"]
    lines.append(f"- Asset ID: {asset['id']}")
    lines.append(f"- Classification: {asset.get('classification', 'unknown')}")

    duration_ms = asset.get('duration_ms', 0)
    if duration_ms:
        lines.append(f"- Duration: {duration_ms}ms ({duration_ms / 1000:.1f}s)")

    lines.append(f"- Upload order: {index + 1} of {total}")

    transcription = asset.get('transcription_text', '')
    if transcription:
        lines.append(f'- Full Transcription ({len(transcription)} chars):')
        lines.append(f'"""\n{transcription}\n"""')
    else:
        lines.append("- Transcription: NOT AVAILABLE")

    # 🔧 09/Fev/2026: Word timestamps NÃO são mais enviados à LLM.
    # A LLM só faz decisões semânticas (qual frase repetiu).
    # O IntraRetakeResolver usa os word timestamps do AssemblyAI
    # de forma DETERMINÍSTICA para calcular segmentos precisos.
    words = asset.get('words', [])
    if words:
        lines.append(f"- Word count: {len(words)} words")
    else:
        lines.append("- Word count: NOT AVAILABLE")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# SERVICE
# ═══════════════════════════════════════════════════════════════

class TranscriptAnalysisService:
    """
    Analisa transcrições em profundidade usando GPT-4o-mini.

    Fluxo:
        1. Recebe transcrições completas dos assets pipeline_ready
        2. Formata prompt com todas as transcrições
        3. LLM analisa narrativa, sound bites, retakes, ordem
        4. Retorna análise estruturada
    """

    def __init__(self):
        self.api_key = OPENAI_API_KEY
        self.model = TRANSCRIPT_LLM_MODEL
        self.max_tokens = TRANSCRIPT_LLM_MAX_TOKENS
        logger.info(f"🎙️ TranscriptAnalysisService init | model={self.model}")

    def analyze(
        self,
        assets: List[Dict],
        retakes: List[Dict] = None,
        format_detected: str = "unknown",
        response_language: str = "Portuguese (pt-BR)",
    ) -> Dict:
        """
        Analisa transcrições de todos os assets pipeline_ready.

        Args:
            assets: Lista de assets com transcrição completa
            retakes: Retakes detectados pelo Triage (para refinamento)
            format_detected: Formato detectado pelo Triage
            response_language: Idioma para campos descritivos

        Returns:
            Resultado da análise com narrativa, sound bites, ordem, etc.
        """
        t0 = time.time()

        if not assets:
            return {"status": "error", "error": "No assets to analyze"}

        self._response_language = response_language
        logger.info(
            f"🎙️ [TRANSCRIPT] Analisando {len(assets)} transcrições... "
            f"(lang={response_language})"
        )

        # Chamar LLM
        llm_result = self._call_llm_analysis(
            assets, retakes or [], format_detected
        )

        total_ms = int((time.time() - t0) * 1000)

        if llm_result.get("status") != "success":
            return {
                "status": "error",
                "error": f"LLM: {llm_result.get('error', 'unknown')}",
                "processing_time_ms": total_ms,
            }

        parsed = llm_result.get("parsed", {})

        result = {
            "status": "success",
            "processing_time_ms": total_ms,
            "llm_time_ms": llm_result.get("time_ms", 0),
            "model": self.model,
            "tokens_in": llm_result.get("tokens_in", 0),
            "tokens_out": llm_result.get("tokens_out", 0),
            "total_assets_analyzed": len(assets),
            "response_language": response_language,
            # Dados da análise (LLM)
            "narrative_analysis": parsed.get("narrative_analysis", {}),
            "sound_bites": parsed.get("sound_bites", []),
            "retakes_refined": parsed.get("retakes_refined", []),
            "intra_retakes": parsed.get("intra_retakes", []),
            "order_analysis": parsed.get("order_analysis", {}),
            "per_asset_analysis": parsed.get("per_asset_analysis", []),
            "summary": parsed.get("summary", ""),
        }

        logger.info(
            f"✅ [TRANSCRIPT] {total_ms}ms | "
            f"llm={llm_result.get('time_ms', 0)}ms | "
            f"tokens={result['tokens_in']}→{result['tokens_out']} | "
            f"sound_bites={len(result['sound_bites'])} | "
            f"assets={len(assets)}"
        )

        return result

    def _call_llm_analysis(
        self,
        assets: List[Dict],
        retakes: List[Dict],
        format_detected: str,
    ) -> Dict:
        """Chama GPT-4o-mini com transcrições completas."""
        if not self.api_key:
            return {"status": "error", "error": "OPENAI_API_KEY não configurada"}

        t0 = time.time()

        try:
            # Montar descrições de cada asset
            descriptions = []
            for i, asset in enumerate(assets):
                desc = _format_transcript_description(asset, i, len(assets))
                descriptions.append(desc)

            all_descriptions = "\n\n".join(descriptions)

            # Seção de retakes (se houver)
            retakes_section = ""
            if retakes:
                retake_lines = ["RETAKE PAIRS detected by Triage (refine these):"]
                for r in retakes:
                    retake_lines.append(
                        f"  - Original: {r.get('original_asset_id', '?')} | "
                        f"Retake: {r.get('retake_asset_id', '?')} | "
                        f"Similarity: {r.get('similarity', 0):.0%} | "
                        f"Triage recommendation: {r.get('recommendation', '?')}"
                    )
                retakes_section = "\n".join(retake_lines)
            else:
                retakes_section = "No retakes detected by Triage."

            response_lang = self._response_language

            system_prompt = TRANSCRIPT_SYSTEM_PROMPT.format(
                response_language=response_lang,
            )

            user_prompt = TRANSCRIPT_USER_PROMPT_TEMPLATE.format(
                num_assets=len(assets),
                format_detected=format_detected,
                assets_description=all_descriptions,
                retakes_section=retakes_section,
                response_language=response_lang,
            )

            # Chamar OpenAI API (texto puro, sem imagens)
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "max_tokens": self.max_tokens,
                    "temperature": 0.3,
                },
                timeout=90,
            )

            time_ms = int((time.time() - t0) * 1000)

            if response.status_code != 200:
                logger.error(
                    f"❌ [TRANSCRIPT-LLM] HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )
                return {
                    "status": "error",
                    "error": f"HTTP {response.status_code}",
                    "time_ms": time_ms,
                }

            result = response.json()
            choice = result.get("choices", [{}])[0]
            raw_text = choice.get("message", {}).get("content", "")
            usage = result.get("usage", {})

            tokens_in = usage.get("prompt_tokens", 0)
            tokens_out = usage.get("completion_tokens", 0)

            parsed = self._parse_json(raw_text)

            logger.info(
                f"✅ [TRANSCRIPT-LLM] {self.model} em {time_ms}ms | "
                f"tokens={tokens_in}→{tokens_out}"
            )

            return {
                "status": "success",
                "parsed": parsed,
                "raw_text": raw_text,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "time_ms": time_ms,
            }

        except requests.exceptions.Timeout:
            return {
                "status": "error",
                "error": "LLM timeout (90s)",
                "time_ms": int((time.time() - t0) * 1000),
            }
        except Exception as e:
            logger.error(f"❌ [TRANSCRIPT-LLM] Erro: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "time_ms": int((time.time() - t0) * 1000),
            }

    def _parse_json(self, raw_text: str) -> dict:
        """Parse JSON da resposta da LLM."""
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            pass

        try:
            if "```json" in raw_text:
                json_str = raw_text.split("```json")[1].split("```")[0].strip()
                return json.loads(json_str)
            elif "```" in raw_text:
                json_str = raw_text.split("```")[1].split("```")[0].strip()
                return json.loads(json_str)
        except (json.JSONDecodeError, IndexError):
            pass

        logger.warning(
            f"⚠️ [TRANSCRIPT-LLM] Falha parse JSON: {raw_text[:200]}..."
        )
        return {"raw_text": raw_text}


# Singleton
_service_instance = None


def get_transcript_analysis_service() -> TranscriptAnalysisService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = TranscriptAnalysisService()
    return _service_instance
