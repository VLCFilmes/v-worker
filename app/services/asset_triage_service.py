"""
📋 Asset Triage Service — Classificação e organização de uploads.

Recebe os assets de um projeto, extrai 1 frame por vídeo (ffmpeg),
coleta transcrições existentes, e usa GPT-4o-mini para classificar
cada asset (talking_head, b_roll, audio_narration, etc.),
detectar retakes e verificar ordem.

Custo estimado: ~$0.009 para 10 vídeos (~R$0.05)
Uma única chamada LLM com todos os assets = eficiente.

Autor: Claude + Vinicius
Data: 08/Fev/2026
"""

import os
import json
import time
import base64
import logging
import tempfile
import subprocess
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
TRIAGE_LLM_MODEL = os.getenv('TRIAGE_LLM_MODEL', 'gpt-4o-mini')
TRIAGE_LLM_MAX_TOKENS = int(os.getenv('TRIAGE_LLM_MAX_TOKENS', '4096'))

# Idioma padrão para respostas de IA (será configurável por projeto futuramente)
DEFAULT_RESPONSE_LANGUAGE = os.getenv('AI_RESPONSE_LANGUAGE', 'Portuguese (pt-BR)')


def get_project_locale(project_id: Optional[str] = None) -> str:
    """
    Retorna o idioma de resposta para um projeto.

    Futuramente: lê project_config.locale do banco.
    Hoje: retorna o default (Portuguese) ou o valor do env AI_RESPONSE_LANGUAGE.

    Quando suportar inglês, o frontend enviará o locale
    e será salvo em project_config.locale.
    """
    if project_id:
        try:
            from app.db import get_db_connection
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT project_config->>'locale' FROM projects WHERE project_id = %s",
                (project_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if row and row[0]:
                locale_map = {
                    'pt-BR': 'Portuguese (pt-BR)',
                    'pt': 'Portuguese (pt-BR)',
                    'en': 'English',
                    'en-US': 'English (en-US)',
                }
                return locale_map.get(row[0], row[0])
        except Exception:
            pass

    return DEFAULT_RESPONSE_LANGUAGE


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

TRIAGE_SYSTEM_PROMPT = """You are an Asset Triage Director for a video production platform.
You receive a list of uploaded media assets, each with:
- A thumbnail frame (if video)
- Transcription text (if available)
- Metadata (duration, filename, upload order)

Your job:
1. CLASSIFY each asset into one of: talking_head, b_roll, audio_narration, screen_capture, image_static, music_only, briefing_prompt
2. DETECT retakes: if two assets have very similar transcription text (70%+ overlap), flag as retake
3. CHECK order: are assets in logical narrative sequence?

NOTE: Do NOT include routing recommendations. Routing is handled by deterministic business logic
based on your classifications. Focus only on accurate classification, retake detection, and order.

Classification rules:
- talking_head = person clearly visible AND speaking (transcription has substantial speech)
- b_roll = no meaningful speech OR scenic/product footage with music/silence
- audio_narration = substantial speech in transcription BUT frame shows no person or irrelevant visual
- screen_capture = software interface / computer screen visible in frame
- image_static = still image, not a video (photo, graphic)
- music_only = audio file with no speech, only music/sound effects
- briefing_prompt = file that looks like a REFERENCE or INSTRUCTION, not actual video content.
  Examples: screenshot of a design/mockup, photo of a whiteboard, audio voice note with instructions,
  image with text/annotations, document photo, anything that seems "out of context" compared to the
  other production assets. These are materials the user uploaded to explain what they want, not to
  include in the final video.

IMPORTANT: briefing_prompt detection is critical. If an upload looks clearly different from the
production assets (e.g., a screenshot among talking-head videos, a short audio note among long
recordings), flag it. Better to flag and let the user correct than to process a reference image
as a b-roll.

Retake detection: compare transcription texts across assets. If two share 70%+ of words/phrases, flag as retake.

Order check: use narrative cues ("first", "next", "finally"), filename patterns (cena_01, cena_02), and semantic flow.

LANGUAGE: Write ALL descriptive text fields ("evidence", "notes", "reason", "summary") in {response_language}.
Always respond in valid JSON format. Be precise and concise."""


TRIAGE_USER_PROMPT_TEMPLATE = """Analyze these {num_assets} assets from a video project:

{assets_description}

Classify each asset, detect any retakes, and check the upload order.

IMPORTANT: Write "evidence", "notes", "reason", and "summary" fields in {response_language}.
Do NOT include routing — routing is handled separately by deterministic logic.

Respond with this exact JSON structure:
{{
  "assets": [
    {{
      "asset_id": "the asset id",
      "filename": "original filename",
      "classification": "talking_head | b_roll | audio_narration | screen_capture | image_static | music_only | briefing_prompt",
      "confidence": 0.95,
      "evidence": "Brief explanation of why this classification (in {response_language})",
      "speaker_visible": true,
      "is_production_asset": true,
      "notes": "Any additional observations (in {response_language})"
    }}
  ],
  "retakes": [
    {{
      "original_asset_id": "id1",
      "retake_asset_id": "id2",
      "similarity": 0.87,
      "overlapping_text": "The shared text segment",
      "recommendation": "use_latest | use_first | review_both",
      "reason": "Brief explanation (in {response_language})"
    }}
  ],
  "order": {{
    "current": ["asset_id_1", "asset_id_2"],
    "suggested": ["asset_id_1", "asset_id_2"],
    "reorder_needed": false,
    "confidence": 0.8,
    "reason": "Explanation of order analysis (in {response_language})"
  }},
  "format_detected": "talking_head_solo | multi_interview | narration_broll | humor_dialogue | tutorial | mixed | unknown",
  "summary": "1-2 sentence summary of what was found (in {response_language})"
}}"""


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _format_asset_description(asset: dict, index: int, total: int) -> str:
    """Format a single asset for the LLM prompt."""
    lines = [f"[Asset {index + 1} of {total}: {asset.get('filename', 'unknown')}]"]
    lines.append(f"- Asset ID: {asset['id']}")
    lines.append(f"- Type: {asset.get('asset_type', 'unknown')}")

    duration = asset.get('duration_ms')
    if duration:
        lines.append(f"- Duration: {duration}ms ({duration / 1000:.1f}s)")

    lines.append(f"- Upload order: {index + 1} of {total}")

    transcription = asset.get('transcription_text', '')
    if transcription:
        preview = transcription[:300]
        if len(transcription) > 300:
            preview += "..."
        lines.append(f'- Transcription: "{preview}"')
    else:
        lines.append("- Transcription: NO TRANSCRIPTION AVAILABLE")

    if asset.get('has_frame'):
        lines.append("- Frame: [image attached below]")
    else:
        lines.append("- Frame: NOT AVAILABLE")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# SERVICE
# ═══════════════════════════════════════════════════════════════

class AssetTriageService:
    """
    Classifica e organiza assets usando frame extraction + GPT-4o-mini.

    Fluxo:
        1. Extrai 1 thumbnail por vídeo (ffmpeg direto da URL)
        2. Coleta transcrições existentes
        3. Envia tudo numa única chamada GPT-4o-mini
        4. Retorna classificação, retakes, ordem, roteamento
    """

    def __init__(self):
        self.api_key = OPENAI_API_KEY
        self.model = TRIAGE_LLM_MODEL
        self.max_tokens = TRIAGE_LLM_MAX_TOKENS
        logger.info(f"📋 AssetTriageService init | model={self.model}")

    def analyze(
        self,
        assets: List[Dict],
        video_urls: Dict[str, str],
        project_id: Optional[str] = None,
    ) -> Dict:
        """
        Analisa e classifica todos os assets de um projeto.

        Args:
            assets: Lista de assets do projeto (de project_assets)
            video_urls: Mapa {asset_id: signed_url} para download de vídeos
            project_id: ID do projeto (para resolver locale)

        Returns:
            Resultado da triagem com classificações, retakes, ordem, roteamento
        """
        t0 = time.time()

        if not assets:
            return {"status": "error", "error": "No assets to analyze"}

        # Resolver idioma de resposta
        self._response_language = get_project_locale(project_id)
        logger.info(f"📋 [TRIAGE] Analisando {len(assets)} assets... (lang={self._response_language})")

        # ─── 1. Coletar thumbnails (cacheados ou extrair) ─────
        frames = {}
        cached_count = 0
        extracted_count = 0

        for asset in assets:
            if asset.get('asset_type') != 'video':
                asset['has_frame'] = False
                continue

            # Tentar usar thumbnail cacheado (extraído durante upload)
            cached_thumb = (asset.get('metadata') or {}).get('thumbnail_b64')
            if cached_thumb:
                frames[asset['id']] = {
                    'base64_data': cached_thumb,
                    'mime_type': 'image/jpeg',
                }
                asset['has_frame'] = True
                cached_count += 1
                logger.info(
                    f"  🖼️ Cache: {asset.get('filename', '?')[:40]}"
                )
                continue

            # Fallback: extrair agora (se não foi cacheado durante upload)
            if asset['id'] in video_urls:
                url = video_urls[asset['id']]
                duration_ms = asset.get('duration_ms', 0)
                seek_s = max(
                    1.0, (duration_ms / 1000) * 0.25
                ) if duration_ms else 1.0

                frame = self._extract_thumbnail(url, seek_s)
                if frame:
                    frames[asset['id']] = frame
                    asset['has_frame'] = True
                    extracted_count += 1
                    logger.info(
                        f"  🖼️ Extraído: {asset.get('filename', '?')[:40]}"
                    )
                else:
                    asset['has_frame'] = False
                    logger.warning(
                        f"  ⚠️ Sem frame: {asset.get('filename', '?')[:40]}"
                    )
            else:
                asset['has_frame'] = False

        frame_time_ms = int((time.time() - t0) * 1000)
        logger.info(
            f"📋 [TRIAGE] Frames: {len(frames)}/{len(assets)} "
            f"(cache={cached_count}, extraídos={extracted_count}) "
            f"em {frame_time_ms}ms"
        )

        # ─── 2. Chamar LLM ───────────────────────────────────
        llm_result = self._call_llm_classification(assets, frames)

        total_ms = int((time.time() - t0) * 1000)

        if llm_result.get("status") != "success":
            return {
                "status": "error",
                "error": f"LLM: {llm_result.get('error', 'unknown')}",
                "processing_time_ms": total_ms,
            }

        parsed = llm_result.get("parsed", {})

        # ─── Routing determinístico (RoutingValidator) ─────────
        # A LLM NÃO decide routing. Ela classifica; a lógica roteia.
        # Ver: app/services/routing_validator.py
        from app.services.routing_validator import build_routing_with_retakes

        classified_assets = parsed.get("assets", [])
        retakes = parsed.get("retakes", [])
        routing = build_routing_with_retakes(classified_assets, retakes)

        result = {
            "status": "success",
            "processing_time_ms": total_ms,
            "frame_extraction_ms": frame_time_ms,
            "llm_time_ms": llm_result.get("time_ms", 0),
            "model": self.model,
            "tokens_in": llm_result.get("tokens_in", 0),
            "tokens_out": llm_result.get("tokens_out", 0),
            "total_assets": len(assets),
            "frames_extracted": len(frames),
            # Dados da triagem (LLM)
            "assets": classified_assets,
            "retakes": retakes,
            "order": parsed.get("order", {}),
            "format_detected": parsed.get("format_detected", "unknown"),
            "summary": parsed.get("summary", ""),
            # Routing (determinístico)
            "routing": routing,
        }

        logger.info(
            f"✅ [TRIAGE] {total_ms}ms | "
            f"frames={frame_time_ms}ms | llm={llm_result.get('time_ms', 0)}ms | "
            f"tokens={result['tokens_in']}→{result['tokens_out']} | "
            f"format={result['format_detected']} | "
            f"vision={len(routing['vision_analysis'])}/{len(classified_assets)}"
        )

        return result

    # ───────────────────────────────────────────────────────────
    # FRAME EXTRACTION (ffmpeg direto da URL)
    # ───────────────────────────────────────────────────────────

    def _extract_thumbnail(
        self, video_url: str, seek_s: float = 1.0
    ) -> Optional[Dict]:
        """
        Extrai 1 frame de um vídeo via URL usando ffmpeg.

        O ffmpeg faz HTTP range request — não baixa o vídeo inteiro.
        -ss ANTES de -i = input seeking (rápido, keyframe).
        """
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                suffix='.jpg', delete=False
            ) as tmp:
                tmp_path = tmp.name

            result = subprocess.run(
                [
                    'ffmpeg', '-y',
                    '-ss', str(seek_s),
                    '-i', video_url,
                    '-frames:v', '1',
                    '-vf', 'scale=480:-1',
                    '-q:v', '5',
                    tmp_path,
                ],
                capture_output=True,
                timeout=30,
            )

            if result.returncode == 0 and os.path.getsize(tmp_path) > 0:
                with open(tmp_path, 'rb') as f:
                    data = base64.b64encode(f.read()).decode()
                return {
                    'base64_data': data,
                    'mime_type': 'image/jpeg',
                }
            else:
                stderr = result.stderr.decode('utf-8', errors='replace')[:200]
                logger.warning(f"⚠️ [TRIAGE] ffmpeg falhou: {stderr}")
                return None

        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️ [TRIAGE] ffmpeg timeout (seek={seek_s}s)")
            return None
        except FileNotFoundError:
            logger.error(
                "❌ [TRIAGE] ffmpeg não encontrado! "
                "Instale com: apt-get install ffmpeg"
            )
            return None
        except Exception as e:
            logger.error(f"❌ [TRIAGE] Erro frame extraction: {e}")
            return None
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    # ───────────────────────────────────────────────────────────
    # LLM CLASSIFICATION (GPT-4o-mini com visão)
    # ───────────────────────────────────────────────────────────

    def _call_llm_classification(
        self, assets: List[Dict], frames: Dict[str, Dict]
    ) -> Dict:
        """Chama GPT-4o-mini com frames + transcrições para classificação."""
        if not self.api_key:
            return {"status": "error", "error": "OPENAI_API_KEY não configurada"}

        t0 = time.time()

        try:
            # Montar descrições de cada asset
            descriptions = []
            for i, asset in enumerate(assets):
                desc = _format_asset_description(asset, i, len(assets))
                descriptions.append(desc)

            all_descriptions = "\n\n".join(descriptions)

            response_lang = getattr(self, '_response_language', DEFAULT_RESPONSE_LANGUAGE)

            user_prompt = TRIAGE_USER_PROMPT_TEMPLATE.format(
                num_assets=len(assets),
                assets_description=all_descriptions,
                response_language=response_lang,
            )

            system_prompt = TRIAGE_SYSTEM_PROMPT.format(
                response_language=response_lang,
            )

            # Montar content array (texto + imagens)
            content = [{"type": "text", "text": user_prompt}]

            for asset in assets:
                frame = frames.get(asset['id'])
                if frame:
                    content.append({
                        "type": "text",
                        "text": (
                            f"\n[Thumbnail: {asset.get('filename', asset['id'][:8])}]"
                        ),
                    })
                    content.append({
                        "type": "image_url",
                        "image_url": {
                            "url": (
                                f"data:{frame['mime_type']};"
                                f"base64,{frame['base64_data']}"
                            ),
                            "detail": "low",  # 85 tokens/image
                        },
                    })

            # Chamar OpenAI API
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
                        {"role": "user", "content": content},
                    ],
                    "max_tokens": self.max_tokens,
                    "temperature": 0.2,
                },
                timeout=60,
            )

            time_ms = int((time.time() - t0) * 1000)

            if response.status_code != 200:
                logger.error(
                    f"❌ [TRIAGE-LLM] HTTP {response.status_code}: "
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
                f"✅ [TRIAGE-LLM] {self.model} em {time_ms}ms | "
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
                "error": "LLM timeout",
                "time_ms": int((time.time() - t0) * 1000),
            }
        except Exception as e:
            logger.error(f"❌ [TRIAGE-LLM] Erro: {e}", exc_info=True)
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

        logger.warning(f"⚠️ [TRIAGE-LLM] Falha parse JSON: {raw_text[:200]}...")
        return {"raw_text": raw_text}


# Singleton
_service_instance = None


def get_asset_triage_service() -> AssetTriageService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = AssetTriageService()
    return _service_instance
