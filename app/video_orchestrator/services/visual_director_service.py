"""
👁️ Visual Director Service — Arquitetura Híbrida (Modal CPU + LLM API)

Fluxo em 2 etapas:
  1. Modal CPU (v-motion-analyzer): RAFT → motion_data + frames_b64
  2. LLM API (GPT-4o-mini vision): frames + motion context → análise semântica

A v-api orquestra — não faz processamento de vídeo.

Endpoints:
  Modal Motion: POST https://fotovinicius2--v-motion-analyzer-analyze-motion.modal.run
  OpenAI:       POST https://api.openai.com/v1/chat/completions

Custo estimado por vídeo:
  - Modal CPU: ~$0.001-0.003 (30-60s × $0.096/h)
  - GPT-4o-mini: ~$0.001 (7 frames × 255 tokens + prompt)
  - Total: ~$0.002-0.004 por análise

Histórico:
    v1: Container Docker local + Gemini 2.0 Flash
    v2: Modal A10G GPU + Qwen2.5-VL-7B (tudo no Modal)
    v3 (atual): Modal CPU (RAFT) + GPT-4o-mini API (híbrido)
"""

import os
import requests
import logging
import time
import json
import base64
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

# ─── Configuração ─────────────────────────────────────────────
MODAL_MOTION_URL = os.getenv(
    'V_MOTION_ANALYZER_URL',
    'https://fotovinicius2--v-motion-analyzer-analyze-motion.modal.run'
)

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
VISION_LLM_MODEL = os.getenv('VISION_LLM_MODEL', 'gpt-4o-mini')
VISION_LLM_MAX_TOKENS = int(os.getenv('VISION_LLM_MAX_TOKENS', '2048'))

# Idioma padrão para respostas de IA
DEFAULT_RESPONSE_LANGUAGE = os.getenv('AI_RESPONSE_LANGUAGE', 'Portuguese (pt-BR)')


# ═══════════════════════════════════════════════════════════════
# SYSTEM + USER PROMPTS
# ═══════════════════════════════════════════════════════════════

SYSTEM_PROMPT_TEMPLATE = """You are an expert video editor and director.
You receive strategically selected frames from key moments in a video,
along with precise computer vision data about camera movement.

Your job is NOT to detect motion (that's already done by algorithms).
Your job IS to describe WHAT is visible in each frame and what changed
between the start and end of each camera movement.

LANGUAGE: Write ALL descriptive text (visual_summary, content descriptions, quality_notes,
rationale, visual_change, framing_change, composition) in {response_language}.
Keep JSON keys in English; only VALUES should be in {response_language}.

Always respond in valid JSON format."""

USER_PROMPT_TEMPLATE = """I have a {duration_s:.1f}s video. Computer vision has already analyzed it.
Here are {num_frames} strategically selected frames and the motion analysis data.

{motion_context}

IMPORTANT — What you should do:
1. For each frame: describe WHAT is visible (objects, people, composition, framing)
2. For dynamic segments: compare the START frame vs END frame and describe what changed
   (e.g. "framing goes from close-up of one camera to wide shot of 10 cameras")
3. Suggest ALL usable segments an editor could pick from this footage (not just one!)
   A single video often has 2-4 distinct usable moments. Report ALL of them.
4. DO NOT try to detect motion — the motion data above is already precise

Provide your analysis in this exact JSON structure:
{{
  "content_type": "talking_head | b_roll | product_shot | screen_capture | mixed | other",
  "visual_summary": "2-3 sentences describing the video content and what happens",
  "frames_description": [
    {{
      "timestamp_ms": 1000,
      "label": "motion_start / motion_end / static / opening / closing",
      "shot_type": "close_up | medium | wide | extreme_close_up",
      "content": "Detailed description of what's visible in this frame",
      "composition": "What's in focus, foreground/background elements"
    }}
  ],
  "segment_transitions": [
    {{
      "segment_id": 0,
      "start_ms": 0,
      "end_ms": 4200,
      "motion_type": "static | pan_right | pan_left | tilt_up | tilt_down | zoom_in | zoom_out",
      "visual_change": "Description of what changed from start to end of this segment",
      "framing_change": "e.g. close_up → wide, or no change for static"
    }}
  ],
  "best_usable_segment": {{
    "in_ms": 4200,
    "out_ms": 7600,
    "rationale": "Why a video editor would pick this specific segment",
    "framing": "Description of the framing in this segment"
  }},
  "usable_segments": [
    {{
      "in_ms": 0,
      "out_ms": 4200,
      "rationale": "Why this segment is usable (visual content, framing quality)",
      "framing": "Description of the framing",
      "motion_type": "static | pan_right | etc"
    }}
  ],
  "quality_notes": "Lighting, focus, stability, exposure observations",
  "dominant_colors": ["#hex1", "#hex2"]
}}

IMPORTANT: `usable_segments` should list ALL segments from this footage that
a video editor could use as b-roll. Include 2-4 segments when possible.
Each segment can later be used as an independent subclip in the final edit.
`best_usable_segment` remains the single TOP pick for backward compatibility.

Be precise. Describe each frame individually. Focus on CONTENT, not motion."""


def _build_motion_context(motion_data: dict) -> str:
    """Formata motion analysis para o prompt da LLM."""
    if not motion_data or not motion_data.get("segments"):
        return "MOTION ANALYSIS: No data available."

    summary = motion_data.get("summary", {})
    segments = motion_data.get("segments", [])
    scene_cuts = motion_data.get("scene_cuts", [])

    lines = [
        "═══ MOTION ANALYSIS (RAFT deep learning optical flow) ═══",
        f"Duration: {summary.get('total_duration_s', 0):.1f}s | "
        f"Static: {summary.get('static_pct', 0)}% | "
        f"Dynamic: {summary.get('dynamic_pct', 0)}%",
    ]

    best = summary.get("best_segment")
    if best:
        lines.append(f"Best segment: {best['start_s']:.1f}s → {best['end_s']:.1f}s")

    lines.append("")
    lines.append("SEGMENTS (detected by computer vision — this is precise):")
    for i, seg in enumerate(segments):
        direction = f" — {seg['direction']}" if seg.get("direction") else ""
        dur = seg["end_s"] - seg["start_s"]
        lines.append(
            f"  Segment {i}: [{seg['start_s']:.1f}s → {seg['end_s']:.1f}s] "
            f"({dur:.1f}s) {seg['type'].upper()}{direction} "
            f"(motion_score={seg['avg_score']})"
        )

    if scene_cuts:
        lines.append("")
        lines.append(f"SCENE CUTS: {len(scene_cuts)} detected at: "
                      + ", ".join(f"{c['time_s']:.1f}s" for c in scene_cuts))

    lines.append("")
    lines.append("FRAMES PROVIDED:")
    lines.append("Each frame is labeled with its role (motion_start, motion_end, static, etc.)")
    lines.append("For dynamic segments, compare START vs END frames to describe what changed.")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# SERVICE
# ═══════════════════════════════════════════════════════════════

class VisualDirectorService:
    """
    Orquestrador híbrido: Modal CPU (RAFT) + LLM API (GPT-4o-mini).

    Fluxo:
        1. Chama Modal v-motion-analyzer → motion_data + frames
        2. Chama GPT-4o-mini com frames + motion context → análise semântica
        3. Combina resultados
    """

    def __init__(self):
        self.motion_endpoint = MODAL_MOTION_URL
        self.motion_timeout = int(os.getenv('MOTION_ANALYZER_TIMEOUT', '300'))
        self.llm_model = VISION_LLM_MODEL
        self.llm_max_tokens = VISION_LLM_MAX_TOKENS
        self.api_key = OPENAI_API_KEY

        logger.info(f"👁️ VisualDirectorService v3 (híbrido)")
        logger.info(f"   Motion: {self.motion_endpoint}")
        logger.info(f"   LLM: {self.llm_model}")

    def analyze(
        self,
        video_url: str,
        duration_ms: int = 0,
        transcription_text: Optional[str] = None,
        transcription_words: Optional[List[Dict]] = None,
        options: Optional[Dict] = None,
        response_language: Optional[str] = None,
    ) -> Dict:
        """
        Análise visual em 2 etapas.

        Step 1: Modal CPU → RAFT motion + frames
        Step 2: GPT-4o-mini → análise semântica
        """
        t0 = time.time()
        options = options or {}

        logger.info(f"👁️ [VISION] Iniciando análise híbrida...")
        logger.info(f"   URL: {video_url[:80]}...")

        # ─── STEP 1: Modal Motion Analyzer (CPU) ─────────────
        logger.info(f"🔍 [STEP 1] Chamando Modal motion analyzer...")
        motion_result = self._call_motion_analyzer(video_url, options)

        if motion_result.get("status") != "success":
            return {
                "status": "error",
                "error": f"Motion analyzer: {motion_result.get('error', 'unknown')}",
                "processing_time_ms": int((time.time() - t0) * 1000),
            }

        motion_data = motion_result["motion_data"]
        frames = motion_result["frames"]
        duration_s = motion_result["duration_s"]
        motion_time_ms = motion_result["processing_time_ms"]
        engine = motion_result.get("engine", "unknown")

        logger.info(
            f"✅ [STEP 1] Motion: {engine} | "
            f"{len(frames)} frames | {duration_s}s | "
            f"{motion_time_ms}ms | "
            f"segments={len(motion_data.get('segments', []))}"
        )

        # ─── STEP 2: LLM API (GPT-4o-mini) ───────────────────
        logger.info(f"🧠 [STEP 2] Chamando {self.llm_model} com {len(frames)} frames...")
        llm_result = self._call_llm_vision(
            frames=frames,
            motion_data=motion_data,
            duration_s=duration_s,
            transcription_text=transcription_text,
            response_language=response_language or DEFAULT_RESPONSE_LANGUAGE,
        )

        total_ms = int((time.time() - t0) * 1000)

        if llm_result.get("status") != "success":
            return {
                "status": "error",
                "error": f"LLM: {llm_result.get('error', 'unknown')}",
                "processing_time_ms": total_ms,
                "motion_data": motion_data,  # Retorna motion mesmo se LLM falhar
            }

        # ─── Combinar resultados ──────────────────────────────
        parsed = llm_result.get("parsed", {})
        result = {
            "status": "success",
            "processing_time_ms": total_ms,
            # Dados semânticos da LLM
            "content_type": parsed.get("content_type", "unknown"),
            "visual_summary": parsed.get("visual_summary", ""),
            "frames_description": parsed.get("frames_description", []),
            "segment_transitions": parsed.get("segment_transitions", []),
            "best_usable_segment": parsed.get("best_usable_segment", {}),
            "quality_notes": parsed.get("quality_notes", ""),
            "dominant_colors": parsed.get("dominant_colors", []),
            # Compat com formato antigo
            "shots": parsed.get("frames_description", []),
            "cut_suggestions": [],
            # Dados de motion (RAFT)
            "motion_analysis": motion_data,
            # Metadata
            "model": self.llm_model,
            "engine": engine,
            "duration_s": duration_s,
            "frames_count": len(frames),
            "video_size_mb": motion_result.get("video_size_mb", 0),
            # Custos
            "motion_time_ms": motion_time_ms,
            "llm_time_ms": llm_result.get("time_ms", 0),
            "tokens_in": llm_result.get("tokens_in", 0),
            "tokens_out": llm_result.get("tokens_out", 0),
            "raw_text": llm_result.get("raw_text", ""),
        }

        logger.info(
            f"✅ [VISION] Análise completa em {total_ms}ms | "
            f"motion={motion_time_ms}ms ({engine}) | "
            f"llm={llm_result.get('time_ms', 0)}ms ({self.llm_model}) | "
            f"tokens={result['tokens_in']}→{result['tokens_out']} | "
            f"content_type={result['content_type']}"
        )

        return result

    # ───────────────────────────────────────────────────────────
    # STEP 1: Modal Motion Analyzer
    # ───────────────────────────────────────────────────────────

    def _call_motion_analyzer(self, video_url: str, options: dict) -> dict:
        """
        Chama v-motion-analyzer no Modal (CPU-only).
        
        Retry com backoff exponencial para lidar com erros de cold start
        do Modal (HTTP 500 "internal error while reading response").
        """
        import time as _time
        
        MAX_RETRIES = 3
        RETRY_DELAYS = [3, 6, 12]  # segundos entre tentativas
        
        payload = {
            "video_url": video_url,
            "num_frames": options.get("num_frames", 8),
            "max_dimension": options.get("max_dimension", 480),
            "analysis_fps": options.get("analysis_fps", 3),
            "analysis_size": options.get("analysis_size", 320),
        }
        
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    self.motion_endpoint,
                    json=payload,
                    timeout=self.motion_timeout,
                )

                if response.status_code == 200:
                    if attempt > 0:
                        logger.info(f"✅ [MOTION] Sucesso após {attempt + 1} tentativas")
                    return response.json()
                
                # Erro 5xx → retry (cold start, sobrecarga)
                if response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    logger.warning(
                        f"⚠️ [MOTION] HTTP {response.status_code} (tentativa {attempt + 1}/{MAX_RETRIES}), "
                        f"retry em {delay}s... [{response.text[:100]}]"
                    )
                    _time.sleep(delay)
                    last_error = f"HTTP {response.status_code}"
                    continue
                
                # Erro 4xx ou última tentativa 5xx → desistir
                logger.error(f"❌ [MOTION] HTTP {response.status_code}: {response.text[:200]}")
                return {"status": "error", "error": f"HTTP {response.status_code}"}

            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    logger.warning(
                        f"⚠️ [MOTION] Timeout tentativa {attempt + 1}/{MAX_RETRIES}, "
                        f"retry em {delay}s..."
                    )
                    _time.sleep(delay)
                    last_error = "timeout"
                    continue
                logger.error(f"❌ [MOTION] Timeout após {MAX_RETRIES} tentativas ({self.motion_timeout}s)")
                return {"status": "error", "error": "timeout"}
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    logger.warning(
                        f"⚠️ [MOTION] Erro tentativa {attempt + 1}/{MAX_RETRIES}: {e}, "
                        f"retry em {delay}s..."
                    )
                    _time.sleep(delay)
                    last_error = str(e)
                    continue
                logger.error(f"❌ [MOTION] Erro após {MAX_RETRIES} tentativas: {e}")
                return {"status": "error", "error": str(e)}
        
        # Fallback (não deveria chegar aqui)
        return {"status": "error", "error": last_error or "unknown"}

    # ───────────────────────────────────────────────────────────
    # STEP 2: LLM Vision API
    # ───────────────────────────────────────────────────────────

    def _call_llm_vision(
        self,
        frames: list,
        motion_data: dict,
        duration_s: float,
        transcription_text: Optional[str] = None,
        response_language: str = 'Portuguese (pt-BR)',
    ) -> dict:
        """Chama GPT-4o-mini com frames + motion context."""
        if not self.api_key:
            return {"status": "error", "error": "OPENAI_API_KEY não configurada"}

        t0 = time.time()

        try:
            # Montar motion context
            motion_context = _build_motion_context(motion_data)

            # System prompt com idioma
            system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
                response_language=response_language,
            )

            # User prompt
            user_prompt = USER_PROMPT_TEMPLATE.format(
                duration_s=duration_s,
                num_frames=len(frames),
                motion_context=motion_context,
            )

            # Se temos transcrição, adicionar ao prompt
            if transcription_text:
                preview = transcription_text[:500]
                if len(transcription_text) > 500:
                    preview += "..."
                user_prompt += f"\n\nTRANSCRIPTION (for context):\n\"{preview}\""

            # Montar content array (texto + imagens)
            content = [{"type": "text", "text": user_prompt}]

            for frame in frames:
                # Label antes da imagem
                content.append({
                    "type": "text",
                    "text": f"\n[Frame @{frame['timestamp_ms']}ms — {frame.get('label', 'unknown')}]",
                })
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:{frame['mime_type']};base64,{frame['base64_data']}",
                        "detail": "low",  # Low detail = 85 tokens/image (mais barato)
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
                    "model": self.llm_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": content},
                    ],
                    "max_tokens": self.llm_max_tokens,
                    "temperature": 0.3,
                },
                timeout=60,
            )

            time_ms = int((time.time() - t0) * 1000)

            if response.status_code != 200:
                logger.error(f"❌ [LLM] HTTP {response.status_code}: {response.text[:300]}")
                return {"status": "error", "error": f"HTTP {response.status_code}", "time_ms": time_ms}

            result = response.json()
            choice = result.get("choices", [{}])[0]
            raw_text = choice.get("message", {}).get("content", "")
            usage = result.get("usage", {})

            tokens_in = usage.get("prompt_tokens", 0)
            tokens_out = usage.get("completion_tokens", 0)

            # Parse JSON da resposta
            parsed = self._parse_llm_json(raw_text)

            logger.info(
                f"✅ [LLM] {self.llm_model} em {time_ms}ms | "
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
            return {"status": "error", "error": "LLM timeout", "time_ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            logger.error(f"❌ [LLM] Erro: {e}", exc_info=True)
            return {"status": "error", "error": str(e), "time_ms": int((time.time() - t0) * 1000)}

    def _parse_llm_json(self, raw_text: str) -> dict:
        """Tenta extrair JSON da resposta da LLM."""
        try:
            # Tentar parse direto
            return json.loads(raw_text)
        except json.JSONDecodeError:
            pass

        # Tentar extrair de code block
        try:
            if "```json" in raw_text:
                json_str = raw_text.split("```json")[1].split("```")[0].strip()
                return json.loads(json_str)
            elif "```" in raw_text:
                json_str = raw_text.split("```")[1].split("```")[0].strip()
                return json.loads(json_str)
        except (json.JSONDecodeError, IndexError):
            pass

        logger.warning(f"⚠️ [LLM] Não conseguiu parsear JSON: {raw_text[:200]}...")
        return {"raw_text": raw_text}

    def health_check(self) -> dict:
        """Verifica saúde dos dois endpoints."""
        motion_ok = False
        llm_ok = bool(self.api_key)

        try:
            r = requests.post(self.motion_endpoint, json={"video_url": ""}, timeout=10)
            motion_ok = r.status_code in (200, 422, 500)  # Qualquer resposta = endpoint vivo
        except Exception:
            pass

        return {"motion": motion_ok, "llm": llm_ok, "model": self.llm_model}


# Singleton
_service_instance = None


def get_visual_director_service() -> VisualDirectorService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = VisualDirectorService()
    return _service_instance
