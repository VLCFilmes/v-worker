"""
🎬 VideoClipper Service — Geração de Edit Decision List (EDL).

Duas funções principais:
  1. analyze() — EDL Editorial: ordem de assets, retakes, b-roll entre segmentos
  2. generate_broll_overlay_edl() — EDL de Overlay: posicionamento preciso de
     b-rolls no timeline via cruzamento semântico (transcrição ↔ visual)

Input (overlay):
  - Transcrição com timestamps (o que é falado e quando)
  - Pipeline state: cut_timestamps, total_duration_ms, speech_segments
  - Vision analysis: visual_summary, best_usable_segment por b-roll
  - Transcript analysis: sound_bites, dead_segments

Output (overlay):
  - EDL com edit_sequence: cada b-roll com timeline_in_ms/out_ms + b_roll_in_ms/out_ms
  - Pronto para converter em track b_roll_overlay no payload do v-editor-python

Custo estimado: ~$0.005-0.02 (input é JSON estruturado, não frames)

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
CLIPPER_LLM_MODEL = os.getenv('CLIPPER_LLM_MODEL', 'gpt-4o-mini')
CLIPPER_LLM_MAX_TOKENS = int(os.getenv('CLIPPER_LLM_MAX_TOKENS', '4096'))
CLIPPER_OVERLAY_MAX_TOKENS = int(os.getenv('CLIPPER_OVERLAY_MAX_TOKENS', '4096'))
MIN_BROLL_GAP_MS = int(os.getenv('MIN_BROLL_GAP_MS', '300'))


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

CLIPPER_SYSTEM_PROMPT = """You are a VideoClipper Director — a professional video editor AI for a video production platform.

You receive structured analysis data from two previous AI directors:
1. **Transcript Director**: retake decisions (which assets to keep/remove), suggested order, per-asset info
2. **Vision Director**: visual segment analysis, motion data, best usable segments, dominant colors

Your job is to create an **Edit Decision List (EDL)** — a plan for how the final video should be assembled.

## CRITICAL RULES — DO NOT VIOLATE:
1. **Include ALL speech assets that Transcript Director marked as keep=true**. Use the ENTIRE asset (full duration).
2. **DO NOT cut, trim, or segment speech assets**. Each talking_head/audio_narration asset goes in as a whole.
3. **Retake duplicates**: Only exclude assets that Transcript Director explicitly marked as retake duplicates (keep=false).
4. **Order**: Follow the Transcript Director's suggested_order for speech assets.
5. **B-roll placement**: Use Vision Director's analysis to place b-roll between speech segments (transitions).
6. **NEVER remove unique speech content**. If in doubt, include the asset.

## B-ROLL RULES:
- B-roll is placed BETWEEN speech segments to cover transitions
- Use the best_usable_segment from Vision Director for each b-roll
- Match b-roll visually to the context of surrounding speech

LANGUAGE: Write ALL descriptive text fields in {response_language}.
Keep JSON keys in English; only VALUES should be in {response_language}.
Always respond in valid JSON format."""


CLIPPER_USER_PROMPT_TEMPLATE = """Create an Edit Decision List (EDL) for this video project.

PROJECT FORMAT: {format_detected}

=== TRANSCRIPT DIRECTOR DECISIONS ===
{transcript_summary}

=== VISUAL ANALYSIS (from Vision Director) ===
{vision_summary}

=== ASSET CLASSIFICATIONS (from Triage) ===
{triage_summary}

---

INSTRUCTIONS:
1. Include ALL speech assets marked as KEEP — use the ENTIRE asset (in_ms=0, out_ms=full duration).
2. Order speech assets according to the SUGGESTED ORDER from Transcript Director.
3. Place b-roll BETWEEN speech segments (transitions), using Vision Director's best segments.
4. Only exclude assets that are retake duplicates marked as REMOVE.
5. DO NOT trim or cut speech assets into segments.

Write descriptive fields in {response_language}.

Respond with this exact JSON structure:
{{
  "format": "{format_detected}",
  "editing_style": "dynamic | contemplative | balanced",

  "edit_sequence": [
    {{
      "order": 1,
      "type": "talking_head | b_roll | audio_narration | screen_capture",
      "asset_id": "uuid",
      "speaker": "Name (if talking_head)",
      "in_ms": 0,
      "out_ms": 0,
      "text": "Brief description of what this asset contains",
      "editorial_purpose": "introduction | development | climax | conclusion | coverage_transition"
    }}
  ],

  "b_roll_placement": [
    {{
      "after_sequence_order": 1,
      "b_roll_asset_id": "uuid",
      "in_ms": 0,
      "out_ms": 0,
      "purpose": "Cover transition between segments",
      "visual_relevance": "How the b-roll matches the narrative at this point"
    }}
  ],

  "retake_decisions": [
    {{
      "kept_asset_id": "uuid",
      "discarded_asset_ids": ["uuid"],
      "reason": "Why this take was chosen over the duplicates"
    }}
  ],

  "summary": "1-2 sentence summary of the editing plan"
}}"""


# ═══════════════════════════════════════════════════════════════
# PROMPTS — B-ROLL OVERLAY (posicionamento no timeline)
# ═══════════════════════════════════════════════════════════════

OVERLAY_SYSTEM_PROMPT = """You are a Video Clipper Director for a video production platform.

You receive:
1. Transcription with word-level timestamps (what is being said and when)
2. Base layer info: cut points, duration, speech segments
3. B-roll analysis: visual descriptions, motion data, best segments
4. Narrative analysis: themes, sound bites, dead segments

Your job is to create an Edit Decision List (EDL) placing b-rolls over the
base layer video. Work in TWO PHASES:

═══ PHASE 1: SEMANTIC MATCHING (priority placements) ═══
Cross-reference what is SAID/WRITTEN with what each b-roll SHOWS.
- If someone says "red motorcycle" and a b-roll shows a red motorcycle,
  that b-roll should appear near that moment.
- These are the strongest, most justified placements.

═══ PHASE 2: FILL & ENRICH (use remaining b-rolls) ═══
After semantic placements, USE THE REMAINING B-ROLLS to enrich the video.
CORE ASSUMPTION: the user uploaded every b-roll expecting to see it in
the final video. Only discard a b-roll if it is truly irrelevant or
of very poor quality. This should be the EXCEPTION, not the rule.
- Place remaining b-rolls at dead segments, pauses, or cut points.
- Match by theme/mood/context even without literal semantic match.
- A product shot can go near any mention of the product category.
- A landscape/atmosphere b-roll fits transitions or reflective moments.

═══ TECHNICAL RULES (non-negotiable) ═══
- Minimum {min_gap_ms}ms gap between any two b-rolls (out_point → next in_point).
  B-rolls closer than {min_gap_ms}ms cause a visual "flash" that disorients viewers.

═══ ANTI-PINGPONG RULE ═══
Avoid showing: base_layer → single b-roll → base_layer → single b-roll.
This "ping-pong" effect feels jarring, ESPECIALLY with short b-rolls.
Instead, GROUP b-rolls in sequences of 2-3 before returning to base layer.
Only show a single b-roll between base layer appearances if the semantic
context DEMANDS it (e.g., a very specific visual reference for one phrase).

═══ QUANTITY GUIDELINE ═══
IMPORTANT: Each b-roll asset typically has MULTIPLE usable segments (listed in
"Usable segments" and "All segments" in the b-roll data). You should aim to
place roughly 2x the number of b-roll assets as placements. For example:
  - 6 b-roll assets → aim for ~10-14 placements
  - 3 b-roll assets → aim for ~5-8 placements
This is a GUIDELINE, not a hard rule. The video timeline must have enough
space, and placements must make editorial sense. But don't be conservative —
reuse assets with different subclips to enrich the video.

═══ RHYTHM & GROUPING GUIDELINES ═══
- Camera MOTION b-rolls (pans, dollies, tracking) add energy and dynamics.
  If a motion b-roll is LONG, you can "chop" it into multiple shorter
  subclips (jump-cuts within the same b-roll) for added dynamism.
  Use different in/out points from the same asset to create this effect.
- STATIC b-rolls (stills from video or photos) work great but should be
  SHORTER in duration (1-3s) and grouped: 2-3 static shots in sequence,
  then a motion shot. Or start with motion, then 2-3 static shots.
- For DYNAMIC edits, jump-cuts within b-rolls are welcome (not just in
  camera movements — any b-roll can be chopped for rhythm).
- Vary the rhythm: don't make all b-rolls the same duration.
- Use the "Usable segments" data when available — each segment is a pre-validated
  subclip recommended by the Vision Director. Use different segments from the
  same asset for variety.
- Fallback to "All segments" (segment_transitions) when usable_segments is
  not available. Each segment has start_ms/end_ms you can use as in/out points.
- A single b-roll asset CAN and SHOULD appear multiple times with different
  subclips when the timeline allows it.

═══ CREATIVE GUIDELINES for talking_head mode ═══
- Cut points between speech segments are OPPORTUNITIES for b-roll sequences.
  Jump cuts are valid modern aesthetic — don't cover them just because they exist.
- Dead segments (hesitations/pauses) are good natural moments for b-roll.
- Sound bites: use judgment — sometimes the face matters, sometimes b-roll enhances.

═══ CREATIVE GUIDELINES for narration mode ═══
- B-rolls ARE the visual content (no base_layer to see).
- Aim to cover the full timeline (minimize gaps).
- Match narration text to visual content.

LANGUAGE: {response_language}
Output strict JSON. Keep JSON keys in English; only VALUES in {response_language}."""


OVERLAY_USER_PROMPT_TEMPLATE = """Create a B-roll overlay EDL for this video.

Mode: {storytelling_mode}
Base layer duration: {total_duration_ms}ms
Phase1 source: {phase1_source}

== TRANSCRIPTION (primary input for semantic matching) ==
{transcription_text}

== CUT POINTS (opportunities, NOT obligations) ==
{cut_points_text}

== SPEECH SEGMENTS ==
{speech_segments_text}

== DEAD SEGMENTS (hesitations/pauses — natural b-roll moments) ==
{dead_segments_text}

== SOUND BITES (strong phrases — use judgment on covering) ==
{sound_bites_text}

== AVAILABLE B-ROLLS ==
{brolls_text}

PHASE 1: Place b-rolls with strong semantic matches first.
PHASE 2: Then use remaining b-rolls to fill/enrich (theme, mood, rhythm).
Try to USE ALL b-rolls — the user expects to see them in the final video.
Group b-rolls in sequences of 2-3 to avoid ping-pong with base layer.
Ensure minimum {min_gap_ms}ms gap between consecutive b-rolls.
Explain each placement decision.

Respond with this exact JSON:
{{
  "status": "success",
  "mode": "{storytelling_mode}",
  "strategy": "semantic_overlay",
  "base_layer_duration_ms": {total_duration_ms},
  "total_b_rolls_placed": 0,

  "edit_sequence": [
    {{
      "order": 1,
      "type": "b_roll_overlay",
      "asset_id": "uuid of b-roll",
      "b_roll_in_ms": 0,
      "b_roll_out_ms": 3400,
      "timeline_in_ms": 4700,
      "timeline_out_ms": 8100,
      "placement_reason": "semantic_match | theme_mood | dead_segment | cut_point | rhythm | fill",
      "semantic_match": "Speaker says X — b-roll shows Y (or: theme/mood match explanation)",
      "visual_match_score": 0.85,
      "source": "vision_best_segment | segment_transition_N"
    }}
  ],

  "placement_rules_applied": {{
    "semantic_matches": 0,
    "dead_segment_placements": 0,
    "cut_point_placements": 0,
    "min_gap_between_brolls_ms": {min_gap_ms},
    "gap_violations_auto_fixed": 0
  }},

  "unused_b_rolls": [
    {{
      "asset_id": "uuid",
      "reason": "Why not used",
      "available_segments": []
    }}
  ],

  "summary": "Summary of b-roll placements and reasoning"
}}"""


# ═══════════════════════════════════════════════════════════════
# HELPERS — ENFORCE GAP
# ═══════════════════════════════════════════════════════════════

def enforce_min_gap(edl: dict, min_gap_ms: int = None) -> dict:
    """
    Valida e corrige gap mínimo entre b-rolls consecutivos.
    Se dois b-rolls estão a menos de min_gap_ms, ajusta o out_ms do primeiro.

    Args:
        edl: EDL com edit_sequence
        min_gap_ms: Gap mínimo em ms (default: MIN_BROLL_GAP_MS env)

    Returns:
        EDL com gaps corrigidos
    """
    if min_gap_ms is None:
        min_gap_ms = MIN_BROLL_GAP_MS

    sequence = edl.get('edit_sequence', [])
    if len(sequence) < 2:
        return edl

    # Ordenar por timeline_in_ms
    sequence.sort(key=lambda x: x.get('timeline_in_ms', 0))

    violations = 0
    for i in range(len(sequence) - 1):
        current_out = sequence[i].get('timeline_out_ms', 0)
        next_in = sequence[i + 1].get('timeline_in_ms', 0)
        gap = next_in - current_out

        if gap < min_gap_ms:
            # Encurtar o b-roll atual para criar o gap
            new_out = next_in - min_gap_ms
            if new_out > sequence[i].get('timeline_in_ms', 0) + 100:
                # Ajustar proporcionalmente o b_roll_out_ms
                original_duration = sequence[i]['timeline_out_ms'] - sequence[i]['timeline_in_ms']
                new_duration = new_out - sequence[i]['timeline_in_ms']
                if original_duration > 0:
                    ratio = new_duration / original_duration
                    broll_duration = sequence[i].get('b_roll_out_ms', 0) - sequence[i].get('b_roll_in_ms', 0)
                    sequence[i]['b_roll_out_ms'] = sequence[i].get('b_roll_in_ms', 0) + int(broll_duration * ratio)

                sequence[i]['timeline_out_ms'] = new_out
                violations += 1
                logger.info(
                    f"⚠️ [CLIPPER] Gap fix: broll[{i}] out {current_out}→{new_out}ms "
                    f"(gap was {gap}ms, now {min_gap_ms}ms)"
                )
            else:
                # Não dá para encurtar mais, remover o b-roll
                logger.warning(
                    f"⚠️ [CLIPPER] Gap fix: broll[{i}] removido (muito curto após ajuste)"
                )
                sequence[i]['_remove'] = True
                violations += 1

    # Remover b-rolls marcados
    sequence = [s for s in sequence if not s.get('_remove')]

    edl['edit_sequence'] = sequence
    edl.setdefault('placement_rules_applied', {})['gap_violations_auto_fixed'] = violations

    if violations:
        logger.info(f"🔧 [CLIPPER] {violations} gap violation(s) corrigidas (min={min_gap_ms}ms)")

    return edl


# ═══════════════════════════════════════════════════════════════
# HELPERS — EDL → TRACK ITEMS
# ═══════════════════════════════════════════════════════════════

def edl_to_track_items(edl: dict, asset_urls: dict) -> list:
    """
    Converte EDL de overlay do Video Clipper em items para tracks.b_roll_overlay.

    Args:
        edl: EDL completa (project_config.video_clipper_overlay_edl)
        asset_urls: Mapeamento asset_id → URL do vídeo

    Returns:
        Lista de items formatados para o payload do v-editor-python
    """
    items = []

    for entry in edl.get('edit_sequence', []):
        asset_id = entry.get('asset_id')
        if not asset_id:
            continue

        video_url = asset_urls.get(asset_id)
        if not video_url:
            logger.warning(f"⚠️ [EDL→TRACK] B-roll {asset_id[:8]} sem URL, pulando")
            continue

        item = {
            "id": f"broll_{entry.get('order', 0)}",
            "type": "b_roll_overlay",
            "src": video_url,
            "b_roll_in_ms": entry.get("b_roll_in_ms", 0),
            "b_roll_out_ms": entry.get("b_roll_out_ms", 0),
            "start_time": entry.get("timeline_in_ms", 0),
            "end_time": entry.get("timeline_out_ms", 0),
            "position": {"x": 0, "y": 0, "width": "100%", "height": "100%"},
            "zIndex": 550,
            "transition_in": entry.get("transition_in", "cut"),
            "transition_out": entry.get("transition_out", "cut"),
            "opacity": entry.get("opacity", 1.0),
            "semantic_match": entry.get("semantic_match", ""),
        }
        items.append(item)

    logger.info(f"✅ [EDL→TRACK] {len(items)} b-roll track items gerados")
    return items


# ═══════════════════════════════════════════════════════════════
# HELPERS — EDITORIAL (prompts antigos)
# ═══════════════════════════════════════════════════════════════

def _build_transcript_summary(transcript_result: dict) -> str:
    """Constrói resumo do Transcript Director para o prompt."""
    if not transcript_result:
        return "No transcript analysis available."

    lines = []

    # Retakes (principal: quais assets manter/remover)
    retakes = transcript_result.get('retakes_refined', [])
    if retakes:
        lines.append(f"RETAKE DECISIONS ({len(retakes)} groups):")
        for r in retakes:
            keep_id = r.get('keep_asset_id', '?')
            remove_ids = r.get('remove_asset_ids', [])
            # Compatibilidade com formato antigo
            if not keep_id or keep_id == '?':
                keep_id = r.get('original_asset_id', '?')
                remove_ids = [r.get('retake_asset_id', '?')]
            lines.append(
                f"  KEEP: {keep_id} | "
                f"REMOVE: {remove_ids} | "
                f"Reason: {r.get('reason', '')[:80]}"
            )
    else:
        lines.append("No retake duplicates detected — ALL speech assets should be included.")

    # Ordem sugerida
    order = transcript_result.get('order_analysis', {})
    if order:
        lines.append(f"\nSUGGESTED ORDER: {order.get('suggested_order', [])}")
        if order.get('reorder_needed'):
            lines.append(f"  REORDER NEEDED: {order.get('reason', '')[:80]}")
        else:
            lines.append("  Order is OK as-is.")

    # Per-asset (quais manter)
    per_asset = transcript_result.get('per_asset_analysis', [])
    if per_asset:
        lines.append(f"\nASSETS ({len(per_asset)}):")
        for pa in per_asset:
            keep = pa.get('keep', True)
            is_dup = pa.get('is_retake_duplicate', False)
            status = "KEEP (include entire asset)" if keep else "REMOVE (retake duplicate)"
            lines.append(
                f"  Asset {pa.get('asset_id', '?')[:8]}: "
                f"speaker={pa.get('speaker_name', '?')}, "
                f"topics={pa.get('topics', [])}, "
                f"status={status}"
            )

    # Sound bites (informacional)
    sound_bites = transcript_result.get('sound_bites', [])
    if sound_bites:
        lines.append(f"\nSOUND BITES ({len(sound_bites)}):")
        for sb in sound_bites:
            lines.append(
                f"  [{sb.get('strength', '?')}] asset {sb.get('asset_id', '?')}: "
                f"\"{sb.get('text', '')[:80]}\""
            )

    return "\n".join(lines) if lines else "No transcript analysis data."


def _build_vision_summary(vision_result: dict) -> str:
    """Constrói resumo do Vision Director para o prompt."""
    if not vision_result:
        return "No vision analysis available."

    lines = []

    # Pode ser resultado único ou múltiplo
    videos = vision_result.get('videos', [])
    if not videos and vision_result.get('status') == 'success':
        # Resultado de um único vídeo
        videos = [vision_result]

    if not videos:
        return "No vision analysis data."

    lines.append(f"Visual analysis of {len(videos)} video(s):")

    for v in videos:
        asset_id = v.get('asset_id', '?')
        content_type = v.get('content_type', '?')
        summary = v.get('visual_summary', '')[:100]
        best = v.get('best_usable_segment', {})

        lines.append(
            f"\n  Asset {asset_id[:8]}: type={content_type}"
        )
        if summary:
            lines.append(f"    Summary: {summary}")

        # Segmentos
        transitions = v.get('segment_transitions', [])
        if transitions:
            lines.append(f"    Segments ({len(transitions)}):")
            for t in transitions:
                lines.append(
                    f"      [{t.get('start_ms', 0)}-{t.get('end_ms', 0)}ms] "
                    f"{t.get('motion_type', '?')} — "
                    f"{t.get('visual_change', '')[:60]}"
                )

        # Melhor segmento
        if best and best.get('in_ms') is not None:
            lines.append(
                f"    BEST SEGMENT: {best.get('in_ms', 0)}-{best.get('out_ms', 0)}ms "
                f"— {best.get('rationale', '')[:80]}"
            )

        # Cores
        colors = v.get('dominant_colors', [])
        if colors:
            lines.append(f"    Colors: {', '.join(colors[:3])}")

    return "\n".join(lines)


def _build_triage_summary(triage_result: dict) -> str:
    """Constrói resumo da Triagem para o prompt."""
    if not triage_result:
        return "No triage data available."

    lines = []

    assets = triage_result.get('assets', [])
    if assets:
        lines.append(f"Classified assets ({len(assets)}):")
        for a in assets:
            lines.append(
                f"  {a.get('asset_id', '?')[:8]}: "
                f"{a.get('classification', '?')} "
                f"(conf: {a.get('confidence', 0):.0%}) "
                f"— {a.get('filename', '?')}"
            )

    routing = triage_result.get('routing', {})
    if routing:
        lines.append(f"\nRouting:")
        lines.append(f"  vision_analysis: {routing.get('vision_analysis', [])}")
        lines.append(f"  pipeline_ready: {routing.get('pipeline_ready', [])}")
        lines.append(f"  background_audio: {routing.get('background_audio', [])}")

    summary = triage_result.get('summary', '')
    if summary:
        lines.append(f"\nTriage summary: {summary}")

    return "\n".join(lines) if lines else "No triage data."


# ═══════════════════════════════════════════════════════════════
# SERVICE
# ═══════════════════════════════════════════════════════════════

class VideoClipperService:
    """
    Gera Edit Decision List combinando Transcript + Vision + Triage.

    Fluxo:
        1. Recebe resultados dos 3 directors anteriores
        2. Constrói prompts com resumos estruturados
        3. LLM gera EDL com cortes, ordem, b-roll placement
        4. Retorna EDL estruturada
    """

    def __init__(self):
        self.api_key = OPENAI_API_KEY
        self.model = CLIPPER_LLM_MODEL
        self.max_tokens = CLIPPER_LLM_MAX_TOKENS
        logger.info(f"🎬 VideoClipperService init | model={self.model}")

    def analyze(
        self,
        triage_result: dict,
        transcript_result: dict,
        vision_result: Optional[dict],
        response_language: str = "Portuguese (pt-BR)",
    ) -> Dict:
        """
        Gera EDL combinando resultados dos directors.

        Args:
            triage_result: Resultado do Asset Triage
            transcript_result: Resultado do Transcript Director
            vision_result: Resultado do Vision Director (pode ser None)
            response_language: Idioma para campos descritivos

        Returns:
            EDL completa com edit_sequence, b_roll_placement, etc.
        """
        t0 = time.time()

        self._response_language = response_language
        format_detected = triage_result.get('format_detected', 'unknown')

        logger.info(
            f"🎬 [CLIPPER] Gerando EDL... "
            f"(format={format_detected}, lang={response_language})"
        )

        # Construir resumos para o prompt
        transcript_summary = _build_transcript_summary(transcript_result)
        vision_summary = _build_vision_summary(vision_result)
        triage_summary = _build_triage_summary(triage_result)

        # Chamar LLM
        llm_result = self._call_llm_edl(
            format_detected,
            transcript_summary,
            vision_summary,
            triage_summary,
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
            "edl_version": "1.0",
            "processing_time_ms": total_ms,
            "llm_time_ms": llm_result.get("time_ms", 0),
            "model": self.model,
            "tokens_in": llm_result.get("tokens_in", 0),
            "tokens_out": llm_result.get("tokens_out", 0),
            "response_language": response_language,
            # EDL data
            "format": parsed.get("format", format_detected),
            "target_duration_ms": parsed.get("target_duration_ms", 0),
            "total_raw_duration_ms": parsed.get("total_raw_duration_ms", 0),
            "compression_ratio": parsed.get("compression_ratio", ""),
            "editing_style": parsed.get("editing_style", "balanced"),
            "edit_sequence": parsed.get("edit_sequence", []),
            "b_roll_placement": parsed.get("b_roll_placement", []),
            "retake_decisions": parsed.get("retake_decisions", []),
            "summary": parsed.get("summary", ""),
        }

        seq_count = len(result['edit_sequence'])
        broll_count = len(result['b_roll_placement'])

        logger.info(
            f"✅ [CLIPPER] {total_ms}ms | "
            f"llm={llm_result.get('time_ms', 0)}ms | "
            f"tokens={result['tokens_in']}→{result['tokens_out']} | "
            f"sequence={seq_count} cortes | "
            f"b_roll={broll_count} placements"
        )

        return result

    # ═══════════════════════════════════════════════════════════
    # B-ROLL OVERLAY EDL (posicionamento no timeline)
    # ═══════════════════════════════════════════════════════════

    def generate_broll_overlay_edl(
        self,
        storytelling_mode: str,
        total_duration_ms: int,
        phase1_source: str,
        transcription_words: list,
        cut_timestamps: list,
        speech_segments: list,
        broll_analyses: list,
        transcript_analysis: Optional[dict] = None,
        response_language: str = "Portuguese (pt-BR)",
    ) -> Dict:
        """
        Gera EDL de overlay para b-rolls com posicionamento preciso no timeline.

        Usa cruzamento semântico: o que é FALADO (transcrição) x o que é MOSTRADO (b-rolls).

        Args:
            storytelling_mode: 'talking_head', 'narration', 'text_video', 'videoclip'
            total_duration_ms: Duração total do base_layer em ms
            phase1_source: 'tectonic', 'tectonic_multi', 'normalized'
            transcription_words: Lista de palavras com timestamps [{word, start, end}, ...]
            cut_timestamps: Lista de timestamps de corte [ms, ms, ...]
            speech_segments: Lista de speech segments com audio_offset e duration
            broll_analyses: Lista de análises visuais dos b-rolls (Vision Director)
            transcript_analysis: Resultado do Transcript Director (sound_bites, dead_segments)
            response_language: Idioma para campos descritivos

        Returns:
            EDL de overlay com edit_sequence para b-roll track
        """
        t0 = time.time()

        logger.info(
            f"🎬 [CLIPPER-OVERLAY] Gerando EDL overlay... "
            f"(mode={storytelling_mode}, duration={total_duration_ms}ms, "
            f"b-rolls={len(broll_analyses)}, words={len(transcription_words)})"
        )

        if not broll_analyses:
            return {
                "status": "no_brolls",
                "message": "Nenhum b-roll disponível para overlay",
                "edit_sequence": [],
            }

        # Montar textos para o prompt
        transcription_text = self._format_transcription(transcription_words)
        cut_points_text = self._format_cut_points(cut_timestamps)
        speech_segments_text = self._format_speech_segments(speech_segments)
        dead_segments_text = self._format_dead_segments(transcript_analysis)
        sound_bites_text = self._format_sound_bites(transcript_analysis)
        brolls_text = self._format_broll_analyses(broll_analyses)

        # Chamar LLM
        llm_result = self._call_llm_overlay(
            storytelling_mode=storytelling_mode,
            total_duration_ms=total_duration_ms,
            phase1_source=phase1_source,
            transcription_text=transcription_text,
            cut_points_text=cut_points_text,
            speech_segments_text=speech_segments_text,
            dead_segments_text=dead_segments_text,
            sound_bites_text=sound_bites_text,
            brolls_text=brolls_text,
            response_language=response_language,
        )

        total_ms = int((time.time() - t0) * 1000)

        if llm_result.get("status") != "success":
            return {
                "status": "error",
                "error": f"LLM: {llm_result.get('error', 'unknown')}",
                "processing_time_ms": total_ms,
                "edit_sequence": [],
            }

        parsed = llm_result.get("parsed", {})

        result = {
            "status": "success",
            "edl_version": "overlay_v1",
            "processing_time_ms": total_ms,
            "llm_time_ms": llm_result.get("time_ms", 0),
            "model": self.model,
            "tokens_in": llm_result.get("tokens_in", 0),
            "tokens_out": llm_result.get("tokens_out", 0),
            "mode": storytelling_mode,
            "strategy": parsed.get("strategy", "semantic_overlay"),
            "base_layer_duration_ms": total_duration_ms,
            "edit_sequence": parsed.get("edit_sequence", []),
            "total_b_rolls_placed": len(parsed.get("edit_sequence", [])),
            "placement_rules_applied": parsed.get("placement_rules_applied", {}),
            "unused_b_rolls": parsed.get("unused_b_rolls", []),
            "summary": parsed.get("summary", ""),
        }

        # Enforce min gap (regra técnica, código, não LLM)
        result = enforce_min_gap(result, MIN_BROLL_GAP_MS)

        placed = len(result['edit_sequence'])
        logger.info(
            f"✅ [CLIPPER-OVERLAY] {total_ms}ms | "
            f"llm={llm_result.get('time_ms', 0)}ms | "
            f"tokens={result['tokens_in']}→{result['tokens_out']} | "
            f"b-rolls placed={placed}"
        )

        return result

    def _call_llm_overlay(
        self,
        storytelling_mode: str,
        total_duration_ms: int,
        phase1_source: str,
        transcription_text: str,
        cut_points_text: str,
        speech_segments_text: str,
        dead_segments_text: str,
        sound_bites_text: str,
        brolls_text: str,
        response_language: str,
    ) -> Dict:
        """Chama LLM para gerar EDL de overlay."""
        if not self.api_key:
            return {"status": "error", "error": "OPENAI_API_KEY não configurada"}

        t0 = time.time()

        try:
            system_prompt = OVERLAY_SYSTEM_PROMPT.format(
                min_gap_ms=MIN_BROLL_GAP_MS,
                response_language=response_language,
            )

            user_prompt = OVERLAY_USER_PROMPT_TEMPLATE.format(
                storytelling_mode=storytelling_mode,
                total_duration_ms=total_duration_ms,
                phase1_source=phase1_source,
                transcription_text=transcription_text,
                cut_points_text=cut_points_text,
                speech_segments_text=speech_segments_text,
                dead_segments_text=dead_segments_text,
                sound_bites_text=sound_bites_text,
                brolls_text=brolls_text,
                min_gap_ms=MIN_BROLL_GAP_MS,
                response_language=response_language,
            )

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
                    "max_tokens": CLIPPER_OVERLAY_MAX_TOKENS,
                    "temperature": 0.3,
                },
                timeout=90,
            )

            time_ms = int((time.time() - t0) * 1000)

            if response.status_code != 200:
                logger.error(
                    f"❌ [CLIPPER-OVERLAY-LLM] HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )
                return {"status": "error", "error": f"HTTP {response.status_code}", "time_ms": time_ms}

            result = response.json()
            choice = result.get("choices", [{}])[0]
            raw_text = choice.get("message", {}).get("content", "")
            usage = result.get("usage", {})

            parsed = self._parse_json(raw_text)

            logger.info(
                f"✅ [CLIPPER-OVERLAY-LLM] {self.model} em {time_ms}ms | "
                f"tokens={usage.get('prompt_tokens', 0)}→{usage.get('completion_tokens', 0)}"
            )

            return {
                "status": "success",
                "parsed": parsed,
                "tokens_in": usage.get("prompt_tokens", 0),
                "tokens_out": usage.get("completion_tokens", 0),
                "time_ms": time_ms,
            }

        except requests.exceptions.Timeout:
            return {"status": "error", "error": "LLM timeout (90s)", "time_ms": int((time.time() - t0) * 1000)}
        except Exception as e:
            logger.error(f"❌ [CLIPPER-OVERLAY-LLM] Erro: {e}", exc_info=True)
            return {"status": "error", "error": str(e), "time_ms": int((time.time() - t0) * 1000)}

    # ═══════════════════════════════════════════════════════════
    # FORMATADORES — OVERLAY
    # ═══════════════════════════════════════════════════════════

    def _format_transcription(self, words: list) -> str:
        """Formata transcrição com timestamps para o prompt."""
        if not words:
            return "No transcription available."

        lines = []
        current_line = ""
        current_start = 0

        for w in words:
            word = w.get('word', w.get('text', ''))
            start = w.get('start', w.get('start_ms', 0))
            # Agrupar palavras em linhas de ~10 palavras
            if not current_line:
                current_start = start
                current_line = word
            else:
                current_line += " " + word

            if len(current_line.split()) >= 10:
                start_s = current_start / 1000 if current_start > 100 else current_start
                lines.append(f"  [{start_s:.1f}s] {current_line}")
                current_line = ""

        # Última linha
        if current_line:
            start_s = current_start / 1000 if current_start > 100 else current_start
            lines.append(f"  [{start_s:.1f}s] {current_line}")

        return "\n".join(lines) if lines else "No transcription words."

    def _format_cut_points(self, cut_timestamps: list) -> str:
        """Formata pontos de corte."""
        if not cut_timestamps:
            return "No cut points (single continuous video)."

        lines = [f"Total: {len(cut_timestamps)} cut points"]
        for i, ts in enumerate(cut_timestamps):
            ts_s = ts / 1000 if ts > 100 else ts
            lines.append(f"  Cut {i + 1}: {ts_s:.2f}s ({ts}ms)")
        return "\n".join(lines)

    def _format_speech_segments(self, segments: list) -> str:
        """Formata speech segments."""
        if not segments:
            return "No speech segments."

        lines = [f"Total: {len(segments)} segments"]
        for i, seg in enumerate(segments):
            offset = seg.get('audio_offset', seg.get('original_start', 0))
            duration = seg.get('duration', 0)
            offset_s = offset if offset < 100 else offset / 1000
            dur_s = duration if duration < 100 else duration / 1000
            lines.append(f"  Seg {i}: offset={offset_s:.2f}s, duration={dur_s:.2f}s")
        return "\n".join(lines)

    def _format_dead_segments(self, transcript_analysis: Optional[dict]) -> str:
        """Formata dead segments do Transcript Director."""
        if not transcript_analysis:
            return "No dead segment data."

        dead_segments = []
        for pa in transcript_analysis.get('per_asset_analysis', []):
            for ds in pa.get('dead_segments', []):
                dead_segments.append(ds)

        if not dead_segments:
            return "No dead segments detected."

        lines = [f"Total: {len(dead_segments)} dead segments"]
        for ds in dead_segments:
            start = ds.get('start_ms', ds.get('start', 0))
            end = ds.get('end_ms', ds.get('end', 0))
            reason = ds.get('reason', ds.get('type', ''))
            start_s = start / 1000 if start > 100 else start
            end_s = end / 1000 if end > 100 else end
            lines.append(f"  [{start_s:.1f}s - {end_s:.1f}s] {reason}")
        return "\n".join(lines)

    def _format_sound_bites(self, transcript_analysis: Optional[dict]) -> str:
        """Formata sound bites do Transcript Director."""
        if not transcript_analysis:
            return "No sound bite data."

        sound_bites = transcript_analysis.get('sound_bites', [])
        if not sound_bites:
            return "No sound bites detected."

        lines = [f"Total: {len(sound_bites)} sound bites"]
        for sb in sound_bites:
            strength = sb.get('strength', '?')
            text = sb.get('text', '')[:80]
            start = sb.get('start_ms', sb.get('timestamp_ms', 0))
            start_s = start / 1000 if start > 100 else start
            lines.append(f"  [{strength}] @{start_s:.1f}s: \"{text}\"")
        return "\n".join(lines)

    def _format_broll_analyses(self, analyses: list) -> str:
        """Formata análises visuais dos b-rolls para o prompt do Clipper.
        
        Mostra TODOS os segmentos usáveis por asset para que a LLM possa
        usar o mesmo b-roll múltiplas vezes com subclips diferentes.
        """
        if not analyses:
            return "No b-roll analyses available."

        lines = [
            f"Total: {len(analyses)} b-roll assets available.",
            "NOTE: Each asset can be used MULTIPLE TIMES with different subclips.",
            "Use usable_segments or segment_transitions as in/out points.",
        ]
        for a in analyses:
            asset_id = a.get('asset_id', '?')
            summary = a.get('visual_summary', '')[:120]
            duration = a.get('duration_s', 0)
            best = a.get('best_usable_segment', {})

            lines.append(f"\n  Asset: {asset_id}")
            lines.append(f"    Visual: \"{summary}\"")
            lines.append(f"    Duration: {duration:.1f}s")

            # Usable segments (novo campo do Vision Director v2)
            usable = a.get('usable_segments', [])
            if usable:
                lines.append(f"    Usable segments ({len(usable)}):")
                for i, seg in enumerate(usable):
                    motion = seg.get('motion_type', '')
                    motion_tag = f" [{motion}]" if motion else ""
                    lines.append(
                        f"      #{i+1}: {seg.get('in_ms', 0)}ms - {seg.get('out_ms', 0)}ms{motion_tag} "
                        f"— {seg.get('rationale', '')[:80]}"
                    )
            elif best and best.get('in_ms') is not None:
                # Fallback: best_usable_segment (backward compat)
                lines.append(
                    f"    Best segment: {best.get('in_ms', 0)}ms - {best.get('out_ms', 0)}ms "
                    f"({best.get('rationale', '')[:60]})"
                )

            # ALL segment transitions (não limitar a 4)
            transitions = a.get('segment_transitions', [])
            if transitions:
                lines.append(f"    All segments ({len(transitions)}):")
                for t in transitions:
                    lines.append(
                        f"      seg{t.get('segment_id', '?')}: [{t.get('start_ms', 0)}-{t.get('end_ms', 0)}ms] "
                        f"{t.get('motion_type', '?')} — {t.get('visual_change', '')[:60]}"
                    )

            # Keywords / content
            frames = a.get('frames_description', [])
            if frames:
                keywords = set()
                for f in frames:
                    content = f.get('content', '')
                    if content:
                        keywords.update(content.lower().split()[:5])
                if keywords:
                    lines.append(f"    Keywords: {', '.join(list(keywords)[:15])}")

            colors = a.get('dominant_colors', [])
            if colors:
                lines.append(f"    Colors: {', '.join(colors[:3])}")

        return "\n".join(lines)

    # ═══════════════════════════════════════════════════════════
    # EDITORIAL EDL (fluxo original)
    # ═══════════════════════════════════════════════════════════

    def _call_llm_edl(
        self,
        format_detected: str,
        transcript_summary: str,
        vision_summary: str,
        triage_summary: str,
    ) -> Dict:
        """Chama GPT-4o-mini para gerar EDL."""
        if not self.api_key:
            return {"status": "error", "error": "OPENAI_API_KEY não configurada"}

        t0 = time.time()

        try:
            response_lang = self._response_language

            system_prompt = CLIPPER_SYSTEM_PROMPT.format(
                response_language=response_lang,
            )

            user_prompt = CLIPPER_USER_PROMPT_TEMPLATE.format(
                format_detected=format_detected,
                transcript_summary=transcript_summary,
                vision_summary=vision_summary,
                triage_summary=triage_summary,
                response_language=response_lang,
            )

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
                    f"❌ [CLIPPER-LLM] HTTP {response.status_code}: "
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
                f"✅ [CLIPPER-LLM] {self.model} em {time_ms}ms | "
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
            logger.error(f"❌ [CLIPPER-LLM] Erro: {e}", exc_info=True)
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
            f"⚠️ [CLIPPER-LLM] Falha parse JSON: {raw_text[:200]}..."
        )
        return {"raw_text": raw_text}


# ═══════════════════════════════════════════════════════════════
# HELPERS — RESOLVE B-ROLL URLS
# ═══════════════════════════════════════════════════════════════

def resolve_broll_urls(project_id: str) -> dict:
    """
    Resolve URLs dos b-rolls do projeto.
    Retorna mapeamento asset_id → URL (B2 ou shared-assets).
    """
    try:
        from app.db import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, file_path, bucket
            FROM project_assets
            WHERE project_id = %s AND asset_type = 'video'
        """, (project_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        urls = {}
        for row in rows:
            asset_id = str(row[0])
            file_path = row[1]
            bucket = row[2]

            # Tentar gerar URL temporária do B2
            try:
                from app.routes.upload import generate_temp_download_url_internal
                url = generate_temp_download_url_internal(
                    bucket_name=bucket,
                    file_path=file_path,
                    duration_seconds=3600,
                )
                if url:
                    urls[asset_id] = url
            except Exception:
                # Fallback: tentar shared-assets path
                urls[asset_id] = f"/shared-assets/{file_path}"

        logger.info(f"🎬 [CLIPPER] Resolvidas {len(urls)} URLs de b-roll para {project_id[:8]}")
        return urls

    except Exception as e:
        logger.warning(f"⚠️ [CLIPPER] Erro ao resolver URLs de b-roll: {e}")
        return {}


# Singleton
_service_instance = None


def get_video_clipper_service() -> VideoClipperService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = VideoClipperService()
    return _service_instance
