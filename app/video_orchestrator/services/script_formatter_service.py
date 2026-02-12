"""
📝 Script Formatter Service — Parsing de tags visuais do roteiro

Parseia anotações visuais inline no roteiro e separa em:
- clean_text: texto puro (sem tags), para timestamps e fraseamento
- scene_overrides: lista de overrides visuais por cena (cartela, background)

Tags suportadas (formato estruturado):
  [CENA N]                → delimitador de cena
  [CARTELA: cor]          → habilita cartela com cor específica
  [CARTELA: cor, tipo: X] → cartela com cor e tipo
  [CARTELA: cor, opacidade: 0.8] → cartela com opacidade
  [BG: tipo]              → background override por cena

Retrocompatível: se não houver tags, retorna texto integral como 1 cena sem overrides.
"""

import re
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# REGEX PATTERNS
# ═══════════════════════════════════════════════════════════════════════════

# [CENA 1] ou [CENA 2] etc. (case-insensitive)
SCENE_PATTERN = re.compile(r'\[CENA\s+(\d+)\]', re.IGNORECASE)

# [CARTELA: amarela] ou [CARTELA: amarela, tipo: destaque] ou [CARTELA: cor, opacidade: 0.8]
CARTELA_PATTERN = re.compile(
    r'\[CARTELA:\s*([^\]]+)\]',
    re.IGNORECASE
)

# [BG: gradiente-azul] ou [BG: imagem-tech]
BG_PATTERN = re.compile(
    r'\[BG:\s*([^\]]+)\]',
    re.IGNORECASE
)

# Qualquer tag entre colchetes que usamos (para remoção do texto limpo)
ALL_TAGS_PATTERN = re.compile(
    r'\[(?:CENA\s+\d+|CARTELA:[^\]]*|BG:[^\]]*)\]',
    re.IGNORECASE
)


# ═══════════════════════════════════════════════════════════════════════════
# COLOR MAP — cores nomeadas → hex
# ═══════════════════════════════════════════════════════════════════════════

COLOR_MAP = {
    "amarela": "#FFD700",
    "amarelo": "#FFD700",
    "vermelha": "#FF4444",
    "vermelho": "#FF4444",
    "azul": "#4488FF",
    "verde": "#44BB44",
    "roxo": "#9944FF",
    "roxo-escuro": "#6622CC",
    "laranja": "#FF8800",
    "rosa": "#FF66AA",
    "branca": "#FFFFFF",
    "branco": "#FFFFFF",
    "preta": "#000000",
    "preto": "#000000",
    "cinza": "#888888",
    "dourada": "#FFD700",
    "dourado": "#FFD700",
    "prata": "#C0C0C0",
}


# ═══════════════════════════════════════════════════════════════════════════
# SERVICE
# ═══════════════════════════════════════════════════════════════════════════

class ScriptFormatterService:
    """
    Parseia tags visuais do roteiro e separa texto limpo de overrides.
    
    Input:  texto bruto do roteiro (com tags inline)
    Output: {
        "scenes": [...],
        "clean_text": "texto sem tags",
        "scene_count": int
    }
    """

    def format(self, raw_text: str) -> Dict[str, Any]:
        """
        Parseia o roteiro e retorna cenas com overrides.
        
        Args:
            raw_text: Roteiro bruto (pode ter tags [CENA], [CARTELA], [BG])
            
        Returns:
            {
                "scenes": [
                    {
                        "scene_index": 0,
                        "raw_text": "texto original com tags",
                        "clean_text": "texto sem tags",
                        "overrides": {
                            "cartela": {"enabled": True, "color": "#FFD700", ...} | None,
                            "background": {"type": "gradiente-azul"} | None
                        }
                    },
                    ...
                ],
                "clean_text": "texto completo sem tags (todas as cenas concatenadas)",
                "scene_count": int,
                "has_overrides": bool
            }
        """
        if not raw_text or not raw_text.strip():
            return {
                "scenes": [],
                "clean_text": "",
                "scene_count": 0,
                "has_overrides": False,
            }

        # 1. Dividir em cenas
        raw_scenes = self._split_into_scenes(raw_text)

        # 2. Para cada cena, extrair overrides e texto limpo
        scenes = []
        all_clean_texts = []
        has_any_override = False

        for idx, scene_text in enumerate(raw_scenes):
            overrides = self._extract_overrides(scene_text)
            clean = self._strip_tags(scene_text)

            if overrides.get("cartela") or overrides.get("background"):
                has_any_override = True

            scenes.append({
                "scene_index": idx,
                "raw_text": scene_text.strip(),
                "clean_text": clean.strip(),
                "overrides": overrides,
            })
            if clean.strip():
                all_clean_texts.append(clean.strip())

        clean_text_full = "\n\n".join(all_clean_texts)

        logger.info(
            f"📝 [ScriptFormatter] {len(scenes)} cenas parseadas, "
            f"has_overrides={has_any_override}, "
            f"clean_text_len={len(clean_text_full)}"
        )

        return {
            "scenes": scenes,
            "clean_text": clean_text_full,
            "scene_count": len(scenes),
            "has_overrides": has_any_override,
        }

    def _split_into_scenes(self, text: str) -> List[str]:
        """
        Divide o texto em cenas usando [CENA N] como delimitador.
        
        Se não houver tags [CENA], trata cada parágrafo (separado por linha em branco)
        como uma cena. Se houver só 1 parágrafo e nenhuma tag, retorna como 1 cena.
        """
        # Verificar se tem tags [CENA]
        scene_matches = list(SCENE_PATTERN.finditer(text))

        if scene_matches:
            # Dividir pelo padrão [CENA N]
            parts = SCENE_PATTERN.split(text)
            # parts alternates: [text_before, scene_num, text, scene_num, text, ...]
            scenes = []
            # O primeiro item é texto antes da primeira [CENA] (pode ser vazio)
            if parts[0].strip():
                scenes.append(parts[0])
            # Depois vem pares (scene_num, text)
            for i in range(1, len(parts), 2):
                if i + 1 < len(parts) and parts[i + 1].strip():
                    scenes.append(parts[i + 1])
            return scenes if scenes else [text]

        # Sem tags [CENA]: dividir por parágrafos (linhas em branco duplas)
        paragraphs = re.split(r'\n\s*\n', text)
        paragraphs = [p.strip() for p in paragraphs if p.strip()]

        return paragraphs if paragraphs else [text]

    def _extract_overrides(self, scene_text: str) -> Dict[str, Any]:
        """Extrai overrides de cartela e background de uma cena."""
        overrides = {}

        # Cartela
        cartela_match = CARTELA_PATTERN.search(scene_text)
        if cartela_match:
            overrides["cartela"] = self._parse_cartela(cartela_match.group(1))

        # Background
        bg_match = BG_PATTERN.search(scene_text)
        if bg_match:
            overrides["background"] = self._parse_background(bg_match.group(1))

        return overrides

    def _parse_cartela(self, raw_value: str) -> Dict[str, Any]:
        """
        Parseia o valor da tag [CARTELA: ...].
        
        Exemplos:
          "amarela"                    → {"enabled": True, "color": "#FFD700"}
          "amarela, tipo: destaque"    → {"enabled": True, "color": "#FFD700", "type": "destaque"}
          "#FF0000, opacidade: 0.8"    → {"enabled": True, "color": "#FF0000", "opacity": 0.8}
          "gradiente-roxo"             → {"enabled": True, "color": "gradiente-roxo"}
        """
        result = {"enabled": True}

        # Separar por vírgula
        parts = [p.strip() for p in raw_value.split(",")]

        # Primeiro item é sempre a cor
        if parts:
            color_raw = parts[0].strip().lower()
            result["color"] = COLOR_MAP.get(color_raw, parts[0].strip())

        # Restante são key:value
        for part in parts[1:]:
            if ":" in part:
                key, val = part.split(":", 1)
                key = key.strip().lower()
                val = val.strip()

                if key == "tipo":
                    result["type"] = val
                elif key == "opacidade":
                    try:
                        result["opacity"] = float(val)
                    except ValueError:
                        result["opacity"] = 1.0
                elif key == "estilo":
                    result["style"] = val

        return result

    def _parse_background(self, raw_value: str) -> Dict[str, Any]:
        """
        Parseia o valor da tag [BG: ...].
        
        Exemplos:
          "gradiente-azul"        → {"type": "gradiente-azul"}
          "imagem-tech-abstrata"  → {"type": "imagem-tech-abstrata"}
          "#1a1a2e"               → {"type": "solid", "color": "#1a1a2e"}
        """
        val = raw_value.strip()

        # Se começa com #, é cor sólida
        if val.startswith("#"):
            return {"type": "solid", "color": val}

        # Se é uma cor nomeada simples
        color_lower = val.lower()
        if color_lower in COLOR_MAP:
            return {"type": "solid", "color": COLOR_MAP[color_lower]}

        # Caso geral: tipo descritivo
        return {"type": val}

    def _strip_tags(self, text: str) -> str:
        """Remove todas as tags visuais do texto, deixando só conteúdo."""
        clean = ALL_TAGS_PATTERN.sub("", text)
        # Limpar espaços e linhas extras resultantes da remoção
        clean = re.sub(r'\n\s*\n\s*\n', '\n\n', clean)
        clean = re.sub(r'^\s+', '', clean, flags=re.MULTILINE)
        return clean.strip()
