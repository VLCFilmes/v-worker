"""
🎨 Color Harmony Service
Gera paletas de cores harmônicas a partir de uma cor primária.

Regras de harmonia:
- Complementar (180°)
- Análogas (±30°)
- Tríade (120°)
- Split Complementary (150°, 210°)
- Monocromática

Fonte da verdade: template-master-v3/items/color-harmony-system.json
"""

import json
import os
import re
import colorsys
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Caminho para o sistema de cores
HARMONY_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "data", "template-master-v3", "items", "color-harmony-system.json"
)


@dataclass
class ColorPalette:
    """Paleta de cores gerada."""
    primary: str
    secondary: str
    accent: str
    text: str
    text_muted: str
    background: str
    border: str
    harmony_type: str


# Cores conhecidas por nome
NAMED_COLORS = {
    # Básicas
    "vermelho": "#FF0000", "red": "#FF0000",
    "verde": "#00FF00", "green": "#00FF00",
    "azul": "#0066FF", "blue": "#0066FF",
    "amarelo": "#FFD700", "yellow": "#FFD700",
    "laranja": "#FF6600", "orange": "#FF6600",
    "roxo": "#8B00FF", "purple": "#8B00FF",
    "rosa": "#FF69B4", "pink": "#FF69B4",
    "preto": "#000000", "black": "#000000",
    "branco": "#FFFFFF", "white": "#FFFFFF",
    "cinza": "#808080", "gray": "#808080", "grey": "#808080",
    
    # Variações
    "azul marinho": "#001F3F", "navy": "#001F3F",
    "azul claro": "#87CEEB", "light blue": "#87CEEB",
    "azul royal": "#4169E1", "royal blue": "#4169E1",
    "verde escuro": "#006400", "dark green": "#006400",
    "verde limão": "#32CD32", "lime": "#32CD32",
    "vermelho escuro": "#8B0000", "dark red": "#8B0000",
    "rosa claro": "#FFB6C1", "light pink": "#FFB6C1",
    "dourado": "#FFD700", "gold": "#FFD700",
    "prata": "#C0C0C0", "silver": "#C0C0C0",
    "coral": "#FF7F50",
    "turquesa": "#40E0D0", "turquoise": "#40E0D0",
    "violeta": "#EE82EE", "violet": "#EE82EE",
    "marrom": "#8B4513", "brown": "#8B4513",
    "bege": "#F5F5DC", "beige": "#F5F5DC",
    
    # Marcas conhecidas
    "instagram": "#E1306C",
    "facebook": "#1877F2",
    "twitter": "#1DA1F2",
    "youtube": "#FF0000",
    "tiktok": "#000000",
    "linkedin": "#0A66C2",
    "whatsapp": "#25D366",
    "spotify": "#1DB954",
    "netflix": "#E50914",
}


class ColorHarmonyService:
    """Serviço de geração de paletas de cores harmônicas."""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._load_config()
    
    def _load_config(self):
        """Carrega configuração do sistema de cores."""
        try:
            with open(HARMONY_PATH, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
            logger.info(f"✅ Color harmony config loaded")
        except FileNotFoundError:
            logger.warning(f"⚠️ Color harmony config not found: {HARMONY_PATH}")
            self._config = {}
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in color harmony config: {e}")
            self._config = {}
    
    def parse_color(self, color_input: str) -> Optional[str]:
        """
        Converte input de cor para hex.
        
        Aceita:
        - Hex: "#FF0000", "FF0000"
        - Nome: "azul", "blue", "azul marinho"
        - Marca: "instagram", "spotify"
        
        Returns:
            Cor em formato hex (#RRGGBB) ou None se não reconhecer
        """
        if not color_input:
            return None
        
        color_input = color_input.strip().lower()
        
        # Já é hex?
        if color_input.startswith("#"):
            if len(color_input) == 7:
                return color_input.upper()
            elif len(color_input) == 4:
                # #RGB -> #RRGGBB
                r, g, b = color_input[1], color_input[2], color_input[3]
                return f"#{r}{r}{g}{g}{b}{b}".upper()
        
        # Hex sem #?
        if re.match(r'^[0-9a-fA-F]{6}$', color_input):
            return f"#{color_input.upper()}"
        
        # Nome conhecido?
        if color_input in NAMED_COLORS:
            return NAMED_COLORS[color_input]
        
        # Tentar match parcial
        for name, hex_color in NAMED_COLORS.items():
            if color_input in name or name in color_input:
                return hex_color
        
        return None
    
    def generate_palette(
        self,
        primary: str,
        harmony_type: str = "complementary",
        theme: str = "dark"
    ) -> ColorPalette:
        """
        Gera paleta completa a partir de cor primária.
        
        Args:
            primary: Cor primária (hex ou nome)
            harmony_type: Tipo de harmonia (complementary, analogous, triadic, monochromatic)
            theme: Tema (dark ou light)
        
        Returns:
            ColorPalette com todas as cores
        """
        # Parse primary color
        primary_hex = self.parse_color(primary)
        if not primary_hex:
            primary_hex = "#0066FF"  # Fallback azul
            logger.warning(f"⚠️ Cor não reconhecida '{primary}', usando fallback")
        
        # Converter para HSL
        h, s, l = self._hex_to_hsl(primary_hex)
        
        # Gerar cores baseado no tipo de harmonia
        if harmony_type == "complementary":
            secondary_h = (h + 30) % 360
            accent_h = (h + 180) % 360
        elif harmony_type == "analogous":
            secondary_h = (h + 30) % 360
            accent_h = (h - 30) % 360
        elif harmony_type == "triadic":
            secondary_h = (h + 120) % 360
            accent_h = (h + 240) % 360
        elif harmony_type == "split_complementary":
            secondary_h = (h + 150) % 360
            accent_h = (h + 210) % 360
        elif harmony_type == "monochromatic":
            secondary_h = h
            accent_h = h
            # Variar saturação e luminosidade
            s = min(100, s + 20)
        else:
            # Default: complementary
            secondary_h = (h + 30) % 360
            accent_h = (h + 180) % 360
        
        # Gerar hex das cores
        secondary_hex = self._hsl_to_hex(secondary_h, s, l)
        accent_hex = self._hsl_to_hex(accent_h, s, l)
        
        # Cores de texto e background baseadas no tema
        if theme == "dark":
            text = "#FFFFFF"
            text_muted = "rgba(255, 255, 255, 0.6)"
            background = "#0D0D0D"
        else:
            text = "#1A1A1A"
            text_muted = "rgba(0, 0, 0, 0.6)"
            background = "#FFFFFF"
        
        # Borda com opacidade
        border = f"{primary_hex}CC"  # 80% opacity
        
        return ColorPalette(
            primary=primary_hex,
            secondary=secondary_hex,
            accent=accent_hex,
            text=text,
            text_muted=text_muted,
            background=background,
            border=border,
            harmony_type=harmony_type
        )
    
    def get_preset_palettes(self) -> List[Dict]:
        """Retorna paletas prontas."""
        if not self._config:
            return []
        
        return self._config.get("preset_palettes", {}).get("palettes", [])
    
    def get_preset_by_id(self, preset_id: str) -> Optional[Dict]:
        """Retorna uma paleta pronta pelo ID."""
        for preset in self.get_preset_palettes():
            if preset["id"] == preset_id:
                return preset
        return None
    
    def apply_preset(self, preset_id: str, theme: str = "dark") -> Optional[ColorPalette]:
        """Aplica uma paleta pronta."""
        preset = self.get_preset_by_id(preset_id)
        if not preset:
            return None
        
        colors = preset.get("colors", {})
        
        return ColorPalette(
            primary=colors.get("primary", "#0066FF"),
            secondary=colors.get("secondary", "#3399FF"),
            accent=colors.get("accent", "#FF6600"),
            text=colors.get("text", "#FFFFFF" if theme == "dark" else "#1A1A1A"),
            text_muted=f"rgba({'255, 255, 255' if theme == 'dark' else '0, 0, 0'}, 0.6)",
            background=colors.get("background", "#0D0D0D" if theme == "dark" else "#FFFFFF"),
            border=f"{colors.get('primary', '#0066FF')}CC",
            harmony_type="preset"
        )
    
    def lighten(self, hex_color: str, percent: float = 20) -> str:
        """Clareia uma cor."""
        h, s, l = self._hex_to_hsl(hex_color)
        l = min(100, l + percent)
        return self._hsl_to_hex(h, s, l)
    
    def darken(self, hex_color: str, percent: float = 20) -> str:
        """Escurece uma cor."""
        h, s, l = self._hex_to_hsl(hex_color)
        l = max(0, l - percent)
        return self._hsl_to_hex(h, s, l)
    
    def saturate(self, hex_color: str, percent: float = 20) -> str:
        """Aumenta saturação."""
        h, s, l = self._hex_to_hsl(hex_color)
        s = min(100, s + percent)
        return self._hsl_to_hex(h, s, l)
    
    def desaturate(self, hex_color: str, percent: float = 20) -> str:
        """Diminui saturação."""
        h, s, l = self._hex_to_hsl(hex_color)
        s = max(0, s - percent)
        return self._hsl_to_hex(h, s, l)
    
    def complementary(self, hex_color: str) -> str:
        """Retorna cor complementar (180°)."""
        h, s, l = self._hex_to_hsl(hex_color)
        return self._hsl_to_hex((h + 180) % 360, s, l)
    
    def analogous(self, hex_color: str, degrees: int = 30) -> Tuple[str, str]:
        """Retorna cores análogas (±degrees)."""
        h, s, l = self._hex_to_hsl(hex_color)
        return (
            self._hsl_to_hex((h - degrees) % 360, s, l),
            self._hsl_to_hex((h + degrees) % 360, s, l)
        )
    
    def _hex_to_hsl(self, hex_color: str) -> Tuple[float, float, float]:
        """Converte hex para HSL."""
        hex_color = hex_color.lstrip('#')
        r, g, b = tuple(int(hex_color[i:i+2], 16) / 255 for i in (0, 2, 4))
        
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        
        return (h * 360, s * 100, l * 100)
    
    def _hsl_to_hex(self, h: float, s: float, l: float) -> str:
        """Converte HSL para hex."""
        h = h / 360
        s = s / 100
        l = l / 100
        
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        
        return "#{:02X}{:02X}{:02X}".format(
            int(r * 255),
            int(g * 255),
            int(b * 255)
        )


# Singleton instance
_service_instance = None

def get_color_harmony_service() -> ColorHarmonyService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = ColorHarmonyService()
    return _service_instance


# Funções de conveniência
def generate_palette(primary: str, harmony_type: str = "complementary", theme: str = "dark") -> ColorPalette:
    """Gera paleta a partir de cor primária."""
    return get_color_harmony_service().generate_palette(primary, harmony_type, theme)


def parse_color(color_input: str) -> Optional[str]:
    """Converte nome de cor para hex."""
    return get_color_harmony_service().parse_color(color_input)


def get_preset_palettes() -> List[Dict]:
    """Lista paletas prontas."""
    return get_color_harmony_service().get_preset_palettes()


# Para uso direto como script
if __name__ == "__main__":
    service = ColorHarmonyService()
    
    print("\n🎨 Color Harmony Service - Testes\n")
    print("=" * 60)
    
    # Teste de parse de cores
    test_colors = [
        "azul", "#FF5500", "instagram", "azul marinho", 
        "linkedin", "verde limão", "invalido123"
    ]
    
    print("\n📝 Parse de cores:")
    for color in test_colors:
        result = service.parse_color(color)
        print(f"   '{color}' → {result or '❌ Não reconhecido'}")
    
    # Teste de geração de paleta
    print("\n\n🎨 Geração de paleta (azul, complementary, dark):")
    palette = service.generate_palette("azul", "complementary", "dark")
    print(f"   Primary:    {palette.primary}")
    print(f"   Secondary:  {palette.secondary}")
    print(f"   Accent:     {palette.accent}")
    print(f"   Text:       {palette.text}")
    print(f"   Background: {palette.background}")
    
    # Teste de presets
    print("\n\n📦 Paletas prontas:")
    presets = service.get_preset_palettes()
    for preset in presets[:5]:
        print(f"   {preset.get('emoji', '')} {preset['name']} ({preset['id']})")
    
    print("\n" + "=" * 60)
