"""
⏱️ Timestamp Generator Service - Geração de Timestamps para Texto

Este serviço gera timestamps artificiais para texto quando não há transcrição
(ex: text_video mode com base sólida/gradiente).

Usado por:
- Generator V3 (ao processar texto)
- Chatbot (quando storytelling_mode != vlog)
- Pipeline (para text_video sem transcrição)

Formato de saída compatível com AssemblyAI:
- start/end em SEGUNDOS (float)
"""

import re
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES DE VELOCIDADE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SpeedConfig:
    """Configuração de velocidade de leitura."""
    ms_per_char: int  # Milissegundos por caractere
    pause_between_words_ms: int  # Pausa entre palavras
    pause_between_phrases_ms: int  # Pausa entre frases


SPEED_PRESETS: Dict[str, SpeedConfig] = {
    "very_slow": SpeedConfig(ms_per_char=100, pause_between_words_ms=150, pause_between_phrases_ms=500),
    "slow": SpeedConfig(ms_per_char=80, pause_between_words_ms=100, pause_between_phrases_ms=400),
    "normal": SpeedConfig(ms_per_char=60, pause_between_words_ms=80, pause_between_phrases_ms=300),
    "fast": SpeedConfig(ms_per_char=40, pause_between_words_ms=50, pause_between_phrases_ms=200),
    "very_fast": SpeedConfig(ms_per_char=25, pause_between_words_ms=30, pause_between_phrases_ms=150),
}


# ═══════════════════════════════════════════════════════════════════════════
# SERVIÇO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

class TimestampGeneratorService:
    """
    Gera timestamps artificiais para texto.
    
    Compatível com o formato AssemblyAI:
    - start/end em segundos (float)
    - Cada palavra tem seu próprio start/end
    - Cada frase agrupa suas palavras
    """
    
    def __init__(self, speed: str = "normal"):
        """
        Inicializa o serviço.
        
        Args:
            speed: Velocidade de leitura (very_slow, slow, normal, fast, very_fast)
        """
        self.speed_config = SPEED_PRESETS.get(speed, SPEED_PRESETS["normal"])
        logger.info(f"⏱️ TimestampGenerator: speed={speed}, {self.speed_config.ms_per_char}ms/char")
    
    def generate_timestamps(
        self,
        text: str,
        max_words_per_phrase: int = 4,
        min_words_per_phrase: int = 2
    ) -> Dict[str, Any]:
        """
        Gera timestamps para um texto completo.
        
        Args:
            text: Texto a ser processado
            max_words_per_phrase: Máximo de palavras por frase
            min_words_per_phrase: Mínimo de palavras por frase
            
        Returns:
            {
                "phrases": [...],
                "total_duration_ms": int,
                "total_duration_seconds": float,
                "word_count": int,
                "phrase_count": int
            }
        """
        if not text or not text.strip():
            return {
                "phrases": [],
                "total_duration_ms": 0,
                "total_duration_seconds": 0.0,
                "word_count": 0,
                "phrase_count": 0
            }
        
        # Limpar e dividir em palavras
        words = self._tokenize(text)
        
        if not words:
            return {
                "phrases": [],
                "total_duration_ms": 0,
                "total_duration_seconds": 0.0,
                "word_count": 0,
                "phrase_count": 0
            }
        
        # Agrupar em frases
        phrases = self._group_into_phrases(words, max_words_per_phrase, min_words_per_phrase)
        
        # Gerar timestamps
        timestamped_phrases = self._apply_timestamps(phrases)
        
        # Calcular duração total
        if timestamped_phrases:
            last_phrase = timestamped_phrases[-1]
            total_duration_ms = int(last_phrase["end"] * 1000)
        else:
            total_duration_ms = 0
        
        result = {
            "phrases": timestamped_phrases,
            "total_duration_ms": total_duration_ms,
            "total_duration_seconds": total_duration_ms / 1000,
            "word_count": len(words),
            "phrase_count": len(timestamped_phrases)
        }
        
        logger.info(f"✅ Timestamps gerados: {result['phrase_count']} frases, "
                   f"{result['word_count']} palavras, {result['total_duration_seconds']:.2f}s")
        
        return result
    
    def _tokenize(self, text: str) -> List[str]:
        """
        Divide texto em palavras, preservando pontuação.
        """
        # Normalizar espaços
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Dividir em palavras
        words = text.split(' ')
        
        # Filtrar vazios
        words = [w for w in words if w.strip()]
        
        return words
    
    def _group_into_phrases(
        self,
        words: List[str],
        max_words: int,
        min_words: int
    ) -> List[List[str]]:
        """
        Agrupa palavras em frases respeitando pontuação.
        """
        phrases = []
        current_phrase = []
        
        for word in words:
            current_phrase.append(word)
            
            # Verificar se deve quebrar a frase
            should_break = False
            
            # Quebrar em pontuação forte
            if word.endswith(('.', '!', '?', ':')):
                should_break = True
            # Quebrar em vírgula se já temos palavras suficientes
            elif word.endswith(',') and len(current_phrase) >= min_words:
                should_break = True
            # Quebrar se atingiu máximo
            elif len(current_phrase) >= max_words:
                should_break = True
            
            if should_break and current_phrase:
                phrases.append(current_phrase)
                current_phrase = []
        
        # Adicionar última frase se sobrou
        if current_phrase:
            # Se a última frase é muito curta, juntar com a anterior
            if len(current_phrase) < min_words and phrases:
                phrases[-1].extend(current_phrase)
            else:
                phrases.append(current_phrase)
        
        return phrases
    
    def _apply_timestamps(self, phrases: List[List[str]]) -> List[Dict[str, Any]]:
        """
        Aplica timestamps às frases e palavras.
        
        Retorna no formato compatível com AssemblyAI (start/end em segundos).
        """
        result = []
        current_time_ms = 0
        
        for phrase_idx, phrase_words in enumerate(phrases):
            phrase_start_ms = current_time_ms
            word_timestamps = []
            
            for word in phrase_words:
                word_start_ms = current_time_ms
                
                # Calcular duração baseada no número de caracteres
                word_duration_ms = len(word) * self.speed_config.ms_per_char
                word_duration_ms = max(word_duration_ms, 100)  # Mínimo 100ms
                
                word_end_ms = word_start_ms + word_duration_ms
                
                word_timestamps.append({
                    "text": word,
                    "start": word_start_ms / 1000,  # Segundos (padrão AssemblyAI)
                    "end": word_end_ms / 1000
                })
                
                # Avançar tempo
                current_time_ms = word_end_ms + self.speed_config.pause_between_words_ms
            
            phrase_end_ms = current_time_ms - self.speed_config.pause_between_words_ms
            
            result.append({
                "id": f"phrase_{phrase_idx}",
                "text": " ".join(phrase_words),
                "start": phrase_start_ms / 1000,  # Segundos (padrão AssemblyAI)
                "end": phrase_end_ms / 1000,
                "words": word_timestamps
            })
            
            # Pausa entre frases
            current_time_ms = phrase_end_ms + self.speed_config.pause_between_phrases_ms
        
        return result
    
    def generate_for_phrases(
        self,
        phrases: List[Dict[str, Any]],
        start_time_ms: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Gera timestamps para frases já agrupadas (ex: vindo do Generator V3).
        
        Útil quando o agrupamento já foi feito mas os timestamps estão faltando.
        
        Args:
            phrases: Lista de frases já agrupadas
            start_time_ms: Tempo inicial em ms
            
        Returns:
            Mesmas frases com timestamps adicionados
        """
        current_time_ms = start_time_ms
        result = []
        
        for phrase_idx, phrase in enumerate(phrases):
            text = phrase.get("text", "")
            words_text = text.split(" ") if text else []
            
            phrase_start_ms = current_time_ms
            word_timestamps = []
            
            for word_text in words_text:
                if not word_text.strip():
                    continue
                    
                word_start_ms = current_time_ms
                word_duration_ms = len(word_text) * self.speed_config.ms_per_char
                word_duration_ms = max(word_duration_ms, 100)
                word_end_ms = word_start_ms + word_duration_ms
                
                word_timestamps.append({
                    "text": word_text,
                    "start": word_start_ms / 1000,
                    "end": word_end_ms / 1000
                })
                
                current_time_ms = word_end_ms + self.speed_config.pause_between_words_ms
            
            phrase_end_ms = current_time_ms - self.speed_config.pause_between_words_ms
            
            result.append({
                **phrase,  # Manter dados originais
                "start": phrase_start_ms / 1000,
                "end": phrase_end_ms / 1000,
                "words": word_timestamps
            })
            
            current_time_ms = phrase_end_ms + self.speed_config.pause_between_phrases_ms
        
        return result


# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO HELPER (para uso direto no pipeline)
# ═══════════════════════════════════════════════════════════════════════════

def ensure_timestamps(
    phrases: List[Dict[str, Any]],
    speed: str = "normal"
) -> List[Dict[str, Any]]:
    """
    Garante que as frases tenham timestamps.
    
    Se já tiverem (ex: vindo de transcrição), retorna como está.
    Se não tiverem, gera timestamps artificiais.
    
    Args:
        phrases: Lista de frases (pode ou não ter timestamps)
        speed: Velocidade de leitura para geração
        
    Returns:
        Frases com timestamps garantidos
    """
    if not phrases:
        return []
    
    # Verificar se já tem timestamps na frase
    # 🐛 FIX 23/Dez: v-services/fraseamento retorna "start_time" e "end_time", não "start" e "end"
    first_phrase = phrases[0]
    has_timestamps = (
        first_phrase.get("start") is not None or
        first_phrase.get("start_ms") is not None or
        first_phrase.get("start_time") is not None  # 🆕 v-services usa start_time
    )
    
    # Verificar se as palavras têm timestamps
    first_words = first_phrase.get("words", [])
    words_have_timestamps = False
    if first_words:
        first_word = first_words[0]
        # 🔧 FIX v2.9.59: Também verificar start_time (usado por alguns serviços internos)
        words_have_timestamps = (
            first_word.get("start") is not None or
            first_word.get("start_ms") is not None or
            first_word.get("start_time") is not None  # 🆕 Compatibilidade com v-services
        )
    
    if has_timestamps and words_have_timestamps:
        logger.info("✅ Frases já têm timestamps - usando existentes")
        
        # 🆕 Normalizar formato: converter start_time/end_time para start/end se necessário
        # Isso garante compatibilidade com o resto do pipeline
        normalized_phrases = []
        for phrase in phrases:
            normalized = dict(phrase)
            
            # Normalizar timestamps da frase
            if "start_time" in normalized and "start" not in normalized:
                normalized["start"] = normalized["start_time"]
            if "end_time" in normalized and "end" not in normalized:
                normalized["end"] = normalized["end_time"]
            
            # 🔧 FIX v2.9.59: Normalizar timestamps das PALAVRAS também
            # Algumas palavras podem ter start_time/end_time em vez de start/end
            if "words" in normalized:
                normalized_words = []
                for word in normalized["words"]:
                    norm_word = dict(word)
                    if "start_time" in norm_word and "start" not in norm_word:
                        # start_time já está em ms, converter para segundos (formato AssemblyAI)
                        norm_word["start"] = norm_word["start_time"] / 1000 if norm_word["start_time"] > 100 else norm_word["start_time"]
                    if "end_time" in norm_word and "end" not in norm_word:
                        norm_word["end"] = norm_word["end_time"] / 1000 if norm_word["end_time"] > 100 else norm_word["end_time"]
                    normalized_words.append(norm_word)
                normalized["words"] = normalized_words
            
            normalized_phrases.append(normalized)
        
        logger.info(f"✅ Timestamps normalizados para {len(normalized_phrases)} frases")
        return normalized_phrases
    
    logger.info("⏱️ Frases sem timestamps - gerando artificialmente...")
    logger.info(f"   • has_timestamps (frase): {has_timestamps}")
    logger.info(f"   • words_have_timestamps: {words_have_timestamps}")
    logger.info(f"   • Primeira frase keys: {list(first_phrase.keys())[:10]}")
    
    service = TimestampGeneratorService(speed=speed)
    return service.generate_for_phrases(phrases)

