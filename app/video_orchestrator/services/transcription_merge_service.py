"""
TranscriptionMergeService - Concatena e ajusta transcrições

Objetivo: Reutilizar transcrições já feitas no upload, evitando:
1. Pagar AssemblyAI duas vezes
2. Esperar transcrição durante o pipeline

Fluxo:
1. Upload → Transcrição individual de cada vídeo
2. Pipeline → Concatena vídeos + Concatena transcrições (este serviço)
3. Pipeline → Detecta silêncios
4. Pipeline → Aplica cortes na transcrição (este serviço)
5. Pipeline → Corta silêncios do vídeo
6. ✅ Transcrição sincronizada sem re-transcrever!
"""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TranscriptionWord:
    """Palavra com timestamps."""
    text: str
    start: float  # segundos
    end: float    # segundos
    confidence: float = 1.0
    speaker: Optional[str] = None


@dataclass
class SilencePeriod:
    """Período de silêncio detectado."""
    start: float  # segundos
    end: float    # segundos
    
    @property
    def duration(self) -> float:
        return self.end - self.start


@dataclass
class ProcessedPhrase:
    """Frase processada com timestamps."""
    id: str
    text: str
    start: float
    end: float
    words: List[Dict[str, Any]]
    style_type: str = 'default'


class TranscriptionMergeService:
    """
    Serviço para concatenar e ajustar transcrições.
    
    Evita re-transcrição após concatenação/corte de silêncios, 
    reutilizando as transcrições já feitas no upload.
    """
    
    def __init__(self):
        self.default_phrase_options = {
            'min_words': 2,
            'max_words': 6,
            'max_pause': 0.8  # segundos
        }
    
    def merge_transcriptions(
        self,
        transcriptions: List[Dict[str, Any]],
        video_durations: List[float]
    ) -> Dict[str, Any]:
        """
        Concatena múltiplas transcrições em uma única.
        
        Args:
            transcriptions: Lista de transcrições (uma por vídeo)
                Cada uma deve ter: { words: [...], duration_s: float }
            video_durations: Duração de cada vídeo em segundos
            
        Returns:
            Transcrição unificada com timestamps ajustados:
            {
                words: [...],
                duration_s: float,
                transcript: str,
                word_count: int,
                provider: 'merged'
            }
        """
        if not transcriptions:
            logger.warning("[TranscriptionMerge] Nenhuma transcrição para mesclar")
            return {
                'words': [],
                'duration_s': 0,
                'transcript': '',
                'word_count': 0,
                'provider': 'merged'
            }
        
        if len(transcriptions) == 1:
            # Apenas uma transcrição, retornar diretamente
            t = transcriptions[0]
            return {
                'words': t.get('words', []),
                'duration_s': t.get('duration_s', 0),
                'transcript': t.get('transcript', ''),
                'word_count': len(t.get('words', [])),
                'provider': 'merged_single'
            }
        
        all_words = []
        full_transcript = ''
        current_offset = 0.0
        
        logger.info(f"[TranscriptionMerge] Mesclando {len(transcriptions)} transcrições")
        
        for i, transcription in enumerate(transcriptions):
            words = transcription.get('words', [])
            transcript = transcription.get('transcript', '')
            
            # Obter duração do vídeo (usar a fornecida ou calcular da transcrição)
            if i < len(video_durations):
                video_duration = video_durations[i]
            elif words:
                # Fallback: usar o end da última palavra
                video_duration = words[-1].get('end', 0) if words else 0
            else:
                video_duration = transcription.get('duration_s', 0)
            
            logger.info(f"  Vídeo {i+1}: {len(words)} palavras, offset={current_offset:.2f}s, duration={video_duration:.2f}s")
            
            # Ajustar timestamps de cada palavra
            for word in words:
                adjusted_word = {
                    'text': word.get('text', ''),
                    'start': word.get('start', 0) + current_offset,
                    'end': word.get('end', 0) + current_offset,
                    'confidence': word.get('confidence', 1.0),
                }
                if 'speaker' in word:
                    adjusted_word['speaker'] = word['speaker']
                    
                all_words.append(adjusted_word)
            
            # Concatenar transcritos
            if transcript:
                if full_transcript:
                    full_transcript += ' '
                full_transcript += transcript
            
            # Avançar offset para próximo vídeo
            current_offset += video_duration
        
        result = {
            'words': all_words,
            'duration_s': current_offset,
            'transcript': full_transcript,
            'word_count': len(all_words),
            'provider': 'merged'
        }
        
        logger.info(f"[TranscriptionMerge] ✅ Resultado: {len(all_words)} palavras, {current_offset:.2f}s total")
        
        return result
    
    def apply_silence_cuts(
        self,
        transcription: Dict[str, Any],
        silence_periods: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Aplica cortes de silêncio na transcrição.
        
        Args:
            transcription: Transcrição concatenada
            silence_periods: Lista de períodos de silêncio
                [{ start: float, end: float, duration: float }]
            
        Returns:
            Transcrição com timestamps ajustados pós-corte
        """
        if not silence_periods:
            logger.info("[TranscriptionMerge] Nenhum silêncio para aplicar")
            return transcription
        
        words = transcription.get('words', [])
        if not words:
            return transcription
        
        # Ordenar silêncios por tempo de início
        silences = sorted(silence_periods, key=lambda s: s.get('start', 0))
        
        logger.info(f"[TranscriptionMerge] Aplicando {len(silences)} cortes de silêncio em {len(words)} palavras")
        
        adjusted_words = []
        total_cut_duration = 0.0
        
        for word in words:
            word_start = word.get('start', 0)
            word_end = word.get('end', 0)
            word_mid = (word_start + word_end) / 2
            
            # Verificar se a palavra está dentro de um silêncio
            is_in_silence = False
            for silence in silences:
                s_start = silence.get('start', 0)
                s_end = silence.get('end', 0)
                
                # Palavra está dentro do silêncio se seu ponto médio está no intervalo
                if s_start <= word_mid <= s_end:
                    is_in_silence = True
                    break
            
            if is_in_silence:
                # Palavra está em um silêncio - remover
                continue
            
            # Calcular offset acumulado de cortes anteriores
            offset = 0.0
            for silence in silences:
                s_start = silence.get('start', 0)
                s_end = silence.get('end', 0)
                s_duration = silence.get('duration', s_end - s_start)
                
                # Se o silêncio termina antes da palavra, adicionar ao offset
                if s_end <= word_start:
                    offset += s_duration
                # Se o silêncio começa durante/depois da palavra, parar
                elif s_start >= word_end:
                    break
            
            # Ajustar timestamps
            adjusted_word = {
                **word,
                'start': max(0, word_start - offset),
                'end': max(0, word_end - offset),
            }
            adjusted_words.append(adjusted_word)
        
        # Calcular nova duração
        if adjusted_words:
            new_duration = adjusted_words[-1]['end']
        else:
            new_duration = 0
        
        # Reconstruir transcript
        new_transcript = ' '.join(w.get('text', '') for w in adjusted_words)
        
        result = {
            'words': adjusted_words,
            'duration_s': new_duration,
            'transcript': new_transcript,
            'word_count': len(adjusted_words),
            'provider': transcription.get('provider', 'merged') + '_silence_cut'
        }
        
        removed_count = len(words) - len(adjusted_words)
        total_silence_duration = sum(s.get('duration', s.get('end', 0) - s.get('start', 0)) for s in silences)
        
        logger.info(f"[TranscriptionMerge] ✅ Silêncios aplicados:")
        logger.info(f"   • Palavras: {len(words)} → {len(adjusted_words)} (-{removed_count})")
        logger.info(f"   • Duração: {transcription.get('duration_s', 0):.2f}s → {new_duration:.2f}s (-{total_silence_duration:.2f}s)")
        
        return result
    
    def group_into_phrases(
        self,
        transcription: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Agrupa palavras em frases baseado em pausas e pontuação.
        
        Args:
            transcription: Transcrição com palavras
            options: Configurações de agrupamento
                - min_words: Mínimo de palavras por frase (default: 2)
                - max_words: Máximo de palavras por frase (default: 6)
                - max_pause: Pausa máxima em segundos antes de quebrar (default: 0.8)
            
        Returns:
            Lista de frases com timestamps
        """
        opts = {**self.default_phrase_options, **(options or {})}
        min_words = opts['min_words']
        max_words = opts['max_words']
        max_pause = opts['max_pause']
        
        words = transcription.get('words', [])
        if not words:
            return []
        
        phrases = []
        current_words = []
        phrase_index = 0
        
        for i, word in enumerate(words):
            current_words.append(word)
            
            # Determinar se deve quebrar a frase
            is_last_word = i == len(words) - 1
            has_enough_words = len(current_words) >= min_words
            reached_max_words = len(current_words) >= max_words
            
            # Verificar pontuação
            text = word.get('text', '')
            has_punctuation = any(text.endswith(p) for p in ['.', '!', '?', ';', ':'])
            
            # Verificar pausa longa
            has_long_pause = False
            if not is_last_word:
                next_word = words[i + 1]
                pause = next_word.get('start', 0) - word.get('end', 0)
                has_long_pause = pause > max_pause
            
            should_break = (
                is_last_word or
                reached_max_words or
                (has_enough_words and (has_punctuation or has_long_pause))
            )
            
            if should_break and current_words:
                phrase_text = ' '.join(w.get('text', '') for w in current_words)
                phrase_start = current_words[0].get('start', 0)
                phrase_end = current_words[-1].get('end', 0)
                
                phrases.append({
                    'id': f'phrase_{phrase_index}',
                    'text': phrase_text,
                    'start': phrase_start,
                    'end': phrase_end,
                    'words': [
                        {
                            'text': w.get('text', ''),
                            'start': w.get('start', 0),
                            'end': w.get('end', 0)
                        }
                        for w in current_words
                    ],
                    'style_type': 'default'
                })
                
                phrase_index += 1
                current_words = []
        
        logger.info(f"[TranscriptionMerge] ✅ Agrupamento: {len(words)} palavras → {len(phrases)} frases")
        
        return phrases


    def detect_silences_from_transcription(
        self,
        transcription: Dict[str, Any],
        min_silence_duration: float = 0.5,
        edge_padding: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        Detecta silêncios baseado nos gaps entre palavras da transcrição.
        
        Complementa a detecção FFmpeg para silêncios internos.
        
        Args:
            transcription: Transcrição com palavras
            min_silence_duration: Duração mínima para considerar silêncio (default: 0.5s)
            edge_padding: Padding para não cortar muito perto das palavras (default: 0.1s)
            
        Returns:
            Lista de períodos de silêncio:
            [{ start: float, end: float, duration: float, source: 'transcription' }]
        """
        words = transcription.get('words', [])
        if len(words) < 2:
            return []
        
        silences = []
        
        # Silêncio no início (antes da primeira palavra)
        first_word_start = words[0].get('start', 0)
        if first_word_start > min_silence_duration:
            silences.append({
                'start': 0,
                'end': first_word_start - edge_padding,
                'duration': first_word_start - edge_padding,
                'source': 'transcription_edge_start'
            })
        
        # Silêncios internos (gaps entre palavras)
        for i in range(len(words) - 1):
            current_word = words[i]
            next_word = words[i + 1]
            
            current_end = current_word.get('end', 0)
            next_start = next_word.get('start', 0)
            gap = next_start - current_end
            
            if gap >= min_silence_duration:
                silences.append({
                    'start': current_end + edge_padding,
                    'end': next_start - edge_padding,
                    'duration': gap - (2 * edge_padding),
                    'source': 'transcription_gap'
                })
        
        # Silêncio no final (depois da última palavra)
        duration_s = transcription.get('duration_s', 0)
        last_word_end = words[-1].get('end', 0)
        trailing_silence = duration_s - last_word_end
        
        if trailing_silence > min_silence_duration:
            silences.append({
                'start': last_word_end + edge_padding,
                'end': duration_s,
                'duration': trailing_silence - edge_padding,
                'source': 'transcription_edge_end'
            })
        
        logger.info(f"[TranscriptionMerge] 🔇 Silêncios detectados via transcrição: {len(silences)}")
        for s in silences:
            logger.info(f"   • {s['start']:.2f}s - {s['end']:.2f}s ({s['duration']:.2f}s) [{s['source']}]")
        
        return silences
    
    def merge_silence_detections(
        self,
        transcription_silences: List[Dict[str, Any]],
        ffmpeg_silences: List[Dict[str, Any]],
        overlap_threshold: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Combina detecções de silêncio de transcrição e FFmpeg.
        
        Estratégia:
        - Silêncios de borda (início/fim): Preferir FFmpeg (mais preciso em dB)
        - Silêncios internos (gaps): Preferir transcrição (mais confiável)
        - Silêncios sobrepostos: Mesclar (usar interseção)
        
        Args:
            transcription_silences: Silêncios detectados via transcrição
            ffmpeg_silences: Silêncios detectados via análise de áudio
            overlap_threshold: Sobreposição mínima para considerar mesmo silêncio
            
        Returns:
            Lista unificada de silêncios
        """
        if not transcription_silences and not ffmpeg_silences:
            return []
        
        if not ffmpeg_silences:
            return transcription_silences
        
        if not transcription_silences:
            return ffmpeg_silences
        
        # Estratégia: usar transcrição para gaps internos, FFmpeg para bordas
        merged = []
        
        # Pegar silêncios de borda do FFmpeg
        for s in ffmpeg_silences:
            # Silêncio começa em 0 ou é muito próximo do fim → borda
            if s.get('start', 0) < 0.5:
                merged.append({**s, 'source': 'ffmpeg_edge_start'})
        
        # Pegar silêncios internos da transcrição
        for s in transcription_silences:
            if s.get('source') == 'transcription_gap':
                merged.append(s)
        
        # Pegar silêncio final do FFmpeg se existir
        # (último silêncio que termina próximo da duração total)
        for s in ffmpeg_silences:
            # Verificar se é silêncio de borda final
            # Assumir que é borda se end está nos últimos 10% do vídeo
            # Isso é uma heurística, pode precisar de ajuste
            pass
        
        # Ordenar por tempo de início
        merged.sort(key=lambda s: s.get('start', 0))
        
        # Remover sobreposições
        final = []
        for silence in merged:
            if not final:
                final.append(silence)
                continue
            
            last = final[-1]
            # Se sobrepõe com o último, mesclar
            if silence['start'] <= last['end']:
                last['end'] = max(last['end'], silence['end'])
                last['duration'] = last['end'] - last['start']
                last['source'] = 'merged'
            else:
                final.append(silence)
        
        logger.info(f"[TranscriptionMerge] 🔇 Silêncios mesclados: {len(final)}")
        
        return final
    
    def map_audio_to_original_timestamps(
        self,
        transcription: Dict[str, Any],
        speech_segments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        🆕 v2.9.0: Mapeia timestamps do áudio concatenado para timestamps originais.
        
        Quando usamos o Hybrid Silence Cut, o Whisper/AssemblyAI transcreve o áudio
        concatenado (todos os speech segments unidos). Os timestamps retornados são
        relativos a esse áudio concatenado.
        
        Esta função mapeia esses timestamps de volta para os timestamps ORIGINAIS
        do vídeo, usando o audio_offset de cada speech segment.
        
        Args:
            transcription: Transcrição com timestamps relativos ao áudio concatenado
                {words: [{text, start, end}], duration_s, transcript}
            speech_segments: Lista de segmentos do hybrid cut
                [{url, original_start, original_end, audio_offset, duration}]
                
        Returns:
            Transcrição com timestamps mapeados para o vídeo original
        """
        words = transcription.get('words', [])
        if not words or not speech_segments:
            logger.warning("[TranscriptionMerge] Sem palavras ou segmentos para mapear")
            return transcription
        
        # Ordenar segmentos por audio_offset
        sorted_segments = sorted(speech_segments, key=lambda s: s.get('audio_offset', 0))
        
        logger.info(f"[TranscriptionMerge] 🗺️ Mapeando {len(words)} palavras para {len(sorted_segments)} segmentos")
        
        mapped_words = []
        
        for word in words:
            word_start = word.get('start', 0)
            word_end = word.get('end', 0)
            word_mid = (word_start + word_end) / 2
            
            # Encontrar em qual segmento esta palavra está (baseado no audio_offset)
            target_segment = None
            for seg in sorted_segments:
                seg_audio_start = seg.get('audio_offset', 0)
                seg_audio_end = seg_audio_start + seg.get('duration', 0)
                
                if seg_audio_start <= word_mid < seg_audio_end:
                    target_segment = seg
                    break
            
            if target_segment is None:
                # Palavra fora de qualquer segmento - tentar usar o último segmento
                if sorted_segments:
                    target_segment = sorted_segments[-1]
                else:
                    # Manter timestamps originais se não encontrar segmento
                    mapped_words.append(word.copy())
                    continue
            
            # Calcular offset para mapear para timestamps originais
            audio_offset = target_segment.get('audio_offset', 0)
            original_start = target_segment.get('original_start', 0)
            
            # A diferença entre audio_offset e original_start nos dá o ajuste
            # Se audio_offset=0 e original_start=1.5, precisamos adicionar 1.5
            # Se audio_offset=5 e original_start=10, uma palavra em audio:6 vai para original:11
            offset_delta = original_start - audio_offset
            
            mapped_word = {
                **word,
                'start': word_start + offset_delta,
                'end': word_end + offset_delta,
                '_audio_start': word_start,  # Manter original para debug
                '_audio_end': word_end,
                '_segment_index': sorted_segments.index(target_segment)
            }
            mapped_words.append(mapped_word)
        
        # Calcular nova duração (baseada no último segmento)
        if sorted_segments:
            last_seg = sorted_segments[-1]
            new_duration = last_seg.get('original_end', 0)
        else:
            new_duration = mapped_words[-1]['end'] if mapped_words else 0
        
        result = {
            **transcription,
            'words': mapped_words,
            'duration_s': new_duration,
            'word_count': len(mapped_words),
            'provider': transcription.get('provider', 'unknown') + '_mapped_to_original'
        }
        
        # Log de verificação
        if mapped_words:
            first_word = mapped_words[0]
            last_word = mapped_words[-1]
            logger.info(f"[TranscriptionMerge] ✅ Mapeamento concluído:")
            logger.info(f"   📍 Primeira palavra: '{first_word.get('text', '')}' @ {first_word.get('start', 0):.2f}s (audio: {first_word.get('_audio_start', 0):.2f}s)")
            logger.info(f"   📍 Última palavra: '{last_word.get('text', '')}' @ {last_word.get('end', 0):.2f}s (audio: {last_word.get('_audio_end', 0):.2f}s)")
            logger.info(f"   ⏱️ Duração total: {new_duration:.2f}s")
        
        return result


# Singleton para reuso
_transcription_merge_service = None

def get_transcription_merge_service() -> TranscriptionMergeService:
    """Retorna instância singleton do serviço."""
    global _transcription_merge_service
    if _transcription_merge_service is None:
        _transcription_merge_service = TranscriptionMergeService()
    return _transcription_merge_service

