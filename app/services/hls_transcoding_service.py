"""
HLS Transcoding Service — Transcodifica vídeos para HLS multi-bitrate

Pega um vídeo finalizado (MP4), transcodifica com ffmpeg para:
- 360p  (800kbps)  — mobile 3G/4G
- 720p  (2.5Mbps) — default
- 1080p (5Mbps)   — desktop/wifi

Gera:
- master.m3u8 (playlist master)
- 360p/stream.m3u8 + segments
- 720p/stream.m3u8 + segments
- 1080p/stream.m3u8 + segments

Upload para Cloudflare R2 via boto3 (S3-compatible API).

v1.0.0 (12/Fev/2026)

NOTA: Requer variáveis de ambiente:
  - R2_ACCOUNT_ID
  - R2_ACCESS_KEY_ID
  - R2_SECRET_ACCESS_KEY
  - R2_BUCKET_NAME (default: "vinicius-ai-hls")
  - R2_PUBLIC_URL (ex: "https://hls.vinicius.ai")
"""

import os
import json
import shutil
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════

# Profiles de transcoding (resolução → config)
HLS_PROFILES = {
    '360p': {
        'width': 640,
        'height': 360,
        'video_bitrate': '800k',
        'audio_bitrate': '96k',
        'maxrate': '856k',
        'bufsize': '1200k',
    },
    '720p': {
        'width': 1280,
        'height': 720,
        'video_bitrate': '2500k',
        'audio_bitrate': '128k',
        'maxrate': '2675k',
        'bufsize': '3750k',
    },
    '1080p': {
        'width': 1920,
        'height': 1080,
        'video_bitrate': '5000k',
        'audio_bitrate': '192k',
        'maxrate': '5350k',
        'bufsize': '7500k',
    },
}

# Segment duration in seconds
HLS_SEGMENT_DURATION = 4


# ═══════════════════════════════════════════
# R2 CLIENT
# ═══════════════════════════════════════════

def _get_r2_client():
    """Cria boto3 client para Cloudflare R2 (S3-compatible)"""
    account_id = os.getenv('R2_ACCOUNT_ID')
    access_key = os.getenv('R2_ACCESS_KEY_ID')
    secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
    
    if not all([account_id, access_key, secret_key]):
        raise ValueError(
            "R2 não configurado. Defina R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY"
        )
    
    return boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version='s3v4',
            retries={'max_attempts': 3},
        ),
        region_name='auto',
    )


def _get_r2_bucket() -> str:
    return os.getenv('R2_BUCKET_NAME', 'vinicius-ai-hls')


def _get_r2_public_url() -> str:
    return os.getenv('R2_PUBLIC_URL', 'https://hls.vinicius.ai')


# ═══════════════════════════════════════════
# TRANSCODING
# ═══════════════════════════════════════════

def _get_video_info(input_path: str) -> Dict[str, Any]:
    """Extrai informações do vídeo via ffprobe"""
    try:
        result = subprocess.run(
            [
                'ffprobe', '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                '-show_format',
                input_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode != 0:
            logger.warning(f"[HLS] ffprobe falhou: {result.stderr[:200]}")
            return {}
        
        info = json.loads(result.stdout)
        
        video_stream = next(
            (s for s in info.get('streams', []) if s.get('codec_type') == 'video'),
            {}
        )
        
        return {
            'width': int(video_stream.get('width', 0)),
            'height': int(video_stream.get('height', 0)),
            'duration': float(info.get('format', {}).get('duration', 0)),
            'has_audio': any(
                s.get('codec_type') == 'audio'
                for s in info.get('streams', [])
            ),
        }
    except Exception as e:
        logger.error(f"[HLS] Erro ffprobe: {e}")
        return {}


def _select_profiles(source_height: int) -> list:
    """Seleciona profiles baseado na resolução fonte (não upscale)"""
    selected = []
    for name, profile in HLS_PROFILES.items():
        if profile['height'] <= source_height:
            selected.append(name)
    
    # Sempre incluir pelo menos 360p
    if not selected:
        selected = ['360p']
    
    return selected


def transcode_to_hls(
    input_path: str,
    output_dir: str,
    profiles: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Transcodifica vídeo para HLS multi-bitrate
    
    Args:
        input_path: Caminho para o MP4 de entrada
        output_dir: Diretório de saída (será criado)
        profiles: Lista de profiles (ex: ['360p', '720p']). 
                  None = auto-select baseado na resolução
    
    Returns:
        {
            'master_playlist': 'path/to/master.m3u8',
            'profiles': ['360p', '720p', '1080p'],
            'segments_count': 42,
            'total_size_bytes': 15000000,
        }
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Obter info do vídeo
    video_info = _get_video_info(input_path)
    source_height = video_info.get('height', 1080)
    has_audio = video_info.get('has_audio', True)
    
    logger.info(f"[HLS] Fonte: {video_info.get('width')}x{source_height}, audio={has_audio}")
    
    # 2. Selecionar profiles
    if profiles is None:
        profiles = _select_profiles(source_height)
    
    logger.info(f"[HLS] Profiles selecionados: {profiles}")
    
    # 3. Transcodificar cada profile
    master_lines = ['#EXTM3U', '#EXT-X-VERSION:3', '']
    total_segments = 0
    
    for profile_name in profiles:
        profile = HLS_PROFILES[profile_name]
        profile_dir = os.path.join(output_dir, profile_name)
        os.makedirs(profile_dir, exist_ok=True)
        
        output_playlist = os.path.join(profile_dir, 'stream.m3u8')
        segment_pattern = os.path.join(profile_dir, 'seg_%03d.ts')
        
        # Construir comando ffmpeg
        cmd = [
            'ffmpeg', '-y',
            '-i', input_path,
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-profile:v', 'main',
            '-level', '4.0',
            '-vf', f"scale={profile['width']}:{profile['height']}:force_original_aspect_ratio=decrease,pad={profile['width']}:{profile['height']}:(ow-iw)/2:(oh-ih)/2",
            '-b:v', profile['video_bitrate'],
            '-maxrate', profile['maxrate'],
            '-bufsize', profile['bufsize'],
        ]
        
        if has_audio:
            cmd.extend([
                '-c:a', 'aac',
                '-b:a', profile['audio_bitrate'],
                '-ar', '44100',
            ])
        else:
            cmd.extend(['-an'])
        
        cmd.extend([
            '-hls_time', str(HLS_SEGMENT_DURATION),
            '-hls_playlist_type', 'vod',
            '-hls_segment_filename', segment_pattern,
            '-hls_flags', 'independent_segments',
            output_playlist,
        ])
        
        logger.info(f"[HLS] Transcodificando {profile_name}...")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 min timeout
        )
        
        if result.returncode != 0:
            logger.error(f"[HLS] Erro no ffmpeg ({profile_name}): {result.stderr[:500]}")
            raise RuntimeError(f"ffmpeg falhou para {profile_name}: {result.stderr[:200]}")
        
        # Contar segmentos
        segments = list(Path(profile_dir).glob('seg_*.ts'))
        total_segments += len(segments)
        
        # Bandwidth para master playlist
        bandwidth = int(profile['video_bitrate'].replace('k', '')) * 1000
        if has_audio:
            bandwidth += int(profile['audio_bitrate'].replace('k', '')) * 1000
        
        master_lines.append(
            f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},"
            f"RESOLUTION={profile['width']}x{profile['height']}"
        )
        master_lines.append(f"{profile_name}/stream.m3u8")
        master_lines.append('')
    
    # 4. Escrever master playlist
    master_path = os.path.join(output_dir, 'master.m3u8')
    with open(master_path, 'w') as f:
        f.write('\n'.join(master_lines))
    
    # 5. Calcular tamanho total
    total_size = sum(
        f.stat().st_size
        for f in Path(output_dir).rglob('*')
        if f.is_file()
    )
    
    logger.info(
        f"[HLS] Transcoding completo: {len(profiles)} profiles, "
        f"{total_segments} segments, {total_size / (1024*1024):.1f}MB"
    )
    
    return {
        'master_playlist': master_path,
        'profiles': profiles,
        'segments_count': total_segments,
        'total_size_bytes': total_size,
    }


# ═══════════════════════════════════════════
# UPLOAD PARA R2
# ═══════════════════════════════════════════

def _get_content_type(filename: str) -> str:
    """Retorna content-type baseado na extensão"""
    ext = filename.rsplit('.', 1)[-1].lower()
    return {
        'm3u8': 'application/vnd.apple.mpegurl',
        'ts': 'video/MP2T',
        'mp4': 'video/mp4',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'png': 'image/png',
    }.get(ext, 'application/octet-stream')


def upload_hls_to_r2(
    hls_dir: str,
    r2_prefix: str,
) -> Dict[str, Any]:
    """
    Faz upload de todos os arquivos HLS para Cloudflare R2
    
    Args:
        hls_dir: Diretório local com os arquivos HLS
        r2_prefix: Prefixo no bucket (ex: "projects/{project_id}/videos/{video_id}")
    
    Returns:
        {
            'master_url': 'https://hls.vinicius.ai/projects/.../master.m3u8',
            'files_uploaded': 45,
            'total_bytes': 15000000,
        }
    """
    client = _get_r2_client()
    bucket = _get_r2_bucket()
    public_url = _get_r2_public_url()
    
    files_uploaded = 0
    total_bytes = 0
    
    for file_path in Path(hls_dir).rglob('*'):
        if not file_path.is_file():
            continue
        
        # Caminho relativo dentro do diretório HLS
        relative_path = file_path.relative_to(hls_dir)
        r2_key = f"{r2_prefix}/{relative_path}"
        
        content_type = _get_content_type(file_path.name)
        file_size = file_path.stat().st_size
        
        logger.debug(f"[R2] Upload: {r2_key} ({content_type}, {file_size}B)")
        
        client.upload_file(
            str(file_path),
            bucket,
            r2_key,
            ExtraArgs={
                'ContentType': content_type,
                'CacheControl': 'public, max-age=31536000',  # 1 ano (imutável)
            },
        )
        
        files_uploaded += 1
        total_bytes += file_size
    
    master_url = f"{public_url}/{r2_prefix}/master.m3u8"
    
    logger.info(
        f"[R2] Upload completo: {files_uploaded} arquivos, "
        f"{total_bytes / (1024*1024):.1f}MB → {master_url}"
    )
    
    return {
        'master_url': master_url,
        'files_uploaded': files_uploaded,
        'total_bytes': total_bytes,
    }


# ═══════════════════════════════════════════
# PIPELINE COMPLETO
# ═══════════════════════════════════════════

def process_video_for_hls(
    input_url: str,
    project_id: str,
    video_id: str,
    profiles: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Pipeline completo: Download → Transcode → Upload R2
    
    Args:
        input_url: URL do vídeo MP4 (B2 signed URL)
        project_id: ID do projeto
        video_id: ID único do vídeo
        profiles: Profiles HLS (None = auto)
    
    Returns:
        {
            'hls_url': 'https://hls.vinicius.ai/projects/.../master.m3u8',
            'profiles': ['360p', '720p', '1080p'],
            'segments_count': 42,
            'total_size_mb': 15.3,
        }
    """
    work_dir = tempfile.mkdtemp(prefix='hls_')
    
    try:
        # 1. Download do vídeo
        input_path = os.path.join(work_dir, 'input.mp4')
        logger.info(f"[HLS] Baixando vídeo para transcoding: {video_id}")
        
        import requests
        response = requests.get(input_url, stream=True, timeout=120)
        response.raise_for_status()
        
        with open(input_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        input_size = os.path.getsize(input_path)
        logger.info(f"[HLS] Download completo: {input_size / (1024*1024):.1f}MB")
        
        # 2. Transcodificar
        hls_dir = os.path.join(work_dir, 'hls')
        transcode_result = transcode_to_hls(input_path, hls_dir, profiles)
        
        # 3. Upload para R2
        r2_prefix = f"projects/{project_id}/videos/{video_id}"
        upload_result = upload_hls_to_r2(hls_dir, r2_prefix)
        
        return {
            'hls_url': upload_result['master_url'],
            'mp4_url': input_url,  # Manter MP4 original como fallback
            'profiles': transcode_result['profiles'],
            'segments_count': transcode_result['segments_count'],
            'total_size_mb': round(transcode_result['total_size_bytes'] / (1024 * 1024), 1),
            'files_uploaded': upload_result['files_uploaded'],
        }
    
    finally:
        # Cleanup
        try:
            shutil.rmtree(work_dir)
        except Exception:
            pass


def is_r2_configured() -> bool:
    """Verifica se R2 está configurado"""
    return all([
        os.getenv('R2_ACCOUNT_ID'),
        os.getenv('R2_ACCESS_KEY_ID'),
        os.getenv('R2_SECRET_ACCESS_KEY'),
    ])
