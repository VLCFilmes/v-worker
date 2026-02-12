"""
🗂️ B2 Paths - Gerador de Paths para Backblaze B2

Nova estrutura de pastas (implementado em 01/Dez/2025):
- users/{user_id}/projects/p_{project_id}_c_{conversation_id}/
  - assets/original/
  - assets/thumbnails/
  - mid-production/
  - exports/
  - metadata/

🆕 v2.9.180 (23/Jan/2026): Migração para estrutura organizada por job
- Feature Flag: USE_NEW_B2_PATHS (default: false)
- Nova estrutura: users/{user_id}/projects/{project_id}/jobs/{job_id}/...
- Rollback instantâneo alterando a variável de ambiente
"""

import os
import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Bucket padrão
DEFAULT_BUCKET = os.environ.get('B2_BUCKET_NAME', 'vinicius-ai-cdn-global')

# 🆕 Feature Flag para novos paths (ROLLBACK: mudar para 'false')
USE_NEW_B2_PATHS = os.environ.get('USE_NEW_B2_PATHS', 'false').lower() == 'true'

if USE_NEW_B2_PATHS:
    logger.info("🆕 [B2Paths] USE_NEW_B2_PATHS=true - Usando estrutura organizada por job")
else:
    logger.info("📁 [B2Paths] USE_NEW_B2_PATHS=false - Usando estrutura legacy")


def generate_project_path(
    user_id: str, 
    project_id: str, 
    conversation_id: str
) -> str:
    """
    Gera o path base da pasta do projeto no B2.
    
    Args:
        user_id: UUID completo do usuário
        project_id: UUID do projeto (usa primeiros 8 chars)
        conversation_id: UUID da conversa (usa primeiros 8 chars)
    
    Returns:
        Path no formato: users/{user_id}/projects/p_{project_short}_c_{conversation_short}
    
    Example:
        >>> generate_project_path("8d04a8bf-...", "7271a9d8-...", "f0a5e770-...")
        "users/8d04a8bf-.../projects/p_7271a9d8_c_f0a5e770"
    """
    # Usar UUIDs completos para o user, mas encurtar project/conversation
    project_short = project_id[:8] if project_id else "unknown"
    conversation_short = conversation_id[:8] if conversation_id else "unknown"
    
    return f"users/{user_id}/projects/p_{project_short}_c_{conversation_short}"


def generate_asset_path(
    user_id: str, 
    project_id: str, 
    conversation_id: str,
    asset_id: str,
    asset_type: str = "original",
    version: int = 1,
    stage: Optional[str] = None,
    extension: str = "mp4"
) -> str:
    """
    Gera o path completo de um asset no B2.
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        conversation_id: UUID da conversa
        asset_id: UUID do asset
        asset_type: Tipo de asset (original, thumbnail, mid-production, export, metadata)
        version: Número da versão (para original e export)
        stage: Etapa de processamento (para mid-production: cut, concat, normalized, captioned)
        extension: Extensão do arquivo
    
    Returns:
        Path completo do asset
    
    Example:
        >>> generate_asset_path(..., asset_type="mid-production", stage="cut")
        "users/.../projects/p_..._c_.../mid-production/{asset_id}_cut.mp4"
    """
    base = generate_project_path(user_id, project_id, conversation_id)
    
    if asset_type == "original":
        return f"{base}/assets/original/{asset_id}_v{version:03d}.{extension}"
    
    elif asset_type == "thumbnail":
        return f"{base}/assets/thumbnails/{asset_id}_thumb.jpg"
    
    elif asset_type == "poster":
        return f"{base}/assets/thumbnails/{asset_id}_poster.jpg"
    
    elif asset_type == "mid-production":
        if not stage:
            raise ValueError("stage é obrigatório para mid-production (cut, concat, normalized, captioned)")
        return f"{base}/mid-production/{asset_id}_{stage}.{extension}"
    
    elif asset_type == "export":
        return f"{base}/exports/{asset_id}_final_v{version:03d}.{extension}"
    
    elif asset_type == "preview":
        return f"{base}/exports/{asset_id}_preview.{extension}"
    
    elif asset_type == "metadata":
        # Para metadata, o asset_id é na verdade o nome do arquivo
        return f"{base}/metadata/{asset_id}.json"
    
    else:
        raise ValueError(f"Tipo de asset desconhecido: {asset_type}")


def generate_metadata_path(
    user_id: str,
    project_id: str,
    conversation_id: str,
    metadata_type: str,
    version: Optional[int] = None
) -> str:
    """
    Gera o path para um arquivo de metadados JSON.
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        conversation_id: UUID da conversa
        metadata_type: Tipo de metadata (project_config, render_input, render_output, history)
        version: Número da versão (opcional)
    
    Returns:
        Path do arquivo de metadata
    
    Example:
        >>> generate_metadata_path(..., "project_config", version=2)
        "users/.../projects/p_..._c_.../metadata/project_config_v002.json"
    """
    base = generate_project_path(user_id, project_id, conversation_id)
    
    if version:
        filename = f"{metadata_type}_v{version:03d}.json"
    else:
        filename = f"{metadata_type}.json"
    
    return f"{base}/metadata/{filename}"


def parse_project_path(path: str) -> dict:
    """
    Extrai informações de um path de projeto.
    
    Args:
        path: Path no B2
    
    Returns:
        Dict com user_id, project_id_short, conversation_id_short
    
    Example:
        >>> parse_project_path("users/8d04a8bf-.../projects/p_7271a9d8_c_f0a5e770/...")
        {"user_id": "8d04a8bf-...", "project_id_short": "7271a9d8", "conversation_id_short": "f0a5e770"}
    """
    parts = path.split('/')
    result = {}
    
    for i, part in enumerate(parts):
        if part == 'users' and i + 1 < len(parts):
            result['user_id'] = parts[i + 1]
        elif part == 'projects' and i + 1 < len(parts):
            project_conv = parts[i + 1]
            # Formato: p_{project_id}_c_{conversation_id}
            if project_conv.startswith('p_') and '_c_' in project_conv:
                project_part, conv_part = project_conv.split('_c_')
                result['project_id_short'] = project_part[2:]  # Remove 'p_'
                result['conversation_id_short'] = conv_part
    
    return result


def get_public_url(path: str, bucket: str = None) -> str:
    """
    Gera a URL pública de um arquivo no B2.
    
    Args:
        path: Path do arquivo no B2
        bucket: Nome do bucket (default: DEFAULT_BUCKET)
    
    Returns:
        URL pública
    """
    bucket = bucket or DEFAULT_BUCKET
    return f"https://f002.backblazeb2.com/file/{bucket}/{path}"


# ============================================================
# Helpers para uso no Video Orchestrator
# ============================================================

def get_mid_production_path(
    job_id: str,
    user_id: str,
    project_id: str,
    conversation_id: str,
    stage: str
) -> str:
    """
    Helper específico para vídeos mid-production do Video Orchestrator.
    
    Args:
        job_id: UUID do job de processamento
        user_id: UUID do usuário
        project_id: UUID do projeto
        conversation_id: UUID da conversa
        stage: Etapa (cut, concat, normalized, captioned)
    
    Returns:
        Path completo para o vídeo intermediário
    """
    return generate_asset_path(
        user_id=user_id,
        project_id=project_id,
        conversation_id=conversation_id,
        asset_id=job_id,  # Usar job_id como identificador
        asset_type="mid-production",
        stage=stage,
        extension="mp4"
    )


def get_export_path(
    video_id: str,
    user_id: str,
    project_id: str,
    conversation_id: str,
    version: int = 1,
    is_preview: bool = False
) -> str:
    """
    Helper específico para vídeos exportados (renders finais).
    
    Args:
        video_id: UUID do vídeo final
        user_id: UUID do usuário
        project_id: UUID do projeto
        conversation_id: UUID da conversa
        version: Número da versão
        is_preview: Se é preview (baixa qualidade)
    
    Returns:
        Path completo para o vídeo exportado
    """
    asset_type = "preview" if is_preview else "export"
    return generate_asset_path(
        user_id=user_id,
        project_id=project_id,
        conversation_id=conversation_id,
        asset_id=video_id,
        asset_type=asset_type,
        version=version,
        extension="mp4"
    )


# ============================================================
# 🆕 v2.9.180: Funções para Nova Estrutura de Paths
# ============================================================
# Estrutura: users/{user_id}/projects/{project_id}/jobs/{job_id}/...
# Controlado por USE_NEW_B2_PATHS feature flag
# ============================================================

def generate_job_base_path(
    user_id: str,
    project_id: str,
    job_id: str
) -> str:
    """
    Gera o path base para um job específico.
    
    Nova estrutura:
        users/{user_id}/projects/{project_id}/jobs/{job_id}
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        job_id: UUID do job
    
    Returns:
        Path base do job
    """
    return f"users/{user_id}/projects/{project_id}/jobs/{job_id}"


def generate_render_path(
    user_id: str,
    project_id: str,
    job_id: str,
    version: int = 1,
    phase: int = 2,
    extension: str = "mp4"
) -> str:
    """
    🆕 Gera path para vídeo renderizado (v-editor output).
    
    Estrutura nova:
        users/{user_id}/projects/{project_id}/jobs/{job_id}/phase{phase}/renders/v{version}/final.{ext}
    
    Estrutura legacy:
        remotion_{job_id}_final.{ext}
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        job_id: UUID do job
        version: Número da versão (1, 2, 3...)
        phase: Fase do pipeline (1 ou 2)
        extension: Extensão do arquivo (mp4, webm)
    
    Returns:
        Path completo para o vídeo renderizado
    """
    if USE_NEW_B2_PATHS and user_id and project_id:
        base = generate_job_base_path(user_id, project_id, job_id)
        path = f"{base}/phase{phase}/renders/v{version}/final.{extension}"
        logger.debug(f"🆕 [B2Paths] Render path (NEW): {path}")
        return path
    else:
        # Legacy: arquivo na raiz
        path = f"remotion_{job_id}_final.{extension}"
        logger.debug(f"📁 [B2Paths] Render path (LEGACY): {path}")
        return path


def generate_render_path_legacy(job_id: str, extension: str = "mp4") -> str:
    """
    Gera path legacy para vídeo renderizado.
    Sempre usa o formato antigo, independente do feature flag.
    
    Usado para compatibilidade com URLs antigas.
    
    Args:
        job_id: UUID do job
        extension: Extensão do arquivo
    
    Returns:
        Path no formato: remotion_{job_id}_final.{ext}
    """
    return f"remotion_{job_id}_final.{extension}"


def generate_matting_path(
    user_id: str,
    project_id: str,
    job_id: str,
    segment_index: int = 0,
    extension: str = "webm"
) -> str:
    """
    🆕 Gera path para foreground de matting (v-matting output).
    
    Estrutura nova:
        users/{user_id}/projects/{project_id}/jobs/{job_id}/phase2/matting/seg_{index}_foreground.{ext}
    
    Estrutura legacy:
        matting/{job_id}_foreground.{ext}
        ou
        {project_path}/matting/{job_id}_foreground.{ext}
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        job_id: UUID do job (ou segment_id para múltiplos segmentos)
        segment_index: Índice do segmento (0, 1, 2...)
        extension: Extensão do arquivo (webm, mp4)
    
    Returns:
        Path completo para o foreground de matting
    """
    if USE_NEW_B2_PATHS and user_id and project_id:
        base = generate_job_base_path(user_id, project_id, job_id)
        path = f"{base}/phase2/matting/seg_{segment_index:03d}_foreground.{extension}"
        logger.debug(f"🆕 [B2Paths] Matting path (NEW): {path}")
        return path
    else:
        # Legacy: pasta matting na raiz
        path = f"matting/{job_id}_seg{segment_index}_foreground.{extension}"
        logger.debug(f"📁 [B2Paths] Matting path (LEGACY): {path}")
        return path


def generate_matting_path_legacy(
    job_id: str,
    project_path: Optional[str] = None,
    extension: str = "webm"
) -> str:
    """
    Gera path legacy para foreground de matting.
    Sempre usa o formato antigo, independente do feature flag.
    
    Args:
        job_id: UUID do job
        project_path: Path do projeto (opcional)
        extension: Extensão do arquivo
    
    Returns:
        Path no formato legacy
    """
    if project_path:
        return f"{project_path}/matting/{job_id}_foreground.{extension}"
    return f"matting/{job_id}_foreground.{extension}"


def generate_phase1_path(
    user_id: str,
    project_id: str,
    job_id: str,
    stage: str,
    segment_index: Optional[int] = None,
    extension: str = "mp4"
) -> str:
    """
    🆕 Gera path para arquivos intermediários da Fase 1.
    
    Estrutura nova:
        users/{user_id}/projects/{project_id}/jobs/{job_id}/phase1/{stage}[_{index}].{ext}
    
    Stages:
        - normalized: Vídeo com áudio normalizado
        - cut_{n}: Cortes de silêncio (seg_000, seg_001...)
        - concat: Vídeo concatenado (sem silêncios)
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        job_id: UUID do job
        stage: Etapa (normalized, cut, concat)
        segment_index: Índice do segmento (para cut)
        extension: Extensão do arquivo
    
    Returns:
        Path completo para o arquivo intermediário
    """
    if USE_NEW_B2_PATHS and user_id and project_id:
        base = generate_job_base_path(user_id, project_id, job_id)
        
        if stage == "cut" and segment_index is not None:
            path = f"{base}/phase1/cuts/seg_{segment_index:03d}.{extension}"
        else:
            path = f"{base}/phase1/{stage}.{extension}"
        
        logger.debug(f"🆕 [B2Paths] Phase1 path (NEW): {path}")
        return path
    else:
        # Legacy: usa estrutura antiga
        if stage == "cut" and segment_index is not None:
            return f"cuts/{job_id}_seg{segment_index}.{extension}"
        return f"mid-production/{job_id}_{stage}.{extension}"


def generate_input_path(
    user_id: str,
    project_id: str,
    job_id: str,
    filename: str = "original.mp4"
) -> str:
    """
    🆕 Gera path para vídeo de entrada original.
    
    Estrutura nova:
        users/{user_id}/projects/{project_id}/jobs/{job_id}/input/{filename}
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        job_id: UUID do job
        filename: Nome do arquivo (default: original.mp4)
    
    Returns:
        Path completo para o vídeo de entrada
    """
    if USE_NEW_B2_PATHS and user_id and project_id:
        base = generate_job_base_path(user_id, project_id, job_id)
        path = f"{base}/input/{filename}"
        logger.debug(f"🆕 [B2Paths] Input path (NEW): {path}")
        return path
    else:
        # Legacy: não muda (uploads já organizados)
        return f"uploads/{job_id}/{filename}"


def generate_metadata_job_path(
    user_id: str,
    project_id: str,
    job_id: str,
    metadata_type: str,
    version: Optional[int] = None
) -> str:
    """
    🆕 Gera path para metadados do job.
    
    Estrutura nova:
        users/{user_id}/projects/{project_id}/jobs/{job_id}/metadata/{type}[_v{version}].json
    
    Tipos:
        - job_config: Configuração do job
        - render_input: Payload enviado ao v-editor
        - render_output: Resultado do v-editor
        - pipeline_log: Log completo do pipeline
    
    Args:
        user_id: UUID do usuário
        project_id: UUID do projeto
        job_id: UUID do job
        metadata_type: Tipo de metadado
        version: Versão (opcional)
    
    Returns:
        Path completo para o arquivo de metadados
    """
    if USE_NEW_B2_PATHS and user_id and project_id:
        base = generate_job_base_path(user_id, project_id, job_id)
        
        if version:
            path = f"{base}/metadata/{metadata_type}_v{version:03d}.json"
        else:
            path = f"{base}/metadata/{metadata_type}.json"
        
        logger.debug(f"🆕 [B2Paths] Metadata path (NEW): {path}")
        return path
    else:
        # Legacy
        if version:
            return f"metadata/{job_id}_{metadata_type}_v{version:03d}.json"
        return f"metadata/{job_id}_{metadata_type}.json"


# ============================================================
# 🔧 Utilitários
# ============================================================

def is_new_path_format(path: str) -> bool:
    """
    Detecta se um path está no formato novo ou legacy.
    
    Args:
        path: Path ou URL do arquivo
    
    Returns:
        True se está no formato novo (users/xxx/projects/xxx/jobs/xxx/...)
    """
    return '/users/' in path and '/jobs/' in path


def get_job_id_from_path(path: str) -> Optional[str]:
    """
    Extrai o job_id de um path (novo ou legacy).
    
    Args:
        path: Path ou URL do arquivo
    
    Returns:
        job_id ou None se não encontrado
    """
    # Formato novo: .../jobs/{job_id}/...
    if '/jobs/' in path:
        parts = path.split('/jobs/')
        if len(parts) > 1:
            job_part = parts[1].split('/')[0]
            return job_part
    
    # Formato legacy: remotion_{job_id}_final.mp4
    if 'remotion_' in path:
        import re
        match = re.search(r'remotion_([a-f0-9-]+)_final', path)
        if match:
            return match.group(1)
    
    # Formato legacy matting: matting/{job_id}_foreground.webm
    if '/matting/' in path:
        import re
        match = re.search(r'/matting/([a-f0-9-]+)_', path)
        if match:
            return match.group(1)
    
    return None


def get_feature_flag_status() -> dict:
    """
    Retorna status atual do feature flag.
    Útil para debugging e monitoramento.
    
    Returns:
        Dict com informações do feature flag
    """
    return {
        "USE_NEW_B2_PATHS": USE_NEW_B2_PATHS,
        "status": "ENABLED (new structure)" if USE_NEW_B2_PATHS else "DISABLED (legacy)",
        "rollback_instruction": "Set USE_NEW_B2_PATHS=false in .env to rollback"
    }

