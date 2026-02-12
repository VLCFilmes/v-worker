"""
🔔 Admin Notification Service — v4.4.2

Envia notificações ao admin (email) quando ocorrem erros críticos
no pipeline de edição de vídeo.

Tipos de notificação:
- pipeline_failure: Step do pipeline falhou definitivamente
- replay_failure: Replay/continue do pipeline falhou
- director_failure: Sandbox Director falhou (circuit breaker ou anti-alucinação)
- render_failure: Webhook de render retornou erro

Rate limiting:
- Agrupa notificações por intervalo (MIN_INTERVAL_SECONDS)
- Evita flood de emails em cascata de erros
"""

import os
import time
import logging
import threading
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ═══ Configuração ═══

# Destinatário das notificações admin
# Prioridade: ADMIN_NOTIFICATION_EMAIL > SMTP_ADMIN_EMAIL
# Nota: SMTP_ADMIN_EMAIL é usado como remetente no email.py, mas como fallback
# para destinatário aqui. Configure ADMIN_NOTIFICATION_EMAIL para separar.
ADMIN_EMAIL = os.environ.get('ADMIN_NOTIFICATION_EMAIL', os.environ.get('SMTP_ADMIN_EMAIL', ''))
NOTIFICATIONS_ENABLED = os.environ.get('ADMIN_NOTIFICATIONS_ENABLED', 'true').lower() == 'true'
MIN_INTERVAL_SECONDS = int(os.environ.get('ADMIN_NOTIFICATION_MIN_INTERVAL', '60'))  # 1 min entre emails

# ═══ Rate limiter simples (in-memory) ═══

_last_sent: dict[str, float] = {}
_lock = threading.Lock()
_pending_buffer: list[dict] = []
_buffer_lock = threading.Lock()


def _should_send(category: str) -> bool:
    """Verifica se já passou tempo suficiente desde o último envio nesta categoria."""
    now = time.time()
    with _lock:
        last = _last_sent.get(category, 0)
        if now - last < MIN_INTERVAL_SECONDS:
            return False
        _last_sent[category] = now
        return True


# ═══ API pública ═══

def notify_pipeline_failure(
    job_id: str,
    step_name: str,
    error_message: str,
    project_id: Optional[str] = None,
    user_id: Optional[str] = None,
    attempt: int = 1,
    extra: Optional[dict] = None,
):
    """
    Notifica admin sobre falha no pipeline.
    
    Chamado de: pipeline_engine.py, bridge.py, worker.py
    """
    if not NOTIFICATIONS_ENABLED or not ADMIN_EMAIL:
        return
    
    category = f"pipeline_{job_id}_{step_name}"
    if not _should_send(category):
        logger.debug(f"[ADMIN-NOTIFY] Rate limited: {category}")
        return
    
    _send_notification_async(
        category="pipeline_failure",
        subject=f"Pipeline Falhou — step: {step_name}",
        details={
            "job_id": job_id,
            "step": step_name,
            "error": error_message,
            "project_id": project_id or "N/A",
            "user_id": user_id or "N/A",
            "attempt": attempt,
            **(extra or {}),
        },
    )


def notify_director_failure(
    job_id: str,
    failure_type: str,  # "circuit_breaker", "anti_hallucination", "error"
    error_message: str,
    instruction: str = "",
    session_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    extra: Optional[dict] = None,
):
    """
    Notifica admin sobre falha no Sandbox Director.
    
    Chamado de: chat_flask.py (bridge)
    """
    if not NOTIFICATIONS_ENABLED or not ADMIN_EMAIL:
        return
    
    category = f"director_{job_id}_{failure_type}"
    if not _should_send(category):
        logger.debug(f"[ADMIN-NOTIFY] Rate limited: {category}")
        return
    
    _send_notification_async(
        category="director_failure",
        subject=f"Director Falhou — {failure_type}",
        details={
            "job_id": job_id,
            "failure_type": failure_type,
            "error": error_message,
            "instruction": instruction[:200] if instruction else "N/A",
            "session_id": session_id or "N/A",
            "conversation_id": conversation_id or "N/A",
            **(extra or {}),
        },
    )


def notify_render_failure(
    job_id: str,
    error_message: str,
    project_id: Optional[str] = None,
    extra: Optional[dict] = None,
):
    """
    Notifica admin sobre falha no render.
    
    Chamado de: callbacks.py
    """
    if not NOTIFICATIONS_ENABLED or not ADMIN_EMAIL:
        return
    
    category = f"render_{job_id}"
    if not _should_send(category):
        logger.debug(f"[ADMIN-NOTIFY] Rate limited: {category}")
        return
    
    _send_notification_async(
        category="render_failure",
        subject=f"Render Falhou — job: {job_id[:8]}",
        details={
            "job_id": job_id,
            "error": error_message,
            "project_id": project_id or "N/A",
            **(extra or {}),
        },
    )


# ═══ Envio assíncrono (thread) ═══

def _send_notification_async(category: str, subject: str, details: dict):
    """Envia notificação em background thread para não bloquear o pipeline."""
    thread = threading.Thread(
        target=_send_notification_sync,
        args=(category, subject, details),
        daemon=True,
    )
    thread.start()


def _send_notification_sync(category: str, subject: str, details: dict):
    """Envia email de notificação (síncrono, roda em thread)."""
    try:
        from app.routes.email import send_email
        
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        full_subject = f"[vinicius.ai] {subject}"
        
        # Montar HTML
        html_body = _build_notification_html(category, details, timestamp)
        text_body = _build_notification_text(category, details, timestamp)
        
        success, error = send_email(
            to=ADMIN_EMAIL,
            subject=full_subject,
            html_body=html_body,
            text_body=text_body,
        )
        
        if success:
            logger.info(f"[ADMIN-NOTIFY] Email enviado: {full_subject}")
        else:
            logger.warning(f"[ADMIN-NOTIFY] Falha ao enviar email: {error}")
            
    except Exception as e:
        logger.error(f"[ADMIN-NOTIFY] Erro ao enviar notificação: {e}", exc_info=True)


# ═══ Templates de email ═══

CATEGORY_LABELS = {
    "pipeline_failure": ("Pipeline Failure", "#DC2626"),
    "director_failure": ("Director Failure", "#D97706"),
    "render_failure": ("Render Failure", "#7C3AED"),
}

CATEGORY_EMOJI = {
    "pipeline_failure": "🔴",
    "director_failure": "🟡",
    "render_failure": "🟣",
}


def _build_notification_html(category: str, details: dict, timestamp: str) -> str:
    label, color = CATEGORY_LABELS.get(category, ("Error", "#DC2626"))
    emoji = CATEGORY_EMOJI.get(category, "🔴")
    
    rows_html = ""
    for key, value in details.items():
        # Truncar valores longos
        display_value = str(value)
        if len(display_value) > 500:
            display_value = display_value[:500] + "..."
        # Escapar HTML básico
        display_value = display_value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        rows_html += f"""
        <tr>
            <td style="padding: 8px 12px; border-bottom: 1px solid #E4E4E7; font-weight: 600; color: #3F3F46; width: 140px; vertical-align: top;">
                {key}
            </td>
            <td style="padding: 8px 12px; border-bottom: 1px solid #E4E4E7; color: #52525B; font-family: monospace; font-size: 13px; word-break: break-all;">
                {display_value}
            </td>
        </tr>"""
    
    return f'''<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin: 0; padding: 0; background-color: #F4F4F5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color: #F4F4F5; padding: 32px 16px;">
<tr><td align="center">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width: 600px; background-color: #FFFFFF; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">

<!-- Header -->
<tr>
<td style="padding: 24px 32px; background-color: #18181B;">
    <table width="100%" cellpadding="0" cellspacing="0">
    <tr>
        <td>
            <span style="color: #FFFFFF; font-weight: bold; font-size: 14px;">vinicius.ai</span>
            <span style="color: #71717A; font-size: 12px; margin-left: 8px;">Admin Alert</span>
        </td>
        <td align="right">
            <span style="display: inline-block; padding: 4px 10px; background-color: {color}; color: #FFFFFF; border-radius: 4px; font-size: 11px; font-weight: 600;">
                {emoji} {label}
            </span>
        </td>
    </tr>
    </table>
</td>
</tr>

<!-- Content -->
<tr>
<td style="padding: 24px 32px;">
    <p style="margin: 0 0 16px; color: #71717A; font-size: 12px;">{timestamp}</p>
    <table width="100%" cellpadding="0" cellspacing="0" style="border: 1px solid #E4E4E7; border-radius: 8px; overflow: hidden;">
        {rows_html}
    </table>
</td>
</tr>

<!-- Footer -->
<tr>
<td style="padding: 16px 32px; background-color: #FAFAFA; border-top: 1px solid #E4E4E7;">
    <p style="margin: 0; color: #A1A1AA; font-size: 11px; text-align: center;">
        Notificação automática do sistema de monitoramento de pipeline
    </p>
</td>
</tr>

</table>
</td></tr>
</table>
</body>
</html>'''


def _build_notification_text(category: str, details: dict, timestamp: str) -> str:
    label, _ = CATEGORY_LABELS.get(category, ("Error", ""))
    emoji = CATEGORY_EMOJI.get(category, "🔴")
    
    lines = [
        f"{emoji} [{label}] — vinicius.ai Admin Alert",
        f"Timestamp: {timestamp}",
        "-" * 40,
    ]
    
    for key, value in details.items():
        display_value = str(value)
        if len(display_value) > 500:
            display_value = display_value[:500] + "..."
        lines.append(f"{key}: {display_value}")
    
    lines.append("-" * 40)
    lines.append("Notificação automática do sistema de monitoramento de pipeline")
    
    return "\n".join(lines)
