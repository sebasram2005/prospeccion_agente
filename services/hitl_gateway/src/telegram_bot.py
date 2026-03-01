"""
Telegram bot for HITL approval flow.

Sends formatted lead notifications with inline approve/edit/reject buttons.
Adapts display and actions based on source platform.
"""

from __future__ import annotations

import os

import structlog
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Bot

logger = structlog.get_logger(__name__)

# Sources where outreach happens ON the platform (no email sent)
PLATFORM_SOURCES = {"upwork", "linkedin", "weworkremotely", "indeed"}

SOURCE_LABELS = {
    "upwork": "Upwork Proposal",
    "linkedin": "LinkedIn Application",
    "weworkremotely": "WeWorkRemotely Application",
    "indeed": "Indeed Application",
}


def get_bot() -> Bot:
    return Bot(token=os.environ["TELEGRAM_BOT_TOKEN"])


def format_lead_message(entry: dict) -> str:
    source = entry.get("source", "email")
    is_platform = source in PLATFORM_SOURCES
    source_label = SOURCE_LABELS.get(source, "Cold Email")
    job_url = entry.get("job_url", "")

    to_email = entry["to_email"]
    subject = entry["subject"]
    body = entry["body"]
    queue_id = entry["id"]

    # Header
    lines = [
        f"🎯 NUEVO LEAD CALIFICADO",
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        f"📌 Tipo: {source_label}",
    ]

    if is_platform and job_url:
        lines.append(f"🔗 Postularse: {job_url}")
    elif not is_platform:
        lines.append(f"📧 Para: {to_email}")

    lines += [
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        f"📝 {'PROPOSAL' if source == 'upwork' else 'BORRADOR'}:",
        f"",
        f"Subject: {subject}",
        f"",
        f"{body}",
        f"",
        f"━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    if is_platform:
        lines.append(f"⚡ Aprueba para marcar como revisado. Copia el texto y postúlate en la plataforma.")
    else:
        lines.append(f"⚡ Aprueba para enviar el email automáticamente.")

    lines.append(f"Queue ID: {queue_id}")

    return "\n".join(lines)


def build_keyboard(queue_id: str, source: str = "email") -> InlineKeyboardMarkup:
    is_platform = source in PLATFORM_SOURCES

    if is_platform:
        approve_text = "✅ Revisado"
    else:
        approve_text = "✅ Enviar"

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    approve_text, callback_data=f"approve:{queue_id}"
                ),
                InlineKeyboardButton(
                    "✏️ Editar", callback_data=f"edit:{queue_id}"
                ),
                InlineKeyboardButton(
                    "❌ Rechazar", callback_data=f"reject:{queue_id}"
                ),
            ]
        ]
    )


async def send_approval_request(entry: dict) -> int | None:
    """Send a Telegram message with the lead draft and action buttons.

    Returns the Telegram message_id on success, None on failure.
    """
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    bot = get_bot()

    source = entry.get("source", "email")
    text = format_lead_message(entry)
    keyboard = build_keyboard(entry["id"], source=source)

    try:
        message = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=keyboard,
            parse_mode=None,
        )
        logger.info(
            "telegram_notification_sent",
            queue_id=entry["id"],
            source=source,
            message_id=message.message_id,
        )
        return message.message_id
    except Exception as exc:
        logger.error(
            "telegram_send_failed",
            queue_id=entry["id"],
            error=str(exc),
        )
        return None
