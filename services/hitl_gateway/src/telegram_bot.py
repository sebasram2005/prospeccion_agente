"""
Telegram bot for HITL approval flow.

Sends formatted lead notifications with inline approve/edit/reject buttons.
"""

from __future__ import annotations

import os

import structlog
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Bot

logger = structlog.get_logger(__name__)


def get_bot() -> Bot:
    return Bot(token=os.environ["TELEGRAM_BOT_TOKEN"])


def format_lead_message(entry: dict) -> str:
    vertical_label = (
        "Tech Services" if entry["vertical"] == "tech" else "Cerrieta Luxury Pets"
    )

    # Extract prospect info from the body (best-effort)
    to_email = entry["to_email"]
    subject = entry["subject"]
    body = entry["body"]
    queue_id = entry["id"]

    text = (
        f"🎯 NUEVO LEAD CALIFICADO\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Vertical: {vertical_label}\n"
        f"📧 Para: {to_email}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 BORRADOR:\n\n"
        f"Subject: {subject}\n\n"
        f"{body}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Queue ID: {queue_id}"
    )
    return text


def build_keyboard(queue_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Aprobar", callback_data=f"approve:{queue_id}"
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

    text = format_lead_message(entry)
    keyboard = build_keyboard(entry["id"])

    try:
        message = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=keyboard,
            parse_mode=None,  # plain text to avoid escaping issues
        )
        logger.info(
            "telegram_notification_sent",
            queue_id=entry["id"],
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
