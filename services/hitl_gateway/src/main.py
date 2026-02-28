"""
HITL Gateway — FastAPI app deployed on Google Cloud Run.

Handles:
1. POST /notify — Receives new lead drafts from scrapers, sends Telegram notification.
2. Telegram webhook — Processes approve/edit/reject callbacks.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel
from telegram import Update, Bot

from .approval_router import ApprovalRouter
from .db_client import get_supabase, LeadsRepository
from .telegram_bot import send_approval_request

# ── Structured logging ──────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        int(os.environ.get("LOG_LEVEL_NUM", "20"))  # INFO=20
    ),
)
logger = structlog.get_logger(__name__)

# ── State tracking for edit flow ────────────────────────────────
# Maps chat_id -> queue_id while waiting for edit instructions
_editing_state: dict[int, str] = {}


# ── Lifespan ────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Set Telegram webhook to our Cloud Run URL
    bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
    gateway_url = os.environ.get("HITL_GATEWAY_URL", "")
    if gateway_url:
        bot = Bot(token=bot_token)
        webhook_url = f"{gateway_url.rstrip('/')}/webhook/{bot_token}"
        await bot.set_webhook(url=webhook_url)
        logger.info("telegram_webhook_set", url=webhook_url)
    yield


app = FastAPI(title="HITL Gateway", lifespan=lifespan)


# ── Models ──────────────────────────────────────────────────────
class NotifyRequest(BaseModel):
    queue_id: str


# ── Routes ──────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/notify")
async def notify(req: NotifyRequest):
    """Called by scrapers after inserting into email_queue.

    Fetches the entry from Supabase and sends a Telegram notification.
    """
    supabase = await get_supabase()
    repo = LeadsRepository(supabase)

    entry = await repo.get_email_queue_entry(req.queue_id)
    if not entry:
        return {"error": "Queue entry not found"}, 404

    msg_id = await send_approval_request(entry)
    if msg_id:
        await repo.set_telegram_message_id(req.queue_id, msg_id)

    return {"status": "notified", "telegram_message_id": msg_id}


@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    """Telegram sends callback queries and messages here."""
    if token != os.environ["TELEGRAM_BOT_TOKEN"]:
        return Response(status_code=403)

    body = await request.json()
    update = Update.de_json(body, Bot(token=os.environ["TELEGRAM_BOT_TOKEN"]))

    supabase = await get_supabase()
    repo = LeadsRepository(supabase)
    router = ApprovalRouter(repo)
    bot = Bot(token=os.environ["TELEGRAM_BOT_TOKEN"])

    # Handle inline button callbacks
    if update.callback_query:
        query = update.callback_query
        data = query.data  # e.g. "approve:abc-123-def"

        if ":" not in data:
            await query.answer("Invalid action.")
            return {"ok": True}

        action, queue_id = data.split(":", 1)

        if action == "approve":
            result = await router.handle_approve(queue_id)
            await query.answer(result)
            await bot.send_message(
                chat_id=query.message.chat_id,
                text=f"✅ {result}\nQueue ID: {queue_id}",
            )

        elif action == "reject":
            result = await router.handle_reject(queue_id)
            await query.answer(result)
            await bot.send_message(
                chat_id=query.message.chat_id,
                text=f"❌ {result}\nQueue ID: {queue_id}",
            )

        elif action == "edit":
            result = await router.handle_edit_request(queue_id)
            await query.answer(result)
            # Track that we're waiting for edit instructions
            _editing_state[query.message.chat_id] = queue_id
            await bot.send_message(
                chat_id=query.message.chat_id,
                text=(
                    f"✏️ Modo edición activado para Queue ID: {queue_id}\n\n"
                    f"Envíame las instrucciones de edición en tu siguiente mensaje."
                ),
            )

    # Handle text messages (edit instructions)
    elif update.message and update.message.text:
        chat_id = update.message.chat_id

        if chat_id in _editing_state:
            queue_id = _editing_state.pop(chat_id)
            instructions = update.message.text

            await bot.send_message(
                chat_id=chat_id,
                text="⏳ Re-redactando email con tus instrucciones...",
            )

            updated = await router.handle_edit_instructions(
                queue_id, instructions
            )
            if updated:
                msg_id = await router.resend_approval_request(updated)
                if not msg_id:
                    await bot.send_message(
                        chat_id=chat_id,
                        text="⚠️ Error al enviar el nuevo borrador.",
                    )
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "⚠️ Error al re-redactar. El email vuelve a estado "
                        "'pending'. Intenta de nuevo."
                    ),
                )

    return {"ok": True}
