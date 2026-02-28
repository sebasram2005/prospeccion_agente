"""
Supabase client for the HITL Gateway service.
"""

from __future__ import annotations

import os

import structlog
from supabase import acreate_client, AClient as AsyncClient

logger = structlog.get_logger(__name__)

_client: AsyncClient | None = None


async def get_supabase() -> AsyncClient:
    global _client
    if _client is None:
        _client = await acreate_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_KEY"],
        )
    return _client


class LeadsRepository:
    def __init__(self, supabase: AsyncClient):
        self.supabase = supabase

    async def get_email_queue_entry(self, queue_id: str) -> dict | None:
        try:
            result = (
                await self.supabase.table("email_queue")
                .select("*")
                .eq("id", queue_id)
                .limit(1)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            logger.error("get_email_queue_failed", queue_id=queue_id, error=str(exc))
            return None

    async def update_email_status(
        self, queue_id: str, status: str, **extra_fields: str
    ) -> None:
        try:
            update_data = {"status": status, **extra_fields}
            await (
                self.supabase.table("email_queue")
                .update(update_data)
                .eq("id", queue_id)
                .execute()
            )
            logger.info(
                "email_status_updated", queue_id=queue_id, new_status=status
            )
        except Exception as exc:
            logger.error(
                "update_email_status_failed",
                queue_id=queue_id,
                status=status,
                error=str(exc),
            )

    async def set_telegram_message_id(
        self, queue_id: str, message_id: int
    ) -> None:
        try:
            await (
                self.supabase.table("email_queue")
                .update({"telegram_message_id": message_id})
                .eq("id", queue_id)
                .execute()
            )
        except Exception as exc:
            logger.error(
                "set_telegram_msg_id_failed",
                queue_id=queue_id,
                error=str(exc),
            )

    async def log_hitl_action(
        self, queue_id: str, action: str, note: str | None = None
    ) -> None:
        try:
            await (
                self.supabase.table("hitl_audit_log")
                .insert(
                    {
                        "email_queue_id": queue_id,
                        "action": action,
                        "operator_note": note,
                    }
                )
                .execute()
            )
            logger.info(
                "hitl_action_logged",
                queue_id=queue_id,
                action=action,
            )
        except Exception as exc:
            logger.error(
                "log_hitl_action_failed",
                queue_id=queue_id,
                action=action,
                error=str(exc),
            )
