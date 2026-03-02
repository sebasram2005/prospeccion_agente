"""
Supabase client for Vertical 1 — Tech Services.
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

    async def is_duplicate(self, url: str) -> bool:
        try:
            result = (
                await self.supabase.table("raw_leads")
                .select("id")
                .eq("url", url)
                .limit(1)
                .execute()
            )
            return len(result.data) > 0
        except Exception as exc:
            logger.error("dedup_check_failed", url=url, error=str(exc))
            return False

    async def insert_raw_lead(self, data: dict) -> str | None:
        try:
            result = (
                await self.supabase.table("raw_leads")
                .insert(data)
                .execute()
            )
            lead_id = result.data[0]["id"]
            logger.info(
                "raw_lead_inserted",
                lead_id=lead_id,
                source=data.get("source"),
                vertical="tech",
            )
            return lead_id
        except Exception as exc:
            logger.error("insert_raw_lead_failed", error=str(exc))
            return None

    async def mark_as_processed(self, lead_id: str) -> None:
        try:
            await (
                self.supabase.table("raw_leads")
                .update({"processed": True})
                .eq("id", lead_id)
                .execute()
            )
        except Exception as exc:
            logger.error(
                "mark_processed_failed", lead_id=lead_id, error=str(exc)
            )

    async def insert_qualified_lead(self, data: dict) -> str | None:
        try:
            result = (
                await self.supabase.table("qualified_leads")
                .insert(data)
                .execute()
            )
            lead_id = result.data[0]["id"]
            logger.info(
                "qualified_lead_inserted",
                lead_id=lead_id,
                vertical="tech",
            )
            return lead_id
        except Exception as exc:
            logger.error("insert_qualified_lead_failed", error=str(exc))
            return None

    async def create_email_queue_entry(self, data: dict) -> str | None:
        try:
            result = (
                await self.supabase.table("email_queue")
                .insert(data)
                .execute()
            )
            queue_id = result.data[0]["id"]
            logger.info("email_queued", queue_id=queue_id, vertical="tech")
            return queue_id
        except Exception as exc:
            logger.error("create_email_queue_failed", error=str(exc))
            return None

    async def fetch_unqualified_leads(self) -> list[dict]:
        """Fetch raw_leads that were processed but never qualified (e.g. Gemini 403)."""
        try:
            raw = await (
                self.supabase.table("raw_leads")
                .select("id, source, url, raw_data")
                .eq("vertical", "tech")
                .eq("processed", True)
                .execute()
            )
            if not raw.data:
                return []

            raw_ids = [r["id"] for r in raw.data]

            qualified = await (
                self.supabase.table("qualified_leads")
                .select("raw_lead_id")
                .in_("raw_lead_id", raw_ids)
                .execute()
            )
            qualified_ids = {q["raw_lead_id"] for q in (qualified.data or [])}

            unqualified = [r for r in raw.data if r["id"] not in qualified_ids]
            logger.info("unqualified_leads_found", count=len(unqualified))
            return unqualified
        except Exception as exc:
            logger.error("fetch_unqualified_failed", error=str(exc))
            return []

    async def is_already_emailed(self, email: str) -> bool:
        try:
            result = (
                await self.supabase.table("email_queue")
                .select("id")
                .eq("to_email", email)
                .in_("status", ["pending", "approved", "sent"])
                .limit(1)
                .execute()
            )
            return len(result.data) > 0
        except Exception as exc:
            logger.error(
                "email_dedup_check_failed", email=email, error=str(exc)
            )
            return False
