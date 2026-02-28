"""
Deduplication checker against Supabase before processing leads.
"""

from __future__ import annotations

import structlog
from supabase import AClient as AsyncClient

logger = structlog.get_logger(__name__)


class DedupChecker:
    """Verifies duplicates in Supabase before processing a lead."""

    def __init__(self, supabase: AsyncClient):
        self.supabase = supabase

    async def is_already_processed(self, url: str) -> bool:
        """Return True if the URL already exists in raw_leads."""
        try:
            result = (
                await self.supabase.table("raw_leads")
                .select("id")
                .eq("url", url)
                .limit(1)
                .execute()
            )
            is_dup = len(result.data) > 0
            if is_dup:
                logger.info("duplicate_lead_skipped", url=url)
            return is_dup
        except Exception as exc:
            logger.error("dedup_check_failed", url=url, error=str(exc))
            return False

    async def is_already_emailed(self, email: str) -> bool:
        """Return True if there is already a sent/pending email to this address."""
        try:
            result = (
                await self.supabase.table("email_queue")
                .select("id")
                .eq("to_email", email)
                .in_("status", ["pending", "approved", "sent"])
                .limit(1)
                .execute()
            )
            is_dup = len(result.data) > 0
            if is_dup:
                logger.info("duplicate_email_skipped", email=email)
            return is_dup
        except Exception as exc:
            logger.error("email_dedup_check_failed", email=email, error=str(exc))
            return False
