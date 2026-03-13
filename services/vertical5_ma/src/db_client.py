"""
Supabase client for Vertical 5 — M&A Silver Tsunami.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

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
            logger.error("dedup_check_failed", url=url, error=str(exc), vertical="ma")
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
                vertical="ma",
            )
            return lead_id
        except Exception as exc:
            logger.error("insert_raw_lead_failed", error=str(exc), vertical="ma")
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
                "mark_processed_failed", lead_id=lead_id, error=str(exc), vertical="ma"
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
                vertical="ma",
            )
            return lead_id
        except Exception as exc:
            logger.error("insert_qualified_lead_failed", error=str(exc), vertical="ma")
            return None

    async def create_email_queue_entry(self, data: dict) -> str | None:
        try:
            result = (
                await self.supabase.table("email_queue")
                .insert(data)
                .execute()
            )
            queue_id = result.data[0]["id"]
            logger.info("email_queued", queue_id=queue_id, vertical="ma")
            return queue_id
        except Exception as exc:
            logger.error("create_email_queue_failed", error=str(exc), vertical="ma")
            return None

    async def fetch_unqualified_leads(self) -> list[dict]:
        """Fetch raw_leads that were processed but never qualified (e.g. Gemini 403)."""
        try:
            raw = await (
                self.supabase.table("raw_leads")
                .select("id, source, url, raw_data")
                .eq("vertical", "ma")
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
            logger.info("unqualified_leads_found", count=len(unqualified), vertical="ma")
            return unqualified
        except Exception as exc:
            logger.error("fetch_unqualified_failed", error=str(exc), vertical="ma")
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
                "email_dedup_check_failed", email=email, error=str(exc), vertical="ma"
            )
            return False

    async def update_keyword_performance(self) -> None:
        """Aggregate keyword stats from raw_leads → qualified_leads → email_queue and upsert."""
        try:
            since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            raw = await (
                self.supabase.table("raw_leads")
                .select("id, source, search_keyword")
                .eq("vertical", "ma")
                .gte("scraped_at", since)
                .not_.is_("search_keyword", "null")
                .neq("search_keyword", "")
                .limit(2000)
                .execute()
            )
            if not raw.data:
                logger.info("keyword_perf_no_data", vertical="ma")
                return

            raw_ids = [r["id"] for r in raw.data]

            qualified = await (
                self.supabase.table("qualified_leads")
                .select("id, raw_lead_id, qualification_result")
                .in_("raw_lead_id", raw_ids)
                .execute()
            )
            qualified_map: dict[str, dict] = {}
            qual_ids: list[str] = []
            for q in (qualified.data or []):
                qualified_map[q["raw_lead_id"]] = q
                qual_ids.append(q["id"])

            approved_set: set[str] = set()
            rejected_set: set[str] = set()
            if qual_ids:
                emails = await (
                    self.supabase.table("email_queue")
                    .select("qualified_lead_id, status")
                    .in_("qualified_lead_id", qual_ids)
                    .in_("status", ["approved", "sent", "rejected"])
                    .execute()
                )
                for e in (emails.data or []):
                    if e["status"] in ("approved", "sent"):
                        approved_set.add(e["qualified_lead_id"])
                    elif e["status"] == "rejected":
                        rejected_set.add(e["qualified_lead_id"])

            stats: dict[tuple[str, str], dict] = {}
            for r in raw.data:
                key = (r["search_keyword"], r["source"])
                if key not in stats:
                    stats[key] = {
                        "found": 0, "qualified": 0, "approved": 0,
                        "rejected": 0, "fit_scores": [],
                    }
                s = stats[key]
                s["found"] += 1

                q = qualified_map.get(r["id"])
                if q:
                    s["qualified"] += 1
                    qr = q.get("qualification_result") or {}
                    if isinstance(qr, dict):
                        s["fit_scores"].append(qr.get("fit_score", 0))
                    if q["id"] in approved_set:
                        s["approved"] += 1
                    if q["id"] in rejected_set:
                        s["rejected"] += 1

            for (keyword, source), s in stats.items():
                avg_fit = sum(s["fit_scores"]) / len(s["fit_scores"]) if s["fit_scores"] else 0
                qual_rate = s["qualified"] / s["found"] if s["found"] else 0
                decided = s["approved"] + s["rejected"]
                approval_rate = s["approved"] / decided if decided else 0.5
                score = (
                    0.4 * approval_rate
                    + 0.3 * qual_rate
                    + 0.2 * (avg_fit / 10.0)
                    + 0.1
                )
                score = min(max(score, 0.0), 1.0)

                await self.supabase.table("keyword_performance").upsert(
                    {
                        "keyword": keyword,
                        "source": source,
                        "leads_found": s["found"],
                        "leads_qualified": s["qualified"],
                        "leads_approved": s["approved"],
                        "leads_rejected": s["rejected"],
                        "avg_fit_score": round(avg_fit, 2),
                        "score": round(score, 4),
                        "last_run_at": datetime.now(timezone.utc).isoformat(),
                    },
                    on_conflict="keyword,source",
                ).execute()

            logger.info("keyword_perf_updated", keywords_tracked=len(stats), vertical="ma")

        except Exception as exc:
            logger.error("keyword_perf_update_failed", error=str(exc), vertical="ma")
