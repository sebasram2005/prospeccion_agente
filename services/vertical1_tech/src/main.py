"""
Vertical 1 — Tech Services: Main entrypoint.

Usage:
    python -m src.main --source upwork
    python -m src.main --source linkedin
    python -m src.main --source all
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

import httpx
import structlog

# ── Structured logging ──────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        int(os.environ.get("LOG_LEVEL_NUM", "20"))
    ),
)
logger = structlog.get_logger(__name__)

# ── Add project root to path for shared imports ─────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from shared.utils.rate_limiter import GeminiRateLimiter
from shared.utils.serper_client import SerperClient

from .db_client import get_supabase, LeadsRepository
from .qualifier import LeadQualifier
from .email_drafter import EmailDrafter
from .scrapers.serper_search import search_leads


async def process_lead(
    lead: dict,
    source: str,
    repo: LeadsRepository,
    qualifier: LeadQualifier,
    drafter: EmailDrafter,
    gemini_limiter: GeminiRateLimiter,
    hitl_url: str,
) -> None:
    """Process a single lead through the full pipeline."""
    url = lead.get("url", "")

    # 1. Dedup check
    if url and await repo.is_duplicate(url):
        logger.info("lead_skipped_duplicate", url=url, source=source)
        return

    # 2. Insert raw lead
    raw_lead_id = await repo.insert_raw_lead(
        {
            "source": source,
            "vertical": "tech",
            "url": url,
            "raw_data": lead,
        }
    )
    if not raw_lead_id:
        return

    # 3. Qualify with Gemini
    raw_text = json.dumps(lead, indent=2)
    await gemini_limiter.acquire()

    result = await qualifier.qualify(raw_text)
    if result is None:
        await repo.mark_as_processed(raw_lead_id)
        return

    # 4. Extract contact info
    first_name = lead.get("company", "there").split()[0] if lead.get("company") else "there"
    company_name = lead.get("company", lead.get("title", "your company"))
    email = lead.get("email", "")

    # 5. Insert qualified lead
    qualified_lead_id = await repo.insert_qualified_lead(
        {
            "raw_lead_id": raw_lead_id,
            "vertical": "tech",
            "first_name": first_name,
            "company_name": company_name,
            "email": email,
            "qualification_result": result.model_dump(),
            "pain_point": result.pain_point,
        }
    )
    if not qualified_lead_id:
        await repo.mark_as_processed(raw_lead_id)
        return

    # 6. Draft email
    draft = drafter.draft(
        first_name=first_name,
        company_name=company_name,
        email=email or "pending@manual-lookup.com",
        pain_point=result.pain_point,
    )
    if not draft:
        await repo.mark_as_processed(raw_lead_id)
        return

    # 7. Check email dedup
    if draft.to_email != "pending@manual-lookup.com":
        if await repo.is_already_emailed(draft.to_email):
            logger.info(
                "lead_skipped_already_emailed",
                email=draft.to_email,
                source=source,
            )
            await repo.mark_as_processed(raw_lead_id)
            return

    # 8. Insert into email queue
    queue_id = await repo.create_email_queue_entry(
        {
            "qualified_lead_id": qualified_lead_id,
            "vertical": "tech",
            "to_email": draft.to_email,
            "subject": draft.subject,
            "body": draft.body,
            "status": "pending",
        }
    )

    # 9. Notify HITL Gateway
    if queue_id and hitl_url:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"{hitl_url.rstrip('/')}/notify",
                    json={"queue_id": queue_id},
                )
                logger.info(
                    "hitl_notified",
                    queue_id=queue_id,
                    status=resp.status_code,
                    source=source,
                )
        except Exception as exc:
            logger.error(
                "hitl_notify_failed",
                queue_id=queue_id,
                error=str(exc),
                source=source,
            )

    await repo.mark_as_processed(raw_lead_id)
    logger.info(
        "lead_processed",
        raw_lead_id=raw_lead_id,
        qualified_lead_id=qualified_lead_id,
        queue_id=queue_id,
        source=source,
        vertical="tech",
    )


async def main(source: str) -> None:
    logger.info("pipeline_start", source=source, vertical="tech")

    # Initialize components
    supabase = await get_supabase()
    repo = LeadsRepository(supabase)
    qualifier = LeadQualifier()
    drafter = EmailDrafter()
    serper_client = SerperClient()
    gemini_limiter = GeminiRateLimiter(max_per_minute=12)
    hitl_url = os.environ.get("HITL_GATEWAY_URL", "")

    # Search via Serper API (all queries in parallel)
    if source in ("upwork", "linkedin", "weworkremotely", "indeed", "all"):
        leads = await search_leads(serper_client, source)
    else:
        logger.error("unknown_source", source=source)
        return

    logger.info("search_complete", source=source, leads_found=len(leads))

    # Process leads concurrently (Gemini rate limiter controls throughput)
    tasks = [
        process_lead(
            lead=lead,
            source=source,
            repo=repo,
            qualifier=qualifier,
            drafter=drafter,
            gemini_limiter=gemini_limiter,
            hitl_url=hitl_url,
        )
        for lead in leads
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for lead, result in zip(leads, results):
        if isinstance(result, Exception):
            logger.error(
                "lead_processing_error",
                source=source,
                error=str(result),
                lead_url=lead.get("url", "unknown"),
            )

    logger.info("pipeline_complete", source=source, vertical="tech")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertical 1 Tech Scraper")
    parser.add_argument(
        "--source",
        required=True,
        choices=["upwork", "linkedin", "weworkremotely", "indeed", "all"],
        help="Data source to search via Serper API",
    )
    args = parser.parse_args()
    asyncio.run(main(args.source))
