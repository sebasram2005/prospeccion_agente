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
from shared.utils.content_enricher import ContentEnricher

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
    enricher: ContentEnricher,
    gemini_limiter: GeminiRateLimiter,
    hitl_url: str,
) -> None:
    """Process a single lead through the full pipeline."""
    url = lead.get("url", "")

    # Resolve actual source (serper_search stores it as source_site)
    lead_source = lead.get("source_site", source)

    # 1. Dedup check
    if url and await repo.is_duplicate(url):
        logger.info("lead_skipped_duplicate", url=url, source=lead_source)
        return

    # 2. Insert raw lead
    raw_lead_id = await repo.insert_raw_lead(
        {
            "source": lead_source,
            "vertical": "tech",
            "url": url,
            "raw_data": lead,
        }
    )
    if not raw_lead_id:
        return

    # 3. Enrich: fetch full page content via Jina Reader (best-effort)
    enriched_lead = await enricher.enrich_lead(lead)

    # 4. Qualify with Gemini (rate limiter re-acquired on each attempt inside qualify)
    raw_text = json.dumps(enriched_lead, indent=2)
    result = await qualifier.qualify(raw_text, rate_limiter=gemini_limiter)
    if result is None:
        await repo.mark_as_processed(raw_lead_id)
        return

    # 5. Extract contact info (use Gemini-extracted data when available)
    company_name = result.inferred_company or lead.get("title", "your company")
    first_name = result.contact_name or "Hiring Manager"
    email = lead.get("email", "")

    # 6. Scrape company website for email if Gemini found a website URL
    if not email and result.company_website:
        scraped_email = await enricher.scrape_email_from_website(
            result.company_website
        )
        if scraped_email:
            email = scraped_email

    # 7. Insert qualified lead
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

    # 8. Draft outreach (AI-generated via Gemini, adapts to platform)
    is_platform_lead = lead_source in (
        "upwork", "linkedin", "weworkremotely", "glassdoor",
        "wellfound", "otta", "efinancialcareers", "remoteok",
    )
    to_address = email or (f"apply-via-{lead_source}" if is_platform_lead else "pending@manual-lookup.com")

    draft = await drafter.draft(
        first_name=first_name,
        company_name=company_name,
        email=to_address,
        pain_point=result.pain_point,
        portfolio_proof=result.portfolio_proof,
        suggested_angle=result.suggested_angle,
        job_title=lead.get("title", ""),
        budget_estimate=result.budget_estimate,
        source=lead_source,
        pricing_model=result.pricing_model,
        contract_value_tier=result.contract_value_tier,
        rate_limiter=gemini_limiter,
    )
    if not draft:
        await repo.mark_as_processed(raw_lead_id)
        return

    # 10. Check email dedup (only for actual email outreach)
    if not is_platform_lead and email:
        if await repo.is_already_emailed(email):
            logger.info(
                "lead_skipped_already_emailed",
                email=email,
                source=lead_source,
            )
            await repo.mark_as_processed(raw_lead_id)
            return

    # 11. Insert into email queue
    queue_id = await repo.create_email_queue_entry(
        {
            "qualified_lead_id": qualified_lead_id,
            "vertical": "tech",
            "to_email": draft.to_email,
            "subject": draft.subject,
            "body": draft.body,
            "status": "pending",
            "source": lead_source,
            "job_url": url,
        }
    )

    # 12. Notify HITL Gateway
    if queue_id and hitl_url:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
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
        fit_score=result.fit_score,
        contact_name=first_name,
        email_found=email != "",
        source=source,
        vertical="tech",
    )


async def requalify() -> None:
    """Re-process raw_leads that failed qualification (e.g. Gemini 403)."""
    logger.info("requalify_start", vertical="tech")

    supabase = await get_supabase()
    repo = LeadsRepository(supabase)
    qualifier = LeadQualifier()
    drafter = EmailDrafter()
    enricher = ContentEnricher()
    gemini_limiter = GeminiRateLimiter(max_per_minute=60)
    hitl_url = os.environ.get("HITL_GATEWAY_URL", "")

    unqualified = await repo.fetch_unqualified_leads()
    if not unqualified:
        logger.info("requalify_nothing_to_do")
        return

    sem = asyncio.Semaphore(10)

    async def _process_one(row: dict) -> None:
        async with sem:
            raw_lead_id = row["id"]
            lead = row.get("raw_data", {})
            lead_source = lead.get("source_site", row.get("source", "unknown"))
            url = row.get("url", "")

            # Reset processed flag so pipeline can re-mark it
            await repo.supabase.table("raw_leads").update(
                {"processed": False}
            ).eq("id", raw_lead_id).execute()

            # Enrich
            enriched_lead = await enricher.enrich_lead(lead)

            # Qualify
            raw_text = json.dumps(enriched_lead, indent=2)
            result = await qualifier.qualify(raw_text, rate_limiter=gemini_limiter)
            if result is None:
                await repo.mark_as_processed(raw_lead_id)
                return

            company_name = result.inferred_company or lead.get("title", "your company")
            first_name = result.contact_name or "Hiring Manager"
            email = lead.get("email", "")

            if not email and result.company_website:
                scraped_email = await enricher.scrape_email_from_website(
                    result.company_website
                )
                if scraped_email:
                    email = scraped_email

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

            is_platform_lead = lead_source in (
                "upwork", "linkedin", "weworkremotely", "glassdoor",
                "wellfound", "otta", "efinancialcareers", "remoteok",
            )
            to_address = email or (f"apply-via-{lead_source}" if is_platform_lead else "pending@manual-lookup.com")

            draft = await drafter.draft(
                first_name=first_name,
                company_name=company_name,
                email=to_address,
                pain_point=result.pain_point,
                portfolio_proof=result.portfolio_proof,
                suggested_angle=result.suggested_angle,
                job_title=lead.get("title", ""),
                budget_estimate=result.budget_estimate,
                source=lead_source,
                pricing_model=result.pricing_model,
                contract_value_tier=result.contract_value_tier,
                rate_limiter=gemini_limiter,
            )
            if not draft:
                await repo.mark_as_processed(raw_lead_id)
                return

            if not is_platform_lead and email:
                if await repo.is_already_emailed(email):
                    await repo.mark_as_processed(raw_lead_id)
                    return

            queue_id = await repo.create_email_queue_entry(
                {
                    "qualified_lead_id": qualified_lead_id,
                    "vertical": "tech",
                    "to_email": draft.to_email,
                    "subject": draft.subject,
                    "body": draft.body,
                    "status": "pending",
                    "source": lead_source,
                    "job_url": url,
                }
            )

            if queue_id and hitl_url:
                try:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        resp = await client.post(
                            f"{hitl_url.rstrip('/')}/notify",
                            json={"queue_id": queue_id},
                        )
                        logger.info("hitl_notified", queue_id=queue_id, status=resp.status_code)
                except Exception as exc:
                    logger.error("hitl_notify_failed", queue_id=queue_id, error=str(exc))

            await repo.mark_as_processed(raw_lead_id)
            logger.info(
                "lead_requalified",
                raw_lead_id=raw_lead_id,
                qualified_lead_id=qualified_lead_id,
                queue_id=queue_id,
                fit_score=result.fit_score,
                source=lead_source,
            )

    results = await asyncio.gather(
        *[_process_one(row) for row in unqualified], return_exceptions=True
    )
    for row, result in zip(unqualified, results):
        if isinstance(result, Exception):
            logger.error("requalify_error", raw_lead_id=row["id"], error=str(result))

    logger.info("requalify_complete", total=len(unqualified), vertical="tech")


async def main(source: str) -> None:
    logger.info("pipeline_start", source=source, vertical="tech")

    # Initialize components
    supabase = await get_supabase()
    repo = LeadsRepository(supabase)
    qualifier = LeadQualifier()
    drafter = EmailDrafter()
    enricher = ContentEnricher()
    serper_client = SerperClient()
    gemini_limiter = GeminiRateLimiter(max_per_minute=60)
    hitl_url = os.environ.get("HITL_GATEWAY_URL", "")

    # Search via Serper API (all queries in parallel)
    valid_sources = {"upwork", "linkedin", "weworkremotely", "glassdoor",
                     "wellfound", "otta", "efinancialcareers", "remoteok"}
    if source in valid_sources or source == "all":
        leads = await search_leads(serper_client, source)
    else:
        logger.error("unknown_source", source=source)
        return

    logger.info("search_complete", source=source, leads_found=len(leads))

    # Process leads with bounded concurrency
    sem = asyncio.Semaphore(10)

    async def _bounded(lead: dict) -> None:
        async with sem:
            await process_lead(
                lead=lead,
                source=source,
                repo=repo,
                qualifier=qualifier,
                drafter=drafter,
                enricher=enricher,
                gemini_limiter=gemini_limiter,
                hitl_url=hitl_url,
            )

    results = await asyncio.gather(
        *[_bounded(lead) for lead in leads], return_exceptions=True
    )
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
        choices=["upwork", "linkedin", "weworkremotely", "glassdoor",
                 "wellfound", "otta", "efinancialcareers", "remoteok", "all"],
        help="Data source to search via Serper API",
    )
    parser.add_argument(
        "--requalify",
        action="store_true",
        help="Re-qualify raw_leads that failed qualification (e.g. after API key fix)",
    )
    args = parser.parse_args()
    if args.requalify:
        asyncio.run(requalify())
    elif args.source:
        asyncio.run(main(args.source))
    else:
        parser.error("either --source or --requalify is required")
