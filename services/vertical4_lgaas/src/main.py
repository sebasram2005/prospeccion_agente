"""
Vertical 4 — LGaaS Prospects: Main entrypoint.

Targets boutique consulting firms in 5 high-value niches as prospects
for Sebastián's Lead Generation as a Service (LGaaS) offering:
  fractional_cfo | ma_advisory | cmmc_security | ai_automation | esg_consulting

Usage:
    python -m services.vertical4_lgaas.src.main --source all
    python -m services.vertical4_lgaas.src.main --source fractional_cfo
    python -m services.vertical4_lgaas.src.main --requalify
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

import httpx
import structlog

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

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from shared.utils.rate_limiter import GeminiRateLimiter
from shared.utils.content_enricher import ContentEnricher

_serper_key = os.environ.get("SERPER_API_KEY_V4") or os.environ.get("SERPER_API_KEY")
if _serper_key:
    os.environ["SERPER_API_KEY"] = _serper_key

from shared.utils.serper_client import SerperClient

from .db_client import get_supabase, LeadsRepository
from .qualifier import LeadQualifier
from .email_drafter import EmailDrafter
from .scrapers.serper_search import search_leads, VALID_SOURCES


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
    """Process a single consulting firm website through the full pipeline."""
    url = lead.get("url", "")
    lead_source = lead.get("source_site", source)

    # 1. Dedup check
    if url and await repo.is_duplicate(url):
        logger.info("lead_skipped_duplicate", url=url, source=lead_source, vertical="lgaas")
        return

    # 2. Insert raw lead
    raw_lead_id = await repo.insert_raw_lead(
        {
            "source": lead_source,
            "vertical": "lgaas",
            "url": url,
            "raw_data": lead,
            "search_keyword": lead.get("search_keyword", ""),
        }
    )
    if not raw_lead_id:
        return

    # 3. Enrich: fetch full page content via Jina Reader
    enriched_lead = await enricher.enrich_lead(lead)

    # 4. Qualify with Gemini
    raw_text = json.dumps(enriched_lead, indent=2)
    result = await qualifier.qualify(raw_text, rate_limiter=gemini_limiter)
    if result is None:
        await repo.mark_as_processed(raw_lead_id)
        return

    # 5. Resolve contact info
    company_name = result.inferred_company or lead.get("title", "your firm")
    first_name = result.contact_name or "there"
    email = result.contact_email or ""

    # 6. If no email from qualifier, try scraping from company website
    if not email and result.company_website:
        scraped_email = await enricher.scrape_email_from_website(result.company_website)
        if scraped_email:
            email = scraped_email

    # 7. Insert qualified lead
    qualified_lead_id = await repo.insert_qualified_lead(
        {
            "raw_lead_id": raw_lead_id,
            "vertical": "lgaas",
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

    # 8. Email dedup
    if email and await repo.is_already_emailed(email):
        logger.info(
            "lead_skipped_already_emailed",
            email=email,
            company=company_name,
            vertical="lgaas",
        )
        await repo.mark_as_processed(raw_lead_id)
        return

    # 9. Draft cold outreach email
    to_address = email or "pending@manual-lookup.com"
    draft = await drafter.draft(
        first_name=first_name,
        company_name=company_name,
        email=to_address,
        pain_point=result.pain_point,
        source=result.niche_category,
        key_technology="",
        suggested_angle=result.suggested_angle,
        rate_limiter=gemini_limiter,
    )
    if not draft:
        await repo.mark_as_processed(raw_lead_id)
        return

    # 10. Insert into email queue
    queue_id = await repo.create_email_queue_entry(
        {
            "qualified_lead_id": qualified_lead_id,
            "vertical": "lgaas",
            "to_email": draft.to_email,
            "subject": draft.subject,
            "body": draft.body,
            "status": "pending",
            "source": lead_source,
            "job_url": url,
        }
    )

    # 11. Notify HITL Gateway
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
                    vertical="lgaas",
                )
        except Exception as exc:
            logger.error(
                "hitl_notify_failed",
                queue_id=queue_id,
                error=str(exc),
                vertical="lgaas",
            )

    await repo.mark_as_processed(raw_lead_id)
    logger.info(
        "lead_processed",
        raw_lead_id=raw_lead_id,
        qualified_lead_id=qualified_lead_id,
        queue_id=queue_id,
        fit_score=result.fit_score,
        niche=result.niche_category,
        company=company_name,
        email_found=email != "",
        source=lead_source,
        vertical="lgaas",
    )


async def requalify() -> None:
    """Re-process raw_leads that failed qualification (e.g. Gemini 403)."""
    logger.info("requalify_start", vertical="lgaas")

    supabase = await get_supabase()
    repo = LeadsRepository(supabase)
    qualifier = LeadQualifier()
    drafter = EmailDrafter()
    enricher = ContentEnricher()
    gemini_limiter = GeminiRateLimiter(max_per_minute=12)
    hitl_url = os.environ.get("HITL_GATEWAY_URL", "")

    unqualified = await repo.fetch_unqualified_leads()
    if not unqualified:
        logger.info("requalify_nothing_to_do", vertical="lgaas")
        return

    sem = asyncio.Semaphore(4)

    async def _process_one(row: dict) -> None:
        async with sem:
            raw_lead_id = row["id"]
            lead = row.get("raw_data", {})
            lead_source = lead.get("source_site", row.get("source", "unknown"))
            url = row.get("url", "")

            await repo.supabase.table("raw_leads").update(
                {"processed": False}
            ).eq("id", raw_lead_id).execute()

            enriched_lead = await enricher.enrich_lead(lead)
            raw_text = json.dumps(enriched_lead, indent=2)
            result = await qualifier.qualify(raw_text, rate_limiter=gemini_limiter)
            if result is None:
                await repo.mark_as_processed(raw_lead_id)
                return

            company_name = result.inferred_company or lead.get("title", "your firm")
            first_name = result.contact_name or "there"
            email = result.contact_email or ""

            if not email and result.company_website:
                scraped_email = await enricher.scrape_email_from_website(result.company_website)
                if scraped_email:
                    email = scraped_email

            qualified_lead_id = await repo.insert_qualified_lead(
                {
                    "raw_lead_id": raw_lead_id,
                    "vertical": "lgaas",
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

            if email and await repo.is_already_emailed(email):
                await repo.mark_as_processed(raw_lead_id)
                return

            to_address = email or "pending@manual-lookup.com"
            draft = await drafter.draft(
                first_name=first_name,
                company_name=company_name,
                email=to_address,
                pain_point=result.pain_point,
                source=result.niche_category,
                key_technology="",
                suggested_angle=result.suggested_angle,
                rate_limiter=gemini_limiter,
            )
            if not draft:
                await repo.mark_as_processed(raw_lead_id)
                return

            queue_id = await repo.create_email_queue_entry(
                {
                    "qualified_lead_id": qualified_lead_id,
                    "vertical": "lgaas",
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
                niche=result.niche_category,
                vertical="lgaas",
            )

    results = await asyncio.gather(
        *[_process_one(row) for row in unqualified], return_exceptions=True
    )
    for row, result in zip(unqualified, results):
        if isinstance(result, Exception):
            logger.error("requalify_error", raw_lead_id=row["id"], error=str(result))

    logger.info("requalify_complete", total=len(unqualified), vertical="lgaas")


async def main(source: str) -> None:
    logger.info("pipeline_start", source=source, vertical="lgaas")

    supabase = await get_supabase()
    repo = LeadsRepository(supabase)
    qualifier = LeadQualifier()
    drafter = EmailDrafter()
    enricher = ContentEnricher()
    serper_client = SerperClient()
    gemini_limiter = GeminiRateLimiter(max_per_minute=12)
    hitl_url = os.environ.get("HITL_GATEWAY_URL", "")

    leads = await search_leads(serper_client, source)
    logger.info("search_complete", source=source, leads_found=len(leads), vertical="lgaas")

    sem = asyncio.Semaphore(4)

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
                vertical="lgaas",
            )

    await repo.update_keyword_performance()
    logger.info("pipeline_complete", source=source, vertical="lgaas")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertical 4 LGaaS Prospects Scraper")
    parser.add_argument(
        "--source",
        choices=[*sorted(VALID_SOURCES), "all"],
        help="Niche sub-vertical to search, or 'all'",
    )
    parser.add_argument(
        "--requalify",
        action="store_true",
        help="Re-qualify raw_leads that failed qualification",
    )
    args = parser.parse_args()
    if args.requalify:
        asyncio.run(requalify())
    elif args.source:
        asyncio.run(main(args.source))
    else:
        parser.error("either --source or --requalify is required")
