"""
Search API-based lead sourcing for Vertical 1 — Tech Services.

Replaces direct HTTP scrapers (upwork_poller.py, linkedin_scraper.py)
with Serper.dev Google Search API queries.

Google has already indexed job board pages, so we get results
without touching Upwork/LinkedIn directly — no IP blocking.
"""

from __future__ import annotations

import os
import re

import structlog

logger = structlog.get_logger(__name__)

SEARCH_CONFIGS = {
    "upwork": {
        "keywords": [
            "Python developer",
            "data analyst freelance",
            "SaaS MVP developer",
            "data pipeline engineer",
        ],
        "query_template": 'site:upwork.com/freelance-jobs "{keyword}" budget',
    },
    "linkedin": {
        "keywords": [
            "Python developer remote contract",
            "data analyst startup",
            "backend engineer freelance",
        ],
        "query_template": 'site:linkedin.com/jobs "{keyword}"',
    },
    "weworkremotely": {
        "keywords": [
            "Python developer",
            "data engineer",
            "backend developer",
        ],
        "query_template": 'site:weworkremotely.com "{keyword}"',
    },
    "indeed": {
        "keywords": [
            "Python developer contract remote",
            "data analyst freelance remote",
        ],
        "query_template": 'site:indeed.com/viewjob "{keyword}"',
    },
}

MIN_BUDGET = int(os.environ.get("MIN_BUDGET_USD", "500"))


def _extract_budget(text: str) -> int:
    """Extract budget amount from snippet text."""
    amounts = re.findall(r"\$[\d,]+", text)
    if not amounts:
        return 0
    return int(amounts[0].replace("$", "").replace(",", ""))


def _normalize_result(result: dict, source: str) -> dict:
    """Normalize a Serper organic result into the pipeline's lead dict format."""
    snippet = result.get("snippet", "")
    budget_amount = _extract_budget(snippet)

    return {
        "title": result.get("title", ""),
        "url": result.get("link", ""),
        "description": snippet,
        "company": "",
        "budget_text": f"${budget_amount:,}" if budget_amount else "",
        "budget_amount": budget_amount,
        "source_site": source,
    }


async def search_leads(serper_client, source: str, rate_limiter=None) -> list[dict]:
    """Search for tech leads using Serper API.

    Args:
        serper_client: Shared SerperClient instance.
        source: One of 'upwork', 'linkedin', 'weworkremotely', 'indeed', or 'all'.
        rate_limiter: Optional rate limiter between queries.

    Returns:
        list[dict] compatible with the existing pipeline.
    """
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN", "30"))
    all_leads: list[dict] = []
    seen_urls: set[str] = set()

    sources = list(SEARCH_CONFIGS.keys()) if source == "all" else [source]

    for src in sources:
        config = SEARCH_CONFIGS.get(src)
        if not config:
            logger.warning("unknown_search_source", source=src)
            continue

        for keyword in config["keywords"]:
            if len(all_leads) >= max_leads:
                break

            query = config["query_template"].format(keyword=keyword)

            if rate_limiter:
                await rate_limiter.wait()

            results = await serper_client.search(query=query, num=10)

            for result in results:
                lead = _normalize_result(result, src)

                # Dedup within this run
                if lead["url"] in seen_urls:
                    continue
                seen_urls.add(lead["url"])

                # For upwork, filter by budget if detectable
                if src == "upwork" and lead["budget_amount"] > 0 and lead["budget_amount"] < MIN_BUDGET:
                    continue

                all_leads.append(lead)

            logger.info(
                "search_keyword_complete",
                source=src,
                keyword=keyword,
                results_found=len(results),
                total_leads=len(all_leads),
            )

        if len(all_leads) >= max_leads:
            break

    logger.info(
        "search_leads_complete",
        source=source,
        total_leads=len(all_leads),
    )
    return all_leads[:max_leads]
