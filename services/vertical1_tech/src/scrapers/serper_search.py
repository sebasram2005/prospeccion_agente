"""
Search API-based lead sourcing for Vertical 1 — Tech Services.

Replaces direct HTTP scrapers (upwork_poller.py, linkedin_scraper.py)
with Serper.dev Google Search API queries.

Google has already indexed job board pages, so we get results
without touching Upwork/LinkedIn directly — no IP blocking.

Keywords are aligned to the full portfolio at sebastianramirezanalytics.com:
  Finance/Quant (NVIDIA, LATAM) · ML/Analytics (Olist) ·
  Forecasting/Optimization (Favorita) · Product Engineering (UNfresh, JobPilot) ·
  BI Dashboards (Power BI, Tableau, Looker) · AI/LLM Integration

To stay within Serper free tier (2,500 queries/month), keywords are split
into two pools (A/B) and rotated every run.
"""

from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timezone

import structlog

logger = structlog.get_logger(__name__)

# ── Full keyword sets per source ────────────────────────────────
# Each source splits keywords into pool_a and pool_b for rotation.

SEARCH_CONFIGS = {
    # ── Upwork (10 keywords per pool) ────────────────────────────
    "upwork": {
        "pool_a": [
            "Python developer",
            "data analyst freelance",
            "SaaS MVP developer",
            "data pipeline engineer",
            "ETL developer",
            # Finance/Quant (NVIDIA, LATAM)
            "financial modeling Python",
            "Monte Carlo simulation",
            # ML/Data Science (Olist)
            "machine learning engineer",
            # BI (Power BI, Tableau, Looker)
            "Power BI developer freelance",
            # Forecasting (Favorita)
            "demand forecasting developer",
        ],
        "pool_b": [
            "Python developer",
            "data analyst freelance",
            "SaaS MVP developer",
            "web scraping project",
            # Finance/Quant
            "risk analysis developer",
            # ML/Data Science
            "customer segmentation analytics",
            "predictive modeling freelance",
            # Product/Full-Stack (UNfresh, JobPilot)
            "FastAPI developer",
            # AI/LLM (JobPilot)
            "AI integration developer",
            # BI
            "Tableau developer",
        ],
        "query_template": 'site:upwork.com/freelance-jobs "{keyword}" budget',
    },
    # ── LinkedIn (7 keywords per pool) ───────────────────────────
    "linkedin": {
        "pool_a": [
            "Python developer remote contract",
            "data analyst startup",
            "backend engineer freelance",
            "data engineer remote",
            "Python automation",
            # Finance/Quant
            "financial analyst Python remote",
            # ML
            "machine learning engineer remote",
        ],
        "pool_b": [
            "Python developer remote contract",
            "data analyst startup",
            "backend engineer freelance",
            "data engineer remote",
            # ML
            "data scientist remote contract",
            # BI
            "business intelligence analyst remote",
            # Product
            "full stack Python developer remote",
        ],
        "query_template": 'site:linkedin.com/jobs "{keyword}"',
    },
    # ── WeWorkRemotely (5 keywords per pool) ─────────────────────
    "weworkremotely": {
        "pool_a": [
            "Python developer",
            "data engineer",
            "backend developer",
            "full stack Python",
            "data scientist",
        ],
        "pool_b": [
            "Python developer",
            "data engineer",
            "backend developer",
            "machine learning",
            "business intelligence",
        ],
        "query_template": 'site:weworkremotely.com "{keyword}"',
    },
    # ── Indeed (4 keywords per pool) ─────────────────────────────
    "indeed": {
        "pool_a": [
            "Python developer contract remote",
            "data analyst freelance remote",
            "Python automation remote",
            "financial analyst Python remote",
        ],
        "pool_b": [
            "Python developer contract remote",
            "data analyst freelance remote",
            "machine learning engineer remote",
            "business intelligence developer remote",
        ],
        "query_template": 'site:indeed.com/viewjob "{keyword}"',
    },
}
# Pool A: 10+7+5+4 = 26 queries/run
# Pool B: 10+7+5+4 = 26 queries/run
# 26 avg × 3 runs/day × 30 days = 2,340 queries/month (under 2,500 free tier)


def _get_pool_for_run() -> str:
    """Determine which keyword pool to use for this run.

    Uses the current UTC hour to alternate: even-hour runs get pool_a,
    odd-hour runs get pool_b.  With cron '*/8 * * * *' (hours 0, 8, 16)
    this produces a predictable A-B-A rotation each day.
    """
    hour = datetime.now(timezone.utc).hour
    pool = "pool_a" if (hour // 8) % 2 == 0 else "pool_b"
    logger.info("keyword_pool_selected", pool=pool, utc_hour=hour)
    return pool

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


async def search_leads(serper_client, source: str) -> list[dict]:
    """Search for tech leads using Serper API.

    All queries run in parallel — Serper is a paid API, no need for delays.
    Keywords are rotated between pool_a / pool_b each run to stay within
    the Serper free tier (2,500 queries/month).

    Args:
        serper_client: Shared SerperClient instance.
        source: One of 'upwork', 'linkedin', 'weworkremotely', 'indeed', or 'all'.

    Returns:
        list[dict] compatible with the existing pipeline.
    """
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN", "30"))
    seen_urls: set[str] = set()
    all_leads: list[dict] = []

    sources = list(SEARCH_CONFIGS.keys()) if source == "all" else [source]
    pool = _get_pool_for_run()

    # Build all queries upfront
    tasks = []
    task_meta = []  # (source, keyword) for each task
    for src in sources:
        config = SEARCH_CONFIGS.get(src)
        if not config:
            logger.warning("unknown_search_source", source=src)
            continue
        keywords = config.get(pool, config.get("pool_a", []))
        for keyword in keywords:
            query = config["query_template"].format(keyword=keyword)
            tasks.append(serper_client.search(query=query, num=10, tbs="qdr:w"))
            task_meta.append((src, keyword))

    # Fire queries in batches of 4 to respect Serper rate limit (5 req/s)
    results_list: list = []
    for i in range(0, len(tasks), 4):
        batch = tasks[i : i + 4]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        results_list.extend(batch_results)
        if i + 4 < len(tasks):
            await asyncio.sleep(1.1)

    for (src, keyword), results in zip(task_meta, results_list):
        if isinstance(results, Exception):
            logger.error("search_query_failed", source=src, keyword=keyword, error=str(results))
            continue

        for result in results:
            lead = _normalize_result(result, src)

            if lead["url"] in seen_urls:
                continue
            seen_urls.add(lead["url"])

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

    logger.info(
        "search_leads_complete",
        source=source,
        total_leads=len(all_leads),
    )
    return all_leads[:max_leads]
