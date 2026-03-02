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

Keywords are distributed across all 8 platforms in three pools (A/B/C),
rotated every run. Each platform appears in at least 2 pools for
balanced coverage. Results are interleaved round-robin across sources
to prevent any single platform from dominating the 30-lead cap.

  Pool A (00:05 UTC): upwork+linkedin+wwr+glassdoor+wellfound+otta+remoteok
  Pool B (08:05 UTC): upwork+linkedin+wwr+glassdoor+efin+wellfound+remoteok
  Pool C (16:05 UTC): upwork+linkedin+wellfound+otta+efin+remoteok
"""

from __future__ import annotations

import asyncio
import os
import re
from collections import Counter
from datetime import datetime, timezone

import structlog

logger = structlog.get_logger(__name__)

# ── Per-source time filter ──────────────────────────────────────
# High-volume platforms: past week. Low-volume: past month for better recall.
TIME_FILTER: dict[str, str] = {
    "upwork": "qdr:w",
    "linkedin": "qdr:w",
    "glassdoor": "qdr:w",
    "weworkremotely": "qdr:m",
    "wellfound": "qdr:m",
    "otta": "qdr:m",
    "efinancialcareers": "qdr:m",
    "remoteok": "qdr:m",
}

# Max leads kept per source before round-robin interleaving.
# Ensures no single platform dominates the MAX_LEADS_PER_RUN cap.
PER_SOURCE_CAP = 8

# ── Full keyword sets per source ────────────────────────────────
# Each source splits keywords into pool_a, pool_b, and/or pool_c for rotation.
# Sources may define any subset of pools; missing pools are silently skipped.

SEARCH_CONFIGS = {
    # ── Upwork (pool_a: 5, pool_b: 5, pool_c: 4 = 14 keywords) ──
    "upwork": {
        "pool_a": [
            "financial model",
            "Monte Carlo simulation",
            "churn prediction",
            "demand forecasting",
            "dashboard development",
        ],
        "pool_b": [
            "DCF valuation",
            "credit scoring",
            "predictive modeling",
            "supply chain analytics",
            "Python automation",
        ],
        "pool_c": [
            "AI strategy consultant",
            "fractional finance",
            "quantitative modeling",
            "fintech product manager",
        ],
        "query_template": 'site:upwork.com/freelance-jobs "{keyword}"',
    },
    # ── LinkedIn (pool_a: 5, pool_b: 5, pool_c: 4 = 14 keywords) ──
    "linkedin": {
        "pool_a": [
            "quantitative analyst remote",
            "data scientist contract",
            "financial analyst Python",
            "machine learning engineer contract",
            "business intelligence developer",
        ],
        "pool_b": [
            "forecasting analyst remote",
            "analytics engineer remote",
            "Python backend developer remote",
            "fintech data analyst",
            "data analyst contract",
        ],
        "pool_c": [
            "fractional CFO remote",
            "AI product strategy",
            "head of analytics remote",
            "founding PM fintech",
        ],
        "query_template": 'site:linkedin.com/jobs "{keyword}"',
    },
    # ── WeWorkRemotely (pool_a: 3, pool_b: 3 = 6 keywords) ────
    "weworkremotely": {
        "pool_a": [
            "data engineer",
            "Python",
            "machine learning",
        ],
        "pool_b": [
            "data scientist",
            "backend engineer",
            "analytics",
        ],
        "query_template": 'site:weworkremotely.com "{keyword}"',
    },
    # ── Glassdoor (pool_a: 3, pool_b: 3 = 6 keywords) ──────────
    # Replaces Indeed (blocked Google indexing since 2017).
    "glassdoor": {
        "pool_a": [
            "data analyst Python remote",
            "quantitative analyst",
            "Python developer contract",
        ],
        "pool_b": [
            "data scientist remote",
            "machine learning engineer",
            "business intelligence analyst",
        ],
        "query_template": 'site:glassdoor.com/job-listing "{keyword}"',
    },
    # ── Wellfound (pool_a: 4, pool_b: 3, pool_c: 5 = 12 keywords) ──
    "wellfound": {
        "pool_a": [
            "data scientist startup",
            "founding engineer",
            "AI product manager",
            "head of data",
        ],
        "pool_b": [
            "fractional CFO",
            "strategic finance lead",
            "founding product manager",
        ],
        "pool_c": [
            "staff economist",
            "fractional CTO",
            "quantitative analyst",
            "machine learning lead",
            "analytics engineer",
        ],
        "query_template": 'site:wellfound.com/jobs "{keyword}"',
    },
    # ── Otta / Welcome to the Jungle (pool_a: 3, pool_c: 5 = 8 keywords) ──
    "otta": {
        "pool_a": [
            "data analyst remote",
            "Python developer",
            "business intelligence",
        ],
        "pool_c": [
            "product strategy lead",
            "data product manager",
            "SaaS strategy",
            "quantitative product manager",
            "senior analytics",
        ],
        "query_template": 'site:welcometothejungle.com/en "{keyword}"',
    },
    # ── eFinancialCareers (pool_b: 4, pool_c: 5 = 9 keywords) ──
    "efinancialcareers": {
        "pool_b": [
            "quantitative developer contract",
            "risk consultant remote",
            "financial engineer contract",
            "derivatives analyst remote",
        ],
        "pool_c": [
            "model validation contract",
            "risk analyst remote",
            "quantitative researcher",
            "credit risk consultant",
            "algo trading developer",
        ],
        "query_template": 'site:efinancialcareers.com "{keyword}"',
    },
    # ── RemoteOK (pool_a: 4, pool_b: 4, pool_c: 4 = 12 keywords) ──
    "remoteok": {
        "pool_a": [
            "data scientist",
            "Python developer",
            "machine learning",
            "data analyst",
        ],
        "pool_b": [
            "data engineer",
            "backend developer",
            "analytics",
            "quantitative analyst",
        ],
        "pool_c": [
            "AI engineer",
            "fintech",
            "full stack Python",
            "business intelligence",
        ],
        "query_template": 'site:remoteok.com/remote-jobs "{keyword}"',
    },
}
# Pool A: 5+5+3+3+4+3+4 = 27 queries (upwork+linkedin+wwr+glassdoor+wellfound+otta+remoteok)
# Pool B: 5+5+3+3+4+3+4 = 27 queries (upwork+linkedin+wwr+glassdoor+efin+wellfound+remoteok)
# Pool C: 4+4+5+5+5+4   = 27 queries (upwork+linkedin+wellfound+otta+efin+remoteok)
# 27 avg x 3 runs/day x 30 days = 2,430 queries/month (under 2,500 free tier)


def _get_pool_for_run() -> str:
    """Determine which keyword pool to use for this run.

    Uses the current UTC hour to rotate among three pools:
    hour 0 → pool_a (base), hour 8 → pool_b (base alt), hour 16 → pool_c (premium).
    With cron '*/8 * * * *' (hours 0, 8, 16) this produces A-B-C each day.
    """
    hour = datetime.now(timezone.utc).hour
    slot = (hour // 8) % 3
    pool = ["pool_a", "pool_b", "pool_c"][slot]
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

    Keywords are rotated between pool_a / pool_b / pool_c each run to stay
    within the Serper free tier (2,500 queries/month). Results are interleaved
    round-robin across sources so no single platform dominates the lead cap.

    Args:
        serper_client: Shared SerperClient instance.
        source: Source name or 'all'.

    Returns:
        list[dict] compatible with the existing pipeline.
    """
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN", "30"))
    seen_urls: set[str] = set()

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
        keywords = config.get(pool, [])
        if not keywords:
            continue  # Source not active in this pool
        tbs = TIME_FILTER.get(src, "qdr:w")
        for keyword in keywords:
            query = config["query_template"].format(keyword=keyword)
            tasks.append(serper_client.search(query=query, num=10, tbs=tbs))
            task_meta.append((src, keyword))

    # Fire queries in batches of 4 to respect Serper rate limit (5 req/s)
    results_list: list = []
    for i in range(0, len(tasks), 4):
        batch = tasks[i : i + 4]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        results_list.extend(batch_results)
        if i + 4 < len(tasks):
            await asyncio.sleep(1.1)

    # Group results by source for round-robin interleaving
    leads_by_source: dict[str, list[dict]] = {}

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

            if src not in leads_by_source:
                leads_by_source[src] = []
            if len(leads_by_source[src]) < PER_SOURCE_CAP:
                leads_by_source[src].append(lead)

        logger.info(
            "search_keyword_complete",
            source=src,
            keyword=keyword,
            results_found=len(results) if not isinstance(results, Exception) else 0,
            total_leads_for_source=len(leads_by_source.get(src, [])),
        )

    # Round-robin interleave: take 1 lead from each source per round
    all_leads: list[dict] = []
    source_iters = {src: iter(leads) for src, leads in leads_by_source.items()}
    exhausted: set[str] = set()

    while len(all_leads) < max_leads and len(exhausted) < len(source_iters):
        for src in list(source_iters.keys()):
            if src in exhausted:
                continue
            try:
                lead = next(source_iters[src])
                all_leads.append(lead)
                if len(all_leads) >= max_leads:
                    break
            except StopIteration:
                exhausted.add(src)

    logger.info(
        "search_leads_complete",
        source=source,
        total_leads=len(all_leads),
        leads_per_source={src: len(leads) for src, leads in leads_by_source.items()},
        leads_selected=dict(Counter(lead["source_site"] for lead in all_leads)),
    )
    return all_leads
