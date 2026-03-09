"""
Search API-based lead sourcing for Vertical 1 — Tech Services.

Replaces direct HTTP scrapers (upwork_poller.py, linkedin_scraper.py)
with Serper.dev Google Search API queries.

Google has already indexed job board pages, so we get results
without touching Upwork/LinkedIn directly — no IP blocking.

Keywords target mid-level roles (0-3 years experience) aligned to portfolio:
  Data Analysis · Python Automation · ETL/Pipelines · BI Dashboards ·
  ML/Forecasting · Financial Modeling · Full-Stack MVPs · Web Scraping

Keywords are distributed across all 8 platforms in three pools (A/B/C),
rotated every run. All platforms appear in all 3 pools for balanced
coverage. Results are interleaved round-robin across sources to prevent
any single platform from dominating the 30-lead cap.

When keyword_performance data is available (after ~6 runs), the system
switches to adaptive (epsilon-greedy) keyword selection instead of
fixed pools. See _select_keywords_adaptive().

  Pool A (00:05 UTC): all 8 platforms
  Pool B (08:05 UTC): all 8 platforms
  Pool C (16:05 UTC): all 8 platforms
"""

from __future__ import annotations

import asyncio
import os
import random
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
    # ── Upwork (pool_a: 5, pool_b: 5, pool_c: 5 = 15 keywords) ──
    # Upwork is inherently freelance; no need for "remote"/"contract" qualifiers.
    # VOLUME STRATEGY: Mix of quick-win small projects + mid-level deliverables.
    "upwork": {
        "pool_a": [
            "Python script",
            "web scraping",
            "data cleaning Python",
            "Excel automation",
            "Streamlit dashboard",
        ],
        "pool_b": [
            "Power BI dashboard",
            "data analysis",
            "Python bot",
            "report automation",
            "inventory optimization",
        ],
        "pool_c": [
            "data visualization",
            "API integration Python",
            "financial model",
            "demand forecasting",
            "Python automation",
        ],
        "query_template": 'site:upwork.com/freelance-jobs "{keyword}"',
    },
    # ── LinkedIn (pool_a: 3, pool_b: 3, pool_c: 3 = 9 keywords) ──
    # Reduced from 14: LinkedIn is employment-focused, not freelance-native.
    # All keywords include "remote" + "contract" to filter full-time/on-site.
    "linkedin": {
        "pool_a": [
            "data analyst remote contract",
            "Python developer remote contract",
            "financial analyst remote contract",
        ],
        "pool_b": [
            "analytics engineer remote contract",
            "BI developer remote contract",
            "consulting data remote contract",
        ],
        "pool_c": [
            "data engineer remote contract",
            "quantitative analyst remote contract",
            "full stack developer remote contract",
        ],
        "query_template": 'site:linkedin.com/jobs "{keyword}"',
    },
    # ── WeWorkRemotely (pool_a: 3, pool_b: 3, pool_c: 3 = 9 keywords) ────
    # Platform is 100% remote; no qualifier needed.
    # Added pool_c for coverage in all 3 pools.
    "weworkremotely": {
        "pool_a": [
            "data analyst",
            "Python developer",
            "business intelligence",
        ],
        "pool_b": [
            "data engineer",
            "backend developer",
            "analytics",
        ],
        "pool_c": [
            "full stack",
            "automation",
            "fintech",
        ],
        "query_template": 'site:weworkremotely.com "{keyword}"',
    },
    # ── Glassdoor (pool_a: 3, pool_b: 3, pool_c: 3 = 9 keywords) ──────────
    # All keywords include "remote" — Glassdoor mixes on-site/remote heavily.
    # Added pool_c for coverage in all 3 pools.
    "glassdoor": {
        "pool_a": [
            "data analyst Python remote",
            "junior data scientist remote",
            "Python developer remote contract",
        ],
        "pool_b": [
            "business intelligence analyst remote",
            "financial analyst remote",
            "ETL developer remote",
        ],
        "pool_c": [
            "data engineer remote",
            "backend developer remote contract",
            "analytics remote contract",
        ],
        "query_template": 'site:glassdoor.com/job-listing "{keyword}"',
    },
    # ── Wellfound (pool_a: 5, pool_b: 5, pool_c: 4 = 14 keywords) ──
    # Boosted from 12: startups value skills over years — good fit.
    # All keywords include "remote" — Wellfound mixes on-site startup jobs.
    "wellfound": {
        "pool_a": [
            "data analyst remote startup",
            "Python developer remote startup",
            "full stack developer remote startup",
            "web scraping remote startup",
            "consulting data remote startup",
        ],
        "pool_b": [
            "data engineer remote startup",
            "backend developer remote startup",
            "analytics engineer remote startup",
            "business intelligence remote startup",
            "fintech developer remote",
        ],
        "pool_c": [
            "machine learning remote startup",
            "quantitative analyst remote",
            "data scientist remote startup",
            "automation engineer remote startup",
        ],
        "query_template": 'site:wellfound.com/jobs "{keyword}"',
    },
    # ── Otta / Welcome to the Jungle (pool_a: 3, pool_b: 3, pool_c: 3 = 9 keywords) ──
    # All keywords include "remote" — WTTJ mixes on-site/remote.
    # Added pool_b for coverage in all 3 pools.
    "otta": {
        "pool_a": [
            "data analyst remote",
            "Python developer remote",
            "business intelligence remote",
        ],
        "pool_b": [
            "data engineer remote",
            "backend developer remote",
            "analytics remote",
        ],
        "pool_c": [
            "analytics engineer remote",
            "full stack developer remote",
            "BI analyst remote",
        ],
        "query_template": 'site:welcometothejungle.com/en "{keyword}"',
    },
    # ── eFinancialCareers (pool_a: 3, pool_b: 3, pool_c: 3 = 9 keywords) ──
    # All keywords include "contract" or "remote" to filter full-time on-site.
    # Added pool_a for coverage in all 3 pools.
    "efinancialcareers": {
        "pool_a": [
            "financial analyst contract",
            "risk analyst remote",
            "data analyst finance remote",
        ],
        "pool_b": [
            "Python developer finance contract",
            "quantitative analyst contract",
            "credit analyst remote",
        ],
        "pool_c": [
            "financial data analyst remote",
            "BI analyst finance remote",
            "analytics developer remote contract",
        ],
        "query_template": 'site:efinancialcareers.com "{keyword}"',
    },
    # ── RemoteOK (pool_a: 5, pool_b: 5, pool_c: 4 = 14 keywords) ──
    # Boosted from 12: 100% remote platform, high freelance signal.
    "remoteok": {
        "pool_a": [
            "data analyst",
            "Python developer",
            "data engineer",
            "web scraping",
            "Streamlit",
        ],
        "pool_b": [
            "backend developer",
            "full stack",
            "analytics",
            "machine learning",
            "report automation",
        ],
        "pool_c": [
            "fintech",
            "data scientist",
            "demand forecasting",
            "Python",
        ],
        "query_template": 'site:remoteok.com/remote-jobs "{keyword}"',
    },
}
# Pool A: 5+3+3+3+5+3+3+5 = 30 → trim to 27 via adaptive selection
# Pool B: 5+3+3+3+5+3+3+5 = 30 → trim to 27 via adaptive selection
# Pool C: 5+3+3+3+4+3+3+4 = 28
# All 8 platforms present in all 3 pools for balanced coverage.
# Static budget: ~28 queries/pool x 3 runs/day x 30 days = 2,520 queries/month
# Adaptive selection trims to 27/pool → 2,430/month (under 2,500 free tier)


def _get_pool_for_run() -> str:
    """Determine which keyword pool to use for this run.

    Uses the current UTC hour to rotate among three pools:
    hour 0 → pool_a, hour 8 → pool_b, hour 16 → pool_c.
    With cron '*/8 * * * *' (hours 0, 8, 16) this produces A-B-C each day.
    """
    hour = datetime.now(timezone.utc).hour
    slot = (hour // 8) % 3
    pool = ["pool_a", "pool_b", "pool_c"][slot]
    logger.info("keyword_pool_selected", pool=pool, utc_hour=hour)
    return pool

# ── Adaptive selection constants ──────────────────────────────
MAX_QUERIES_PER_POOL = int(os.environ.get("MAX_QUERIES_PER_POOL", "27"))
COLD_START_THRESHOLD = 6  # minimum keyword_performance rows before switching to adaptive
EPSILON = 0.2  # exploration rate for epsilon-greedy
MIN_KEYWORDS_PER_SOURCE = 2  # minimum keywords per active source

MIN_BUDGET = int(os.environ.get("MIN_BUDGET_USD", "100"))


def _extract_budget(text: str) -> int:
    """Extract budget amount from snippet text."""
    amounts = re.findall(r"\$[\d,]+", text)
    if not amounts:
        return 0
    return int(amounts[0].replace("$", "").replace(",", ""))


def _normalize_result(result: dict, source: str, keyword: str = "") -> dict:
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
        "search_keyword": keyword,
    }


def _get_all_keywords_for_source(source: str) -> list[str]:
    """Get all unique keywords across all pools for a given source."""
    config = SEARCH_CONFIGS.get(source, {})
    keywords: list[str] = []
    seen: set[str] = set()
    for pool_key in ("pool_a", "pool_b", "pool_c"):
        for kw in config.get(pool_key, []):
            if kw not in seen:
                keywords.append(kw)
                seen.add(kw)
    return keywords


def _select_keywords_adaptive(
    sources: list[str],
    keyword_scores: list[dict],
    budget: int,
) -> list[tuple[str, str]]:
    """Select keywords using epsilon-greedy multi-armed bandit.

    Args:
        sources: List of platform names to select keywords for.
        keyword_scores: Rows from keyword_performance table.
        budget: Max total keywords to select.

    Returns:
        List of (source, keyword) tuples to search.
    """
    # Build score lookup: (keyword, source) → score
    score_map: dict[tuple[str, str], float] = {}
    for row in keyword_scores:
        score_map[(row["keyword"], row["source"])] = float(row.get("score", 0.5))

    # Build candidate list per source
    candidates: dict[str, list[tuple[str, float]]] = {}
    for src in sources:
        all_kws = _get_all_keywords_for_source(src)
        scored = [(kw, score_map.get((kw, src), 0.5)) for kw in all_kws]
        candidates[src] = scored

    # Allocate budget across sources proportionally to their best scores
    source_max_scores = {
        src: max((s for _, s in kws), default=0.5) if kws else 0.5
        for src, kws in candidates.items()
    }
    total_score = sum(source_max_scores.values()) or 1.0
    allocation: dict[str, int] = {}
    remaining = budget

    for src in sources:
        alloc = max(MIN_KEYWORDS_PER_SOURCE, round(budget * source_max_scores[src] / total_score))
        # Don't allocate more than available keywords
        alloc = min(alloc, len(candidates.get(src, [])))
        allocation[src] = alloc
        remaining -= alloc

    # Redistribute excess or deficit
    if remaining < 0:
        # Over-allocated — trim sources with most allocation first
        for src in sorted(allocation, key=lambda s: allocation[s], reverse=True):
            if remaining >= 0:
                break
            trim = min(-remaining, allocation[src] - MIN_KEYWORDS_PER_SOURCE)
            if trim > 0:
                allocation[src] -= trim
                remaining += trim

    selected: list[tuple[str, str]] = []

    for src, n_keywords in allocation.items():
        kws = candidates.get(src, [])
        if not kws or n_keywords <= 0:
            continue

        # Epsilon-greedy selection
        n_exploit = max(1, int(n_keywords * (1 - EPSILON)))
        n_explore = n_keywords - n_exploit

        # Exploitation: top scoring keywords
        sorted_kws = sorted(kws, key=lambda x: x[1], reverse=True)
        exploit_picks = [kw for kw, _ in sorted_kws[:n_exploit]]

        # Exploration: random from remaining
        remaining_kws = [kw for kw, _ in sorted_kws[n_exploit:]]
        if remaining_kws and n_explore > 0:
            explore_picks = random.sample(remaining_kws, min(n_explore, len(remaining_kws)))
        else:
            explore_picks = []

        for kw in exploit_picks + explore_picks:
            selected.append((src, kw))

    logger.info(
        "keyword_selection_adaptive",
        mode="adaptive",
        total_selected=len(selected),
        budget=budget,
        per_source={src: n for src, n in allocation.items()},
        exploration_pct=EPSILON,
    )
    return selected


async def search_leads(serper_client, source: str, keyword_scores: list[dict] | None = None) -> list[dict]:
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

    active_sources = list(SEARCH_CONFIGS.keys()) if source == "all" else [source]

    # Decide selection mode: adaptive (MAB) vs fixed pools
    use_adaptive = (
        keyword_scores is not None
        and len(keyword_scores) >= COLD_START_THRESHOLD
    )

    if use_adaptive:
        keyword_pairs = _select_keywords_adaptive(
            active_sources, keyword_scores, MAX_QUERIES_PER_POOL,
        )
    else:
        pool = _get_pool_for_run()
        keyword_pairs = []
        for src in active_sources:
            config = SEARCH_CONFIGS.get(src)
            if not config:
                logger.warning("unknown_search_source", source=src)
                continue
            keywords = config.get(pool, [])
            for keyword in keywords:
                keyword_pairs.append((src, keyword))
        logger.info(
            "keyword_selection_static",
            mode="static",
            pool=pool,
            total_queries=len(keyword_pairs),
        )

    # Build all queries upfront
    tasks = []
    task_meta = []  # (source, keyword) for each task
    for src, keyword in keyword_pairs:
        config = SEARCH_CONFIGS.get(src)
        if not config:
            continue
        tbs = TIME_FILTER.get(src, "qdr:w")
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
            lead = _normalize_result(result, src, keyword=keyword)

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
