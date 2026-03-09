"""
Serper-based lead sourcing for Vertical 4 — LGaaS Prospects.

Targets boutique consulting firms in 5 high-value niches as prospects
for Sebastián's Lead Generation as a Service (LGaaS) offering:
  1. fractional_cfo   — Fractional CFO agencies
  2. ma_advisory      — M&A advisory boutiques (lower middle market)
  3. cmmc_security    — CMMC/Cybersecurity compliance consultants
  4. ai_automation    — AI automation / workflow agencies
  5. esg_consulting   — ESG / sustainability consultants

Each niche has 3 Google Dork queries (one per pool), targeting
firm websites, LinkedIn company profiles, and directories like Clutch.

Budget: 5 queries/run × 3 runs/day × 30 days = 450 queries/month.
Uses SERPER_API_KEY_V4 env var if set, else falls back to SERPER_API_KEY.

Pools rotate by UTC hour (same convention as v1/v3):
  pool_a → hours 0-7
  pool_b → hours 8-15
  pool_c → hours 16-23
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from datetime import datetime, timezone

import structlog

logger = structlog.get_logger(__name__)

# Max leads kept per niche before round-robin interleaving.
PER_SOURCE_CAP = 6

# ── Search configs: 5 niches × 3 pools × 1 query each ───────────
SEARCH_CONFIGS: dict[str, dict[str, list[str]]] = {
    # ── Fractional CFO ────────────────────────────────────────────
    "fractional_cfo": {
        "pool_a": [
            'intitle:"fractional CFO" (services OR firm OR agency) boutique -job -indeed -glassdoor',
        ],
        "pool_b": [
            'intitle:"fractional CFO" boutique (services OR advisory OR firm) -job -indeed',
        ],
        "pool_c": [
            '"fractional CFO" firm boutique "portfolio companies" OR "Series A" OR "runway" -job',
        ],
    },

    # ── M&A Advisory ──────────────────────────────────────────────
    "ma_advisory": {
        "pool_a": [
            'intitle:"M&A advisory" boutique "lower middle market" (firm OR advisors) -job -hiring',
        ],
        "pool_b": [
            'intitle:"mergers acquisitions" boutique (advisor OR advisory) "sell-side" OR "buy-side" -job',
        ],
        "pool_c": [
            '"M&A advisory" boutique "success fee" "deal value" "lower middle market" -careers -job',
        ],
    },

    # ── CMMC / Cybersecurity ──────────────────────────────────────
    "cmmc_security": {
        "pool_a": [
            'intitle:"CMMC compliance" (consultant OR firm OR consultancy) boutique -"big 4" -job',
        ],
        "pool_b": [
            'site:linkedin.com/company/ "CMMC" OR "NIST 800-171" compliance consulting boutique -hiring',
        ],
        "pool_c": [
            '"CMMC Level 2" compliance "vCISO" OR "gap assessment" consulting boutique "defense contractor" -job',
        ],
    },

    # ── AI Automation ─────────────────────────────────────────────
    "ai_automation": {
        "pool_a": [
            'intitle:"AI automation" agency boutique (workflow OR implementation OR "professional services") -job -hiring',
        ],
        "pool_b": [
            'site:clutch.co "AI automation" OR "workflow automation" agency boutique',
        ],
        "pool_c": [
            '"AI automation" agency boutique ("n8n" OR "Make" OR "Zapier" OR "process automation") -job',
        ],
    },

    # ── ESG Consulting ────────────────────────────────────────────
    "esg_consulting": {
        "pool_a": [
            'intitle:"ESG consulting" boutique (advisory OR firm OR services) -job -hiring -indeed',
        ],
        "pool_b": [
            'intitle:"ESG" OR intitle:"sustainability consulting" boutique "CSRD" compliance -job',
        ],
        "pool_c": [
            '"ESG reporting" OR "carbon footprint" consulting boutique manufacturing "supply chain" -job',
        ],
    },
}

VALID_SOURCES = set(SEARCH_CONFIGS.keys())


def _get_pool_for_run() -> str:
    """Rotate pool by UTC hour: 0-7 → pool_a, 8-15 → pool_b, 16-23 → pool_c."""
    hour = datetime.now(timezone.utc).hour
    slot = (hour // 8) % 3
    pool = ["pool_a", "pool_b", "pool_c"][slot]
    logger.info("keyword_pool_selected", pool=pool, utc_hour=hour, vertical="lgaas")
    return pool


def _normalize_result(result: dict, source: str, keyword: str = "") -> dict:
    """Normalize a Serper organic result into the pipeline lead dict format."""
    return {
        "title": result.get("title", ""),
        "url": result.get("link", ""),
        "description": result.get("snippet", ""),
        "company": "",
        "source_site": source,
        "search_keyword": keyword,
    }


async def search_leads(serper_client, source: str) -> list[dict]:
    """Search for LGaaS prospect (boutique consulting firm) leads via Serper.

    Args:
        serper_client: Shared SerperClient instance.
        source: Niche sub-vertical name or 'all'.

    Returns:
        list[dict] compatible with the pipeline.
    """
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN_V4", "25"))
    seen_urls: set[str] = set()

    active_sources = list(SEARCH_CONFIGS.keys()) if source == "all" else [source]
    pool = _get_pool_for_run()

    # Build query list
    tasks = []
    task_meta: list[tuple[str, str]] = []  # (source, keyword)

    for src in active_sources:
        config = SEARCH_CONFIGS.get(src)
        if not config:
            logger.warning("unknown_search_source", source=src, vertical="lgaas")
            continue
        queries = config.get(pool, [])
        for query in queries:
            tasks.append(serper_client.search(query=query, num=10, gl="us"))
            task_meta.append((src, query))

    logger.info(
        "keyword_selection_static",
        mode="static",
        pool=pool,
        total_queries=len(tasks),
        vertical="lgaas",
    )

    # Fire in batches of 4 (Serper rate limit: 5 req/s)
    results_list: list = []
    for i in range(0, len(tasks), 4):
        batch = tasks[i: i + 4]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        results_list.extend(batch_results)
        if i + 4 < len(tasks):
            await asyncio.sleep(1.1)

    # Group by source for round-robin interleaving
    leads_by_source: dict[str, list[dict]] = {}

    for (src, keyword), results in zip(task_meta, results_list):
        if isinstance(results, Exception):
            logger.error(
                "search_query_failed",
                source=src,
                keyword=keyword[:80],
                error=str(results),
                vertical="lgaas",
            )
            continue

        for result in results:
            lead = _normalize_result(result, src, keyword=keyword)
            url = lead["url"]

            if _is_noise_domain(url):
                continue

            if url in seen_urls:
                continue
            seen_urls.add(url)

            if src not in leads_by_source:
                leads_by_source[src] = []
            if len(leads_by_source[src]) < PER_SOURCE_CAP:
                leads_by_source[src].append(lead)

        logger.info(
            "search_keyword_complete",
            source=src,
            query_snippet=keyword[:60],
            results_found=len(results) if not isinstance(results, Exception) else 0,
            leads_for_source=len(leads_by_source.get(src, [])),
            vertical="lgaas",
        )

    # Round-robin interleave across niches
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
        pool=pool,
        total_leads=len(all_leads),
        per_source={src: len(leads) for src, leads in leads_by_source.items()},
        selected={src: c for src, c in Counter(l["source_site"] for l in all_leads).items()},
        vertical="lgaas",
    )
    return all_leads


# Domains to skip — job boards, social media, news sites, directories
_NOISE_DOMAINS = (
    "indeed.com", "glassdoor.com", "linkedin.com", "ziprecruiter.com",
    "monster.com", "careerbuilder.com", "simplyhired.com",
    "amazon.com", "alibaba.com", "ebay.com",
    "pinterest.com", "instagram.com", "facebook.com", "twitter.com", "x.com",
    "youtube.com", "reddit.com", "quora.com",
    "wikipedia.org", "bloomberg.com", "reuters.com",
    "yelp.com", "bbb.org", "crunchbase.com", "angellist.com",
)


def _is_noise_domain(url: str) -> bool:
    """Return True if the URL is from a noise domain to skip."""
    url_lower = url.lower()
    return any(domain in url_lower for domain in _NOISE_DOMAINS)
