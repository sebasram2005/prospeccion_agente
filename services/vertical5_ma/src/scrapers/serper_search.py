"""
Serper-based lead sourcing for Vertical 5 — M&A Silver Tsunami.

Targets traditional, owner-operated businesses in Florida as acquisition
candidates for SunBridge Advisors (boutique M&A, lower middle market).
"Silver Tsunami" thesis: founders of mature businesses (10-30+ years old)
who may be approaching retirement age and considering an exit.

4 search niches:
  1. hvac_plumbing      — Family-owned HVAC, plumbing, mechanical services
  2. manufacturing      — Custom manufacturers, machine shops, fabricators
  3. b2b_saas           — Founder-operated B2B software established 15+ years ago
  4. veteran_founders   — LinkedIn profiles: owners/founders in target industries

Each niche has 3 Google Dork queries (one per pool), rotating by UTC hour.
Budget: 4 queries/run × 3 runs/day × 30 days = 360 queries/month.
Uses SERPER_API_KEY_V5 env var if set, else falls back to SERPER_API_KEY.

Pools rotate by UTC hour (same convention as v1/v3/v4):
  pool_a → hours 0-7
  pool_b → hours 8-15
  pool_c → hours 16-23
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse

import structlog

logger = structlog.get_logger(__name__)

# Max leads kept per niche before round-robin interleaving.
PER_SOURCE_CAP = 6

# ── Search configs: 4 niches × 3 pools × 1 query each ───────────────────────
SEARCH_CONFIGS: dict[str, dict[str, list[str]]] = {

    # ── HVAC & Plumbing ───────────────────────────────────────────────────────
    "hvac_plumbing": {
        "pool_a": [
            '"HVAC" OR "air conditioning" "Florida" "family owned" "serving since 19" -job -hiring -careers',
        ],
        "pool_b": [
            'intitle:"HVAC" OR intitle:"plumbing" "Florida" "family business" "established" "19" -job -careers',
        ],
        "pool_c": [
            '("plumbing" OR "mechanical services") "Florida" "family owned" "since 19" -job -ziprecruiter',
        ],
    },

    # ── Manufacturing & Machining ─────────────────────────────────────────────
    "manufacturing": {
        "pool_a": [
            '("manufacturing" OR "machining") "Florida" "established in 19" "about us" -job -hiring',
        ],
        "pool_b": [
            'intitle:"manufacturing" OR intitle:"machining" "Florida" "family owned" "since 19" -careers -hiring',
        ],
        "pool_c": [
            '("metal fabrication" OR "custom manufacturing") "Florida" "serving customers since 19" -job',
        ],
    },

    # ── B2B SaaS / Legacy Software ────────────────────────────────────────────
    "b2b_saas": {
        "pool_a": [
            'intitle:"software" "Florida" "founded in" ("1995" OR "1998" OR "2000" OR "2002" OR "2005") "B2B" -job -hiring',
        ],
        "pool_b": [
            '("B2B software" OR "enterprise software") "Florida" "founder" OR "CEO" "since 200" OR "since 199" -job -linkedin',
        ],
        "pool_c": [
            'site:clutch.co "Florida" "software" ("founded: 199" OR "founded: 200" OR "founded: 201")',
        ],
    },

    # ── Veteran Founders (LinkedIn Profiles) ─────────────────────────────────
    "veteran_founders": {
        "pool_a": [
            'site:linkedin.com/in/ intitle:"Owner" "HVAC" "Florida" ("Class of 199" OR "Class of 198")',
        ],
        "pool_b": [
            'site:linkedin.com/in/ (intitle:"Owner" OR intitle:"Founder") "Manufacturing" "Florida" "19"',
        ],
        "pool_c": [
            'site:linkedin.com/in/ (intitle:"President" OR intitle:"CEO") "Florida" ("HVAC" OR "Plumbing" OR "Machining") "since 19"',
        ],
    },
}

VALID_SOURCES = set(SEARCH_CONFIGS.keys())


def _get_pool_for_run() -> str:
    """Rotate pool by UTC hour: 0-7 → pool_a, 8-15 → pool_b, 16-23 → pool_c."""
    hour = datetime.now(timezone.utc).hour
    slot = (hour // 8) % 3
    pool = ["pool_a", "pool_b", "pool_c"][slot]
    logger.info("keyword_pool_selected", pool=pool, utc_hour=hour, vertical="ma")
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
    """Search for Silver Tsunami acquisition targets via Serper.

    Args:
        serper_client: Shared SerperClient instance.
        source: Niche sub-vertical name or 'all'.

    Returns:
        list[dict] compatible with the pipeline.
    """
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN_V5", "25"))
    seen_urls: set[str] = set()

    active_sources = list(SEARCH_CONFIGS.keys()) if source == "all" else [source]
    pool = _get_pool_for_run()

    # Build query list
    tasks = []
    task_meta: list[tuple[str, str]] = []  # (source, keyword)

    for src in active_sources:
        config = SEARCH_CONFIGS.get(src)
        if not config:
            logger.warning("unknown_search_source", source=src, vertical="ma")
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
        vertical="ma",
    )

    # Fire in batches of 4 (Serper rate limit: 5 req/s)
    results_list: list = []
    for i in range(0, len(tasks), 4):
        batch = tasks[i : i + 4]
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
                vertical="ma",
            )
            continue

        for result in results:
            lead = _normalize_result(result, src, keyword=keyword)
            url = lead["url"]

            if _is_noise_domain(url, src):
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
            vertical="ma",
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
        vertical="ma",
    )
    return all_leads


# Domains to skip — job boards, news sites, generic directories.
# LinkedIn /company/ and /in/ pages are explicitly allowed (high-signal targets).
_NOISE_DOMAINS: set[str] = {
    "indeed.com", "glassdoor.com", "linkedin.com", "ziprecruiter.com",
    "monster.com", "careerbuilder.com", "simplyhired.com",
    "amazon.com", "alibaba.com", "ebay.com",
    "pinterest.com", "instagram.com", "facebook.com", "twitter.com", "x.com",
    "youtube.com", "reddit.com", "quora.com",
    "wikipedia.org", "bloomberg.com", "reuters.com",
    "yelp.com", "bbb.org", "crunchbase.com", "angellist.com",
}


def _is_noise_domain(url: str, source: str = "") -> bool:
    """Return True if the URL is from a noise domain to skip.

    LinkedIn /company/ and /in/ pages are always allowed through —
    they are the primary signal for the veteran_founders pool.
    """
    try:
        netloc = urlparse(url.lower()).netloc.removeprefix("www.")
        if netloc == "linkedin.com":
            url_lower = url.lower()
            if "/company/" in url_lower or "/in/" in url_lower:
                return False
            return True
        return netloc in _NOISE_DOMAINS or any(
            netloc.endswith("." + d) for d in _NOISE_DOMAINS
        )
    except Exception:
        return True
