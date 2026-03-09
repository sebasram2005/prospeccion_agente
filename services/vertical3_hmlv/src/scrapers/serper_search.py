"""
Serper-based lead sourcing for Vertical 3 — HMLV Manufacturers.

Targets company websites directly via Google Dorks (not job boards).
Each of the 5 industry sub-verticals has 3 dork queries (one per pool),
split geographically: pool_a/pool_b target US, pool_c targets EU.

Budget: 5 queries/run × 3 runs/day × 30 days = 450 queries/month.
Uses SERPER_API_KEY_V3 env var if set, else falls back to SERPER_API_KEY.

Pools rotate by UTC hour (same as vertical1):
  pool_a → hours 0-7   (US dorks)
  pool_b → hours 8-15  (US dorks, different angle)
  pool_c → hours 16-23 (EU dorks)
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse

import structlog

logger = structlog.get_logger(__name__)

# ── Geographic targeting per pool ────────────────────────────────
GEO_PER_POOL: dict[str, str] = {
    "pool_a": "us",
    "pool_b": "us",
    "pool_c": "gb",   # EU — Google UK index has strong EU manufacturing coverage
}

# Max leads kept per source sub-vertical before round-robin interleaving.
PER_SOURCE_CAP = 6

# ── Search configs: 5 industries × 3 pools × 1 query each ───────
# Each pool has exactly 1 query per industry = 5 queries per run.
# Queries are Google Dorks from the ICP intelligence brief.
SEARCH_CONFIGS: dict[str, dict[str, list[str]]] = {
    # ── Trade Show Exhibits ───────────────────────────────────────
    "trade_show": {
        "pool_a": [
            'intitle:"custom trade show booth" OR intitle:"exhibit builder" intext:"in-house fabrication" OR intext:"CNC" -site:pinterest.com',
        ],
        "pool_b": [
            'inurl:capabilities "custom exhibit fabrication" "millwork" OR "woodworking" "booth design" -"rental only"',
        ],
        "pool_c": [
            'intitle:"stand builder" OR intitle:"exhibition stand contractor" inurl:services "in-house production" "Europe" OR "Germany"',
        ],
    },

    # ── Marine Decking ────────────────────────────────────────────
    "marine_decking": {
        "pool_a": [
            'inurl:services "marine decking" OR "boat flooring" "custom CNC" OR "CNC routed" -"buy online" -"add to cart"',
        ],
        "pool_b": [
            'intext:"digital templating" OR "Prodim" OR "Proliner" "marine decking" "fabrication"',
        ],
        "pool_c": [
            '"marine flooring solutions" "CAD design" "CNC cutting" "custom logos" "boat layout"',
        ],
    },

    # ── Architectural Millwork ────────────────────────────────────
    "millwork": {
        "pool_a": [
            'intitle:"architectural millwork" OR intitle:"custom casework" "AWI" "shop drawings" "CNC"',
        ],
        "pool_b": [
            'intext:"custom corporate furniture" OR "commercial millwork" "fabrication facility" "edgebanding" "nested based"',
        ],
        "pool_c": [
            '"architectural woodwork" "custom hospitality furniture" "design-to-fabrication" "CNC machining"',
        ],
    },

    # ── Industrial Crating ────────────────────────────────────────
    "crating": {
        "pool_a": [
            'intitle:"custom wood crates" OR intitle:"industrial packaging" "ISPM-15" "design and engineering" -pallet',
        ],
        "pool_b": [
            'inurl:crating "custom shipping crates" "CAD design" OR "Crate Pro" "heavy machinery"',
        ],
        "pool_c": [
            '"industrial packaging solutions" "knock-down crates" "reusable crates" "CNC cutting"',
        ],
    },

    # ── Metal Facades ─────────────────────────────────────────────
    "metal_facades": {
        "pool_a": [
            'intitle:"metal facade" OR intitle:"architectural metal" "sheet metal fabrication" "laser cutting" OR "press brake"',
        ],
        "pool_b": [
            'intext:"custom metal cladding" OR "ACP panels" "BIM" "CAD/CAM" "shop drawings"',
        ],
        "pool_c": [
            '"architectural metalwork" "custom fabrication" "G-code" "nesting software" "facade"',
        ],
    },
}

VALID_SOURCES = set(SEARCH_CONFIGS.keys())


def _get_pool_for_run() -> str:
    """Rotate pool by UTC hour: 0-7 → pool_a, 8-15 → pool_b, 16-23 → pool_c."""
    hour = datetime.now(timezone.utc).hour
    slot = (hour // 8) % 3
    pool = ["pool_a", "pool_b", "pool_c"][slot]
    logger.info("keyword_pool_selected", pool=pool, utc_hour=hour, vertical="hmlv")
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
    """Search for HMLV manufacturer leads via Serper Google Dorks.

    Args:
        serper_client: Shared SerperClient instance.
        source: Industry sub-vertical name or 'all'.

    Returns:
        list[dict] compatible with the pipeline.
    """
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN_V3", "25"))
    seen_urls: set[str] = set()

    active_sources = list(SEARCH_CONFIGS.keys()) if source == "all" else [source]
    pool = _get_pool_for_run()
    geo = GEO_PER_POOL[pool]

    # Build query list
    tasks = []
    task_meta: list[tuple[str, str]] = []  # (source, keyword)

    for src in active_sources:
        config = SEARCH_CONFIGS.get(src)
        if not config:
            logger.warning("unknown_search_source", source=src, vertical="hmlv")
            continue
        queries = config.get(pool, [])
        for query in queries:
            tasks.append(
                serper_client.search(query=query, num=10, gl=geo)
            )
            task_meta.append((src, query))

    logger.info(
        "keyword_selection_static",
        mode="static",
        pool=pool,
        geo=geo,
        total_queries=len(tasks),
        vertical="hmlv",
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
                vertical="hmlv",
            )
            continue

        for result in results:
            lead = _normalize_result(result, src, keyword=keyword)
            url = lead["url"]

            # Skip obvious non-manufacturer domains
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
            vertical="hmlv",
        )

    # Round-robin interleave across sources
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
        vertical="hmlv",
    )
    return all_leads


# Domains to skip — these are aggregators, marketplaces, review sites, or content farms
_NOISE_DOMAINS: set[str] = {
    "amazon.com", "alibaba.com", "aliexpress.com", "ebay.com",
    "etsy.com", "made-in-china.com", "thomasnet.com", "globalsources.com",
    "pinterest.com", "instagram.com", "facebook.com", "linkedin.com",
    "yelp.com", "houzz.com", "angi.com", "thumbtack.com",
    "youtube.com", "reddit.com", "quora.com",
    "wikipedia.org", "indeed.com", "glassdoor.com",
}


def _is_noise_domain(url: str) -> bool:
    """Return True if the URL is from a noise domain to skip.

    Uses exact netloc matching (via urllib.parse) to avoid false positives
    like x.com matching apex.com. LinkedIn /company/ pages are allowed through
    as they can surface manufacturer profiles.
    """
    try:
        netloc = urlparse(url.lower()).netloc.removeprefix("www.")
        # Allow LinkedIn company profile pages — can surface target manufacturers
        if netloc == "linkedin.com" and "/company/" in url.lower():
            return False
        return netloc in _NOISE_DOMAINS or any(
            netloc.endswith("." + d) for d in _NOISE_DOMAINS
        )
    except Exception:
        return True
