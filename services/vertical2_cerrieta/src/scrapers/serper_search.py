"""
Search API-based lead sourcing for Vertical 2 — Cerrieta.

Replaces instagram_scraper.py with Serper.dev Google Search API queries.
Google Maps scraper remains unchanged (API-based, works fine).
"""

from __future__ import annotations

import asyncio
import os
import re

import structlog

logger = structlog.get_logger(__name__)

INSTAGRAM_SEARCH_KEYWORDS = [
    "luxury pet boutique",
    "cat cafe premium",
    "luxury pet furniture store",
    "designer pet accessories shop",
]


def _extract_username_from_url(url: str) -> str:
    """Extract Instagram username from URL."""
    match = re.search(r"instagram\.com/([^/?]+)", url)
    if match:
        username = match.group(1)
        if username not in ("p", "explore", "accounts", "reel", "stories", "reels"):
            return username
    return ""


def _extract_follower_count(snippet: str) -> int:
    """Try to extract follower count from Google snippet of Instagram profile."""
    match = re.search(r"([\d,.]+)\s*[Kk]\s*[Ff]ollowers", snippet)
    if match:
        num_str = match.group(1).replace(",", "")
        return int(float(num_str) * 1000)

    match = re.search(r"([\d,]+)\s*[Ff]ollowers", snippet)
    if match:
        return int(match.group(1).replace(",", ""))

    return 0


def _normalize_instagram_result(result: dict) -> dict:
    """Normalize a Serper result into pipeline-compatible Instagram lead dict."""
    url = result.get("link", "")
    username = _extract_username_from_url(url)
    snippet = result.get("snippet", "")
    follower_count = _extract_follower_count(snippet)

    return {
        "username": username,
        "name": username,
        "bio": snippet,
        "follower_count": follower_count,
        "business_email": None,
        "url": url,
    }


async def search_instagram_leads(serper_client) -> list[dict]:
    """Search for Instagram profiles matching Cerrieta's target market.

    All queries run in parallel — Serper is a paid API, no need for delays.

    Returns list[dict] compatible with existing instagram scraper output format.
    """
    max_profiles = int(os.environ.get("MAX_LEADS_PER_RUN", "15"))
    seen_urls: set[str] = set()
    all_profiles: list[dict] = []

    # Fire all queries in parallel
    queries = [f'site:instagram.com "{kw}"' for kw in INSTAGRAM_SEARCH_KEYWORDS]
    tasks = [serper_client.search(query=q, num=10) for q in queries]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)

    for keyword, results in zip(INSTAGRAM_SEARCH_KEYWORDS, results_list):
        if isinstance(results, Exception):
            logger.error("instagram_search_failed", keyword=keyword, error=str(results))
            continue

        for result in results:
            profile = _normalize_instagram_result(result)

            if not profile["username"]:
                continue

            if profile["url"] in seen_urls:
                continue
            seen_urls.add(profile["url"])

            all_profiles.append(profile)

        logger.info(
            "instagram_search_complete",
            keyword=keyword,
            results_found=len(results),
            total_profiles=len(all_profiles),
            source="instagram",
        )

    logger.info(
        "instagram_search_all_complete",
        total_profiles=len(all_profiles),
        source="instagram",
    )
    return all_profiles[:max_profiles]
