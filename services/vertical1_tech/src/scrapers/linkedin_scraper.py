"""
LinkedIn public profile scraper for Vertical 1 — Tech Services.

Uses unauthenticated access via guest navigation.
Extracts JSON-LD structured data — no DOM parsing of dynamic content.
"""

from __future__ import annotations

import json
import os
import random

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

SEARCH_QUERIES = [
    "startup founder messy data",
    "tech recruiter automation",
    "fractional CFO data pipeline",
    "agency owner scaling infrastructure",
    "SaaS founder manual reporting",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

MAX_PROFILES = 20

LINKEDIN_GUEST_JOBS_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"


def _build_headers() -> dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    }


def _extract_jsonld(html: str) -> list[dict]:
    """Extract JSON-LD objects from HTML."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
        except (json.JSONDecodeError, TypeError):
            continue

    return results


def _extract_profiles_from_html(html: str) -> list[dict]:
    """Fallback: extract basic profile info from HTML structure."""
    soup = BeautifulSoup(html, "lxml")
    profiles = []

    for card in soup.select(
        ".base-card, .result-card, .job-search-card, li"
    )[:MAX_PROFILES]:
        try:
            title_el = card.select_one(
                "h3, .base-search-card__title, .result-card__title"
            )
            subtitle_el = card.select_one(
                "h4, .base-search-card__subtitle, .result-card__subtitle"
            )
            link_el = card.select_one("a[href*='linkedin.com']")

            if not title_el:
                continue

            url = link_el.get("href", "") if link_el else ""
            if url and "?" in url:
                url = url.split("?")[0]

            profiles.append(
                {
                    "title": title_el.get_text(strip=True),
                    "company": subtitle_el.get_text(strip=True)
                    if subtitle_el
                    else "",
                    "url": url,
                    "description": card.get_text(" ", strip=True)[:500],
                }
            )
        except Exception:
            continue

    return profiles


async def scrape_linkedin(rate_limiter) -> list[dict]:
    """Scrape LinkedIn public job/profile listings.

    Uses guest access endpoints — no authentication required.
    Max 20 profiles per run with conservative delays.
    """
    all_profiles: list[dict] = []

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        for query in SEARCH_QUERIES:
            if len(all_profiles) >= MAX_PROFILES:
                break

            await rate_limiter.wait()

            try:
                # Use LinkedIn's guest jobs API
                params = {
                    "keywords": query,
                    "location": "United States",
                    "start": "0",
                    "trk": "guest_homepage-basic_guest_nav_menu_jobs",
                }

                response = await client.get(
                    LINKEDIN_GUEST_JOBS_URL,
                    params=params,
                    headers=_build_headers(),
                )

                if response.status_code == 429:
                    logger.warning(
                        "linkedin_rate_limited",
                        query=query,
                        source="linkedin",
                    )
                    await rate_limiter.on_rate_limit()
                    continue

                if response.status_code != 200:
                    logger.warning(
                        "linkedin_http_error",
                        status=response.status_code,
                        query=query,
                        source="linkedin",
                    )
                    continue

                # Try JSON-LD first
                jsonld_data = _extract_jsonld(response.text)
                if jsonld_data:
                    for item in jsonld_data:
                        if item.get("@type") in (
                            "JobPosting",
                            "Person",
                            "Organization",
                        ):
                            profile = {
                                "title": item.get("title", item.get("name", "")),
                                "company": item.get(
                                    "hiringOrganization", {}
                                ).get("name", ""),
                                "url": item.get("url", ""),
                                "description": item.get("description", "")[:500],
                            }
                            all_profiles.append(profile)
                else:
                    # Fallback to HTML parsing
                    profiles = _extract_profiles_from_html(response.text)
                    all_profiles.extend(profiles)

                logger.info(
                    "linkedin_query_scraped",
                    query=query,
                    profiles_found=len(all_profiles),
                    source="linkedin",
                )

            except httpx.TimeoutException:
                logger.warning(
                    "linkedin_timeout", query=query, source="linkedin"
                )
                continue
            except Exception as exc:
                logger.error(
                    "linkedin_scrape_error",
                    query=query,
                    error=str(exc),
                    source="linkedin",
                )
                continue

    logger.info(
        "linkedin_scrape_complete",
        total_profiles=len(all_profiles),
        source="linkedin",
    )
    return all_profiles[:MAX_PROFILES]
