"""
Google Maps / Places API scraper for Vertical 2 — Cerrieta.

Uses Google Places Text Search API (free $200/month credit).
Enriches results by scraping websites for mailto: links.
"""

from __future__ import annotations

import os
import re

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

SEARCH_TEMPLATES = [
    "luxury pet boutique {city}",
    "cat cafe {city}",
    "high-end veterinary {city}",
    "luxury pet store {city}",
    "designer pet accessories {city}",
]

MIN_RATING = 4.2
MIN_REVIEWS = 20


def _get_target_cities() -> list[str]:
    raw = os.environ.get(
        "TARGET_CITIES",
        "New York,Miami,Los Angeles,London,Paris,Barcelona",
    )
    return [c.strip() for c in raw.split(",") if c.strip()]


async def _extract_email_from_website(
    client: httpx.AsyncClient, url: str
) -> str | None:
    """Try to find an email address on a business website."""
    try:
        resp = await client.get(url, timeout=10.0, follow_redirects=True)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")

        # Look for mailto: links
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                if "@" in email:
                    return email

        # Fallback: regex in page text
        text = soup.get_text()
        emails = re.findall(
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text
        )
        # Filter out common non-business emails
        for email in emails:
            lower = email.lower()
            if not any(
                skip in lower
                for skip in [
                    "example.com",
                    "wixpress",
                    "sentry.io",
                    "wordpress",
                    "schema.org",
                ]
            ):
                return email

    except Exception as exc:
        logger.debug(
            "email_extraction_failed", url=url, error=str(exc)
        )

    return None


async def scrape_gmaps(rate_limiter) -> list[dict]:
    """Scrape Google Places API for luxury pet businesses.

    Returns list of dicts with: name, address, website, phone, email,
    place_id, rating, reviews_count, city, query.
    """
    api_key = os.environ["GOOGLE_PLACES_API_KEY"]
    cities = _get_target_cities()
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN", "30"))
    all_places: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for city in cities:
            for template in SEARCH_TEMPLATES:
                if len(all_places) >= max_leads:
                    break

                query = template.format(city=city)
                await rate_limiter.wait()

                try:
                    resp = await client.get(
                        PLACES_SEARCH_URL,
                        params={
                            "query": query,
                            "key": api_key,
                        },
                    )

                    if resp.status_code != 200:
                        logger.warning(
                            "gmaps_http_error",
                            status=resp.status_code,
                            query=query,
                            source="gmaps",
                        )
                        continue

                    data = resp.json()
                    results = data.get("results", [])

                    for place in results:
                        rating = place.get("rating", 0)
                        reviews = place.get("user_ratings_total", 0)

                        if rating < MIN_RATING or reviews < MIN_REVIEWS:
                            continue

                        place_id = place.get("place_id", "")
                        place_url = (
                            f"https://www.google.com/maps/place/?q=place_id:{place_id}"
                        )

                        entry = {
                            "name": place.get("name", ""),
                            "address": place.get("formatted_address", ""),
                            "place_id": place_id,
                            "rating": rating,
                            "reviews_count": reviews,
                            "city": city,
                            "query": query,
                            "url": place_url,
                            "website": None,
                            "phone": None,
                            "email": None,
                        }

                        # Get details (website, phone)
                        try:
                            await rate_limiter.wait()
                            details_resp = await client.get(
                                PLACE_DETAILS_URL,
                                params={
                                    "place_id": place_id,
                                    "fields": "website,formatted_phone_number",
                                    "key": api_key,
                                },
                            )
                            if details_resp.status_code == 200:
                                details = details_resp.json().get("result", {})
                                entry["website"] = details.get("website")
                                entry["phone"] = details.get(
                                    "formatted_phone_number"
                                )
                        except Exception as exc:
                            logger.debug(
                                "place_details_failed",
                                place_id=place_id,
                                error=str(exc),
                            )

                        # Try to extract email from website
                        if entry["website"]:
                            email = await _extract_email_from_website(
                                client, entry["website"]
                            )
                            entry["email"] = email

                        all_places.append(entry)

                    logger.info(
                        "gmaps_query_scraped",
                        query=query,
                        results_found=len(results),
                        qualified=sum(
                            1
                            for r in results
                            if r.get("rating", 0) >= MIN_RATING
                            and r.get("user_ratings_total", 0) >= MIN_REVIEWS
                        ),
                        source="gmaps",
                    )

                except httpx.TimeoutException:
                    logger.warning(
                        "gmaps_timeout", query=query, source="gmaps"
                    )
                    continue
                except Exception as exc:
                    logger.error(
                        "gmaps_scrape_error",
                        query=query,
                        error=str(exc),
                        source="gmaps",
                    )
                    continue

            if len(all_places) >= max_leads:
                break

    logger.info(
        "gmaps_scrape_complete",
        total_places=len(all_places),
        source="gmaps",
    )
    return all_places[:max_leads]
