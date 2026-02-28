"""
Instagram hashtag scraper for Vertical 2 — Cerrieta.

Uses curl_cffi with Chrome TLS fingerprint for bypass.
Very conservative rate limiting (no proxies).
Includes mock mode for development.
"""

from __future__ import annotations

import json
import os
import random
import re

import structlog

logger = structlog.get_logger(__name__)

TARGET_HASHTAGS = [
    "luxurypetfurniture",
    "luxuryinterior",
    "catdesign",
    "modernpets",
    "designerfurniture",
]

MIN_FOLLOWERS = 3000
MAX_PROFILES = 15

MOCK_PROFILES = [
    {
        "username": "luxpaws_boutique",
        "bio": "Curated luxury accessories for the modern pet. Sustainable, design-forward pieces for discerning pet parents. NYC 🐾",
        "follower_count": 12400,
        "business_email": "hello@luxpaws.com",
        "url": "https://www.instagram.com/luxpaws_boutique/",
    },
    {
        "username": "whiskers_and_co",
        "bio": "Premium cat cafe & boutique | Organic treats | Design furniture | London & Paris | wholesale inquiries welcome",
        "follower_count": 8700,
        "business_email": "partnerships@whiskersandco.com",
        "url": "https://www.instagram.com/whiskers_and_co/",
    },
    {
        "username": "casa_felina_design",
        "bio": "Interior design studio specializing in pet-integrated luxury spaces. Featured in Architectural Digest. Miami Beach 🏖️",
        "follower_count": 24300,
        "business_email": "info@casafelinadesign.com",
        "url": "https://www.instagram.com/casa_felina_design/",
    },
]


async def _scrape_real(rate_limiter) -> list[dict]:
    """Scrape Instagram hashtag pages using curl_cffi."""
    try:
        from curl_cffi.requests import AsyncSession
    except ImportError:
        logger.error(
            "curl_cffi_not_installed",
            source="instagram",
            msg="Install curl-cffi: pip install curl-cffi",
        )
        return []

    all_profiles: list[dict] = []

    async with AsyncSession(impersonate="chrome120") as session:
        for hashtag in TARGET_HASHTAGS:
            if len(all_profiles) >= MAX_PROFILES:
                break

            await rate_limiter.wait()

            url = f"https://www.instagram.com/explore/tags/{hashtag}/"

            try:
                resp = await session.get(
                    url,
                    headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                )

                if resp.status_code == 429:
                    logger.warning(
                        "instagram_rate_limited",
                        hashtag=hashtag,
                        source="instagram",
                    )
                    await rate_limiter.on_rate_limit()
                    continue

                if resp.status_code != 200:
                    logger.warning(
                        "instagram_http_error",
                        status=resp.status_code,
                        hashtag=hashtag,
                        source="instagram",
                    )
                    continue

                # Try to extract shared data JSON
                profiles = _extract_profiles_from_html(resp.text)

                for profile in profiles:
                    if profile.get("follower_count", 0) >= MIN_FOLLOWERS:
                        all_profiles.append(profile)

                logger.info(
                    "instagram_hashtag_scraped",
                    hashtag=hashtag,
                    profiles_found=len(profiles),
                    source="instagram",
                )

            except Exception as exc:
                logger.error(
                    "instagram_scrape_error",
                    hashtag=hashtag,
                    error=str(exc),
                    source="instagram",
                )
                continue

    logger.info(
        "instagram_scrape_complete",
        total_profiles=len(all_profiles),
        source="instagram",
    )
    return all_profiles[:MAX_PROFILES]


def _extract_profiles_from_html(html: str) -> list[dict]:
    """Extract profile data from Instagram page HTML.

    Looks for window._sharedData or similar JSON payloads.
    """
    profiles = []

    # Try window._sharedData
    match = re.search(
        r"window\._sharedData\s*=\s*({.*?});\s*</script>", html, re.DOTALL
    )
    if not match:
        # Try window.__additionalDataLoaded
        match = re.search(
            r"window\.__additionalDataLoaded\s*\([^,]+,\s*({.*?})\)\s*;",
            html,
            re.DOTALL,
        )

    if not match:
        return profiles

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return profiles

    # Navigate the data structure to find user nodes
    edges = []

    # Path for hashtag pages
    try:
        tag_page = data.get("entry_data", {}).get("TagPage", [{}])[0]
        media = tag_page.get("graphql", {}).get("hashtag", {})
        top_posts = media.get("edge_hashtag_to_top_posts", {})
        edges = top_posts.get("edges", [])
    except (KeyError, IndexError):
        pass

    for edge in edges:
        node = edge.get("node", {})
        owner = node.get("owner", {})

        username = owner.get("username", "")
        if not username:
            continue

        profile = {
            "username": username,
            "bio": "",
            "follower_count": 0,
            "business_email": None,
            "url": f"https://www.instagram.com/{username}/",
        }

        # If we have full profile data
        if "edge_followed_by" in owner:
            profile["follower_count"] = (
                owner.get("edge_followed_by", {}).get("count", 0)
            )
        if "biography" in owner:
            profile["bio"] = owner.get("biography", "")
        if "business_email" in owner:
            profile["business_email"] = owner.get("business_email")

        profiles.append(profile)

    return profiles


async def scrape_instagram(rate_limiter) -> list[dict]:
    """Scrape Instagram for luxury pet-related profiles.

    Uses mock data if INSTAGRAM_MOCK=true.
    """
    if os.environ.get("INSTAGRAM_MOCK", "false").lower() == "true":
        logger.info(
            "instagram_mock_mode",
            profiles_returned=len(MOCK_PROFILES),
            source="instagram",
        )
        return MOCK_PROFILES

    return await _scrape_real(rate_limiter)
