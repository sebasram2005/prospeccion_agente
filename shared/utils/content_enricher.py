"""
Content enrichment via Jina Reader API + company website email scraping.

Jina Reader converts any URL into clean LLM-friendly markdown.
Free tier: ~100 RPM without API key, higher with free key.

Email scraping reuses the proven pattern from gmaps_scraper.py.
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from urllib.parse import urljoin, urlparse

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

JINA_READER_BASE = "https://r.jina.ai/"

# Max chars of enriched content to pass to Gemini (avoid context bloat)
MAX_CONTENT_CHARS = 4000

# Common non-business email domains to filter out
SKIP_EMAIL_DOMAINS = frozenset(
    {
        "example.com",
        "wixpress.com",
        "sentry.io",
        "wordpress.com",
        "schema.org",
        "w3.org",
        "googleapis.com",
        "gravatar.com",
        "facebook.com",
        "twitter.com",
        "linkedin.com",
        "instagram.com",
        "github.com",
    }
)

# Sub-paths to try when scraping a company website for contact emails
CONTACT_PATHS = ("", "/contact", "/about", "/contact-us", "/about-us", "/team")


class JinaRateLimiter:
    """Token-bucket limiter for Jina Reader API (free tier ~100 RPM)."""

    def __init__(self, max_per_minute: int = 90):
        self.max_per_minute = max_per_minute
        self._lock = asyncio.Lock()
        self._timestamps: list[float] = []

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._timestamps = [t for t in self._timestamps if now - t < 60.0]

            if len(self._timestamps) >= self.max_per_minute:
                oldest = self._timestamps[0]
                sleep_time = 60.0 - (now - oldest) + 0.1
                if sleep_time > 0:
                    logger.info(
                        "jina_rate_limit_wait",
                        sleep_seconds=round(sleep_time, 1),
                    )
                    await asyncio.sleep(sleep_time)
                    now = time.monotonic()
                    self._timestamps = [
                        t for t in self._timestamps if now - t < 60.0
                    ]

            self._timestamps.append(time.monotonic())


def _is_valid_business_email(email: str) -> bool:
    """Filter out generic/non-business emails."""
    lower = email.lower()
    domain = lower.split("@", 1)[-1] if "@" in lower else ""
    return bool(domain) and domain not in SKIP_EMAIL_DOMAINS


def _extract_emails_from_html(html: str) -> list[str]:
    """Extract email addresses from HTML using mailto: links and regex."""
    emails: list[str] = []
    soup = BeautifulSoup(html, "lxml")

    # 1. mailto: links (highest confidence)
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.startswith("mailto:"):
            email = href.replace("mailto:", "").split("?")[0].strip()
            if "@" in email and _is_valid_business_email(email):
                emails.append(email)

    # 2. Regex fallback in page text
    text = soup.get_text()
    found = re.findall(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text
    )
    for email in found:
        if _is_valid_business_email(email) and email not in emails:
            emails.append(email)

    return emails


class ContentEnricher:
    """Fetches full page content via Jina Reader and scrapes company emails."""

    def __init__(self):
        self.jina_api_key = os.environ.get("JINA_API_KEY", "")
        self.rate_limiter = JinaRateLimiter()

    async def fetch_page_content(self, url: str) -> str | None:
        """Fetch full webpage content as markdown via Jina Reader API.

        Returns truncated markdown string or None on failure.
        """
        if not url:
            return None

        await self.rate_limiter.acquire()

        headers = {"Accept": "text/markdown"}
        if self.jina_api_key:
            headers["Authorization"] = f"Bearer {self.jina_api_key}"

        jina_url = f"{JINA_READER_BASE}{url}"

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.get(jina_url, headers=headers)

                if resp.status_code != 200:
                    logger.warning(
                        "jina_fetch_http_error",
                        url=url,
                        status=resp.status_code,
                    )
                    return None

                content = resp.text.strip()
                if not content:
                    return None

                # Truncate to avoid saturating Gemini context
                if len(content) > MAX_CONTENT_CHARS:
                    content = content[:MAX_CONTENT_CHARS] + "\n\n[...truncated]"

                logger.info(
                    "jina_fetch_success",
                    url=url,
                    content_length=len(content),
                )
                return content

        except httpx.TimeoutException:
            logger.warning("jina_fetch_timeout", url=url)
            return None
        except Exception as exc:
            logger.error("jina_fetch_error", url=url, error=str(exc))
            return None

    async def scrape_email_from_website(self, website_url: str) -> str | None:
        """Scrape a company website for contact email addresses.

        Tries the homepage plus common contact/about paths.
        Returns the first valid business email found, or None.
        """
        if not website_url:
            return None

        parsed = urlparse(website_url)
        if not parsed.scheme:
            website_url = f"https://{website_url}"

        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc}"

        try:
            async with httpx.AsyncClient(
                timeout=10.0,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; ProspectBot/1.0)"},
            ) as client:
                for path in CONTACT_PATHS:
                    target = urljoin(base_url, path) if path else website_url
                    try:
                        resp = await client.get(target)
                        if resp.status_code != 200:
                            continue

                        emails = _extract_emails_from_html(resp.text)
                        if emails:
                            logger.info(
                                "email_scraped",
                                website=website_url,
                                path=path or "/",
                                email=emails[0],
                            )
                            return emails[0]

                    except (httpx.TimeoutException, httpx.ConnectError):
                        continue

        except Exception as exc:
            logger.debug(
                "email_scrape_failed",
                website=website_url,
                error=str(exc),
            )

        return None

    async def enrich_lead(self, lead: dict) -> dict:
        """Enrich a lead dict with full page content from its URL.

        Best-effort: if enrichment fails, returns the original lead unchanged.
        Never raises — the pipeline must not break due to enrichment failures.
        """
        url = lead.get("url", "")
        if not url:
            return lead

        try:
            content = await self.fetch_page_content(url)
            if content:
                lead["full_description"] = content
                logger.info(
                    "lead_enriched",
                    url=url,
                    content_chars=len(content),
                )
            else:
                logger.info("lead_enrichment_skipped", url=url, reason="no_content")
        except Exception as exc:
            logger.error("lead_enrichment_error", url=url, error=str(exc))

        return lead
