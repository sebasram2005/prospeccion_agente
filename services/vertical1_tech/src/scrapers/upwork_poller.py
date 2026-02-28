"""
Upwork job feed poller for Vertical 1 — Tech Services.

Polls Upwork search URLs using httpx with Chrome-like headers.
Filters by budget >= $500 and deduplicates via Supabase.
"""

from __future__ import annotations

import json
import os
import re

import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)

SEARCH_KEYWORDS = [
    "Python developer",
    "data analyst",
    "SaaS MVP",
    "data pipeline",
    "ETL",
    "automation",
]

MIN_BUDGET = int(os.environ.get("MIN_BUDGET_USD", "500"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.upwork.com/nx/search/jobs/"


def _build_headers(ua_index: int = 0) -> dict[str, str]:
    return {
        "User-Agent": USER_AGENTS[ua_index % len(USER_AGENTS)],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _extract_budget(text: str) -> int:
    """Extract budget amount from strings like '$500', '$1,000 - $2,500', etc."""
    amounts = re.findall(r"\$[\d,]+", text)
    if not amounts:
        return 0
    # Take the first (minimum) amount
    return int(amounts[0].replace("$", "").replace(",", ""))


def _parse_jobs_from_html(html: str) -> list[dict]:
    """Parse job listings from Upwork search results HTML."""
    soup = BeautifulSoup(html, "lxml")
    jobs = []

    # Try to find JSON-LD structured data first
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                jobs.append(_normalize_jsonld_job(data))
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "JobPosting":
                        jobs.append(_normalize_jsonld_job(item))
        except (json.JSONDecodeError, TypeError):
            continue

    # Fallback: parse from HTML sections
    if not jobs:
        for section in soup.select("section.job-tile"):
            try:
                title_el = section.select_one("h2 a, h3 a")
                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                url = title_el.get("href", "")
                if url and not url.startswith("http"):
                    url = f"https://www.upwork.com{url}"

                desc_el = section.select_one(".job-description, .text-body")
                description = desc_el.get_text(strip=True) if desc_el else ""

                budget_el = section.select_one(
                    ".budget, [data-test='budget'], .js-budget"
                )
                budget_text = budget_el.get_text(strip=True) if budget_el else ""

                jobs.append(
                    {
                        "title": title,
                        "url": url,
                        "description": description,
                        "budget_text": budget_text,
                        "budget_amount": _extract_budget(budget_text),
                    }
                )
            except Exception as exc:
                logger.warning("job_parse_error", error=str(exc))
                continue

    return jobs


def _normalize_jsonld_job(data: dict) -> dict:
    """Normalize a JSON-LD JobPosting into our internal format."""
    salary = data.get("baseSalary", {})
    budget_text = ""
    budget_amount = 0

    if isinstance(salary, dict):
        value = salary.get("value", {})
        if isinstance(value, dict):
            budget_amount = int(value.get("value", 0) or value.get("minValue", 0) or 0)
            budget_text = f"${budget_amount:,}"
        elif isinstance(value, (int, float)):
            budget_amount = int(value)
            budget_text = f"${budget_amount:,}"

    url = data.get("url", "")
    if url and not url.startswith("http"):
        url = f"https://www.upwork.com{url}"

    return {
        "title": data.get("title", ""),
        "url": url,
        "description": data.get("description", ""),
        "budget_text": budget_text,
        "budget_amount": budget_amount,
        "company": data.get("hiringOrganization", {}).get("name", ""),
    }


async def scrape_upwork(rate_limiter) -> list[dict]:
    """Scrape Upwork job listings matching target keywords.

    Returns a list of job dicts with keys: title, url, description,
    budget_text, budget_amount, company.
    """
    max_leads = int(os.environ.get("MAX_LEADS_PER_RUN", "30"))
    all_jobs: list[dict] = []

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        for idx, keyword in enumerate(SEARCH_KEYWORDS):
            if len(all_jobs) >= max_leads:
                break

            await rate_limiter.wait()

            params = {
                "q": keyword,
                "sort": "recency",
                "per_page": "10",
            }

            try:
                response = await client.get(
                    BASE_URL,
                    params=params,
                    headers=_build_headers(idx),
                )

                if response.status_code == 429:
                    logger.warning(
                        "upwork_rate_limited",
                        keyword=keyword,
                        source="upwork",
                    )
                    await rate_limiter.on_rate_limit()
                    continue

                if response.status_code != 200:
                    logger.warning(
                        "upwork_http_error",
                        status=response.status_code,
                        keyword=keyword,
                        source="upwork",
                    )
                    continue

                jobs = _parse_jobs_from_html(response.text)

                for job in jobs:
                    if job.get("budget_amount", 0) >= MIN_BUDGET:
                        all_jobs.append(job)

                logger.info(
                    "upwork_keyword_scraped",
                    keyword=keyword,
                    jobs_found=len(jobs),
                    jobs_qualified=sum(
                        1 for j in jobs if j.get("budget_amount", 0) >= MIN_BUDGET
                    ),
                    source="upwork",
                )

            except httpx.TimeoutException:
                logger.warning(
                    "upwork_timeout", keyword=keyword, source="upwork"
                )
                continue
            except Exception as exc:
                logger.error(
                    "upwork_scrape_error",
                    keyword=keyword,
                    error=str(exc),
                    source="upwork",
                )
                continue

    logger.info(
        "upwork_scrape_complete",
        total_jobs=len(all_jobs),
        source="upwork",
    )
    return all_jobs[:max_leads]
