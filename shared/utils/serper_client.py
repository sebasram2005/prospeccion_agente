"""
Async Serper.dev client for Google Search API.
Shared across all verticals.

Free tier: 2,500 queries/month — no credit card required.
"""

from __future__ import annotations

import os
from typing import Literal

import httpx
import structlog

logger = structlog.get_logger(__name__)

SERPER_SEARCH_URL = "https://google.serper.dev/search"

SearchType = Literal["search", "news"]


class SerperClient:
    """Async client for Serper.dev Google Search API."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ["SERPER_API_KEY"]

    async def search(
        self,
        query: str,
        num: int = 10,
        gl: str = "us",
        hl: str = "en",
    ) -> list[dict]:
        """Execute a Google search via Serper and return organic results.

        Returns list of dicts with keys: title, link, snippet, position.
        """
        payload = {"q": query, "num": num, "gl": gl, "hl": hl}
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    SERPER_SEARCH_URL, json=payload, headers=headers
                )

                if response.status_code != 200:
                    logger.error(
                        "serper_api_error",
                        status=response.status_code,
                        query=query,
                        body=response.text[:500],
                    )
                    return []

                data = response.json()
                results = data.get("organic", [])

                logger.info(
                    "serper_search_complete",
                    query=query,
                    results_count=len(results),
                )
                return results

        except Exception as exc:
            logger.error("serper_request_failed", query=query, error=str(exc))
            return []
