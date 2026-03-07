"""
Lead qualifier for Vertical 3 — HMLV Manufacturers.

Scores manufacturer websites 1-10 as ICP prospects for a CAD/BOM/DXF SaaS.
Threshold: fit_score >= 7 to qualify (stricter than vertical1 — B2B sales, not job apps).
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Literal

import httpx
import structlog
from pydantic import BaseModel, ValidationError

logger = structlog.get_logger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "shared" / "prompts"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"

MIN_SCORE = int(os.environ.get("HMLV_MIN_SCORE", "7"))


class HMLVQualificationResult(BaseModel):
    is_hmlv_manufacturer: Literal["YES", "NO"]
    fit_score: int
    industry_category: Literal[
        "trade_show_exhibits",
        "marine_decking",
        "architectural_millwork",
        "industrial_crating",
        "metal_facades",
        "other",
    ]
    red_flags: list[str]
    green_flags: list[str]
    technical_reasoning: str
    pain_point: str
    key_technology: str
    inferred_company: str
    contact_name: str
    contact_email: str
    company_website: str
    suggested_angle: Literal[
        "BOM-automation",
        "Nesting-optimization",
        "Quote-to-cash",
        "DXF-export",
        "CAD-ERP-bridge",
    ]


class LeadQualifier:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"].strip()
        self.system_prompt = self._load_system_prompt()

    def _load_system_prompt(self) -> str:
        prompt_path = PROMPTS_DIR / "vertical3_system_prompt.txt"
        return prompt_path.read_text(encoding="utf-8")

    async def qualify(
        self, raw_text: str, rate_limiter=None
    ) -> HMLVQualificationResult | None:
        """Qualify a manufacturer website. Returns None if below threshold or on error."""
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            if rate_limiter is not None:
                await rate_limiter.acquire()

            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(
                        GEMINI_API_URL,
                        headers={
                            "x-goog-api-key": self.api_key,
                            "Content-Type": "application/json",
                        },
                        json={
                            "system_instruction": {
                                "parts": [{"text": self.system_prompt}]
                            },
                            "contents": [{"parts": [{"text": raw_text}]}],
                            "generationConfig": {
                                "responseMimeType": "application/json",
                                "temperature": 0.1,
                            },
                        },
                    )

                    if resp.status_code == 429:
                        backoff = min(2 ** attempt * 5, 30)
                        logger.warning(
                            "gemini_429_backoff",
                            attempt=attempt,
                            backoff_seconds=backoff,
                            vertical="hmlv",
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            logger.error("gemini_429_exhausted_retries", vertical="hmlv")
                            return None

                    resp.raise_for_status()

                body = resp.json()
                text = body["candidates"][0]["content"]["parts"][0]["text"].strip()

                # Strip markdown code fences if present
                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3].strip()

                data = json.loads(text)
                result = HMLVQualificationResult(**data)

                if result.is_hmlv_manufacturer == "NO" or result.fit_score < MIN_SCORE:
                    logger.info(
                        "lead_not_qualified",
                        vertical="hmlv",
                        fit_score=result.fit_score,
                        reasoning=result.technical_reasoning,
                        red_flags=result.red_flags,
                    )
                    return None

                logger.info(
                    "lead_qualified",
                    vertical="hmlv",
                    fit_score=result.fit_score,
                    industry=result.industry_category,
                    pain_point=result.pain_point,
                    angle=result.suggested_angle,
                    company=result.inferred_company,
                    key_tech=result.key_technology,
                    contact=result.contact_name,
                    green_flags=result.green_flags,
                )
                return result

            except (json.JSONDecodeError, ValidationError) as exc:
                logger.warning(
                    "qualification_parse_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="hmlv",
                )
                if attempt == max_retries:
                    logger.error("qualification_failed_all_retries", vertical="hmlv")
                    return None

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "qualification_api_error",
                    attempt=attempt,
                    status_code=exc.response.status_code,
                    error=str(exc),
                    vertical="hmlv",
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return None

            except Exception as exc:
                logger.error(
                    "qualification_unexpected_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="hmlv",
                )
                if attempt == max_retries:
                    return None
