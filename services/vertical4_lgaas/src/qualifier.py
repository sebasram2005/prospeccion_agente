"""
Lead qualifier for Vertical 4 — LGaaS Prospects.

Scores boutique consulting firm websites 1-10 as prospects for
Sebastián's Lead Generation as a Service (LGaaS) offering.
Threshold: fit_score >= 7 (same as v3 — these are B2B sales, not job apps).
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Literal

import httpx
import structlog
from pydantic import BaseModel, ValidationError, field_validator

logger = structlog.get_logger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "shared" / "prompts"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"

MIN_SCORE = int(os.environ.get("LGAAS_MIN_SCORE", "7"))


class LGaaSQualificationResult(BaseModel):
    is_target_firm: Literal["YES", "NO"]
    fit_score: int
    niche_category: Literal[
        "fractional_cfo",
        "ma_advisory",
        "cmmc_security",
        "ai_automation",
        "esg_consulting",
        "other",
    ]
    red_flags: list[str]
    green_flags: list[str]
    technical_reasoning: str
    pain_point: str
    estimated_ticket: str
    inferred_company: str
    contact_name: str
    contact_email: str
    company_website: str
    suggested_angle: Literal[
        "roi-calculator",
        "competitor-benchmark",
        "capacity-unlock",
        "cost-of-inaction",
        "proof-of-concept",
    ]

    @field_validator("suggested_angle", mode="before")
    @classmethod
    def normalize_angle(cls, v: str) -> str:
        """Gemini sometimes returns underscores instead of hyphens. Normalize both."""
        if not v:
            return "roi-calculator"
        return v.replace("_", "-")


class LeadQualifier:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"].strip()
        self.system_prompt = self._load_system_prompt()

    def _load_system_prompt(self) -> str:
        prompt_path = PROMPTS_DIR / "vertical4_system_prompt.txt"
        return prompt_path.read_text(encoding="utf-8")

    async def qualify(
        self, raw_text: str, rate_limiter=None
    ) -> LGaaSQualificationResult | None:
        """Qualify a consulting firm website. Returns None if below threshold or on error."""
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
                            vertical="lgaas",
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            logger.error("gemini_429_exhausted_retries", vertical="lgaas")
                            return None

                    resp.raise_for_status()

                body = resp.json()
                text = body["candidates"][0]["content"]["parts"][0]["text"].strip()

                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3].strip()

                data = json.loads(text)
                result = LGaaSQualificationResult(**data)

                if result.is_target_firm == "NO" or result.fit_score < MIN_SCORE:
                    logger.info(
                        "lead_not_qualified",
                        vertical="lgaas",
                        fit_score=result.fit_score,
                        reasoning=result.technical_reasoning,
                        red_flags=result.red_flags,
                    )
                    return None

                logger.info(
                    "lead_qualified",
                    vertical="lgaas",
                    fit_score=result.fit_score,
                    niche=result.niche_category,
                    pain_point=result.pain_point,
                    angle=result.suggested_angle,
                    company=result.inferred_company,
                    contact=result.contact_name,
                    green_flags=result.green_flags,
                )
                return result

            except (json.JSONDecodeError, ValidationError) as exc:
                logger.warning(
                    "qualification_parse_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="lgaas",
                )
                if attempt == max_retries:
                    logger.error("qualification_failed_all_retries", vertical="lgaas")
                    return None

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "qualification_api_error",
                    attempt=attempt,
                    status_code=exc.response.status_code,
                    error=str(exc),
                    vertical="lgaas",
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
                    vertical="lgaas",
                )
                if attempt == max_retries:
                    return None
