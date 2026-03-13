"""
Lead qualifier for Vertical 5 — M&A Silver Tsunami.

Scores traditional Florida businesses 1-10 as acquisition targets for
SunBridge Advisors (boutique M&A, lower middle market).
Threshold: fit_score >= 7.

High scores → boring, traditional, mature businesses (HVAC, manufacturing,
B2B SaaS 15+ years old) with signs of founder fatigue or succession need.
Low scores → modern companies, startups, consultants, franchises.
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

MIN_SCORE = int(os.environ.get("MA_MIN_SCORE", "7"))


class MAQualificationResult(BaseModel):
    # ── Core M&A analysis fields ──────────────────────────────────────────────
    fit_score: int
    founder_name: str | None
    company_name: str
    estimated_years_active: str     # e.g. "Founded in 1995" or "Operating since 1989"
    momentum_signal: str            # e.g. "Family HVAC company operating 30+ years, possible founder fatigue"
    industry_niche: str             # e.g. "HVAC Services", "Metal Fabrication"
    suggested_angle: Literal[
        "market-valuation",
        "succession-planning",
        "industry-consolidation",
    ]
    is_qualified: bool              # True if fit_score >= 7

    # ── Contact extraction fields (required for outreach pipeline) ────────────
    contact_email: str              # Email found in scraped content, empty string if not found
    company_website: str            # Root domain URL of the business

    @field_validator("suggested_angle", mode="before")
    @classmethod
    def normalize_angle(cls, v: str) -> str:
        """Gemini sometimes returns underscores instead of hyphens. Normalize both."""
        if not v:
            return "market-valuation"
        return v.replace("_", "-")

    @field_validator("estimated_years_active", "industry_niche", "momentum_signal",
                     "company_name", "contact_email", "company_website", mode="before")
    @classmethod
    def none_to_empty_string(cls, v: object) -> str:
        """Gemini occasionally returns null for string fields. Coerce to empty string."""
        if v is None:
            return ""
        return str(v)

    @field_validator("founder_name", mode="before")
    @classmethod
    def empty_to_none(cls, v: object) -> str | None:
        """Treat empty string as None for optional founder_name."""
        if v == "" or v is None:
            return None
        return str(v)


class LeadQualifier:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"].strip()
        self.system_prompt = self._load_system_prompt()

    def _load_system_prompt(self) -> str:
        prompt_path = PROMPTS_DIR / "vertical5_system_prompt.txt"
        return prompt_path.read_text(encoding="utf-8")

    async def qualify(
        self, raw_text: str, rate_limiter=None
    ) -> MAQualificationResult | None:
        """Qualify a business as a Silver Tsunami acquisition target.

        Returns None if below threshold (fit_score < 7) or on unrecoverable error.
        """
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
                            vertical="ma",
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            logger.error("gemini_429_exhausted_retries", vertical="ma")
                            return None

                    resp.raise_for_status()

                body = resp.json()
                text = body["candidates"][0]["content"]["parts"][0]["text"].strip()

                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3].strip()

                data = json.loads(text)
                result = MAQualificationResult(**data)

                # Enforce threshold — trust fit_score over Gemini's is_qualified flag
                if result.fit_score < MIN_SCORE:
                    logger.info(
                        "lead_not_qualified",
                        vertical="ma",
                        fit_score=result.fit_score,
                        company=result.company_name,
                        momentum_signal=result.momentum_signal,
                    )
                    return None

                logger.info(
                    "lead_qualified",
                    vertical="ma",
                    fit_score=result.fit_score,
                    company=result.company_name,
                    industry=result.industry_niche,
                    estimated_years=result.estimated_years_active,
                    angle=result.suggested_angle,
                    founder=result.founder_name,
                )
                return result

            except (json.JSONDecodeError, ValidationError) as exc:
                logger.warning(
                    "qualification_parse_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="ma",
                )
                if attempt == max_retries:
                    logger.error("qualification_failed_all_retries", vertical="ma")
                    return None

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "qualification_api_error",
                    attempt=attempt,
                    status_code=exc.response.status_code,
                    error=str(exc),
                    vertical="ma",
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
                    vertical="ma",
                )
                if attempt == max_retries:
                    return None
