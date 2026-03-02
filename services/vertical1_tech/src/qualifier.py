"""
Lead qualifier using Gemini 2.5 Flash-Lite for Vertical 1 — Tech Services.

Uses the Gemini REST API directly via httpx (no SDK dependency conflicts).
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import httpx
import structlog
from pydantic import BaseModel, ValidationError
from typing import Literal

logger = structlog.get_logger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "shared" / "prompts"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"


class TechQualificationResult(BaseModel):
    qualified: Literal["YES", "NO"]
    fit_score: int
    reasoning: str
    pain_point: str
    portfolio_proof: str
    suggested_angle: Literal[
        "ROI-focused",
        "Time-saving",
        "Technical architecture",
        "Risk-reduction",
        "Revenue-uplift",
    ]
    inferred_company: str
    contact_name: str
    company_website: str
    budget_estimate: str
    pricing_model: Literal["hourly", "project", "retainer", "outcome-based"] = "hourly"
    contract_value_tier: Literal["entry", "standard", "premium"] = "standard"


class LeadQualifier:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"].strip()
        self.system_prompt = self._load_system_prompt()

    def _load_system_prompt(self) -> str:
        prompt_path = PROMPTS_DIR / "vertical1_system_prompt.txt"
        return prompt_path.read_text(encoding="utf-8")

    async def qualify(self, raw_text: str, rate_limiter=None) -> TechQualificationResult | None:
        """Qualify a lead using Gemini. Returns None if not qualified or on error.

        Args:
            raw_text: JSON string with lead data.
            rate_limiter: GeminiRateLimiter — re-acquired before each attempt
                          to properly count retries against the sliding window.
        """
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            # Re-acquire rate limiter before every attempt (including retries)
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
                            "contents": [
                                {"parts": [{"text": raw_text}]}
                            ],
                            "generationConfig": {
                                "responseMimeType": "application/json",
                                "temperature": 0.1,
                            },
                        },
                    )

                    # Handle 429 specifically: backoff and retry
                    if resp.status_code == 429:
                        backoff = min(2 ** attempt * 5, 30)  # 10s, 20s, 30s
                        logger.warning(
                            "gemini_429_backoff",
                            attempt=attempt,
                            backoff_seconds=backoff,
                            vertical="tech",
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            logger.error("gemini_429_exhausted_retries", vertical="tech")
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
                result = TechQualificationResult(**data)

                if result.qualified == "NO" or result.fit_score < 4:
                    logger.info(
                        "lead_not_qualified",
                        vertical="tech",
                        reasoning=result.reasoning,
                        fit_score=result.fit_score,
                    )
                    return None

                logger.info(
                    "lead_qualified",
                    vertical="tech",
                    fit_score=result.fit_score,
                    pain_point=result.pain_point,
                    angle=result.suggested_angle,
                    contact_name=result.contact_name,
                    company_website=result.company_website,
                    pricing_model=result.pricing_model,
                    contract_value_tier=result.contract_value_tier,
                )
                return result

            except (json.JSONDecodeError, ValidationError) as exc:
                logger.warning(
                    "qualification_parse_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="tech",
                )
                if attempt == max_retries:
                    logger.error(
                        "qualification_failed_all_retries", vertical="tech"
                    )
                    return None

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "qualification_api_error",
                    attempt=attempt,
                    status_code=exc.response.status_code,
                    error=str(exc),
                    vertical="tech",
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return None

            except Exception as exc:
                logger.error(
                    "qualification_api_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="tech",
                )
                if attempt == max_retries:
                    return None
