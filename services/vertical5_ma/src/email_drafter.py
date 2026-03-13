"""
Email drafter for Vertical 5 — M&A Silver Tsunami.

Generates confidential B2B outreach emails on behalf of Eduardo at
SunBridge Advisors targeting traditional Florida business owners who
may be approaching an inflection point (succession, exit planning).

Rules:
  - Never use the words "sell", "buy", or "acquisition"
  - Under 75 words
  - High-level executive tone
  - CTA: offer a free Market Valuation or 10-minute Confidential Chat
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path

import httpx
import structlog

logger = structlog.get_logger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "shared" / "prompts"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"


@dataclass
class EmailDraft:
    to_email: str
    subject: str
    body: str
    vertical: str = "ma"


class EmailDrafter:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"].strip()
        self.system_prompt = self._load_system_prompt()

    def _load_system_prompt(self) -> str:
        path = PROMPTS_DIR / "vertical5_email_prompt.txt"
        return path.read_text(encoding="utf-8")

    async def draft(
        self,
        founder_name: str,
        company_name: str,
        email: str,
        momentum_signal: str,
        estimated_years_active: str,
        industry_niche: str,
        suggested_angle: str = "market-valuation",
        rate_limiter=None,
    ) -> EmailDraft | None:
        """Generate a confidential outreach email from Eduardo / SunBridge Advisors."""
        user_prompt = json.dumps({
            "founder_name": founder_name,
            "company_name": company_name,
            "momentum_signal": momentum_signal,
            "estimated_years_active": estimated_years_active,
            "industry_niche": industry_niche,
            "suggested_angle": suggested_angle,
        })

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
                            "contents": [{"parts": [{"text": user_prompt}]}],
                            "generationConfig": {
                                "responseMimeType": "application/json",
                                "temperature": 0.7,
                            },
                        },
                    )

                    if resp.status_code == 429:
                        backoff = min(2 ** attempt * 5, 30)
                        logger.warning(
                            "drafter_429_backoff",
                            attempt=attempt,
                            backoff_seconds=backoff,
                            vertical="ma",
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            return self._fallback_draft(
                                founder_name, company_name, email,
                                momentum_signal, estimated_years_active,
                            )

                    resp.raise_for_status()

                body_resp = resp.json()
                text = body_resp["candidates"][0]["content"]["parts"][0]["text"].strip()

                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3].strip()

                data = json.loads(text)
                subject = data.get("subject", "")
                email_body = data.get("body", "")

                if not subject or not email_body:
                    raise ValueError("Empty subject or body from Gemini")

                logger.info(
                    "email_drafted_ai",
                    vertical="ma",
                    to=email,
                    company=company_name,
                    angle=suggested_angle,
                )
                return EmailDraft(to_email=email, subject=subject, body=email_body)

            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.warning(
                    "drafter_parse_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="ma",
                )
                if attempt == max_retries:
                    return self._fallback_draft(
                        founder_name, company_name, email,
                        momentum_signal, estimated_years_active,
                    )

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "drafter_api_error",
                    attempt=attempt,
                    status_code=exc.response.status_code,
                    error=str(exc),
                    vertical="ma",
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return self._fallback_draft(
                        founder_name, company_name, email,
                        momentum_signal, estimated_years_active,
                    )

            except Exception as exc:
                logger.error(
                    "drafter_unexpected_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="ma",
                )
                if attempt == max_retries:
                    return self._fallback_draft(
                        founder_name, company_name, email,
                        momentum_signal, estimated_years_active,
                    )

        return None

    def _fallback_draft(
        self,
        founder_name: str,
        company_name: str,
        email: str,
        momentum_signal: str,
        estimated_years_active: str,
    ) -> EmailDraft:
        """Static fallback if Gemini fails — ensures no qualified lead is lost."""
        name_line = f"Hi {founder_name}," if founder_name else "Hi,"
        subject = f"Quick question about {company_name}"
        body = (
            f"{name_line}\n\n"
            f"Given {company_name}'s track record ({estimated_years_active}), "
            f"I wanted to reach out confidentially. We advise business owners in "
            f"Florida on how companies like yours are being valued right now given "
            f"recent sector consolidation.\n\n"
            f"Would a brief 10-minute confidential chat make sense?\n\n"
            f"Eduardo\n"
            f"SunBridge Advisors"
        )
        logger.warning(
            "email_drafted_fallback",
            vertical="ma",
            to=email,
            company=company_name,
        )
        return EmailDraft(to_email=email, subject=subject, body=body)
