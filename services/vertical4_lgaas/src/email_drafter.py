"""
Email drafter for Vertical 4 — LGaaS Prospects.

Generates cold B2B sales emails pitching Sebastián's Lead Generation
as a Service (LGaaS) to boutique consulting firms.
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
    vertical: str = "lgaas"


class EmailDrafter:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"].strip()
        self.system_prompt = self._load_system_prompt()

    def _load_system_prompt(self) -> str:
        path = PROMPTS_DIR / "vertical4_email_prompt.txt"
        return path.read_text(encoding="utf-8")

    async def draft(
        self,
        first_name: str,
        company_name: str,
        email: str,
        pain_point: str,
        source: str = "fractional_cfo",
        key_technology: str = "",
        suggested_angle: str = "roi-calculator",
        rate_limiter=None,
    ) -> EmailDraft | None:
        """Generate a personalized cold sales email using Gemini."""
        user_prompt = json.dumps({
            "source": source,
            "first_name": first_name,
            "company_name": company_name,
            "pain_point": pain_point,
            "key_technology": key_technology,
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
                            vertical="lgaas",
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            return self._fallback_draft(first_name, company_name, email, pain_point, source)

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
                    vertical="lgaas",
                    source=source,
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
                    vertical="lgaas",
                )
                if attempt == max_retries:
                    return self._fallback_draft(first_name, company_name, email, pain_point, source)

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "drafter_api_error",
                    attempt=attempt,
                    status_code=exc.response.status_code,
                    error=str(exc),
                    vertical="lgaas",
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return self._fallback_draft(first_name, company_name, email, pain_point, source)

            except Exception as exc:
                logger.error(
                    "drafter_unexpected_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="lgaas",
                )
                if attempt == max_retries:
                    return self._fallback_draft(first_name, company_name, email, pain_point, source)

        return None

    def _fallback_draft(
        self,
        first_name: str,
        company_name: str,
        email: str,
        pain_point: str,
        source: str = "fractional_cfo",
    ) -> EmailDraft:
        """Static fallback if Gemini fails — ensures no qualified lead is lost."""
        subject = f"Automated lead generation for {company_name}"
        body = (
            f"Hi {first_name},\n\n"
            f"I noticed {company_name} operates in a space where finding qualified "
            f"prospects is the main growth bottleneck. "
            f"I built an autonomous prospecting system that identifies pre-qualified leads, "
            f"enriches them with full-page analysis via Jina Reader, and drafts personalized "
            f"outreach using Gemini AI — all running at ~$0.50/month operational cost.\n\n"
            f"If {pain_point or 'manual prospecting'} sounds familiar, "
            f"I can walk you through a live demo where the system finds prospects for "
            f"your specific ICP in real time.\n\n"
            f"Sebastian\n"
            f"https://sebastianramirezanalytics.com"
        )
        logger.warning(
            "email_drafted_fallback",
            vertical="lgaas",
            to=email,
            company=company_name,
        )
        return EmailDraft(to_email=email, subject=subject, body=body)
