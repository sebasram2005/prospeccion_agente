"""
Email drafter for Vertical 1 — Tech Services.

Uses Gemini to generate personalized outreach emails based on
the professional profile and lead qualification data.
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
    vertical: str = "tech"


class EmailDrafter:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"].strip()
        self.profile = self._load_profile()
        self.system_prompt = self._load_system_prompt()

    def _load_profile(self) -> str:
        path = PROMPTS_DIR / "sebastian_profile.md"
        return path.read_text(encoding="utf-8")

    def _load_system_prompt(self) -> str:
        path = PROMPTS_DIR / "email_system_prompt.txt"
        template = path.read_text(encoding="utf-8")
        return template.replace("{profile}", self.profile)

    async def draft(
        self,
        first_name: str,
        company_name: str,
        email: str,
        pain_point: str,
        portfolio_proof: str = "",
        suggested_angle: str = "ROI-focused",
        job_title: str = "",
        budget_estimate: str = "",
        source: str = "email",
        rate_limiter=None,
    ) -> EmailDraft | None:
        """Generate a personalized message using Gemini.

        Adapts output to source: Upwork proposal, LinkedIn message,
        cover letter, or cold email.
        """
        user_prompt = json.dumps({
            "source": source,
            "first_name": first_name,
            "company_name": company_name,
            "pain_point": pain_point,
            "portfolio_proof": portfolio_proof,
            "suggested_angle": suggested_angle,
            "job_title": job_title,
            "budget_estimate": budget_estimate,
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
                            "contents": [
                                {"parts": [{"text": user_prompt}]}
                            ],
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
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            return self._fallback_draft(
                                first_name, company_name, email,
                                pain_point, portfolio_proof,
                            )

                    resp.raise_for_status()

                body = resp.json()
                text = body["candidates"][0]["content"]["parts"][0]["text"].strip()

                # Strip markdown code fences
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
                    vertical="tech",
                    source=source,
                    to=email,
                    company=company_name,
                    angle=suggested_angle,
                )
                return EmailDraft(
                    to_email=email,
                    subject=subject,
                    body=email_body,
                )

            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.warning(
                    "drafter_parse_error",
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt == max_retries:
                    return self._fallback_draft(
                        first_name, company_name, email,
                        pain_point, portfolio_proof,
                    )

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "drafter_api_error",
                    attempt=attempt,
                    status_code=exc.response.status_code,
                    error=str(exc),
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return self._fallback_draft(
                        first_name, company_name, email,
                        pain_point, portfolio_proof,
                    )

            except Exception as exc:
                logger.error(
                    "drafter_unexpected_error",
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt == max_retries:
                    return self._fallback_draft(
                        first_name, company_name, email,
                        pain_point, portfolio_proof,
                    )

        return None

    def _fallback_draft(
        self,
        first_name: str,
        company_name: str,
        email: str,
        pain_point: str,
        portfolio_proof: str,
    ) -> EmailDraft:
        """Static fallback if Gemini fails — ensures no lead is lost."""
        proof = portfolio_proof or "I recently completed a similar project with measurable results."
        subject = f"Re: {pain_point}" if pain_point else f"Quick question for {company_name}"
        body = (
            f"Hi {first_name},\n\n"
            f"Your posting about {pain_point} caught my attention. {proof}\n\n"
            f"Would a 15-minute call this week make sense to discuss?\n\n"
            f"Sebastian\n"
            f"https://sebastianramirezanalytics.com"
        )
        logger.warning(
            "email_drafted_fallback",
            vertical="tech",
            to=email,
            company=company_name,
        )
        return EmailDraft(to_email=email, subject=subject, body=body)
