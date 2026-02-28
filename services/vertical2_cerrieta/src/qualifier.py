"""
Lead qualifier using Gemini 2.5 Flash-Lite for Vertical 2 — Cerrieta.

Uses the Gemini REST API directly via httpx (no SDK dependency conflicts).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import httpx
import structlog
from pydantic import BaseModel
from typing import Literal

logger = structlog.get_logger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "shared" / "prompts"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"


class CerrietaQualificationResult(BaseModel):
    qualified: Literal["YES", "NO"]
    reasoning: str
    aesthetic_match: str
    suggested_angle: Literal["Margin/Wholesale focus", "Aesthetic/Design focus"]


class LeadQualifier:
    def __init__(self):
        self.api_key = os.environ["GEMINI_API_KEY"]
        self.system_prompt = self._load_system_prompt()

    def _load_system_prompt(self) -> str:
        prompt_path = PROMPTS_DIR / "vertical2_system_prompt.txt"
        return prompt_path.read_text(encoding="utf-8")

    async def qualify(self, raw_text: str) -> CerrietaQualificationResult | None:
        """Qualify a lead using Gemini. Returns None if not qualified or on error."""
        max_retries = 3
        for attempt in range(1, max_retries + 1):
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
                    resp.raise_for_status()

                body = resp.json()
                text = body["candidates"][0]["content"]["parts"][0]["text"].strip()

                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3].strip()

                data = json.loads(text)
                result = CerrietaQualificationResult(**data)

                if result.qualified == "NO":
                    logger.info(
                        "lead_not_qualified",
                        vertical="cerrieta",
                        reasoning=result.reasoning,
                    )
                    return None

                logger.info(
                    "lead_qualified",
                    vertical="cerrieta",
                    aesthetic_match=result.aesthetic_match,
                    angle=result.suggested_angle,
                )
                return result

            except (json.JSONDecodeError, Exception) as exc:
                logger.warning(
                    "qualification_parse_error",
                    attempt=attempt,
                    error=str(exc),
                    vertical="cerrieta",
                )
                if attempt == max_retries:
                    logger.error(
                        "qualification_failed_all_retries",
                        vertical="cerrieta",
                    )
                    return None
