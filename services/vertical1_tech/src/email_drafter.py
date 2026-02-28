"""
Email drafter for Vertical 1 — Tech Services.

Uses Jinja2 to render personalized outreach emails.
"""

from __future__ import annotations

from dataclasses import dataclass

import jinja2
import structlog

logger = structlog.get_logger(__name__)

SUBJECT_TEMPLATE = "{% if company_name %}the {{ pain_point }} at {{ company_name }}{% else %}{{ pain_point }}{% endif %}"

BODY_TEMPLATE = """\
Hi {{ first_name }},

I noticed your recent post regarding the {{ pain_point }}. {{ portfolio_proof }}

I'm a Data Analyst and Quantitative Systems Developer based in Colombia (C1 English). I help teams turn complex data into production-ready systems — from Monte Carlo risk models to ML pipelines that predict churn with 0.89 AUC — at a 40-60% cost advantage versus US/EU hires.

I have a specific idea for how I could tackle the {{ pain_point }}. Worth a brief chat this week?

Best,
Sebastian
https://sebastianramirezanalytics.com"""


@dataclass
class EmailDraft:
    to_email: str
    subject: str
    body: str
    vertical: str = "tech"


class EmailDrafter:
    def __init__(self):
        self.env = jinja2.Environment(
            undefined=jinja2.StrictUndefined,
            autoescape=False,
        )
        self.subject_tpl = self.env.from_string(SUBJECT_TEMPLATE)
        self.body_tpl = self.env.from_string(BODY_TEMPLATE)

    def draft(
        self,
        first_name: str,
        company_name: str,
        email: str,
        pain_point: str,
        portfolio_proof: str = "",
    ) -> EmailDraft | None:
        try:
            ctx = {
                "first_name": first_name,
                "company_name": company_name,
                "pain_point": pain_point,
                "portfolio_proof": portfolio_proof or "I recently completed a similar project with measurable results.",
            }
            subject = self.subject_tpl.render(ctx)
            body = self.body_tpl.render(ctx)

            logger.info(
                "email_drafted",
                vertical="tech",
                to=email,
                company=company_name,
            )
            return EmailDraft(
                to_email=email,
                subject=subject,
                body=body,
            )
        except jinja2.UndefinedError as exc:
            logger.error(
                "email_draft_failed",
                vertical="tech",
                error=str(exc),
            )
            return None
