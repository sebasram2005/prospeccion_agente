"""
Email drafter for Vertical 2 — Cerrieta Luxury Pet Furniture.

Uses Jinja2 to render personalized outreach emails.
"""

from __future__ import annotations

from dataclasses import dataclass

import jinja2
import structlog

logger = structlog.get_logger(__name__)

SUBJECT_TEMPLATE = "wholesale / {{ aesthetic_match }} pieces for {{ store_name }}"

BODY_TEMPLATE = """\
Hi {{ first_name }},

I've been following {{ store_name }} and admire how beautifully you've curated your {{ aesthetic_match }} collections.

High-income cat owners are increasingly rejecting generic plastic accessories in favor of design-forward furniture that integrates with their home architecture. I'm the founder of Cerrieta—we design premium, 3D parametric wooden cat furniture.

We're actively expanding our B2B retail partnerships and offer highly attractive wholesale margins for boutiques with your exact aesthetic profile.

Would you be open to me sending over our 2026 catalog and wholesale pricing sheet?

Best,
Sebastian | Cerrieta"""


@dataclass
class EmailDraft:
    to_email: str
    subject: str
    body: str
    vertical: str = "cerrieta"


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
        store_name: str,
        email: str,
        aesthetic_match: str,
    ) -> EmailDraft | None:
        try:
            ctx = {
                "first_name": first_name,
                "store_name": store_name,
                "aesthetic_match": aesthetic_match,
            }
            subject = self.subject_tpl.render(ctx)
            body = self.body_tpl.render(ctx)

            logger.info(
                "email_drafted",
                vertical="cerrieta",
                to=email,
                store=store_name,
            )
            return EmailDraft(
                to_email=email,
                subject=subject,
                body=body,
            )
        except jinja2.UndefinedError as exc:
            logger.error(
                "email_draft_failed",
                vertical="cerrieta",
                error=str(exc),
            )
            return None
