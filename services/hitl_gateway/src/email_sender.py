"""
Email sender via Brevo SMTP using aiosmtplib.
"""

from __future__ import annotations

import os
from email.message import EmailMessage

import aiosmtplib
import structlog

logger = structlog.get_logger(__name__)


class EmailSender:
    def __init__(self):
        self.host = os.environ.get("BREVO_SMTP_HOST", "smtp-relay.brevo.com")
        self.port = int(os.environ.get("BREVO_SMTP_PORT", "587"))
        self.user = os.environ["BREVO_SMTP_USER"]
        self.password = os.environ["BREVO_SMTP_PASSWORD"]

    async def send(
        self,
        to_email: str,
        subject: str,
        body: str,
        vertical: str,
    ) -> bool:
        sender_name = os.environ.get(
            f"SENDER_{'V1' if vertical == 'tech' else 'V2'}_NAME",
            "Sebastian",
        )
        sender_email = os.environ.get(
            f"SENDER_{'V1' if vertical == 'tech' else 'V2'}_EMAIL",
            self.user,
        )

        msg = EmailMessage()
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = sender_email
        msg["List-Unsubscribe"] = f"<mailto:{sender_email}?subject=unsubscribe>"
        msg.set_content(body)

        try:
            await aiosmtplib.send(
                msg,
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                start_tls=True,
            )
            logger.info(
                "email_sent",
                to=to_email,
                subject=subject,
                vertical=vertical,
            )
            return True
        except Exception as exc:
            logger.error(
                "email_send_failed",
                to=to_email,
                subject=subject,
                error=str(exc),
            )
            return False
