"""
HITL approval state machine.

States:
    PENDING ──[approve]──→ APPROVED ──[send()]──→ SENT (email leads)
    PENDING ──[approve]──→ APPROVED (platform leads — no email sent)
    PENDING ──[reject]───→ REJECTED
    PENDING ──[edit]─────→ EDITING
    EDITING ──[user text]──→ [re-draft LLM] ──→ PENDING (new draft)
"""

from __future__ import annotations

import os

import google.generativeai as genai
import structlog

from .db_client import LeadsRepository
from .email_sender import EmailSender
from .telegram_bot import send_approval_request, PLATFORM_SOURCES

logger = structlog.get_logger(__name__)


class ApprovalRouter:
    def __init__(self, repo: LeadsRepository):
        self.repo = repo
        self.email_sender = EmailSender()
        genai.configure(api_key=os.environ["GEMINI_API_KEY"])
        self.model = genai.GenerativeModel(
            model_name="gemini-2.5-flash-lite-preview-06-17",
            generation_config=genai.GenerationConfig(
                temperature=0.3,
            ),
        )

    async def handle_approve(self, queue_id: str) -> str:
        entry = await self.repo.get_email_queue_entry(queue_id)
        if not entry:
            return "Entry not found."

        if entry["status"] not in ("pending", "edited"):
            return f"Cannot approve: status is '{entry['status']}'."

        source = entry.get("source", "email")
        is_platform = source in PLATFORM_SOURCES

        await self.repo.update_email_status(queue_id, "approved")
        await self.repo.log_hitl_action(queue_id, "approve")

        # Platform leads: mark as approved, user applies manually
        if is_platform:
            return f"Marked as reviewed. Copy the proposal and apply on {source.title()}."

        # Email leads: send automatically
        success = await self.email_sender.send(
            to_email=entry["to_email"],
            subject=entry["subject"],
            body=entry["body"],
            vertical=entry["vertical"],
        )

        if success:
            await self.repo.update_email_status(queue_id, "sent")
            await self.repo.log_hitl_action(queue_id, "sent")
            return "Email sent successfully."
        else:
            await self.repo.update_email_status(queue_id, "approved")
            return "Email send failed. Status remains 'approved' for retry."

    async def handle_reject(self, queue_id: str) -> str:
        entry = await self.repo.get_email_queue_entry(queue_id)
        if not entry:
            return "Entry not found."

        await self.repo.update_email_status(queue_id, "rejected")
        await self.repo.log_hitl_action(queue_id, "reject")
        return "Lead rejected."

    async def handle_edit_request(self, queue_id: str) -> str:
        entry = await self.repo.get_email_queue_entry(queue_id)
        if not entry:
            return "Entry not found."

        await self.repo.update_email_status(queue_id, "editing")
        await self.repo.log_hitl_action(queue_id, "edit")
        return "Send me your edit instructions in the next message."

    async def handle_edit_instructions(
        self, queue_id: str, instructions: str
    ) -> dict | None:
        """Re-draft the email using LLM with the operator's edit instructions.

        Returns the updated entry dict on success, None on failure.
        """
        entry = await self.repo.get_email_queue_entry(queue_id)
        if not entry:
            return None

        source = entry.get("source", "email")
        content_type = "proposal" if source == "upwork" else "email"

        prompt = (
            f"You are an {content_type} editor. Rewrite the following {content_type} draft "
            f"based on the operator's instructions.\n\n"
            f"ORIGINAL SUBJECT: {entry['subject']}\n\n"
            f"ORIGINAL BODY:\n{entry['body']}\n\n"
            f"EDIT INSTRUCTIONS: {instructions}\n\n"
            f"Respond with ONLY the new {content_type} in this exact format:\n"
            f"SUBJECT: <new subject>\n"
            f"BODY:\n<new body>"
        )

        try:
            response = await self.model.generate_content_async(prompt)
            text = response.text.strip()

            # Parse subject and body from response
            if "SUBJECT:" in text and "BODY:" in text:
                parts = text.split("BODY:", 1)
                new_subject = (
                    parts[0].replace("SUBJECT:", "").strip()
                )
                new_body = parts[1].strip()
            else:
                new_subject = entry["subject"]
                new_body = text

            await self.repo.update_email_status(
                queue_id,
                "pending",
                subject=new_subject,
                body=new_body,
                edit_instructions=instructions,
            )
            await self.repo.log_hitl_action(
                queue_id, "edit", note=instructions
            )

            updated = await self.repo.get_email_queue_entry(queue_id)
            return updated

        except Exception as exc:
            logger.error(
                "edit_redraft_failed",
                queue_id=queue_id,
                error=str(exc),
            )
            # Revert to pending so it can be tried again
            await self.repo.update_email_status(queue_id, "pending")
            return None

    async def resend_approval_request(self, entry: dict) -> int | None:
        """Send a new Telegram message with the updated draft."""
        msg_id = await send_approval_request(entry)
        if msg_id:
            await self.repo.set_telegram_message_id(entry["id"], msg_id)
        return msg_id
