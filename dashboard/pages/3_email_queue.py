"""
Email Queue — View drafts, approve/reject from the dashboard.
"""

import streamlit as st
import pandas as pd

from utils.supabase_client import get_email_queue, get_qualified_leads, update_email_status
from utils.helpers import status_badge

st.header("Email Queue")

# ── Load data ────────────────────────────────────────────────────
days = st.sidebar.selectbox("Period", [7, 14, 30, 90], index=2, format_func=lambda d: f"Last {d} days", key="eq_days")
emails = get_email_queue(days=days)
qualified = get_qualified_leads(days=days)

if emails.empty:
    st.info("No emails in queue for this period.")
    st.stop()

# ── Merge with qualified leads for fit_score/company ─────────────
if not qualified.empty:
    emails = emails.merge(
        qualified[["id", "fit_score", "company_name", "first_name"]],
        left_on="qualified_lead_id",
        right_on="id",
        how="left",
        suffixes=("", "_qual"),
    )

# ── Filter by status ─────────────────────────────────────────────
statuses = emails["status"].unique().tolist()
selected_status = st.sidebar.multiselect("Status", statuses, default=statuses)
df = emails[emails["status"].isin(selected_status)]

# ── KPIs ─────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total", len(emails))
c2.metric("Pending", len(emails[emails["status"] == "pending"]))
c3.metric("Sent", len(emails[emails["status"] == "sent"]))
c4.metric("Rejected", len(emails[emails["status"] == "rejected"]))

st.divider()

# ── Email list ───────────────────────────────────────────────────
for idx, row in df.iterrows():
    status = row["status"]
    score = int(row["fit_score"]) if "fit_score" in row and pd.notna(row.get("fit_score")) else "?"
    company = row.get("company_name", "Unknown")
    to_email = row["to_email"]
    subject = row["subject"]

    header_html = f"{status_badge(status)} **[{score}]** {subject[:70]}"
    with st.expander(header_html):
        st.markdown(f"**To:** {to_email}")
        st.markdown(f"**Company:** {company}")
        st.markdown(f"**Subject:** {subject}")
        st.divider()
        st.markdown(row["body"])
        st.divider()
        st.caption(f"Created: {row['created_at']} | Updated: {row['updated_at']}")

        # HITL actions
        if status == "pending":
            col_approve, col_reject, _ = st.columns([1, 1, 4])
            with col_approve:
                if st.button("Approve", key=f"approve_{row['id']}", type="primary"):
                    if update_email_status(row["id"], "approved"):
                        st.success("Approved!")
                        st.rerun()
                    else:
                        st.error("Failed to update.")
            with col_reject:
                if st.button("Reject", key=f"reject_{row['id']}"):
                    if update_email_status(row["id"], "rejected"):
                        st.warning("Rejected.")
                        st.rerun()
                    else:
                        st.error("Failed to update.")
