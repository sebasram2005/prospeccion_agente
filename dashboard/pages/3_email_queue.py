"""
Email Queue — View drafts, approve/reject with HITL controls.
"""

import streamlit as st
import pandas as pd

from utils.supabase_client import get_email_queue, get_qualified_leads, update_email_status
from utils.helpers import STATUS_COLORS

# ── Header ────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 8px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        ✉️ Email Queue
    </h1>
    <p style="color: #8B8BA0; font-size: 0.9rem; margin: 4px 0 0 0;">
        AI-drafted outreach emails with human-in-the-loop approval
    </p>
</div>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────
days = st.sidebar.selectbox(
    "Period", [7, 14, 30, 90], index=2,
    format_func=lambda d: f"Last {d} days", key="eq_days",
)
emails = get_email_queue(days=days)
qualified = get_qualified_leads(days=days)

if emails.empty:
    st.info("No emails in queue for this period.")
    st.stop()

# ── Merge with qualified leads ────────────────────────────────────
if not qualified.empty:
    emails = emails.merge(
        qualified[["id", "fit_score", "company_name", "first_name"]],
        left_on="qualified_lead_id", right_on="id",
        how="left", suffixes=("", "_qual"),
    )

# ── Filter by status ──────────────────────────────────────────────
statuses = emails["status"].unique().tolist()
selected_status = st.sidebar.multiselect("Status", statuses, default=statuses)
df = emails[emails["status"].isin(selected_status)]

# ── KPIs ──────────────────────────────────────────────────────────
total = len(emails)
n_pending = len(emails[emails["status"] == "pending"])
n_sent = len(emails[emails["status"] == "sent"])
n_rejected = len(emails[emails["status"] == "rejected"])
n_approved = len(emails[emails["status"] == "approved"])

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total", total)
c2.metric("Pending", n_pending)
c3.metric("Approved", n_approved)
c4.metric("Sent", n_sent)
c5.metric("Rejected", n_rejected)

# ── Status distribution bar ───────────────────────────────────────
if total > 0:
    parts = []
    for status, count in [("sent", n_sent), ("approved", n_approved), ("pending", n_pending), ("rejected", n_rejected)]:
        if count > 0:
            pct = count / total * 100
            color = STATUS_COLORS.get(status, "#6B7280")
            parts.append(
                f'<div style="width:{pct}%; background:{color}; height:6px; border-radius:3px;" '
                f'title="{status}: {count}"></div>'
            )
    st.markdown(
        '<div style="display:flex; gap:3px; margin: 4px 0 16px 0; border-radius:3px; overflow:hidden;">'
        + "".join(parts) + '</div>',
        unsafe_allow_html=True,
    )

# ── Email cards ───────────────────────────────────────────────────
for _, row in df.iterrows():
    status = row["status"]
    score = int(row["fit_score"]) if "fit_score" in row and pd.notna(row.get("fit_score")) else "?"
    company = row.get("company_name", "Unknown") or "Unknown"
    color = STATUS_COLORS.get(status, "#6B7280")

    # Header with status badge
    badge = (
        f'<span style="background:{color}; color:white; padding:2px 10px; '
        f'border-radius:12px; font-size:0.72rem; font-weight:600; '
        f'letter-spacing:0.04em;">{status.upper()}</span>'
    )
    header = f"{badge}  **[{score}/10]** {row['subject'][:70]}"

    with st.expander(header):
        # Email metadata
        meta_col1, meta_col2, meta_col3 = st.columns(3)
        with meta_col1:
            st.markdown(f"**To:** `{row['to_email']}`")
        with meta_col2:
            st.markdown(f"**Company:** {company}")
        with meta_col3:
            st.markdown(f"**Vertical:** `{row.get('vertical', '—')}`")

        st.markdown("---")

        # Subject
        st.markdown(f"**Subject:** {row['subject']}")

        # Email body in a styled container
        st.markdown(f"""
        <div style="
            background: #0E1117;
            border: 1px solid rgba(99,102,241,0.15);
            border-radius: 8px;
            padding: 16px 20px;
            margin: 8px 0;
            font-size: 0.88rem;
            line-height: 1.6;
            color: #D0D0E0;
            white-space: pre-wrap;
        ">{row['body']}</div>
        """, unsafe_allow_html=True)

        # Timestamps
        created = pd.to_datetime(row["created_at"]).strftime("%b %d, %Y %H:%M")
        updated = pd.to_datetime(row["updated_at"]).strftime("%b %d, %Y %H:%M")
        st.caption(f"Created: {created}  ·  Updated: {updated}")

        # HITL actions
        if status == "pending":
            st.markdown("---")
            col_approve, col_reject, _ = st.columns([1, 1, 4])
            with col_approve:
                if st.button("✓ Approve", key=f"approve_{row['id']}", type="primary"):
                    if update_email_status(row["id"], "approved"):
                        st.success("Approved — will be sent automatically.")
                        st.rerun()
                    else:
                        st.error("Failed to update.")
            with col_reject:
                if st.button("✕ Reject", key=f"reject_{row['id']}"):
                    if update_email_status(row["id"], "rejected"):
                        st.warning("Rejected.")
                        st.rerun()
                    else:
                        st.error("Failed to update.")
