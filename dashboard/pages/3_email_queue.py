"""
Outreach Queue — View AI-drafted proposals/emails, approve/reject with HITL controls.
"""

import streamlit as st
import pandas as pd

from utils.supabase_client import get_email_queue, get_qualified_leads, update_email_status
from utils.helpers import STATUS_COLORS, SOURCE_COLORS

PLATFORM_SOURCES = {"upwork", "linkedin", "weworkremotely", "indeed"}

SOURCE_LABELS = {
    "upwork": "Upwork Proposal",
    "linkedin": "LinkedIn Message",
    "weworkremotely": "WWR Application",
    "indeed": "Indeed Application",
}

# ── Header ────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 8px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        ✉️ Outreach Queue
    </h1>
    <p style="color: #C8C8D8; font-size: 0.9rem; margin: 4px 0 0 0;">
        AI-drafted proposals and emails with human-in-the-loop approval
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
    st.info("No outreach in queue for this period.")
    st.stop()

# ── Merge with qualified leads ────────────────────────────────────
if not qualified.empty:
    emails = emails.merge(
        qualified[["id", "fit_score", "company_name", "first_name"]],
        left_on="qualified_lead_id", right_on="id",
        how="left", suffixes=("", "_qual"),
    )

# ── Sidebar filters ──────────────────────────────────────────────
statuses = emails["status"].unique().tolist()
selected_status = st.sidebar.multiselect("Status", statuses, default=statuses)

sources = emails["source"].dropna().unique().tolist() if "source" in emails.columns else []
if sources:
    selected_sources = st.sidebar.multiselect("Source", sorted(sources), default=sources)
    df = emails[emails["status"].isin(selected_status) & emails["source"].isin(selected_sources)]
else:
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
            color = STATUS_COLORS.get(status, "#9CA3B0")
            parts.append(
                f'<div style="width:{pct}%; background:{color}; height:6px; border-radius:3px;" '
                f'title="{status}: {count}"></div>'
            )
    st.markdown(
        '<div style="display:flex; gap:3px; margin: 4px 0 16px 0; border-radius:3px; overflow:hidden;">'
        + "".join(parts) + '</div>',
        unsafe_allow_html=True,
    )

# ── Outreach cards ────────────────────────────────────────────────
for _, row in df.iterrows():
    status = row["status"]
    source = row.get("source", "email") or "email"
    job_url = row.get("job_url", "") or ""
    is_platform = source in PLATFORM_SOURCES
    source_label = SOURCE_LABELS.get(source, "Cold Email")
    score = int(row["fit_score"]) if "fit_score" in row and pd.notna(row.get("fit_score")) else "?"
    company = row.get("company_name", "Unknown") or "Unknown"
    status_color = STATUS_COLORS.get(status, "#9CA3B0")
    source_color = SOURCE_COLORS.get(source, "#9CA3B0")

    # Header — expander labels only support markdown, not HTML
    status_icons = {"pending": "🟡", "approved": "🟣", "sent": "🟢", "rejected": "🔴"}
    status_icon = status_icons.get(status, "⚪")
    header = f"{status_icon} **{status.upper()}** · {source_label} · [{score}/10] {row['subject'][:60]}"

    with st.expander(header):
        # Rendered badges inside the expander
        st.markdown(
            f'<span style="background:{status_color}; color:white; padding:2px 10px; '
            f'border-radius:12px; font-size:0.72rem; font-weight:600; '
            f'letter-spacing:0.04em;">{status.upper()}</span> '
            f'<span style="background:{source_color}20; color:{source_color}; padding:2px 10px; '
            f'border:1px solid {source_color}50; border-radius:12px; font-size:0.72rem; '
            f'font-weight:600;">{source_label}</span>',
            unsafe_allow_html=True,
        )

        # Metadata row
        meta_cols = st.columns(3)
        with meta_cols[0]:
            if is_platform:
                st.markdown(f"**Platform:** {source_label}")
            else:
                st.markdown(f"**To:** `{row['to_email']}`")
        with meta_cols[1]:
            st.markdown(f"**Company:** {company}")
        with meta_cols[2]:
            if job_url:
                st.markdown(f"[Open Job Post ↗]({job_url})")

        st.markdown("---")

        # Subject
        st.markdown(f"**Subject:** {row['subject']}")

        # Body in styled container
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

        # Platform hint
        if is_platform:
            st.markdown(f"""
            <div style="
                background: rgba(245,158,11,0.08);
                border: 1px solid rgba(245,158,11,0.25);
                border-radius: 8px;
                padding: 10px 14px;
                margin: 8px 0;
                font-size: 0.8rem;
                color: #F59E0B;
            ">
                Copy the text above and paste it into your {source.title()} application.
            </div>
            """, unsafe_allow_html=True)

        # Timestamps
        created = pd.to_datetime(row["created_at"]).strftime("%b %d, %Y %H:%M")
        updated = pd.to_datetime(row["updated_at"]).strftime("%b %d, %Y %H:%M")
        st.caption(f"Created: {created}  ·  Updated: {updated}")

        # HITL actions
        if status == "pending":
            st.markdown("---")
            approve_label = "✓ Mark Reviewed" if is_platform else "✓ Send Email"
            col_approve, col_reject, _ = st.columns([1, 1, 4])
            with col_approve:
                if st.button(approve_label, key=f"approve_{row['id']}", type="primary"):
                    if update_email_status(row["id"], "approved"):
                        if is_platform:
                            st.success(f"Marked as reviewed. Apply on {source.title()}.")
                        else:
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
