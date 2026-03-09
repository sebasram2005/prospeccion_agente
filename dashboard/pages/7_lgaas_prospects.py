"""
LGaaS Prospects — B2B Sales Pipeline Dashboard.

Full-stack view of the Vertical 4 pipeline:
  • Overview: KPIs, niche funnel, score distribution, timeline
  • Prospects: Firm profiles with ICP scores, pain points, contact info
  • Outreach: Cold email queue with HITL approve/reject controls
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.supabase_client import (
    get_lgaas_raw_leads,
    get_lgaas_qualified_leads,
    get_lgaas_email_queue,
    update_email_status,
)
from utils.helpers import (
    STATUS_COLORS,
    LGAAS_NICHE_COLORS,
    LGAAS_NICHE_LABELS,
    LGAAS_NICHE_ICONS,
    LGAAS_ANGLE_COLORS,
    fit_score_color,
    status_badge,
    truncate,
)


def _href(url: str) -> str:
    if not url:
        return url
    if url.startswith(("http://", "https://")):
        return url
    return "https://" + url


# ── Shared chart theme ────────────────────────────────────────────
CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#D1D1E0"),
    margin=dict(l=0, r=0, t=10, b=0),
    height=320,
    showlegend=False,
)

# ── Page header ───────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 12px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        🚀 LGaaS Prospects
    </h1>
    <p style="color: #C8C8D8; font-size: 0.9rem; margin: 4px 0 0 0;">
        B2B sales pipeline — boutique consulting firms as LGaaS service clients
    </p>
</div>
""", unsafe_allow_html=True)

# ── Sidebar controls ──────────────────────────────────────────────
days = st.sidebar.selectbox(
    "Period", [7, 14, 30, 90], index=2,
    format_func=lambda d: f"Last {d} days", key="lgaas_days",
)

# ── Load data ─────────────────────────────────────────────────────
raw = get_lgaas_raw_leads(days=days)
qualified = get_lgaas_qualified_leads(days=days)
emails = get_lgaas_email_queue(days=days)

now = datetime.now(timezone.utc)
today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

n_raw        = len(raw)
n_raw_today  = len(raw[raw["scraped_at"] >= today_start]) if not raw.empty else 0
n_qual       = len(qualified)
n_qual_today = len(qualified[qualified["qualified_at"] >= today_start]) if not qualified.empty else 0
n_pending    = len(emails[emails["status"] == "pending"]) if not emails.empty else 0
n_sent       = len(emails[emails["status"] == "sent"]) if not emails.empty else 0
avg_score    = round(qualified["fit_score"].mean(), 1) if not qualified.empty else 0
conv_rate    = round(n_qual / n_raw * 100, 1) if n_raw > 0 else 0
n_with_email = len(qualified[qualified["email"].astype(str).str.contains("@", na=False)]) if not qualified.empty else 0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Firms Scraped", f"{n_raw:,}", f"+{n_raw_today} today")
k2.metric("Qualified (≥7)", f"{n_qual:,}", f"+{n_qual_today} today")
k3.metric("Conversion Rate", f"{conv_rate}%")
k4.metric("Emails Found", f"{n_with_email:,}")
k5.metric("Outreach Pending", f"{n_pending:,}", f"{n_sent} sent")
k6.metric("Avg ICP Score", f"{avg_score}/10")

# ── Tabs ──────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Overview", "🏢 Prospects", "📧 Outreach"])


# ════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ════════════════════════════════════════════════════════════════
with tab1:
    col_left, col_right = st.columns(2)

    # Pipeline funnel
    with col_left:
        st.markdown("#### Pipeline Funnel")
        n_approved = len(emails[emails["status"].isin(["approved", "sent"])]) if not emails.empty else 0
        funnel_data = {
            "Stage": ["Scraped", "Qualified", "Emailed", "Approved/Sent"],
            "Count": [n_raw, n_qual, len(emails), n_approved],
        }
        fig_funnel = go.Figure(go.Funnel(
            y=funnel_data["Stage"],
            x=funnel_data["Count"],
            marker=dict(color=["#6366F1", "#10B981", "#F59E0B", "#22C55E"]),
            textinfo="value+percent initial",
            textfont=dict(color="#D1D1E0"),
        ))
        fig_funnel.update_layout(**CHART)
        st.plotly_chart(fig_funnel, use_container_width=True)

    # Niche breakdown
    with col_right:
        st.markdown("#### Qualified by Niche")
        if not qualified.empty:
            niche_counts = qualified["niche_category"].value_counts().reset_index()
            niche_counts.columns = ["niche", "count"]
            niche_counts["label"] = niche_counts["niche"].map(
                lambda n: LGAAS_NICHE_LABELS.get(n, n)
            )
            niche_counts["color"] = niche_counts["niche"].map(
                lambda n: LGAAS_NICHE_COLORS.get(n, "#9CA3AF")
            )
            fig_niche = px.bar(
                niche_counts, x="count", y="label", orientation="h",
                color="niche",
                color_discrete_map=LGAAS_NICHE_COLORS,
            )
            fig_niche.update_layout(
                **CHART,
                yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
            )
            st.plotly_chart(fig_niche, use_container_width=True)
        else:
            st.info("No qualified leads yet.")

    # ICP score distribution
    col_score, col_timeline = st.columns(2)

    with col_score:
        st.markdown("#### ICP Score Distribution")
        if not qualified.empty:
            fig_score = px.histogram(
                qualified, x="fit_score", nbins=10,
                color_discrete_sequence=["#6366F1"],
            )
            fig_score.update_layout(**CHART, bargap=0.1)
            fig_score.update_traces(marker_line_color="#1a1a2e", marker_line_width=1)
            st.plotly_chart(fig_score, use_container_width=True)
        else:
            st.info("No data.")

    with col_timeline:
        st.markdown("#### Scraping Activity")
        if not raw.empty:
            raw_copy = raw.copy()
            raw_copy["date"] = raw_copy["scraped_at"].dt.date
            daily = raw_copy.groupby("date").size().reset_index(name="leads")
            fig_time = px.area(
                daily, x="date", y="leads",
                color_discrete_sequence=["#6366F1"],
            )
            fig_time.update_layout(
                **CHART,
                xaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
                yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
            )
            st.plotly_chart(fig_time, use_container_width=True)
        else:
            st.info("No data.")

    # Angle distribution
    if not qualified.empty and "suggested_angle" in qualified.columns:
        st.markdown("#### Suggested Outreach Angles")
        angle_counts = qualified["suggested_angle"].value_counts().reset_index()
        angle_counts.columns = ["angle", "count"]
        fig_angle = px.bar(
            angle_counts, x="angle", y="count",
            color="angle",
            color_discrete_map=LGAAS_ANGLE_COLORS,
        )
        fig_angle.update_layout(
            **{**CHART, "height": 280},
            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
            yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
        )
        st.plotly_chart(fig_angle, use_container_width=True)


# ════════════════════════════════════════════════════════════════
# TAB 2 — PROSPECTS
# ════════════════════════════════════════════════════════════════
with tab2:
    if qualified.empty:
        st.info("No qualified prospects yet. Run the scraper to populate.")
    else:
        # Filters
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            niche_options = ["All"] + sorted(qualified["niche_category"].unique().tolist())
            niche_filter = st.selectbox("Niche", niche_options, key="lgaas_niche_filter")
        with fc2:
            min_score = st.slider("Min ICP Score", 1, 10, 7, key="lgaas_score_filter")
        with fc3:
            email_filter = st.selectbox(
                "Email", ["All", "With email", "Without email"], key="lgaas_email_filter"
            )

        filtered = qualified.copy()
        if niche_filter != "All":
            filtered = filtered[filtered["niche_category"] == niche_filter]
        filtered = filtered[filtered["fit_score"] >= min_score]
        if email_filter == "With email":
            filtered = filtered[filtered["email"].astype(str).str.contains("@", na=False)]
        elif email_filter == "Without email":
            filtered = filtered[~filtered["email"].astype(str).str.contains("@", na=False)]

        st.caption(f"Showing {len(filtered)} of {len(qualified)} qualified prospects")

        for _, row in filtered.sort_values("fit_score", ascending=False).iterrows():
            niche = row.get("niche_category", "other")
            niche_icon = LGAAS_NICHE_ICONS.get(niche, "🏢")
            niche_label = LGAAS_NICHE_LABELS.get(niche, niche)
            score = int(row.get("fit_score", 0))
            company = row.get("inferred_company") or row.get("company_name") or "Unknown Firm"
            score_color = fit_score_color(score)

            with st.expander(
                f"{niche_icon} **{company}** — Score {score}/10 · {niche_label}",
                expanded=False,
            ):
                c1, c2 = st.columns([2, 1])
                with c1:
                    pain = row.get("pain_point", "")
                    if pain:
                        st.markdown(f"**Pain Point:** {pain}")
                    ticket = row.get("estimated_ticket", "")
                    if ticket:
                        st.markdown(f"**Estimated Ticket:** {ticket}")
                    reasoning = row.get("technical_reasoning", "")
                    if reasoning:
                        st.markdown(f"**Reasoning:** *{reasoning}*")

                    green = row.get("green_flags", [])
                    red = row.get("red_flags", [])
                    if green:
                        st.markdown("**Green Flags:** " + " · ".join(f"✅ {f}" for f in green[:5]))
                    if red:
                        st.markdown("**Red Flags:** " + " · ".join(f"🚩 {f}" for f in red[:3]))

                with c2:
                    st.markdown(
                        f'<div style="font-size:2.5rem;font-weight:700;color:{score_color}">'
                        f'{score}<span style="font-size:1rem;color:#CBD5E1">/10</span></div>',
                        unsafe_allow_html=True,
                    )
                    angle = row.get("suggested_angle", "")
                    if angle:
                        angle_color = LGAAS_ANGLE_COLORS.get(angle, "#6366F1")
                        st.markdown(
                            f'<span style="background:{angle_color};color:white;padding:2px 10px;'
                            f'border-radius:12px;font-size:0.72rem;font-weight:600">'
                            f'{angle.upper()}</span>',
                            unsafe_allow_html=True,
                        )

                    contact = row.get("contact_name", "")
                    contact_email = row.get("contact_email", "") or row.get("email", "")
                    website = row.get("company_website", "")
                    if contact:
                        st.markdown(f"👤 {contact}")
                    if contact_email and "@" in str(contact_email):
                        st.markdown(f"📧 {contact_email}")
                    if website:
                        st.markdown(f"🌐 [{truncate(website, 40)}]({_href(website)})")


# ════════════════════════════════════════════════════════════════
# TAB 3 — OUTREACH
# ════════════════════════════════════════════════════════════════
with tab3:
    if emails.empty:
        st.info("No emails in queue yet.")
    else:
        # Status filter
        status_options = ["All", "pending", "approved", "sent", "rejected"]
        status_filter = st.selectbox("Status", status_options, key="lgaas_status_filter")

        filtered_emails = emails.copy()
        if status_filter != "All":
            filtered_emails = filtered_emails[filtered_emails["status"] == status_filter]

        # Summary row
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("Pending", len(emails[emails["status"] == "pending"]))
        sc2.metric("Approved", len(emails[emails["status"] == "approved"]))
        sc3.metric("Sent", len(emails[emails["status"] == "sent"]))
        sc4.metric("Rejected", len(emails[emails["status"] == "rejected"]))

        st.markdown("---")

        for _, row in filtered_emails.iterrows():
            status = row.get("status", "pending")
            subject = row.get("subject", "(no subject)")
            to_email = row.get("to_email", "")
            niche = row.get("source", "unknown")
            niche_icon = LGAAS_NICHE_ICONS.get(niche, "🏢")
            created = row.get("created_at")
            created_str = created.strftime("%b %d %H:%M") if pd.notna(created) else ""

            with st.expander(
                f"{niche_icon} {subject} · {to_email} · {created_str}",
                expanded=False,
            ):
                st.markdown(
                    status_badge(status),
                    unsafe_allow_html=True,
                )

                job_url = row.get("job_url", "")
                if job_url:
                    st.markdown(f"[View Firm Website]({_href(job_url)})")

                st.markdown("**Email Body:**")
                body_text = row.get("body", "").replace("\n", "<br>")
                st.markdown(
                    f'<div style="background:#12121F;border:1px solid rgba(99,102,241,0.2);'
                    f'border-radius:8px;padding:14px 16px;font-size:0.88rem;'
                    f'color:#E2E8F0;line-height:1.6;white-space:pre-wrap;">'
                    f'{body_text}</div>',
                    unsafe_allow_html=True,
                )

                if status == "pending":
                    ba, br = st.columns(2)
                    with ba:
                        if st.button("✅ Approve", key=f"lgaas_approve_{row['id']}"):
                            if update_email_status(row["id"], "approved"):
                                st.success("Approved")
                                st.rerun()
                    with br:
                        if st.button("❌ Reject", key=f"lgaas_reject_{row['id']}"):
                            if update_email_status(row["id"], "rejected"):
                                st.error("Rejected")
                                st.rerun()
