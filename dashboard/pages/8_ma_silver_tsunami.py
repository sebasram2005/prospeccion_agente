"""
M&A Silver Tsunami — Acquisition Target Pipeline.

Full-stack view of the Vertical 5 pipeline:
  • Overview: KPIs, source funnel, score distribution, timeline, angle breakdown
  • Targets: Company profiles with fit scores, momentum signals, founder info
  • Outreach: Confidential email queue with HITL approve/reject controls
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.supabase_client import (
    get_ma_raw_leads,
    get_ma_qualified_leads,
    get_ma_email_queue,
    update_email_status,
)
from utils.helpers import (
    STATUS_COLORS,
    MA_SOURCE_COLORS,
    MA_SOURCE_LABELS,
    MA_SOURCE_ICONS,
    MA_ANGLE_COLORS,
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


# ── Shared chart theme ─────────────────────────────────────────────
CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#D1D1E0"),
    margin=dict(l=0, r=0, t=10, b=0),
    height=320,
    showlegend=False,
)

# ── Page header ────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 12px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        🏦 M&A Silver Tsunami
    </h1>
    <p style="color: #C8C8D8; font-size: 0.9rem; margin: 4px 0 0 0;">
        Acquisition target pipeline — traditional Florida businesses near succession
    </p>
</div>
""", unsafe_allow_html=True)

# ── Sidebar controls ───────────────────────────────────────────────
days = st.sidebar.selectbox(
    "Period", [7, 14, 30, 90], index=2,
    format_func=lambda d: f"Last {d} days", key="ma_days",
)

# ── Load data ──────────────────────────────────────────────────────
raw = get_ma_raw_leads(days=days)
qualified = get_ma_qualified_leads(days=days)
emails = get_ma_email_queue(days=days)

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
n_with_email = (
    len(qualified[qualified["contact_email"].astype(str).str.contains("@", na=False)])
    if not qualified.empty else 0
)
n_founders   = (
    len(qualified[qualified["founder_name"].astype(str).str.len() > 0])
    if not qualified.empty else 0
)

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Businesses Scraped", f"{n_raw:,}", f"+{n_raw_today} today")
k2.metric("Qualified (≥7)", f"{n_qual:,}", f"+{n_qual_today} today")
k3.metric("Conversion Rate", f"{conv_rate}%")
k4.metric("Emails Found", f"{n_with_email:,}")
k5.metric("Outreach Pending", f"{n_pending:,}", f"{n_sent} sent")
k6.metric("Avg Fit Score", f"{avg_score}/10")

# ── Tabs ───────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Overview", "🏢 Targets", "📧 Outreach"])


# ═══════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════
with tab1:
    col_left, col_right = st.columns(2)

    # Pipeline funnel
    with col_left:
        st.markdown("#### Pipeline Funnel")
        n_approved = len(emails[emails["status"].isin(["approved", "sent"])]) if not emails.empty else 0
        fig_funnel = go.Figure(go.Funnel(
            y=["Scraped", "Qualified", "In Outreach", "Approved/Sent"],
            x=[n_raw, n_qual, len(emails), n_approved],
            marker=dict(color=["#6366F1", "#10B981", "#F59E0B", "#22C55E"]),
            textinfo="value+percent initial",
            textfont=dict(color="#D1D1E0"),
        ))
        fig_funnel.update_layout(**CHART)
        st.plotly_chart(fig_funnel, use_container_width=True)

    # Source breakdown
    with col_right:
        st.markdown("#### Scraped by Source")
        if not raw.empty and "source" in raw.columns:
            source_counts = raw["source"].value_counts().reset_index()
            source_counts.columns = ["source", "count"]
            source_counts["label"] = source_counts["source"].map(
                lambda s: MA_SOURCE_LABELS.get(s, s)
            )
            fig_src = px.bar(
                source_counts, x="count", y="label", orientation="h",
                color="source",
                color_discrete_map=MA_SOURCE_COLORS,
            )
            fig_src.update_layout(
                **CHART,
                yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
            )
            st.plotly_chart(fig_src, use_container_width=True)
        else:
            st.info("No scraping data yet.")

    # Score distribution + timeline
    col_score, col_timeline = st.columns(2)

    with col_score:
        st.markdown("#### Fit Score Distribution")
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
                color_discrete_sequence=["#F59E0B"],
            )
            fig_time.update_layout(
                **CHART,
                xaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
                yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
            )
            st.plotly_chart(fig_time, use_container_width=True)
        else:
            st.info("No data.")

    # Outreach angle breakdown
    if not qualified.empty and "suggested_angle" in qualified.columns:
        col_angle, col_industry = st.columns(2)

        with col_angle:
            st.markdown("#### Outreach Angles")
            angle_counts = qualified["suggested_angle"].value_counts().reset_index()
            angle_counts.columns = ["angle", "count"]
            fig_angle = px.bar(
                angle_counts, x="angle", y="count",
                color="angle",
                color_discrete_map=MA_ANGLE_COLORS,
            )
            fig_angle.update_layout(
                **{**CHART, "height": 260},
                xaxis=dict(gridcolor="rgba(0,0,0,0)"),
                yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
            )
            st.plotly_chart(fig_angle, use_container_width=True)

        with col_industry:
            st.markdown("#### Top Industries")
            if "industry_niche" in qualified.columns:
                ind_counts = (
                    qualified["industry_niche"]
                    .replace("", "Unknown")
                    .value_counts()
                    .head(8)
                    .reset_index()
                )
                ind_counts.columns = ["industry", "count"]
                fig_ind = px.bar(
                    ind_counts, x="count", y="industry", orientation="h",
                    color_discrete_sequence=["#10B981"],
                )
                fig_ind.update_layout(
                    **{**CHART, "height": 260},
                    yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
                    xaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
                )
                st.plotly_chart(fig_ind, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════
# TAB 2 — TARGETS
# ═══════════════════════════════════════════════════════════════════
with tab2:
    if qualified.empty:
        st.info("No qualified targets yet. Run the scraper to populate.")
    else:
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            angle_options = ["All"] + sorted(qualified["suggested_angle"].unique().tolist())
            angle_filter = st.selectbox("Angle", angle_options, key="ma_angle_filter")
        with fc2:
            min_score = st.slider("Min Fit Score", 1, 10, 7, key="ma_score_filter")
        with fc3:
            email_filter = st.selectbox(
                "Email", ["All", "With email", "Without email"], key="ma_email_filter"
            )
        with fc4:
            sort_order = st.selectbox(
                "Sort", ["Newest first", "Oldest first"], key="ma_sort_order"
            )

        filtered = qualified.copy()
        if angle_filter != "All":
            filtered = filtered[filtered["suggested_angle"] == angle_filter]
        filtered = filtered[filtered["fit_score"] >= min_score]
        if email_filter == "With email":
            filtered = filtered[
                filtered["contact_email"].astype(str).str.contains("@", na=False)
            ]
        elif email_filter == "Without email":
            filtered = filtered[
                ~filtered["contact_email"].astype(str).str.contains("@", na=False)
            ]

        filtered = filtered.sort_values(
            "qualified_at", ascending=(sort_order == "Oldest first")
        )

        st.caption(f"Showing {len(filtered)} of {len(qualified)} qualified targets")

        for _, row in filtered.iterrows():
            score = int(row.get("fit_score", 0))
            company = row.get("company_name") or "Unknown Business"
            industry = row.get("industry_niche", "") or "Unknown Industry"
            score_color = fit_score_color(score)

            with st.expander(
                f"🏢 **{company}** — Score {score}/10 · {industry}",
                expanded=False,
            ):
                c1, c2 = st.columns([2, 1])

                with c1:
                    years = row.get("estimated_years_active", "")
                    if years:
                        st.markdown(f"**Tenure:** {years}")

                    momentum = row.get("momentum_signal", "") or row.get("pain_point", "")
                    if momentum:
                        st.markdown(f"**Signal:** *{momentum}*")

                with c2:
                    st.markdown(
                        f'<div style="font-size:2.5rem;font-weight:700;color:{score_color}">'
                        f'{score}<span style="font-size:1rem;color:#CBD5E1">/10</span></div>',
                        unsafe_allow_html=True,
                    )
                    angle = row.get("suggested_angle", "")
                    if angle:
                        angle_color = MA_ANGLE_COLORS.get(angle, "#6366F1")
                        st.markdown(
                            f'<span style="background:{angle_color};color:white;padding:2px 10px;'
                            f'border-radius:12px;font-size:0.72rem;font-weight:600">'
                            f'{angle.upper()}</span>',
                            unsafe_allow_html=True,
                        )

                    founder = row.get("founder_name", "") or row.get("first_name", "")
                    contact_email = row.get("contact_email", "") or row.get("email", "")
                    website = row.get("company_website", "")

                    if founder and str(founder) not in ("", "None", "nan"):
                        st.markdown(f"👤 {founder}")
                    if contact_email and "@" in str(contact_email):
                        st.markdown(f"📧 {contact_email}")
                    if website:
                        st.markdown(f"🌐 [{truncate(website, 38)}]({_href(website)})")


# ═══════════════════════════════════════════════════════════════════
# TAB 3 — OUTREACH
# ═══════════════════════════════════════════════════════════════════
with tab3:
    if emails.empty:
        st.info("No emails in queue yet.")
    else:
        status_options = ["All", "pending", "approved", "sent", "rejected"]
        status_filter = st.selectbox("Status", status_options, key="ma_status_filter")

        filtered_emails = emails.copy()
        if status_filter != "All":
            filtered_emails = filtered_emails[filtered_emails["status"] == status_filter]

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
            source = row.get("source", "unknown")
            source_icon = MA_SOURCE_ICONS.get(source, "🏢")
            created = row.get("created_at")
            created_str = created.strftime("%b %d %H:%M") if pd.notna(created) else ""

            with st.expander(
                f"{source_icon} {subject} · {to_email} · {created_str}",
                expanded=False,
            ):
                st.markdown(status_badge(status), unsafe_allow_html=True)

                job_url = row.get("job_url", "")
                if job_url:
                    st.markdown(f"[View Business Website]({_href(job_url)})")

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
                        if st.button("✅ Approve", key=f"ma_approve_{row['id']}"):
                            if update_email_status(row["id"], "approved"):
                                st.success("Approved")
                                st.rerun()
                    with br:
                        if st.button("❌ Reject", key=f"ma_reject_{row['id']}"):
                            if update_email_status(row["id"], "rejected"):
                                st.error("Rejected")
                                st.rerun()
