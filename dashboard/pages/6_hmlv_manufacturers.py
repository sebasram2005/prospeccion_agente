"""
HMLV Manufacturers — B2B SaaS Prospecting Dashboard.

Full-stack view of the Vertical 3 pipeline:
  • Overview: KPIs, industry funnel, score distribution, timeline
  • Manufacturers: Company profiles with ICP scores, flags, tech stack
  • Outreach: Cold email queue with HITL approve/reject controls
  • Intelligence: Tech stack analysis, pain points, flag patterns, dork performance
"""

from __future__ import annotations

from collections import Counter

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.supabase_client import (
    get_hmlv_raw_leads,
    get_hmlv_qualified_leads,
    get_hmlv_email_queue,
    update_email_status,
)
from utils.helpers import (
    STATUS_COLORS,
    INDUSTRY_COLORS,
    INDUSTRY_LABELS,
    INDUSTRY_ICONS,
    ANGLE_COLORS,
    HMLV_SOURCE_COLORS,
    fit_score_color,
)

def _href(url: str) -> str:
    """Ensure a URL has a protocol prefix so browsers treat it as absolute."""
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
        🏭 HMLV Manufacturers
    </h1>
    <p style="color: #C8C8D8; font-size: 0.9rem; margin: 4px 0 0 0;">
        B2B SaaS prospecting — CAD/BOM/DXF automation for custom manufacturers
    </p>
</div>
""", unsafe_allow_html=True)

# ── Sidebar controls ──────────────────────────────────────────────
days = st.sidebar.selectbox(
    "Period", [7, 14, 30, 90], index=2,
    format_func=lambda d: f"Last {d} days", key="hmlv_days",
)

# ── Load data ─────────────────────────────────────────────────────
raw = get_hmlv_raw_leads(days=days)
qualified = get_hmlv_qualified_leads(days=days)
emails = get_hmlv_email_queue(days=days)

# ── Top KPIs (always visible above tabs) ─────────────────────────
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

n_raw      = len(raw)
n_raw_today = len(raw[raw["scraped_at"] >= today_start]) if not raw.empty else 0
n_qual     = len(qualified)
n_qual_today = len(qualified[qualified["qualified_at"] >= today_start]) if not qualified.empty else 0
n_pending  = len(emails[emails["status"] == "pending"]) if not emails.empty else 0
n_sent     = len(emails[emails["status"] == "sent"]) if not emails.empty else 0
avg_score  = round(qualified["fit_score"].mean(), 1) if not qualified.empty else 0
conv_rate  = round(n_qual / n_raw * 100, 1) if n_raw > 0 else 0
n_with_email = len(qualified[qualified["email"].astype(str).str.contains("@", na=False)]) if not qualified.empty else 0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Websites Scraped", f"{n_raw:,}", f"+{n_raw_today} today")
k2.metric("ICP Qualified (≥7)", f"{n_qual:,}", f"+{n_qual_today} today")
k3.metric("Conversion Rate", f"{conv_rate}%")
k4.metric("Emails Found", f"{n_with_email:,}")
k5.metric("Outreach Pending", f"{n_pending:,}", f"{n_sent} sent")
k6.metric("Avg ICP Score", f"{avg_score}/10")

st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

# ── Industry breakdown strip ──────────────────────────────────────
all_industries = list(INDUSTRY_COLORS.keys())
ind_cols = st.columns(len(all_industries))

for i, ind in enumerate(all_industries):
    icon  = INDUSTRY_ICONS.get(ind, "🏭")
    label = INDUSTRY_LABELS.get(ind, ind)
    color = INDUSTRY_COLORS.get(ind, "#9CA3AF")

    n_ind_raw  = len(raw[raw["source"] == ind.replace("_exhibits", "").replace("architectural_", "").replace("industrial_", "")]) if not raw.empty else 0
    n_ind_qual = len(qualified[qualified["industry_category"] == ind]) if not qualified.empty else 0
    n_ind_mail = len(emails[emails["source"].isin([ind, ind.replace("architectural_millwork", "millwork").replace("trade_show_exhibits", "trade_show").replace("industrial_crating", "crating")])]) if not emails.empty else 0

    with ind_cols[i]:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {color}18, {color}0a);
            border: 1px solid {color}45;
            border-radius: 10px;
            padding: 12px 14px;
            text-align: center;
        ">
            <div style="font-size: 1.3rem; margin-bottom: 2px;">{icon}</div>
            <div style="font-size: 0.72rem; font-weight: 700; color: {color}; margin-bottom: 8px; text-transform:uppercase; letter-spacing:0.04em;">{label}</div>
            <div style="font-size: 1.3rem; font-weight: 700; color: #FAFAFA;">{n_ind_qual}</div>
            <div style="font-size: 0.68rem; color: #9CA3AF;">qualified</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ── Main tabs ─────────────────────────────────────────────────────
tab_overview, tab_manufacturers, tab_outreach, tab_intel = st.tabs([
    "📊 Overview",
    "🏭 Manufacturers",
    "✉️ Outreach",
    "🔬 Intelligence",
])


# ═══════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════
with tab_overview:
    col_funnel, col_scores = st.columns(2)

    # Funnel
    with col_funnel:
        st.markdown("#### Pipeline Funnel")
        n_emails_total = len(emails) if not emails.empty else 0
        stages = ["Websites Scraped", "ICP Qualified (≥7)", "Outreach Drafted", "Sent"]
        values = [n_raw, n_qual, n_emails_total, n_sent]
        fig_funnel = go.Figure(go.Funnel(
            y=stages, x=values,
            textinfo="value+percent initial",
            textfont=dict(size=12, family="Inter"),
            marker=dict(
                color=["#F59E0B", "#10B981", "#6366F1", "#34D399"],
                line=dict(width=0),
            ),
            connector=dict(line=dict(color="rgba(99,102,241,0.2)", width=1)),
        ))
        fig_funnel.update_layout(**CHART)
        st.plotly_chart(fig_funnel, use_container_width=True)

    # Score Distribution
    with col_scores:
        st.markdown("#### ICP Score Distribution")
        if not qualified.empty:
            fig_hist = px.histogram(
                qualified, x="fit_score", nbins=4,
                color_discrete_sequence=["#10B981"],
                labels={"fit_score": "ICP Score", "count": "Manufacturers"},
            )
            fig_hist.update_traces(
                marker_line_color="rgba(16,185,129,0.6)",
                marker_line_width=1,
            )
            fig_hist.add_vline(
                x=7, line_dash="dot", line_color="#F59E0B",
                annotation_text="threshold",
                annotation_font_color="#F59E0B",
                annotation_position="top right",
            )
            fig_hist.update_layout(
                **CHART,
                xaxis=dict(dtick=1, range=[0, 11], gridcolor="rgba(16,185,129,0.08)"),
                yaxis=dict(gridcolor="rgba(16,185,129,0.08)"),
                bargap=0.1,
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("No qualified manufacturers yet.")

    # Industry Score Comparison
    col_ind_score, col_ind_vol = st.columns(2)

    with col_ind_score:
        st.markdown("#### Avg ICP Score by Industry")
        if not qualified.empty:
            ind_scores = (
                qualified.groupby("industry_category")["fit_score"]
                .agg(["mean", "count"])
                .reset_index()
            )
            ind_scores.columns = ["Industry", "Avg Score", "Count"]
            ind_scores["Avg Score"] = ind_scores["Avg Score"].round(1)
            ind_scores["label"] = ind_scores["Avg Score"].astype(str) + " (" + ind_scores["Count"].astype(str) + " co.)"
            ind_scores["color"] = ind_scores["Industry"].map(INDUSTRY_COLORS)
            ind_scores["display"] = ind_scores["Industry"].map(INDUSTRY_LABELS).fillna(ind_scores["Industry"])

            fig_ind = px.bar(
                ind_scores, x="display", y="Avg Score",
                color="Industry", color_discrete_map=INDUSTRY_COLORS,
                text="label",
            )
            fig_ind.update_traces(textposition="outside", textfont_size=11)
            fig_ind.update_layout(
                **CHART,
                xaxis=dict(title="", gridcolor="rgba(0,0,0,0)"),
                yaxis=dict(range=[0, 10.5], gridcolor="rgba(99,102,241,0.08)"),
            )
            st.plotly_chart(fig_ind, use_container_width=True)
        else:
            st.info("No data yet.")

    with col_ind_vol:
        st.markdown("#### Scraping Volume by Sub-Vertical")
        if not raw.empty:
            vol = raw["source"].value_counts().reset_index()
            vol.columns = ["Source", "Count"]
            fig_vol = px.pie(
                vol, names="Source", values="Count",
                color="Source", color_discrete_map=HMLV_SOURCE_COLORS,
                hole=0.5,
            )
            fig_vol.update_traces(textinfo="percent+label", textfont_size=11)
            fig_vol.update_layout(
                **{**CHART, "showlegend": False},
            )
            st.plotly_chart(fig_vol, use_container_width=True)
        else:
            st.info("No data yet.")

    # Timeline
    st.markdown("#### Discovery Timeline")
    if not raw.empty:
        raw_daily = raw.set_index("scraped_at").resample("D").size().reset_index(name="Scraped")
        raw_daily.rename(columns={"scraped_at": "date"}, inplace=True)

        qual_daily = pd.DataFrame(columns=["date", "Qualified"])
        if not qualified.empty:
            qual_daily = (
                qualified.set_index("qualified_at")
                .resample("D").size()
                .reset_index(name="Qualified")
            )
            qual_daily.rename(columns={"qualified_at": "date"}, inplace=True)

        merged_tl = pd.merge(raw_daily, qual_daily, on="date", how="outer").fillna(0)

        if len(merged_tl) <= 1 and not merged_tl.empty:
            center = merged_tl["date"].iloc[0]
            x_range = [center - pd.Timedelta(days=1), center + pd.Timedelta(days=1)]
        elif not merged_tl.empty:
            x_range = [
                merged_tl["date"].min() - pd.Timedelta(hours=12),
                merged_tl["date"].max() + pd.Timedelta(hours=12),
            ]
        else:
            x_range = None

        fig_tl = go.Figure()
        fig_tl.add_trace(go.Scatter(
            x=merged_tl["date"], y=merged_tl["Scraped"],
            name="Scraped", mode="lines",
            line=dict(color="#F59E0B", width=2.5),
            fill="tozeroy", fillcolor="rgba(245,158,11,0.08)",
        ))
        fig_tl.add_trace(go.Scatter(
            x=merged_tl["date"], y=merged_tl["Qualified"],
            name="ICP Qualified", mode="lines",
            line=dict(color="#10B981", width=2.5),
            fill="tozeroy", fillcolor="rgba(16,185,129,0.08)",
        ))
        layout_tl = {
            **CHART,
            "showlegend": True,
            "height": 260,
            "legend": dict(
                orientation="h", yanchor="bottom", y=1.02,
                xanchor="right", x=1, font=dict(size=12),
            ),
            "xaxis": dict(
                type="date", dtick=86400000, tickformat="%b %d",
                gridcolor="rgba(99,102,241,0.08)",
            ),
            "yaxis": dict(gridcolor="rgba(99,102,241,0.08)"),
            "hovermode": "x unified",
        }
        if x_range:
            layout_tl["xaxis"]["range"] = x_range
        fig_tl.update_layout(**layout_tl)
        st.plotly_chart(fig_tl, use_container_width=True)
    else:
        st.info("No data for this period.")

    # Email status breakdown
    if not emails.empty:
        st.markdown("#### Outreach Status Breakdown")
        status_counts = emails["status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        colors = [STATUS_COLORS.get(s, "#9CA3AF") for s in status_counts["Status"]]
        fig_status = px.bar(
            status_counts, x="Status", y="Count",
            color="Status",
            color_discrete_map=STATUS_COLORS,
            text="Count",
        )
        fig_status.update_traces(textposition="outside", textfont_size=12)
        fig_status.update_layout(
            **{**CHART, "height": 260},
            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
            yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
        )
        st.plotly_chart(fig_status, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════
# TAB 2 — MANUFACTURERS
# ═══════════════════════════════════════════════════════════════════
with tab_manufacturers:
    if qualified.empty:
        st.info("No qualified manufacturers yet. Run the pipeline with `--source all`.")
        st.stop()

    # Filters
    filter_cols = st.columns(4)
    with filter_cols[0]:
        all_industries_present = sorted(qualified["industry_category"].dropna().unique().tolist())
        ind_display = {k: f"{INDUSTRY_ICONS.get(k,'')} {INDUSTRY_LABELS.get(k, k)}" for k in all_industries_present}
        sel_industries = st.multiselect(
            "Industry",
            options=all_industries_present,
            default=all_industries_present,
            format_func=lambda x: ind_display.get(x, x),
            key="mfr_industry",
        )
    with filter_cols[1]:
        score_range = st.slider("Min ICP Score", 7, 10, 7, key="mfr_score")
    with filter_cols[2]:
        angle_options = sorted(qualified["suggested_angle"].dropna().unique().tolist())
        sel_angles = st.multiselect("Angle", angle_options, default=angle_options, key="mfr_angle")
    with filter_cols[3]:
        contact_filter = st.selectbox(
            "Contact", ["All", "Email found", "No email"], key="mfr_contact"
        )

    # Apply filters
    df_mfr = qualified[qualified["industry_category"].isin(sel_industries)]
    df_mfr = df_mfr[df_mfr["fit_score"] >= score_range]
    if sel_angles:
        df_mfr = df_mfr[df_mfr["suggested_angle"].isin(sel_angles)]
    if contact_filter == "Email found":
        df_mfr = df_mfr[df_mfr["email"].astype(str).str.contains("@", na=False)]
    elif contact_filter == "No email":
        df_mfr = df_mfr[~df_mfr["email"].astype(str).str.contains("@", na=False)]

    df_mfr = df_mfr.sort_values("fit_score", ascending=False)

    stat1, stat2, stat3, stat4 = st.columns(4)
    stat1.metric("Showing", len(df_mfr))
    stat2.metric("Perfect ICP (9-10)", int((df_mfr["fit_score"] >= 9).sum()))
    stat3.metric("With Email", int(df_mfr["email"].astype(str).str.contains("@", na=False).sum()))
    stat4.metric("With Tech Stack", int((df_mfr["key_technology"].astype(str).str.len() > 2).sum()))

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Manufacturer cards grid (2 columns)
    card_cols = st.columns(2)

    for i, (_, row) in enumerate(df_mfr.iterrows()):
        score     = int(row["fit_score"]) if pd.notna(row["fit_score"]) else 0
        score_col = fit_score_color(score)
        industry  = row.get("industry_category", "other") or "other"
        ind_color = INDUSTRY_COLORS.get(industry, "#9CA3AF")
        ind_label = INDUSTRY_LABELS.get(industry, industry)
        ind_icon  = INDUSTRY_ICONS.get(industry, "🏭")
        company   = (row.get("company_name") or row.get("inferred_company") or "Unknown Manufacturer")[:50]
        website   = row.get("company_website", "") or ""
        angle     = row.get("suggested_angle", "") or ""
        angle_col = ANGLE_COLORS.get(angle, "#9CA3AF")
        pain      = (row.get("pain_point", "") or "")[:120]
        tech      = (row.get("key_technology", "") or "")[:80]
        contact   = row.get("first_name", "") or row.get("contact_name", "") or ""
        email_val = row.get("email", "") or ""
        reasoning = (row.get("technical_reasoning", "") or "")[:180]
        green_flags = row.get("green_flags", []) or []
        red_flags   = row.get("red_flags", []) or []
        url         = row.get("url", "") or ""

        with card_cols[i % 2]:
            expander_label = f"{ind_icon} **{score}/10** · {company} · {ind_label}"

            with st.expander(expander_label, expanded=(score >= 9)):
                # Score + industry badges
                st.markdown(f"""
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-bottom:12px;">
                    <div style="
                        background: {score_col}22; border: 1.5px solid {score_col}80;
                        border-radius: 8px; padding: 4px 14px;
                        font-size: 1.4rem; font-weight: 700; color: {score_col};
                    ">{score}<span style="font-size:0.9rem;color:#9CA3AF;">/10</span></div>
                    <span style="background:{ind_color}20; color:{ind_color}; padding:3px 12px;
                        border:1px solid {ind_color}50; border-radius:20px;
                        font-size:0.72rem; font-weight:700; letter-spacing:0.04em;">
                        {ind_icon} {ind_label.upper()}
                    </span>
                    <span style="background:{angle_col}18; color:{angle_col}; padding:3px 12px;
                        border:1px solid {angle_col}50; border-radius:20px;
                        font-size:0.72rem; font-weight:600;">
                        {angle}
                    </span>
                </div>
                """, unsafe_allow_html=True)

                # Two-column details
                c_left, c_right = st.columns([3, 2])

                with c_left:
                    if website:
                        display_url = website.replace('https://','').replace('http://','').rstrip('/')
                        st.markdown(f'🌐 <a href="{_href(website)}" target="_blank" rel="noopener noreferrer">{display_url}</a>', unsafe_allow_html=True)
                    if contact:
                        st.markdown(f"👤 **{contact}**" + (f" · `{email_val}`" if "@" in email_val else ""))
                    elif "@" in email_val:
                        st.markdown(f"📧 `{email_val}`")

                    if tech:
                        st.markdown(f"""
                        <div style="margin:8px 0 4px 0;">
                            <span style="font-size:0.75rem; color:#9CA3AF; text-transform:uppercase; letter-spacing:0.05em;">Tech Stack</span><br>
                            <code style="font-size:0.82rem; color:#A78BFA;">{tech}</code>
                        </div>
                        """, unsafe_allow_html=True)

                with c_right:
                    if pain:
                        st.markdown(f"""
                        <div style="
                            background: rgba(245,158,11,0.08);
                            border-left: 3px solid #F59E0B;
                            border-radius: 0 6px 6px 0;
                            padding: 8px 12px;
                            font-size: 0.82rem; color: #FCD34D;
                            font-style: italic;
                        ">"{pain}"</div>
                        """, unsafe_allow_html=True)

                # Green flags
                if green_flags:
                    flags_html = " ".join([
                        f'<span style="background:#10B98118; color:#10B981; padding:2px 8px; '
                        f'border:1px solid #10B98140; border-radius:20px; font-size:0.68rem;">{f}</span>'
                        for f in green_flags[:6]
                    ])
                    st.markdown(
                        f'<div style="margin:8px 0 4px 0; display:flex; flex-wrap:wrap; gap:4px;">{flags_html}</div>',
                        unsafe_allow_html=True,
                    )

                # Red flags (collapsed)
                if red_flags:
                    with st.expander(f"⚠️ {len(red_flags)} Red Flag(s)", expanded=False):
                        for rf in red_flags:
                            st.markdown(f'<span style="color:#EF4444; font-size:0.8rem;">• {rf}</span>', unsafe_allow_html=True)

                # AI reasoning
                if reasoning:
                    st.caption(f"🤖 {reasoning}")

                if url:
                    st.markdown(f'<a href="{_href(url)}" target="_blank" rel="noopener noreferrer">View source page ↗</a>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
# TAB 3 — OUTREACH
# ═══════════════════════════════════════════════════════════════════
with tab_outreach:
    if emails.empty:
        st.info("No outreach emails in queue yet.")
        st.stop()

    # Merge with qualified for context
    if not qualified.empty:
        emails_merged = emails.merge(
            qualified[["id", "fit_score", "company_name", "industry_category", "pain_point", "suggested_angle"]],
            left_on="qualified_lead_id", right_on="id",
            how="left", suffixes=("", "_qual"),
        )
    else:
        emails_merged = emails.copy()
        for col in ["fit_score", "company_name", "industry_category", "pain_point", "suggested_angle"]:
            emails_merged[col] = None

    # Filters
    f1, f2, f3 = st.columns(3)
    with f1:
        status_opts = sorted(emails_merged["status"].dropna().unique().tolist())
        sel_statuses = st.multiselect("Status", status_opts, default=status_opts, key="out_status")
    with f2:
        ind_opts = sorted(emails_merged["industry_category"].dropna().unique().tolist()) if "industry_category" in emails_merged.columns else []
        sel_ind_out = st.multiselect("Industry", ind_opts, default=ind_opts, key="out_ind")
    with f3:
        source_opts = sorted(emails_merged["source"].dropna().unique().tolist())
        sel_src = st.multiselect("Source", source_opts, default=source_opts, key="out_src")

    df_out = emails_merged[emails_merged["status"].isin(sel_statuses)]
    if sel_ind_out and "industry_category" in df_out.columns:
        df_out = df_out[df_out["industry_category"].isin(sel_ind_out)]
    if sel_src:
        df_out = df_out[df_out["source"].isin(sel_src)]

    # Status KPIs
    o1, o2, o3, o4, o5 = st.columns(5)
    o1.metric("Total", len(emails))
    o2.metric("Pending", len(emails[emails["status"] == "pending"]))
    o3.metric("Approved", len(emails[emails["status"] == "approved"]))
    o4.metric("Sent", len(emails[emails["status"] == "sent"]))
    o5.metric("Rejected", len(emails[emails["status"] == "rejected"]))

    # Status bar
    total_out = len(emails)
    if total_out > 0:
        parts = []
        for st_name in ["sent", "approved", "pending", "rejected"]:
            cnt = len(emails[emails["status"] == st_name])
            if cnt > 0:
                pct = cnt / total_out * 100
                color = STATUS_COLORS.get(st_name, "#9CA3AF")
                parts.append(
                    f'<div style="width:{pct}%;background:{color};height:6px;border-radius:3px;" title="{st_name}: {cnt}"></div>'
                )
        st.markdown(
            '<div style="display:flex;gap:3px;margin:4px 0 16px 0;border-radius:3px;overflow:hidden;">'
            + "".join(parts) + '</div>',
            unsafe_allow_html=True,
        )

    # Email cards
    status_icons = {"pending": "🟡", "approved": "🟣", "sent": "🟢", "rejected": "🔴"}

    for _, row in df_out.iterrows():
        status    = row["status"]
        source    = row.get("source", "") or ""
        score     = int(row["fit_score"]) if pd.notna(row.get("fit_score")) else "?"
        company   = row.get("company_name", "Unknown") or "Unknown"
        industry  = row.get("industry_category", "") or ""
        ind_label = INDUSTRY_LABELS.get(industry, industry)
        ind_icon  = INDUSTRY_ICONS.get(industry, "🏭")
        angle     = row.get("suggested_angle", "") or ""
        angle_col = ANGLE_COLORS.get(angle, "#9CA3AF")
        pain      = (row.get("pain_point", "") or "")[:100]
        status_color = STATUS_COLORS.get(status, "#9CA3AF")
        ind_color    = INDUSTRY_COLORS.get(industry, "#9CA3AF")
        status_icon  = status_icons.get(status, "⚪")

        subject_preview = (row["subject"] or "")[:70]
        header = f"{status_icon} **{status.upper()}** · {ind_icon} {ind_label} · [{score}/10] {subject_preview}"

        with st.expander(header):
            # Badges row
            st.markdown(f"""
            <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px;">
                <span style="background:{status_color}; color:white; padding:2px 10px;
                    border-radius:12px; font-size:0.72rem; font-weight:600;">{status.upper()}</span>
                <span style="background:{ind_color}20; color:{ind_color}; padding:2px 10px;
                    border:1px solid {ind_color}50; border-radius:12px; font-size:0.72rem; font-weight:600;">
                    {ind_icon} {ind_label}
                </span>
                <span style="background:{angle_col}18; color:{angle_col}; padding:2px 10px;
                    border:1px solid {angle_col}50; border-radius:12px; font-size:0.72rem;">{angle}</span>
            </div>
            """, unsafe_allow_html=True)

            # Metadata
            m1, m2, m3 = st.columns(3)
            with m1:
                st.markdown(f"**To:** `{row['to_email']}`")
            with m2:
                st.markdown(f"**Company:** {company}")
            with m3:
                if row.get("job_url"):
                    st.markdown(f'<a href="{_href(row["job_url"])}" target="_blank" rel="noopener noreferrer">View website ↗</a>', unsafe_allow_html=True)

            if pain:
                st.markdown(f"""
                <div style="background:rgba(245,158,11,0.08); border-left:3px solid #F59E0B;
                    padding:6px 12px; border-radius:0 6px 6px 0; font-size:0.82rem;
                    color:#FCD34D; margin:6px 0; font-style:italic;">{pain}</div>
                """, unsafe_allow_html=True)

            st.markdown("---")
            st.markdown(f"**Subject:** {row['subject']}")
            st.markdown(f"""
            <div style="
                background: #0E1117;
                border: 1px solid rgba(99,102,241,0.15);
                border-radius: 8px;
                padding: 16px 20px;
                margin: 8px 0;
                font-size: 0.88rem;
                line-height: 1.7;
                color: #D0D0E0;
                white-space: pre-wrap;
            ">{row['body']}</div>
            """, unsafe_allow_html=True)

            created = pd.to_datetime(row["created_at"]).strftime("%b %d, %Y %H:%M")
            st.caption(f"Created: {created}")

            # HITL controls
            if status == "pending":
                st.markdown("---")
                col_app, col_rej, _ = st.columns([1, 1, 4])
                with col_app:
                    if st.button("✓ Approve", key=f"hmlv_approve_{row['id']}", type="primary"):
                        if update_email_status(row["id"], "approved"):
                            st.success("Approved — will be sent automatically.")
                            st.rerun()
                        else:
                            st.error("Update failed.")
                with col_rej:
                    if st.button("✕ Reject", key=f"hmlv_reject_{row['id']}"):
                        if update_email_status(row["id"], "rejected"):
                            st.warning("Rejected.")
                            st.rerun()
                        else:
                            st.error("Update failed.")


# ═══════════════════════════════════════════════════════════════════
# TAB 4 — INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════
with tab_intel:
    if qualified.empty:
        st.info("Intelligence analysis requires qualified leads. Run the pipeline first.")
        st.stop()

    # ── Tech Stack Analysis ───────────────────────────────────────
    st.markdown("#### Tech Stack Intelligence")
    st.caption("CAD/CAM/ERP software and CNC hardware detected in manufacturer websites")

    tech_entries = qualified["key_technology"].dropna().tolist()
    tech_entries = [t for t in tech_entries if str(t).strip()]

    if tech_entries:
        # Split comma-separated tech stacks and count individual tools
        tech_tokens: list[str] = []
        for entry in tech_entries:
            parts = [p.strip() for p in str(entry).replace(";", ",").replace("/", ",").split(",")]
            tech_tokens.extend([p for p in parts if len(p) > 1])

        tech_counter = Counter(tech_tokens).most_common(20)
        tech_df = pd.DataFrame(tech_counter, columns=["Technology", "Count"])

        col_tech_bar, col_tech_ind = st.columns([2, 1])

        with col_tech_bar:
            fig_tech = px.bar(
                tech_df, x="Count", y="Technology", orientation="h",
                color="Count",
                color_continuous_scale=[[0, "#6366F1"], [0.5, "#8B5CF6"], [1, "#10B981"]],
                text="Count",
            )
            fig_tech.update_traces(textposition="outside", textfont_size=11)
            fig_tech.update_layout(
                **{**CHART, "height": 480, "showlegend": False},
                yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_tech, use_container_width=True)

        with col_tech_ind:
            st.markdown("**Top Tools by Industry**")
            for ind in all_industries_present:
                ind_techs = qualified[qualified["industry_category"] == ind]["key_technology"].dropna()
                tokens = []
                for t in ind_techs:
                    tokens.extend([p.strip() for p in str(t).replace(";", ",").split(",") if p.strip()])
                if tokens:
                    top = Counter(tokens).most_common(3)
                    ind_color = INDUSTRY_COLORS.get(ind, "#9CA3AF")
                    ind_label = INDUSTRY_LABELS.get(ind, ind)
                    ind_icon  = INDUSTRY_ICONS.get(ind, "🏭")
                    top_str = ", ".join([t for t, _ in top])
                    st.markdown(f"""
                    <div style="margin-bottom:8px;">
                        <span style="color:{ind_color}; font-size:0.78rem; font-weight:700;">
                            {ind_icon} {ind_label}
                        </span><br>
                        <span style="color:#C8C8D8; font-size:0.76rem;">{top_str}</span>
                    </div>
                    """, unsafe_allow_html=True)
    else:
        st.info("No technology data yet in qualified leads.")

    st.markdown("---")

    # ── Green / Red Flag Analysis ─────────────────────────────────
    st.markdown("#### Flag Pattern Analysis")
    col_green, col_red = st.columns(2)

    with col_green:
        st.markdown("##### ✅ Most Common Green Flags")
        all_green: list[str] = []
        for flags in qualified["green_flags"]:
            if isinstance(flags, list):
                all_green.extend(flags)
        if all_green:
            green_counter = Counter(all_green).most_common(12)
            gf_df = pd.DataFrame(green_counter, columns=["Flag", "Count"])
            fig_gf = px.bar(
                gf_df, x="Count", y="Flag", orientation="h",
                color_discrete_sequence=["#10B981"],
                text="Count",
            )
            fig_gf.update_traces(textposition="outside", textfont_size=10)
            fig_gf.update_layout(
                **{**CHART, "height": 420},
                yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(16,185,129,0.08)"),
            )
            st.plotly_chart(fig_gf, use_container_width=True)
        else:
            st.info("No flag data yet.")

    with col_red:
        st.markdown("##### 🚩 Most Common Red Flags")

        # Merge raw + all_leads to also analyse disqualified if present
        all_red: list[str] = []
        for flags in qualified["red_flags"]:
            if isinstance(flags, list):
                all_red.extend(flags)
        if all_red:
            red_counter = Counter(all_red).most_common(12)
            rf_df = pd.DataFrame(red_counter, columns=["Flag", "Count"])
            fig_rf = px.bar(
                rf_df, x="Count", y="Flag", orientation="h",
                color_discrete_sequence=["#EF4444"],
                text="Count",
            )
            fig_rf.update_traces(textposition="outside", textfont_size=10)
            fig_rf.update_layout(
                **{**CHART, "height": 420},
                yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(239,68,68,0.08)"),
            )
            st.plotly_chart(fig_rf, use_container_width=True)
        else:
            st.info("No red flag data yet (all leads passed).")

    st.markdown("---")

    # ── Pain Point Clustering ─────────────────────────────────────
    st.markdown("#### Pain Point Heatmap by Industry")
    st.caption("Most common pain point keywords across qualified manufacturers")

    pain_keywords = [
        "manual quoting", "BOM", "DXF", "nesting", "estimating",
        "rework", "material waste", "shop drawings", "Excel",
        "CAM", "CAD", "quote-to-cash", "toolpath", "G-code",
    ]

    heatmap_data = []
    for ind in (all_industries_present or ["other"]):
        ind_subset = qualified[qualified["industry_category"] == ind]["pain_point"].dropna()
        ind_label = INDUSTRY_LABELS.get(ind, ind)
        for kw in pain_keywords:
            count = int(ind_subset.str.lower().str.contains(kw.lower(), na=False).sum())
            heatmap_data.append({"Industry": ind_label, "Keyword": kw, "Count": count})

    if heatmap_data:
        hm_df = pd.DataFrame(heatmap_data)
        hm_pivot = hm_df.pivot(index="Keyword", columns="Industry", values="Count").fillna(0)
        fig_hm = px.imshow(
            hm_pivot,
            color_continuous_scale=[[0, "#0E1117"], [0.3, "rgba(99,102,241,0.19)"], [1, "#10B981"]],
            aspect="auto",
            text_auto=True,
        )
        fig_hm.update_layout(
            **{**CHART, "height": 380, "showlegend": False},
            xaxis=dict(side="bottom"),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_hm, use_container_width=True)

    st.markdown("---")

    # ── Outreach Angle Distribution ───────────────────────────────
    st.markdown("#### Suggested Outreach Angles")
    col_angle_pie, col_angle_by_ind = st.columns(2)

    with col_angle_pie:
        angle_counts = qualified["suggested_angle"].value_counts().reset_index()
        angle_counts.columns = ["Angle", "Count"]
        fig_angle = px.pie(
            angle_counts, names="Angle", values="Count",
            color="Angle", color_discrete_map=ANGLE_COLORS,
            hole=0.4,
        )
        fig_angle.update_traces(textinfo="percent+label", textfont_size=11)
        fig_angle.update_layout(**{**CHART, "showlegend": False})
        st.plotly_chart(fig_angle, use_container_width=True)

    with col_angle_by_ind:
        st.markdown("**Dominant Angle per Industry**")
        for ind in (all_industries_present or []):
            subset = qualified[qualified["industry_category"] == ind]["suggested_angle"]
            if subset.empty:
                continue
            dominant = subset.value_counts().idxmax()
            dom_color = ANGLE_COLORS.get(dominant, "#9CA3AF")
            ind_icon  = INDUSTRY_ICONS.get(ind, "🏭")
            ind_label = INDUSTRY_LABELS.get(ind, ind)
            ind_color = INDUSTRY_COLORS.get(ind, "#9CA3AF")
            st.markdown(f"""
            <div style="
                display:flex; justify-content:space-between; align-items:center;
                padding: 8px 12px; margin-bottom:6px;
                background: {ind_color}10; border: 1px solid {ind_color}30;
                border-radius: 8px;
            ">
                <span style="color:{ind_color}; font-size:0.8rem; font-weight:600;">{ind_icon} {ind_label}</span>
                <span style="background:{dom_color}20; color:{dom_color}; padding:2px 10px;
                    border:1px solid {dom_color}50; border-radius:12px; font-size:0.72rem; font-weight:700;">
                    {dominant}
                </span>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Dork Query Performance Table ──────────────────────────────
    st.markdown("#### Google Dork Performance")
    st.caption("Which search queries found the highest-scoring manufacturers (based on search_keyword metadata)")

    if not raw.empty and "search_keyword" in raw.columns:
        dork_df = raw[raw["search_keyword"].astype(str).str.len() > 5].copy()

        if not dork_df.empty and not qualified.empty:
            dork_qual = dork_df.merge(
                qualified[["raw_lead_id", "fit_score", "industry_category"]],
                left_on="id", right_on="raw_lead_id", how="left",
            )
            dork_stats = (
                dork_qual.groupby("search_keyword")
                .agg(
                    found=("id", "count"),
                    qualified=("fit_score", lambda x: x.notna().sum()),
                    avg_score=("fit_score", "mean"),
                    source=("source", "first"),
                )
                .reset_index()
            )
            dork_stats["qual_rate"] = (dork_stats["qualified"] / dork_stats["found"] * 100).round(1)
            dork_stats["avg_score"] = dork_stats["avg_score"].round(1)
            dork_stats["keyword_short"] = dork_stats["search_keyword"].str[:80] + "..."
            dork_stats = dork_stats.sort_values("avg_score", ascending=False)

            display_dork = dork_stats[["keyword_short", "source", "found", "qualified", "qual_rate", "avg_score"]].copy()
            display_dork.columns = ["Dork Query", "Sub-Vertical", "Scraped", "Qualified", "Qual. %", "Avg Score"]

            st.dataframe(
                display_dork,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Qual. %": st.column_config.ProgressColumn(
                        "Qual. %", min_value=0, max_value=100, format="%.1f%%"
                    ),
                    "Avg Score": st.column_config.ProgressColumn(
                        "Avg Score", min_value=0, max_value=10, format="%.1f"
                    ),
                },
            )
        else:
            st.info("Not enough data to compute dork performance yet.")
    else:
        st.info("No search keyword metadata yet.")
