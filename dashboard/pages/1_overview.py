"""
Command Center — KPIs, pipeline funnel, and real-time trends.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

from utils.supabase_client import get_raw_leads, get_qualified_leads, get_email_queue

# ── Header ────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 8px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        ⚡ Command Center
    </h1>
    <p style="color: #C8C8D8; font-size: 0.9rem; margin: 4px 0 0 0;">
        Real-time view of the autonomous prospecting pipeline
    </p>
</div>
""", unsafe_allow_html=True)

# ── Time filter ───────────────────────────────────────────────────
col_filter, _ = st.columns([1, 3])
with col_filter:
    days = st.selectbox(
        "Period",
        [7, 14, 30, 90],
        index=2,
        format_func=lambda d: f"Last {d} days",
    )

# ── Load data ─────────────────────────────────────────────────────
raw = get_raw_leads(days=days)
qualified = get_qualified_leads(days=days)
emails = get_email_queue(days=days)

now = datetime.now(timezone.utc)
today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

raw_total = len(raw)
raw_today = len(raw[raw["scraped_at"] >= today_start]) if not raw.empty else 0
qual_total = len(qualified)
qual_today = len(qualified[qualified["qualified_at"] >= today_start]) if not qualified.empty else 0
sent_total = len(emails[emails["status"] == "sent"]) if not emails.empty else 0
pending_total = len(emails[emails["status"] == "pending"]) if not emails.empty else 0
avg_score = round(qualified["fit_score"].mean(), 1) if not qualified.empty else 0
conversion_rate = round(qual_total / raw_total * 100, 1) if raw_total > 0 else 0

# ── KPI Row ───────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Raw Leads", f"{raw_total:,}", f"+{raw_today} today")
k2.metric("Qualified", f"{qual_total:,}", f"+{qual_today} today")
k3.metric("Conversion Rate", f"{conversion_rate}%")
k4.metric("Emails Sent", f"{sent_total:,}", f"{pending_total} pending")
k5.metric("Avg Fit Score", f"{avg_score}/10")

st.markdown("<div style='height: 8px'></div>", unsafe_allow_html=True)

# ── Vertical Breakdown ────────────────────────────────────────────
if not raw.empty:
    st.markdown("#### Pipeline by Vertical")
    vertical_labels = {
        "tech": ("🖥️", "Tech Services", "#6366F1"),
    }
    verticals = [
        v for v in raw["vertical"].dropna().unique().tolist()
        if v in vertical_labels
    ] if "vertical" in raw.columns else []

    if verticals:
        vert_cols = st.columns(len(verticals))
        for i, v in enumerate(sorted(verticals)):
            icon, label, color = vertical_labels.get(v, ("📊", v, "#6366F1"))
            v_raw = len(raw[raw["vertical"] == v])
            v_qual = len(qualified[qualified["vertical"] == v]) if not qualified.empty and "vertical" in qualified.columns else 0
            v_emails = len(emails[emails["vertical"] == v]) if not emails.empty and "vertical" in emails.columns else 0
            v_rate = round(v_qual / v_raw * 100, 1) if v_raw > 0 else 0

            with vert_cols[i]:
                st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, {color}15, {color}08);
                    border: 1px solid {color}40;
                    border-radius: 12px;
                    padding: 16px 20px;
                ">
                    <div style="font-size: 1.4rem; margin-bottom: 4px;">{icon}</div>
                    <div style="font-size: 0.85rem; font-weight: 600; color: {color}; margin-bottom: 10px;">{label}</div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                        <span style="font-size: 0.75rem; color: #C8C8D8;">Scraped</span>
                        <span style="font-size: 0.85rem; font-weight: 600; color: #FAFAFA;">{v_raw}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                        <span style="font-size: 0.75rem; color: #C8C8D8;">Qualified</span>
                        <span style="font-size: 0.85rem; font-weight: 600; color: #FAFAFA;">{v_qual}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                        <span style="font-size: 0.75rem; color: #C8C8D8;">Emails</span>
                        <span style="font-size: 0.85rem; font-weight: 600; color: #FAFAFA;">{v_emails}</span>
                    </div>
                    <div style="
                        margin-top: 8px;
                        padding-top: 8px;
                        border-top: 1px solid {color}30;
                        display: flex; justify-content: space-between;
                    ">
                        <span style="font-size: 0.75rem; color: #C8C8D8;">Conv. rate</span>
                        <span style="font-size: 0.85rem; font-weight: 700; color: {color};">{v_rate}%</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("<div style='height: 8px'></div>", unsafe_allow_html=True)

# ── Funnel + Score Distribution ───────────────────────────────────
col_funnel, col_dist = st.columns(2)

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#D1D1E0"),
    margin=dict(l=0, r=0, t=10, b=0),
    height=320,
)

with col_funnel:
    st.markdown("#### Pipeline Funnel")
    stages = ["Scraped", "Qualified", "Email Drafted", "Sent"]
    values = [raw_total, qual_total, len(emails), sent_total]

    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        textfont=dict(size=13, family="Inter"),
        marker=dict(
            color=["#6366F1", "#8B5CF6", "#A78BFA", "#10B981"],
            line=dict(width=0),
        ),
        connector=dict(line=dict(color="rgba(99,102,241,0.2)", width=1)),
    ))
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

with col_dist:
    st.markdown("#### Fit Score Distribution")
    if not qualified.empty:
        fig2 = px.histogram(
            qualified,
            x="fit_score",
            nbins=10,
            color_discrete_sequence=["#6366F1"],
            labels={"fit_score": "Fit Score", "count": "Leads"},
        )
        fig2.update_traces(
            marker_line_color="rgba(99,102,241,0.6)",
            marker_line_width=1,
        )
        fig2.update_layout(
            **CHART_LAYOUT,
            showlegend=False,
            xaxis=dict(dtick=1, gridcolor="rgba(99,102,241,0.08)"),
            yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
            bargap=0.1,
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No qualified leads yet.")

# ── Timeline ──────────────────────────────────────────────────────
st.markdown("#### Leads Over Time")
if not raw.empty:
    raw_daily = raw.set_index("scraped_at").resample("D").size().reset_index(name="Scraped")
    raw_daily.rename(columns={"scraped_at": "date"}, inplace=True)

    qual_daily = pd.DataFrame(columns=["date", "Qualified"])
    if not qualified.empty:
        qual_daily = qualified.set_index("qualified_at").resample("D").size().reset_index(name="Qualified")
        qual_daily.rename(columns={"qualified_at": "date"}, inplace=True)

    merged = pd.merge(raw_daily, qual_daily, on="date", how="outer").fillna(0)

    # Ensure readable x-axis even with single-day data
    if len(merged) <= 1:
        center = merged["date"].iloc[0] if not merged.empty else pd.Timestamp.now()
        x_min = center - pd.Timedelta(days=1)
        x_max = center + pd.Timedelta(days=1)
    else:
        x_min = merged["date"].min() - pd.Timedelta(hours=12)
        x_max = merged["date"].max() + pd.Timedelta(hours=12)

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=merged["date"], y=merged["Scraped"],
        name="Scraped",
        mode="lines",
        line=dict(color="#6366F1", width=2.5),
        fill="tozeroy",
        fillcolor="rgba(99,102,241,0.08)",
    ))
    fig3.add_trace(go.Scatter(
        x=merged["date"], y=merged["Qualified"],
        name="Qualified",
        mode="lines",
        line=dict(color="#10B981", width=2.5),
        fill="tozeroy",
        fillcolor="rgba(16,185,129,0.08)",
    ))
    fig3.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#D1D1E0"),
        margin=dict(l=0, r=0, t=10, b=0),
        height=280,
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
            font=dict(size=12),
        ),
        xaxis=dict(
            type="date",
            dtick=86400000,
            tickformat="%b %d",
            range=[x_min, x_max],
            gridcolor="rgba(99,102,241,0.08)",
        ),
        yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
        hovermode="x unified",
    )
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("No data yet for the selected period.")
