"""
Overview page — KPIs, funnel, and trends.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

from utils.supabase_client import get_raw_leads, get_qualified_leads, get_email_queue

st.header("Overview")

# ── Time filter ──────────────────────────────────────────────────
col_filter, _ = st.columns([1, 3])
with col_filter:
    days = st.selectbox("Period", [7, 14, 30, 90], index=2, format_func=lambda d: f"Last {d} days")

# ── Load data ────────────────────────────────────────────────────
raw = get_raw_leads(days=days)
qualified = get_qualified_leads(days=days)
emails = get_email_queue(days=days)

now = datetime.now(timezone.utc)
today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

raw_today = len(raw[raw["scraped_at"] >= today_start]) if not raw.empty else 0
qual_today = len(qualified[qualified["qualified_at"] >= today_start]) if not qualified.empty else 0
sent_total = len(emails[emails["status"] == "sent"]) if not emails.empty else 0
avg_score = round(qualified["fit_score"].mean(), 1) if not qualified.empty else 0

# ── KPIs ─────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("Raw Leads", len(raw), f"+{raw_today} today")
k2.metric("Qualified", len(qualified), f"+{qual_today} today")
k3.metric("Emails Sent", sent_total)
k4.metric("Avg Fit Score", avg_score)

st.divider()

# ── Funnel ───────────────────────────────────────────────────────
col_funnel, col_dist = st.columns(2)

with col_funnel:
    st.subheader("Pipeline Funnel")
    pending = len(emails[emails["status"] == "pending"]) if not emails.empty else 0
    approved = len(emails[emails["status"] == "approved"]) if not emails.empty else 0

    stages = ["Raw Leads", "Qualified", "Queued", "Sent"]
    values = [len(raw), len(qualified), len(emails), sent_total]

    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        marker=dict(color=["#6366F1", "#8B5CF6", "#A78BFA", "#10B981"]),
    ))
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300)
    st.plotly_chart(fig, use_container_width=True)

with col_dist:
    st.subheader("Fit Score Distribution")
    if not qualified.empty:
        fig2 = px.histogram(
            qualified,
            x="fit_score",
            nbins=10,
            color_discrete_sequence=["#6366F1"],
            labels={"fit_score": "Fit Score"},
        )
        fig2.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=300,
            showlegend=False,
            xaxis=dict(dtick=1),
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No qualified leads yet.")

# ── Timeline ─────────────────────────────────────────────────────
st.subheader("Leads Over Time")
if not raw.empty:
    raw_daily = raw.set_index("scraped_at").resample("D").size().reset_index(name="Raw Leads")
    qual_daily = pd.DataFrame({"date": [], "Qualified": []})
    if not qualified.empty:
        qual_daily = qualified.set_index("qualified_at").resample("D").size().reset_index(name="Qualified")
        qual_daily.rename(columns={"qualified_at": "date"}, inplace=True)
    raw_daily.rename(columns={"scraped_at": "date"}, inplace=True)

    merged = pd.merge(raw_daily, qual_daily, on="date", how="outer").fillna(0)
    fig3 = px.line(
        merged,
        x="date",
        y=["Raw Leads", "Qualified"],
        labels={"date": "", "value": "Count", "variable": ""},
        color_discrete_map={"Raw Leads": "#6366F1", "Qualified": "#10B981"},
    )
    fig3.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=250)
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("No data yet for the selected period.")
