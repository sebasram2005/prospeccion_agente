"""
Analytics — Performance by source, keyword, and time.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.supabase_client import get_raw_leads, get_qualified_leads, get_email_queue
from utils.helpers import SOURCE_COLORS

st.header("Analytics")

# ── Load data ────────────────────────────────────────────────────
days = st.sidebar.selectbox("Period", [7, 14, 30, 90], index=2, format_func=lambda d: f"Last {d} days", key="analytics_days")
raw = get_raw_leads(days=days)
qualified = get_qualified_leads(days=days)
emails = get_email_queue(days=days)

if raw.empty:
    st.info("No data for this period.")
    st.stop()

# ── Leads by Source ──────────────────────────────────────────────
col_source, col_score = st.columns(2)

with col_source:
    st.subheader("Leads by Source")
    source_counts = raw["source"].value_counts().reset_index()
    source_counts.columns = ["Source", "Count"]
    fig1 = px.bar(
        source_counts,
        x="Source",
        y="Count",
        color="Source",
        color_discrete_map=SOURCE_COLORS,
    )
    fig1.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300, showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)

with col_score:
    st.subheader("Avg Fit Score by Source")
    if not qualified.empty:
        # Merge source from raw
        qual_with_source = qualified.merge(
            raw[["id", "source"]],
            left_on="raw_lead_id",
            right_on="id",
            how="left",
            suffixes=("", "_raw"),
        )
        avg_scores = qual_with_source.groupby("source")["fit_score"].mean().reset_index()
        avg_scores.columns = ["Source", "Avg Score"]
        avg_scores["Avg Score"] = avg_scores["Avg Score"].round(1)
        fig2 = px.bar(
            avg_scores,
            x="Source",
            y="Avg Score",
            color="Source",
            color_discrete_map=SOURCE_COLORS,
            text="Avg Score",
        )
        fig2.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No qualified leads yet.")

# ── Conversion Funnel by Source ──────────────────────────────────
st.subheader("Conversion by Source")
if not qualified.empty:
    qual_with_source = qualified.merge(
        raw[["id", "source"]],
        left_on="raw_lead_id",
        right_on="id",
        how="left",
        suffixes=("", "_raw"),
    )
    raw_by_source = raw["source"].value_counts()
    qual_by_source = qual_with_source["source"].value_counts()

    sent_by_source = pd.Series(dtype=int)
    if not emails.empty:
        sent_emails = emails[emails["status"] == "sent"]
        if not sent_emails.empty:
            sent_with_source = sent_emails.merge(
                qualified[["id", "raw_lead_id"]],
                left_on="qualified_lead_id",
                right_on="id",
                how="left",
                suffixes=("", "_qual"),
            ).merge(
                raw[["id", "source"]],
                left_on="raw_lead_id",
                right_on="id",
                how="left",
                suffixes=("", "_raw"),
            )
            sent_by_source = sent_with_source["source"].value_counts()

    conversion_data = []
    for source in raw_by_source.index:
        raw_count = raw_by_source.get(source, 0)
        qual_count = qual_by_source.get(source, 0)
        sent_count = sent_by_source.get(source, 0)
        rate = round(qual_count / raw_count * 100, 1) if raw_count > 0 else 0
        conversion_data.append({
            "Source": source,
            "Raw": raw_count,
            "Qualified": qual_count,
            "Sent": sent_count,
            "Conversion %": rate,
        })

    conv_df = pd.DataFrame(conversion_data)
    st.dataframe(conv_df, use_container_width=True, hide_index=True)

# ── Timeline by Source ───────────────────────────────────────────
st.subheader("Leads Timeline by Source")
if not raw.empty:
    raw_daily = raw.groupby([raw["scraped_at"].dt.date, "source"]).size().reset_index(name="Count")
    raw_daily.columns = ["Date", "Source", "Count"]
    fig3 = px.line(
        raw_daily,
        x="Date",
        y="Count",
        color="Source",
        color_discrete_map=SOURCE_COLORS,
    )
    fig3.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300)
    st.plotly_chart(fig3, use_container_width=True)
