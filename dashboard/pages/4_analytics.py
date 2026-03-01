"""
Analytics — Performance metrics by source, conversion, and time.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.supabase_client import get_raw_leads, get_qualified_leads, get_email_queue
from utils.helpers import SOURCE_COLORS

# ── Header ────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 8px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        📊 Analytics
    </h1>
    <p style="color: #8B8BA0; font-size: 0.9rem; margin: 4px 0 0 0;">
        Source performance, conversion rates, and pipeline efficiency
    </p>
</div>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────
days = st.sidebar.selectbox(
    "Period", [7, 14, 30, 90], index=2,
    format_func=lambda d: f"Last {d} days", key="analytics_days",
)
raw = get_raw_leads(days=days)
qualified = get_qualified_leads(days=days)
emails = get_email_queue(days=days)

if raw.empty:
    st.info("No data for this period.")
    st.stop()

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#A0A0B8"),
    margin=dict(l=0, r=0, t=10, b=0),
    height=340,
    showlegend=False,
)

# ── Source Performance ────────────────────────────────────────────
st.markdown("#### Source Performance")
col_source, col_score = st.columns(2)

with col_source:
    source_counts = raw["source"].value_counts().reset_index()
    source_counts.columns = ["Source", "Count"]
    fig1 = px.bar(
        source_counts, x="Source", y="Count",
        color="Source", color_discrete_map=SOURCE_COLORS,
        text="Count",
    )
    fig1.update_traces(textposition="outside", textfont_size=12)
    fig1.update_layout(
        **CHART_LAYOUT,
        xaxis=dict(gridcolor="rgba(0,0,0,0)"),
        yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
    )
    st.plotly_chart(fig1, use_container_width=True)

with col_score:
    if not qualified.empty:
        qual_with_source = qualified.merge(
            raw[["id", "source"]], left_on="raw_lead_id", right_on="id",
            how="left", suffixes=("", "_raw"),
        )
        avg_scores = qual_with_source.groupby("source")["fit_score"].agg(["mean", "count"]).reset_index()
        avg_scores.columns = ["Source", "Avg Score", "Count"]
        avg_scores["Avg Score"] = avg_scores["Avg Score"].round(1)
        avg_scores["label"] = avg_scores["Avg Score"].astype(str) + " (" + avg_scores["Count"].astype(str) + ")"

        fig2 = px.bar(
            avg_scores, x="Source", y="Avg Score",
            color="Source", color_discrete_map=SOURCE_COLORS,
            text="label",
        )
        fig2.update_traces(textposition="outside", textfont_size=11)
        fig2.update_layout(
            **CHART_LAYOUT,
            yaxis=dict(range=[0, 10], gridcolor="rgba(99,102,241,0.08)"),
            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No qualified leads yet.")

# ── Conversion Table ──────────────────────────────────────────────
st.markdown("#### Conversion Funnel by Source")
if not qualified.empty:
    qual_with_source = qualified.merge(
        raw[["id", "source"]], left_on="raw_lead_id", right_on="id",
        how="left", suffixes=("", "_raw"),
    )
    raw_by_source = raw["source"].value_counts()
    qual_by_source = qual_with_source["source"].value_counts()

    sent_by_source = pd.Series(dtype=int)
    if not emails.empty:
        sent_emails = emails[emails["status"] == "sent"]
        if not sent_emails.empty:
            sent_with_source = sent_emails.merge(
                qualified[["id", "raw_lead_id"]],
                left_on="qualified_lead_id", right_on="id",
                how="left", suffixes=("", "_qual"),
            ).merge(
                raw[["id", "source"]],
                left_on="raw_lead_id", right_on="id",
                how="left", suffixes=("", "_raw"),
            )
            sent_by_source = sent_with_source["source"].value_counts()

    conversion_data = []
    for source in raw_by_source.index:
        raw_count = int(raw_by_source.get(source, 0))
        qual_count = int(qual_by_source.get(source, 0))
        sent_count = int(sent_by_source.get(source, 0))
        conv_rate = round(qual_count / raw_count * 100, 1) if raw_count > 0 else 0
        send_rate = round(sent_count / qual_count * 100, 1) if qual_count > 0 else 0
        conversion_data.append({
            "Source": source,
            "Scraped": raw_count,
            "Qualified": qual_count,
            "Emails Sent": sent_count,
            "Qualification %": conv_rate,
            "Send %": send_rate,
        })

    conv_df = pd.DataFrame(conversion_data)
    st.dataframe(
        conv_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Qualification %": st.column_config.ProgressColumn(
                "Qual. Rate", min_value=0, max_value=100, format="%.1f%%",
            ),
            "Send %": st.column_config.ProgressColumn(
                "Send Rate", min_value=0, max_value=100, format="%.1f%%",
            ),
        },
    )

# ── Timeline ──────────────────────────────────────────────────────
st.markdown("#### Leads Timeline by Source")
raw_daily = raw.groupby([raw["scraped_at"].dt.date, "source"]).size().reset_index(name="Count")
raw_daily.columns = ["Date", "Source", "Count"]

fig3 = px.area(
    raw_daily, x="Date", y="Count", color="Source",
    color_discrete_map=SOURCE_COLORS,
)
fig3.update_layout(
    **CHART_LAYOUT,
    height=300,
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
        font=dict(size=11),
    ),
    xaxis=dict(
        type="date",
        dtick=86400000,
        tickformat="%b %d",
        gridcolor="rgba(99,102,241,0.08)",
    ),
    yaxis=dict(gridcolor="rgba(99,102,241,0.08)"),
    hovermode="x unified",
)
st.plotly_chart(fig3, use_container_width=True)
