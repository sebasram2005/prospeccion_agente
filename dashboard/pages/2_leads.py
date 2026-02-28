"""
Leads Explorer — Filterable table with qualification details.
"""

import streamlit as st
import pandas as pd

from utils.supabase_client import get_raw_leads, get_qualified_leads
from utils.helpers import fit_score_color

st.header("Leads Explorer")

# ── Load data ────────────────────────────────────────────────────
days = st.sidebar.selectbox("Period", [7, 14, 30, 90], index=2, format_func=lambda d: f"Last {d} days", key="leads_days")
raw = get_raw_leads(days=days)
qualified = get_qualified_leads(days=days)

if raw.empty:
    st.info("No leads found for this period.")
    st.stop()

# ── Merge raw + qualified ────────────────────────────────────────
if not qualified.empty:
    merged = raw.merge(
        qualified[["raw_lead_id", "fit_score", "first_name", "company_name", "email",
                    "pain_point", "suggested_angle", "portfolio_proof", "reasoning",
                    "contact_name", "company_website"]],
        left_on="id",
        right_on="raw_lead_id",
        how="left",
    )
else:
    merged = raw.copy()
    for col in ["fit_score", "first_name", "company_name", "email", "pain_point",
                "suggested_angle", "portfolio_proof", "reasoning", "contact_name", "company_website"]:
        merged[col] = None

# ── Sidebar filters ──────────────────────────────────────────────
sources = sorted(merged["source"].dropna().unique().tolist())
selected_sources = st.sidebar.multiselect("Source", sources, default=sources)

score_range = st.sidebar.slider("Fit Score", 0, 10, (0, 10))

status_filter = st.sidebar.radio(
    "Status",
    ["All", "Qualified Only", "Unprocessed Only"],
    index=0,
)

# ── Apply filters ────────────────────────────────────────────────
df = merged[merged["source"].isin(selected_sources)]
df = df[(df["fit_score"].fillna(0) >= score_range[0]) & (df["fit_score"].fillna(0) <= score_range[1])]

if status_filter == "Qualified Only":
    df = df[df["fit_score"].notna()]
elif status_filter == "Unprocessed Only":
    df = df[df["processed"] == False]

# ── Stats ────────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)
c1.metric("Showing", len(df))
c2.metric("Qualified", df["fit_score"].notna().sum())
c3.metric("Avg Score", round(df["fit_score"].dropna().mean(), 1) if df["fit_score"].notna().any() else "N/A")

# ── Table ────────────────────────────────────────────────────────
display_cols = ["scraped_at", "source", "title", "company_name", "fit_score",
                "first_name", "email", "url"]
display = df[display_cols].copy()
display.columns = ["Date", "Source", "Title", "Company", "Score", "Contact", "Email", "URL"]
display["Date"] = display["Date"].dt.strftime("%Y-%m-%d %H:%M")

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "URL": st.column_config.LinkColumn("URL", display_text="Open"),
        "Score": st.column_config.NumberColumn("Score", format="%d"),
    },
)

# ── Expandable details ───────────────────────────────────────────
st.subheader("Lead Details")
qualified_df = df[df["fit_score"].notna()]

if qualified_df.empty:
    st.info("Select a period with qualified leads to see details.")
else:
    for _, row in qualified_df.head(20).iterrows():
        score = int(row["fit_score"]) if pd.notna(row["fit_score"]) else 0
        color = fit_score_color(score)
        label = f"**[{score}/10]** {row['title'][:80]} — {row.get('company_name', '')}"
        with st.expander(label):
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"**Source:** {row['source']}")
                st.markdown(f"**Contact:** {row.get('first_name', 'N/A')}")
                st.markdown(f"**Email:** {row.get('email', 'N/A')}")
                st.markdown(f"**Company Website:** {row.get('company_website', 'N/A')}")
            with col_b:
                st.markdown(f"**Angle:** {row.get('suggested_angle', 'N/A')}")
                st.markdown(f"**Pain Point:** {row.get('pain_point', 'N/A')}")
            st.markdown(f"**Portfolio Proof:** {row.get('portfolio_proof', 'N/A')}")
            st.markdown(f"**Reasoning:** {row.get('reasoning', 'N/A')}")
            if row.get("url"):
                st.markdown(f"[Open Job Post]({row['url']})")
