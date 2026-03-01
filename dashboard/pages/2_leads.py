"""
Leads Explorer — Filterable table with qualification details.
"""

import streamlit as st
import pandas as pd

from utils.supabase_client import get_raw_leads, get_qualified_leads
from utils.helpers import fit_score_color

# ── Header ────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 8px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        🎯 Leads Explorer
    </h1>
    <p style="color: #8B8BA0; font-size: 0.9rem; margin: 4px 0 0 0;">
        Browse, filter, and inspect every lead the system has captured
    </p>
</div>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────
days = st.sidebar.selectbox(
    "Period", [7, 14, 30, 90], index=2,
    format_func=lambda d: f"Last {d} days", key="leads_days",
)
raw = get_raw_leads(days=days)
qualified = get_qualified_leads(days=days)

if raw.empty:
    st.info("No leads found for this period.")
    st.stop()

# ── Merge raw + qualified ─────────────────────────────────────────
qual_cols = [
    "raw_lead_id", "fit_score", "first_name", "company_name", "email",
    "pain_point", "suggested_angle", "portfolio_proof", "reasoning",
    "contact_name", "company_website",
]
if not qualified.empty:
    merged = raw.merge(
        qualified[qual_cols], left_on="id", right_on="raw_lead_id", how="left",
    )
else:
    merged = raw.copy()
    for col in qual_cols[1:]:
        merged[col] = None

# ── Sidebar filters ───────────────────────────────────────────────
sources = sorted(merged["source"].dropna().unique().tolist())
selected_sources = st.sidebar.multiselect("Source", sources, default=sources)
score_range = st.sidebar.slider("Fit Score", 0, 10, (0, 10))
status_filter = st.sidebar.radio(
    "Status", ["All", "Qualified Only", "Unprocessed Only"], index=0,
)

# ── Apply filters ─────────────────────────────────────────────────
df = merged[merged["source"].isin(selected_sources)]
df = df[
    (df["fit_score"].fillna(0) >= score_range[0])
    & (df["fit_score"].fillna(0) <= score_range[1])
]
if status_filter == "Qualified Only":
    df = df[df["fit_score"].notna()]
elif status_filter == "Unprocessed Only":
    df = df[df["processed"] == False]  # noqa: E712

# ── Stats row ─────────────────────────────────────────────────────
qualified_count = int(df["fit_score"].notna().sum())
avg = round(df["fit_score"].dropna().mean(), 1) if df["fit_score"].notna().any() else "—"
high_fit = int((df["fit_score"].dropna() >= 7).sum()) if df["fit_score"].notna().any() else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Showing", f"{len(df):,}")
c2.metric("Qualified", f"{qualified_count:,}")
c3.metric("Avg Score", avg)
c4.metric("High-Fit (7+)", high_fit)

# ── Table ─────────────────────────────────────────────────────────
display_cols = [
    "scraped_at", "source", "vertical", "title", "company_name",
    "fit_score", "first_name", "email", "url",
]
display = df[display_cols].copy()
display.columns = [
    "Date", "Source", "Vertical", "Title", "Company",
    "Score", "Contact", "Email", "URL",
]
display["Date"] = display["Date"].dt.strftime("%b %d, %H:%M")
display["Vertical"] = display["Vertical"].map(
    {"tech": "Tech Services"}
).fillna("—")

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "URL": st.column_config.LinkColumn("URL", display_text="Open ↗"),
        "Score": st.column_config.ProgressColumn(
            "Score", min_value=0, max_value=10, format="%d",
        ),
    },
)

# ── Expandable details ────────────────────────────────────────────
qualified_df = df[df["fit_score"].notna()].copy()

if qualified_df.empty:
    st.info("Select a period with qualified leads to see details.")
else:
    st.markdown("#### Lead Details")
    for _, row in qualified_df.head(25).iterrows():
        score = int(row["fit_score"]) if pd.notna(row["fit_score"]) else 0
        color = fit_score_color(score)
        company = row.get("company_name", "") or ""
        title_text = (row.get("title", "") or "")[:80]

        label = f"**{score}/10** — {title_text}"
        if company:
            label += f" · {company}"

        with st.expander(label):
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"""
                <div style="color: #FAFAFA; font-size: 0.9rem; line-height: 2;">
                    <div><span style="color: #8B8BA0;">Source:</span> {row['source']}</div>
                    <div><span style="color: #8B8BA0;">Contact:</span> {row.get('first_name') or '—'}</div>
                    <div><span style="color: #8B8BA0;">Email:</span> {row.get('email') or '—'}</div>
                    <div><span style="color: #8B8BA0;">Company:</span> {company or '—'}</div>
                    <div><span style="color: #8B8BA0;">Website:</span> {row.get('company_website') or '—'}</div>
                </div>
                """, unsafe_allow_html=True)
            with col_b:
                # Score indicator
                st.markdown(f"""
                <div style="
                    background: {color}25;
                    border: 1px solid {color}60;
                    border-radius: 10px;
                    padding: 12px 16px;
                    text-align: center;
                    margin-bottom: 12px;
                ">
                    <div style="font-size: 2rem; font-weight: 700; color: {color};">{score}<span style="font-size: 1rem; color: #8B8BA0;">/10</span></div>
                    <div style="font-size: 0.75rem; color: #8B8BA0; text-transform: uppercase; letter-spacing: 0.05em;">Fit Score</div>
                </div>
                """, unsafe_allow_html=True)
                st.markdown(f'<div style="color:#FAFAFA;"><b>Suggested Angle:</b> {row.get("suggested_angle") or "—"}</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="color:#FAFAFA;"><b>Pain Point:</b> {row.get("pain_point") or "—"}</div>', unsafe_allow_html=True)

            if row.get("reasoning"):
                st.markdown(f'<div style="color:#FAFAFA;"><b>AI Reasoning:</b> {row["reasoning"]}</div>', unsafe_allow_html=True)
            if row.get("portfolio_proof"):
                st.markdown(f'<div style="color:#FAFAFA;"><b>Portfolio Proof:</b> {row["portfolio_proof"]}</div>', unsafe_allow_html=True)
            if row.get("url"):
                st.markdown(f"[Open Original Post ↗]({row['url']})")
