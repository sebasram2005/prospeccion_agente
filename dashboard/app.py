"""
Prospecting Agent Dashboard — Main entry point.

Run: streamlit run dashboard/app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Prospecting Agent — Live Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Import font ──────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="st-"] {
    font-family: 'Inter', sans-serif;
}

/* ── Metric cards ─────────────────────────────────────── */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1E1E2E 0%, #2A2A3E 100%);
    border: 1px solid rgba(99, 102, 241, 0.2);
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
}
[data-testid="stMetricLabel"] {
    font-size: 0.82rem !important;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #D1D1E0 !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.8rem !important;
    font-weight: 700;
    color: #FAFAFA !important;
}
[data-testid="stMetricDelta"] {
    font-size: 0.8rem !important;
}

/* ── Sidebar ──────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0E1117 0%, #151520 100%);
    border-right: 1px solid rgba(99, 102, 241, 0.15);
}
section[data-testid="stSidebar"] * {
    color: #FFFFFF !important;
}

/* ── Expanders ────────────────────────────────────────── */
[data-testid="stExpander"] {
    background: #1E1E2E;
    border: 1px solid rgba(99, 102, 241, 0.15);
    border-radius: 10px;
    margin-bottom: 8px;
}
[data-testid="stExpander"] summary span p {
    color: #FAFAFA !important;
    font-weight: 500;
}

/* ── Dataframe ────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
}

/* ── Tabs ─────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0;
    padding: 8px 20px;
    font-weight: 600;
}

/* ── Buttons ──────────────────────────────────────────── */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #4F46E5, #7C3AED);
    border: none;
    border-radius: 8px;
    font-weight: 600;
    transition: all 0.2s;
}
.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg, #6366F1, #8B5CF6);
    box-shadow: 0 4px 12px rgba(99, 102, 241, 0.4);
}

/* ── Dividers ─────────────────────────────────────────── */
hr {
    border-color: rgba(99, 102, 241, 0.15) !important;
}

/* ── Sidebar navigation — always visible ──────────────── */
[data-testid="stSidebarNav"] {
    display: block !important;
    visibility: visible !important;
}
[data-testid="stSidebarNav"] a {
    color: #FAFAFA !important;
}
[data-testid="stSidebarNavItems"] {
    display: block !important;
}

/* ── Hide default streamlit branding ──────────────────── */
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ── Navigation ────────────────────────────────────────────────────
pages = [
    st.Page("pages/1_overview.py", title="Command Center", icon="⚡", default=True),
    st.Page("pages/2_leads.py", title="Leads Explorer", icon="🎯"),
    st.Page("pages/3_email_queue.py", title="Email Queue", icon="✉️"),
    st.Page("pages/4_analytics.py", title="Analytics", icon="📊"),
    st.Page("pages/5_architecture.py", title="System Architecture", icon="🏗️"),
    st.Page("pages/6_hmlv_manufacturers.py", title="HMLV Manufacturers", icon="🏭"),
]

nav = st.navigation(pages)

# ── Sidebar branding ──────────────────────────────────────────────
st.sidebar.markdown("""
<div style="text-align:center; padding: 8px 0 16px 0;">
    <div style="font-size: 2.2rem; margin-bottom: 4px;">⚡</div>
    <div style="font-size: 1.1rem; font-weight: 700; color: #FAFAFA; letter-spacing: -0.02em;">
        Prospecting Agent
    </div>
    <div style="font-size: 0.75rem; color: #C8C8D8; margin-top: 2px;">
        Autonomous B2B Lead Generation
    </div>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")

# System status
st.sidebar.markdown("""
<div style="
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.1), rgba(16, 185, 129, 0.05));
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 12px;
">
    <div style="display: flex; align-items: center; gap: 8px;">
        <div style="width: 8px; height: 8px; background: #10B981; border-radius: 50%; box-shadow: 0 0 8px #10B981;"></div>
        <span style="font-size: 0.8rem; font-weight: 600; color: #10B981;">SYSTEM OPERATIONAL</span>
    </div>
    <div style="font-size: 0.7rem; color: #9CA3B0; margin-top: 4px;">
        Scrapers run via GitHub Actions cron
    </div>
</div>
""", unsafe_allow_html=True)

# Cost badge
st.sidebar.markdown("""
<div style="
    background: linear-gradient(135deg, rgba(99, 102, 241, 0.15), rgba(139, 92, 246, 0.1));
    border: 1px solid rgba(99, 102, 241, 0.3);
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 12px;
">
    <div style="font-size: 1.3rem; font-weight: 700; color: #A78BFA;">$0<span style="font-size: 0.75rem; color: #C8C8D8;">/month</span></div>
    <div style="font-size: 0.7rem; color: #C8C8D8;">100% free-tier infrastructure</div>
</div>
""", unsafe_allow_html=True)

# Tech stack
st.sidebar.markdown("""
<div style="margin-bottom: 8px;">
    <div style="font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; color: #9CA3B0; margin-bottom: 8px;">Tech Stack</div>
    <div style="display: flex; flex-wrap: wrap; gap: 4px;">
        <span style="background: #1E1E2E; border: 1px solid #2A2A3E; padding: 3px 8px; border-radius: 6px; font-size: 0.68rem; color: #D1D1E0;">Python</span>
        <span style="background: #1E1E2E; border: 1px solid #2A2A3E; padding: 3px 8px; border-radius: 6px; font-size: 0.68rem; color: #D1D1E0;">Gemini AI</span>
        <span style="background: #1E1E2E; border: 1px solid #2A2A3E; padding: 3px 8px; border-radius: 6px; font-size: 0.68rem; color: #D1D1E0;">Supabase</span>
        <span style="background: #1E1E2E; border: 1px solid #2A2A3E; padding: 3px 8px; border-radius: 6px; font-size: 0.68rem; color: #D1D1E0;">Cloud Run</span>
        <span style="background: #1E1E2E; border: 1px solid #2A2A3E; padding: 3px 8px; border-radius: 6px; font-size: 0.68rem; color: #D1D1E0;">GitHub Actions</span>
        <span style="background: #1E1E2E; border: 1px solid #2A2A3E; padding: 3px 8px; border-radius: 6px; font-size: 0.68rem; color: #D1D1E0;">Telegram Bot</span>
    </div>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")
st.sidebar.markdown(
    '<div style="text-align:center;">'
    '<a href="https://github.com/sebasram2005/prospeccion_agente" target="_blank" '
    'style="color: #9CA3B0; text-decoration: none; font-size: 0.75rem;">'
    '↗ View Source on GitHub'
    '</a></div>',
    unsafe_allow_html=True,
)
st.sidebar.caption(
    '<div style="text-align:center; color: #9CA3B0;">Built by Sebastian Ramirez</div>',
    unsafe_allow_html=True,
)

nav.run()
