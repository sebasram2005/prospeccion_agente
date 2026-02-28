"""
Prospecting Agent Dashboard — Main entry point.

Run: streamlit run dashboard/app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Prospecting Agent",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = [
    st.Page("pages/1_overview.py", title="Overview", icon="📊", default=True),
    st.Page("pages/2_leads.py", title="Leads Explorer", icon="🔍"),
    st.Page("pages/3_email_queue.py", title="Email Queue", icon="📧"),
    st.Page("pages/4_analytics.py", title="Analytics", icon="📈"),
]

nav = st.navigation(pages)

st.sidebar.markdown("---")
st.sidebar.caption("Prospecting Agent v1.0")

nav.run()
