"""
System Architecture — Pipeline visualization, tech stack, and cost breakdown.
"""

import streamlit as st

# ── Header ────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-bottom: 8px;">
    <h1 style="margin:0; font-size: 1.8rem; font-weight: 700; letter-spacing: -0.03em;">
        🏗️ System Architecture
    </h1>
    <p style="color: #C8C8D8; font-size: 0.9rem; margin: 4px 0 0 0;">
        How this autonomous prospecting agent works under the hood
    </p>
</div>
""", unsafe_allow_html=True)

# ── Pipeline Diagram ──────────────────────────────────────────────
st.markdown("#### End-to-End Pipeline")

st.markdown("""
<div style="
    background: linear-gradient(135deg, #1E1E2E 0%, #252535 100%);
    border: 1px solid rgba(99,102,241,0.2);
    border-radius: 16px;
    padding: 32px;
    margin: 8px 0 24px 0;
">
    <div style="display: flex; align-items: center; justify-content: center; gap: 0; flex-wrap: wrap;">
        <!-- Step 1 -->
        <div style="text-align: center; min-width: 130px;">
            <div style="
                background: linear-gradient(135deg, #6366F1, #4F46E5);
                width: 56px; height: 56px; border-radius: 14px;
                display: flex; align-items: center; justify-content: center;
                margin: 0 auto 8px auto; font-size: 1.5rem;
                box-shadow: 0 4px 16px rgba(99,102,241,0.3);
            ">🔍</div>
            <div style="font-weight: 600; font-size: 0.85rem; color: #FAFAFA;">Scrape</div>
            <div style="font-size: 0.7rem; color: #9CA3B0; margin-top: 2px;">Upwork · LinkedIn<br>Serper · Jina</div>
        </div>
        <div style="color: #4F46E5; font-size: 1.5rem; margin: 0 8px;">→</div>
        <!-- Step 2 -->
        <div style="text-align: center; min-width: 130px;">
            <div style="
                background: linear-gradient(135deg, #8B5CF6, #7C3AED);
                width: 56px; height: 56px; border-radius: 14px;
                display: flex; align-items: center; justify-content: center;
                margin: 0 auto 8px auto; font-size: 1.5rem;
                box-shadow: 0 4px 16px rgba(139,92,246,0.3);
            ">🧠</div>
            <div style="font-weight: 600; font-size: 0.85rem; color: #FAFAFA;">Qualify</div>
            <div style="font-size: 0.7rem; color: #9CA3B0; margin-top: 2px;">Gemini AI scores<br>fit 1-10 + reasoning</div>
        </div>
        <div style="color: #4F46E5; font-size: 1.5rem; margin: 0 8px;">→</div>
        <!-- Step 3 -->
        <div style="text-align: center; min-width: 130px;">
            <div style="
                background: linear-gradient(135deg, #A78BFA, #8B5CF6);
                width: 56px; height: 56px; border-radius: 14px;
                display: flex; align-items: center; justify-content: center;
                margin: 0 auto 8px auto; font-size: 1.5rem;
                box-shadow: 0 4px 16px rgba(167,139,250,0.3);
            ">✍️</div>
            <div style="font-weight: 600; font-size: 0.85rem; color: #FAFAFA;">Draft Email</div>
            <div style="font-size: 0.7rem; color: #9CA3B0; margin-top: 2px;">AI-personalized<br>outreach per lead</div>
        </div>
        <div style="color: #4F46E5; font-size: 1.5rem; margin: 0 8px;">→</div>
        <!-- Step 4 -->
        <div style="text-align: center; min-width: 130px;">
            <div style="
                background: linear-gradient(135deg, #F59E0B, #D97706);
                width: 56px; height: 56px; border-radius: 14px;
                display: flex; align-items: center; justify-content: center;
                margin: 0 auto 8px auto; font-size: 1.5rem;
                box-shadow: 0 4px 16px rgba(245,158,11,0.3);
            ">👤</div>
            <div style="font-weight: 600; font-size: 0.85rem; color: #FAFAFA;">HITL Review</div>
            <div style="font-size: 0.7rem; color: #9CA3B0; margin-top: 2px;">Telegram bot<br>approve / reject</div>
        </div>
        <div style="color: #4F46E5; font-size: 1.5rem; margin: 0 8px;">→</div>
        <!-- Step 5 -->
        <div style="text-align: center; min-width: 130px;">
            <div style="
                background: linear-gradient(135deg, #10B981, #059669);
                width: 56px; height: 56px; border-radius: 14px;
                display: flex; align-items: center; justify-content: center;
                margin: 0 auto 8px auto; font-size: 1.5rem;
                box-shadow: 0 4px 16px rgba(16,185,129,0.3);
            ">📨</div>
            <div style="font-weight: 600; font-size: 0.85rem; color: #FAFAFA;">Send</div>
            <div style="font-size: 0.7rem; color: #9CA3B0; margin-top: 2px;">Brevo SMTP<br>auto-delivery</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Active Vertical ───────────────────────────────────────────────
st.markdown("#### Active Vertical")

st.markdown("""
<div style="
    background: linear-gradient(135deg, #6366F115, #6366F108);
    border: 1px solid #6366F140;
    border-radius: 12px;
    padding: 24px 28px;
">
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 12px;">
        <div style="font-size: 1.5rem;">🖥️</div>
        <div style="font-weight: 700; font-size: 1.1rem; color: #6366F1;">
            Tech Services — Freelance & Agency Prospecting
        </div>
    </div>
    <div style="font-size: 0.85rem; color: #D1D1E0; line-height: 1.7; margin-bottom: 14px;">
        Scrapes <strong>Upwork</strong> job posts and <strong>LinkedIn</strong> profiles
        seeking freelance developers, data engineers, and analytics consultants.
        Gemini AI qualifies each lead based on budget, tech stack fit, project scope,
        and client history. Personalized outreach emails are drafted automatically
        and sent from <code>sebastian@sebastianramirezanalytics.com</code> after human approval.
    </div>
    <div style="display: flex; gap: 6px; flex-wrap: wrap;">
        <span style="background:#22C55E20; border:1px solid #22C55E50; padding:4px 12px; border-radius:6px; font-size:0.75rem; color:#22C55E; font-weight:600;">Upwork</span>
        <span style="background:#60A5FA20; border:1px solid #60A5FA50; padding:4px 12px; border-radius:6px; font-size:0.75rem; color:#60A5FA; font-weight:600;">LinkedIn</span>
        <span style="background:#FF6B6B20; border:1px solid #FF6B6B50; padding:4px 12px; border-radius:6px; font-size:0.75rem; color:#FF6B6B; font-weight:600;">Serper (Google Search)</span>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)

# ── Tech Stack ────────────────────────────────────────────────────
st.markdown("#### Tech Stack")

stack = [
    ("Scraping & Orchestration", [
        ("GitHub Actions", "Cron-scheduled workflows trigger scrapers on schedule", "#60A5FA"),
        ("Python + asyncio", "Fully async scrapers with httpx for non-blocking I/O", "#60A5FA"),
        ("Serper API", "Google Search results for lead enrichment", "#60A5FA"),
        ("Jina Reader", "Web content extraction for qualification context", "#FF6B6B"),
    ]),
    ("AI & Qualification", [
        ("Gemini 2.5 Flash-Lite", "Lead scoring (1-10), pain point detection, angle suggestion", "#8B5CF6"),
        ("Pydantic", "Structured output validation for LLM responses", "#F472B6"),
        ("Jinja2", "Email template rendering with lead-specific variables", "#F87171"),
    ]),
    ("Data & Infrastructure", [
        ("Supabase (PostgreSQL)", "Persistent storage for leads, emails, and audit logs", "#34D399"),
        ("Google Cloud Run", "Serverless HITL gateway — scales to zero", "#60A5FA"),
        ("Telegram Bot API", "Real-time approval notifications on mobile", "#38BDF8"),
        ("Brevo SMTP", "Transactional email delivery with tracking", "#34D399"),
    ]),
    ("Monitoring & Frontend", [
        ("Streamlit", "This dashboard — real-time analytics and HITL controls", "#FF4B4B"),
        ("structlog", "JSON-formatted structured logging for observability", "#D1D1E0"),
        ("Plotly", "Interactive charts and funnel visualizations", "#818CF8"),
    ]),
]

for category, items in stack:
    st.markdown(f"**{category}**")
    cols = st.columns(len(items))
    for i, (name, desc, color) in enumerate(items):
        with cols[i]:
            st.markdown(f"""
            <div style="
                background: {color}10;
                border: 1px solid {color}35;
                border-radius: 10px;
                padding: 14px 16px;
                height: 100%;
            ">
                <div style="font-weight: 600; font-size: 0.85rem; color: {color}; margin-bottom: 4px;">{name}</div>
                <div style="font-size: 0.73rem; color: #C8C8D8; line-height: 1.5;">{desc}</div>
            </div>
            """, unsafe_allow_html=True)
    st.markdown("<div style='height: 8px'></div>", unsafe_allow_html=True)

# ── Cost Breakdown ────────────────────────────────────────────────
st.markdown("#### Cost Breakdown")
st.markdown("""
<div style="
    background: linear-gradient(135deg, rgba(16,185,129,0.08), rgba(16,185,129,0.03));
    border: 1px solid rgba(16,185,129,0.25);
    border-radius: 12px;
    padding: 24px 28px;
">
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
        <div style="font-size: 2rem; font-weight: 800; color: #10B981;">$0</div>
        <div>
            <div style="font-size: 0.9rem; font-weight: 600; color: #FAFAFA;">Monthly operational cost</div>
            <div style="font-size: 0.78rem; color: #C8C8D8;">Every component runs on a free tier</div>
        </div>
    </div>
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 8px;">
        <div style="display:flex; justify-content:space-between; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
            <span style="font-size:0.8rem; color:#D1D1E0;">GitHub Actions</span>
            <span style="font-size:0.8rem; font-weight:600; color:#10B981;">Free (2,000 min/mo)</span>
        </div>
        <div style="display:flex; justify-content:space-between; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
            <span style="font-size:0.8rem; color:#D1D1E0;">Supabase</span>
            <span style="font-size:0.8rem; font-weight:600; color:#10B981;">Free (500 MB)</span>
        </div>
        <div style="display:flex; justify-content:space-between; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
            <span style="font-size:0.8rem; color:#D1D1E0;">Gemini API</span>
            <span style="font-size:0.8rem; font-weight:600; color:#10B981;">Free tier</span>
        </div>
        <div style="display:flex; justify-content:space-between; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
            <span style="font-size:0.8rem; color:#D1D1E0;">Cloud Run</span>
            <span style="font-size:0.8rem; font-weight:600; color:#10B981;">Free (2M req/mo)</span>
        </div>
        <div style="display:flex; justify-content:space-between; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
            <span style="font-size:0.8rem; color:#D1D1E0;">Brevo SMTP</span>
            <span style="font-size:0.8rem; font-weight:600; color:#10B981;">Free (300 emails/day)</span>
        </div>
        <div style="display:flex; justify-content:space-between; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
            <span style="font-size:0.8rem; color:#D1D1E0;">Streamlit Cloud</span>
            <span style="font-size:0.8rem; font-weight:600; color:#10B981;">Free (public apps)</span>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)

# ── Design Principles ─────────────────────────────────────────────
st.markdown("#### Design Principles")
p1, p2, p3 = st.columns(3)

with p1:
    st.markdown("""
    <div style="
        background: #1E1E2E;
        border: 1px solid rgba(99,102,241,0.15);
        border-radius: 10px;
        padding: 18px 20px;
    ">
        <div style="font-size: 1.2rem; margin-bottom: 6px;">🔄</div>
        <div style="font-weight: 600; font-size: 0.88rem; color: #FAFAFA; margin-bottom: 6px;">Fully Async</div>
        <div style="font-size: 0.78rem; color: #C8C8D8; line-height: 1.5;">
            Every I/O operation is non-blocking. asyncio + httpx + async Supabase client
            for maximum throughput with minimal resources.
        </div>
    </div>
    """, unsafe_allow_html=True)

with p2:
    st.markdown("""
    <div style="
        background: #1E1E2E;
        border: 1px solid rgba(99,102,241,0.15);
        border-radius: 10px;
        padding: 18px 20px;
    ">
        <div style="font-size: 1.2rem; margin-bottom: 6px;">🛡️</div>
        <div style="font-weight: 600; font-size: 0.88rem; color: #FAFAFA; margin-bottom: 6px;">Human-in-the-Loop</div>
        <div style="font-size: 0.78rem; color: #C8C8D8; line-height: 1.5;">
            No email is sent without human approval. Telegram bot delivers drafts
            for review, ensuring quality and preventing spam.
        </div>
    </div>
    """, unsafe_allow_html=True)

with p3:
    st.markdown("""
    <div style="
        background: #1E1E2E;
        border: 1px solid rgba(99,102,241,0.15);
        border-radius: 10px;
        padding: 18px 20px;
    ">
        <div style="font-size: 1.2rem; margin-bottom: 6px;">📐</div>
        <div style="font-weight: 600; font-size: 0.88rem; color: #FAFAFA; margin-bottom: 6px;">Modular Verticals</div>
        <div style="font-size: 0.78rem; color: #C8C8D8; line-height: 1.5;">
            Each vertical is an independent service with its own scrapers, prompts,
            and email templates. Add a new market in hours, not days.
        </div>
    </div>
    """, unsafe_allow_html=True)
