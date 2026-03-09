"""
Formatting helpers for the dashboard.
"""

STATUS_COLORS = {
    "pending": "#F59E0B",
    "approved": "#6366F1",
    "sent": "#10B981",
    "rejected": "#EF4444",
    "edited": "#8B5CF6",
    "editing": "#8B5CF6",
}

SOURCE_COLORS = {
    "upwork": "#22C55E",
    "linkedin": "#60A5FA",
    "weworkremotely": "#5EEAD4",
    "glassdoor": "#818CF8",
    "remoteok": "#FF6B6B",
    "wellfound": "#FF7043",
    "otta": "#AB47BC",
    "efinancialcareers": "#26C6DA",
}

# ── HMLV Vertical 3 ───────────────────────────────────────────────
INDUSTRY_COLORS = {
    "trade_show_exhibits":   "#F59E0B",
    "marine_decking":        "#06B6D4",
    "architectural_millwork":"#8B5CF6",
    "industrial_crating":    "#EF4444",
    "metal_facades":         "#6366F1",
    "other":                 "#9CA3AF",
}

INDUSTRY_LABELS = {
    "trade_show_exhibits":    "Trade Show",
    "marine_decking":         "Marine Decking",
    "architectural_millwork": "Millwork",
    "industrial_crating":     "Ind. Crating",
    "metal_facades":          "Metal Facades",
    "other":                  "Other",
}

INDUSTRY_ICONS = {
    "trade_show_exhibits":    "🏛️",
    "marine_decking":         "⛵",
    "architectural_millwork": "🪵",
    "industrial_crating":     "📦",
    "metal_facades":          "🔩",
    "other":                  "🏭",
}

ANGLE_COLORS = {
    "BOM-automation":     "#10B981",
    "Nesting-optimization":"#6366F1",
    "Quote-to-cash":      "#F59E0B",
    "DXF-export":         "#EC4899",
    "CAD-ERP-bridge":     "#8B5CF6",
}

HMLV_SOURCE_COLORS = {
    "trade_show":    "#F59E0B",
    "marine_decking":"#06B6D4",
    "millwork":      "#8B5CF6",
    "crating":       "#EF4444",
    "metal_facades": "#6366F1",
}

# ── LGaaS Vertical 4 ──────────────────────────────────────────────
LGAAS_NICHE_COLORS = {
    "fractional_cfo":  "#10B981",
    "ma_advisory":     "#6366F1",
    "cmmc_security":   "#EF4444",
    "ai_automation":   "#F59E0B",
    "esg_consulting":  "#06B6D4",
    "other":           "#9CA3AF",
}

LGAAS_NICHE_LABELS = {
    "fractional_cfo":  "Fractional CFO",
    "ma_advisory":     "M&A Advisory",
    "cmmc_security":   "CMMC Security",
    "ai_automation":   "AI Automation",
    "esg_consulting":  "ESG Consulting",
    "other":           "Other",
}

LGAAS_NICHE_ICONS = {
    "fractional_cfo":  "💰",
    "ma_advisory":     "🤝",
    "cmmc_security":   "🔐",
    "ai_automation":   "🤖",
    "esg_consulting":  "🌱",
    "other":           "🏢",
}

LGAAS_ANGLE_COLORS = {
    "roi-calculator":       "#10B981",
    "competitor-benchmark": "#6366F1",
    "capacity-unlock":      "#F59E0B",
    "cost-of-inaction":     "#EF4444",
    "proof-of-concept":     "#8B5CF6",
}


def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#9CA3B0")
    return (
        f'<span style="background:{color};color:white;padding:2px 10px;'
        f'border-radius:12px;font-size:0.72rem;font-weight:600;'
        f'letter-spacing:0.04em">{status.upper()}</span>'
    )


def fit_score_color(score: int) -> str:
    if score >= 8:
        return "#10B981"
    if score >= 5:
        return "#F59E0B"
    return "#EF4444"


def truncate(text: str, max_len: int = 80) -> str:
    if not text:
        return ""
    return text[:max_len] + "..." if len(text) > max_len else text
