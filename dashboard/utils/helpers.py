"""
Formatting helpers for the dashboard.
"""

STATUS_COLORS = {
    "pending": "#F59E0B",
    "approved": "#3B82F6",
    "sent": "#10B981",
    "rejected": "#EF4444",
    "edited": "#8B5CF6",
    "editing": "#8B5CF6",
}

SOURCE_COLORS = {
    "upwork": "#14A800",
    "linkedin": "#0A66C2",
    "weworkremotely": "#1B1B1B",
    "indeed": "#003A9B",
    "gmaps": "#4285F4",
    "instagram": "#E1306C",
}


def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#6B7280")
    return f'<span style="background-color:{color};color:white;padding:2px 8px;border-radius:4px;font-size:0.85em">{status.upper()}</span>'


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
