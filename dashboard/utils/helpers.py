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
    "upwork": "#14A800",
    "linkedin": "#0A66C2",
    "weworkremotely": "#4ECDC4",
    "indeed": "#003A9B",
    "serper": "#FF6B6B",
}


def status_badge(status: str) -> str:
    color = STATUS_COLORS.get(status, "#6B7280")
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
