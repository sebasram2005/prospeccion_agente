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
    "indeed": "#818CF8",
    "serper": "#FF6B6B",
    "wellfound": "#FF7043",
    "otta": "#AB47BC",
    "efinancialcareers": "#26C6DA",
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
