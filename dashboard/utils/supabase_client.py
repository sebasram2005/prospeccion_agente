"""
Supabase client for the Streamlit dashboard.

Uses the sync Supabase client with Streamlit caching.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from supabase import create_client, Client


@st.cache_resource
def get_client() -> Client:
    return create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"],
    )


@st.cache_data(ttl=300)
def get_raw_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("raw_leads")
        .select("id, source, vertical, url, raw_data, scraped_at, processed")
        .eq("vertical", "tech")
        .gte("scraped_at", since)
        .order("scraped_at", desc=True)
        .limit(1000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    # Extract title from raw_data JSONB
    df["title"] = df["raw_data"].apply(
        lambda x: x.get("title", "") if isinstance(x, dict) else ""
    )
    return df


@st.cache_data(ttl=300)
def get_qualified_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("qualified_leads")
        .select(
            "id, raw_lead_id, vertical, first_name, company_name, email, "
            "qualification_result, pain_point, qualified_at"
        )
        .eq("vertical", "tech")
        .gte("qualified_at", since)
        .order("qualified_at", desc=True)
        .limit(1000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["qualified_at"] = pd.to_datetime(df["qualified_at"])
    # Extract fields from qualification_result JSONB
    df["fit_score"] = df["qualification_result"].apply(
        lambda x: x.get("fit_score", 0) if isinstance(x, dict) else 0
    )
    df["suggested_angle"] = df["qualification_result"].apply(
        lambda x: x.get("suggested_angle", "") if isinstance(x, dict) else ""
    )
    df["portfolio_proof"] = df["qualification_result"].apply(
        lambda x: x.get("portfolio_proof", "") if isinstance(x, dict) else ""
    )
    df["reasoning"] = df["qualification_result"].apply(
        lambda x: x.get("reasoning", "") if isinstance(x, dict) else ""
    )
    df["contact_name"] = df["qualification_result"].apply(
        lambda x: x.get("contact_name", "") if isinstance(x, dict) else ""
    )
    df["company_website"] = df["qualification_result"].apply(
        lambda x: x.get("company_website", "") if isinstance(x, dict) else ""
    )
    df["pricing_model"] = df["qualification_result"].apply(
        lambda x: x.get("pricing_model", "hourly") if isinstance(x, dict) else "hourly"
    )
    df["contract_value_tier"] = df["qualification_result"].apply(
        lambda x: x.get("contract_value_tier", "standard") if isinstance(x, dict) else "standard"
    )
    return df


@st.cache_data(ttl=300)
def get_email_queue(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("email_queue")
        .select(
            "id, qualified_lead_id, vertical, to_email, subject, body, "
            "status, source, job_url, created_at, updated_at"
        )
        .gte("created_at", since)
        .order("created_at", desc=True)
        .limit(500)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df


@st.cache_data(ttl=300)
def get_audit_log(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("hitl_audit_log")
        .select("id, email_queue_id, action, operator_note, acted_at")
        .gte("acted_at", since)
        .order("acted_at", desc=True)
        .limit(500)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["acted_at"] = pd.to_datetime(df["acted_at"])
    return df


@st.cache_data(ttl=300)
def get_keyword_performance() -> pd.DataFrame:
    client = get_client()
    result = (
        client.table("keyword_performance")
        .select(
            "keyword, source, leads_found, leads_qualified, leads_approved, "
            "leads_rejected, avg_fit_score, score, last_run_at"
        )
        .order("score", desc=True)
        .limit(200)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["last_run_at"] = pd.to_datetime(df["last_run_at"])
    return df


# ── HMLV Vertical 3 ──────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_hmlv_raw_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("raw_leads")
        .select("id, source, vertical, url, raw_data, scraped_at, processed")
        .eq("vertical", "hmlv")
        .gte("scraped_at", since)
        .order("scraped_at", desc=True)
        .limit(2000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["title"] = df["raw_data"].apply(
        lambda x: x.get("title", "") if isinstance(x, dict) else ""
    )
    df["search_keyword"] = df["raw_data"].apply(
        lambda x: x.get("search_keyword", "") if isinstance(x, dict) else ""
    )
    return df


@st.cache_data(ttl=300)
def get_hmlv_qualified_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("qualified_leads")
        .select(
            "id, raw_lead_id, vertical, first_name, company_name, email, "
            "qualification_result, pain_point, qualified_at"
        )
        .eq("vertical", "hmlv")
        .gte("qualified_at", since)
        .order("qualified_at", desc=True)
        .limit(2000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["qualified_at"] = pd.to_datetime(df["qualified_at"])

    def _get(row, key, default=""):
        return row.get(key, default) if isinstance(row, dict) else default

    df["fit_score"]           = df["qualification_result"].apply(lambda x: _get(x, "fit_score", 0))
    df["industry_category"]   = df["qualification_result"].apply(lambda x: _get(x, "industry_category", "other"))
    df["red_flags"]           = df["qualification_result"].apply(lambda x: _get(x, "red_flags", []))
    df["green_flags"]         = df["qualification_result"].apply(lambda x: _get(x, "green_flags", []))
    df["technical_reasoning"] = df["qualification_result"].apply(lambda x: _get(x, "technical_reasoning", ""))
    df["key_technology"]      = df["qualification_result"].apply(lambda x: _get(x, "key_technology", ""))
    df["company_website"]     = df["qualification_result"].apply(lambda x: _get(x, "company_website", ""))
    df["suggested_angle"]     = df["qualification_result"].apply(lambda x: _get(x, "suggested_angle", ""))
    df["inferred_company"]    = df["qualification_result"].apply(lambda x: _get(x, "inferred_company", ""))
    df["contact_name"]        = df["qualification_result"].apply(lambda x: _get(x, "contact_name", ""))
    df["contact_email"]       = df["qualification_result"].apply(lambda x: _get(x, "contact_email", ""))
    return df


@st.cache_data(ttl=300)
def get_hmlv_email_queue(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("email_queue")
        .select(
            "id, qualified_lead_id, vertical, to_email, subject, body, "
            "status, source, job_url, created_at, updated_at"
        )
        .eq("vertical", "hmlv")
        .gte("created_at", since)
        .order("created_at", desc=True)
        .limit(500)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df


# ── LGaaS Vertical 4 ─────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_lgaas_raw_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("raw_leads")
        .select("id, source, vertical, url, raw_data, scraped_at, processed")
        .eq("vertical", "lgaas")
        .gte("scraped_at", since)
        .order("scraped_at", desc=True)
        .limit(2000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["title"] = df["raw_data"].apply(
        lambda x: x.get("title", "") if isinstance(x, dict) else ""
    )
    df["search_keyword"] = df["raw_data"].apply(
        lambda x: x.get("search_keyword", "") if isinstance(x, dict) else ""
    )
    return df


@st.cache_data(ttl=300)
def get_lgaas_qualified_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("qualified_leads")
        .select(
            "id, raw_lead_id, vertical, first_name, company_name, email, "
            "qualification_result, pain_point, qualified_at"
        )
        .eq("vertical", "lgaas")
        .gte("qualified_at", since)
        .order("qualified_at", desc=True)
        .limit(2000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["qualified_at"] = pd.to_datetime(df["qualified_at"])

    def _get(row, key, default=""):
        return row.get(key, default) if isinstance(row, dict) else default

    df["fit_score"]           = df["qualification_result"].apply(lambda x: _get(x, "fit_score", 0))
    df["niche_category"]      = df["qualification_result"].apply(lambda x: _get(x, "niche_category", "other"))
    df["red_flags"]           = df["qualification_result"].apply(lambda x: _get(x, "red_flags", []))
    df["green_flags"]         = df["qualification_result"].apply(lambda x: _get(x, "green_flags", []))
    df["technical_reasoning"] = df["qualification_result"].apply(lambda x: _get(x, "technical_reasoning", ""))
    df["estimated_ticket"]    = df["qualification_result"].apply(lambda x: _get(x, "estimated_ticket", ""))
    df["company_website"]     = df["qualification_result"].apply(lambda x: _get(x, "company_website", ""))
    df["suggested_angle"]     = df["qualification_result"].apply(lambda x: _get(x, "suggested_angle", ""))
    df["inferred_company"]    = df["qualification_result"].apply(lambda x: _get(x, "inferred_company", ""))
    df["contact_name"]        = df["qualification_result"].apply(lambda x: _get(x, "contact_name", ""))
    df["contact_email"]       = df["qualification_result"].apply(lambda x: _get(x, "contact_email", ""))
    return df


@st.cache_data(ttl=300)
def get_lgaas_email_queue(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("email_queue")
        .select(
            "id, qualified_lead_id, vertical, to_email, subject, body, "
            "status, source, job_url, created_at, updated_at"
        )
        .eq("vertical", "lgaas")
        .gte("created_at", since)
        .order("created_at", desc=True)
        .limit(500)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df


# ── M&A Silver Tsunami Vertical 5 ────────────────────────────────

@st.cache_data(ttl=300)
def get_ma_raw_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("raw_leads")
        .select("id, source, vertical, url, raw_data, scraped_at, processed")
        .eq("vertical", "ma")
        .gte("scraped_at", since)
        .order("scraped_at", desc=True)
        .limit(2000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["title"] = df["raw_data"].apply(
        lambda x: x.get("title", "") if isinstance(x, dict) else ""
    )
    df["search_keyword"] = df["raw_data"].apply(
        lambda x: x.get("search_keyword", "") if isinstance(x, dict) else ""
    )
    return df


@st.cache_data(ttl=300)
def get_ma_qualified_leads(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("qualified_leads")
        .select(
            "id, raw_lead_id, vertical, first_name, company_name, email, "
            "qualification_result, pain_point, qualified_at"
        )
        .eq("vertical", "ma")
        .gte("qualified_at", since)
        .order("qualified_at", desc=True)
        .limit(2000)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["qualified_at"] = pd.to_datetime(df["qualified_at"])

    def _get(row, key, default=""):
        return row.get(key, default) if isinstance(row, dict) else default

    df["fit_score"]             = df["qualification_result"].apply(lambda x: _get(x, "fit_score", 0))
    df["founder_name"]          = df["qualification_result"].apply(lambda x: _get(x, "founder_name", ""))
    df["estimated_years_active"]= df["qualification_result"].apply(lambda x: _get(x, "estimated_years_active", ""))
    df["momentum_signal"]       = df["qualification_result"].apply(lambda x: _get(x, "momentum_signal", ""))
    df["industry_niche"]        = df["qualification_result"].apply(lambda x: _get(x, "industry_niche", ""))
    df["suggested_angle"]       = df["qualification_result"].apply(lambda x: _get(x, "suggested_angle", ""))
    df["contact_email"]         = df["qualification_result"].apply(lambda x: _get(x, "contact_email", ""))
    df["company_website"]       = df["qualification_result"].apply(lambda x: _get(x, "company_website", ""))
    df["is_qualified"]          = df["qualification_result"].apply(lambda x: _get(x, "is_qualified", False))
    return df


@st.cache_data(ttl=300)
def get_ma_email_queue(days: int = 30) -> pd.DataFrame:
    client = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    result = (
        client.table("email_queue")
        .select(
            "id, qualified_lead_id, vertical, to_email, subject, body, "
            "status, source, job_url, created_at, updated_at"
        )
        .eq("vertical", "ma")
        .gte("created_at", since)
        .order("created_at", desc=True)
        .limit(500)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df


def update_email_status(queue_id: str, status: str) -> bool:
    """Update email queue status (for HITL approve/reject from dashboard)."""
    try:
        client = get_client()
        client.table("email_queue").update({"status": status}).eq(
            "id", queue_id
        ).execute()
        # Clear cache so changes reflect immediately
        get_email_queue.clear()
        return True
    except Exception:
        return False
