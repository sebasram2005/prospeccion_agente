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
