-- ============================================================
-- Migration 003: Keyword performance tracking
-- Adds search_keyword to raw_leads + keyword_performance table
-- ============================================================

-- Add search_keyword column to raw_leads
ALTER TABLE raw_leads ADD COLUMN IF NOT EXISTS search_keyword VARCHAR(200);
CREATE INDEX IF NOT EXISTS idx_raw_leads_keyword ON raw_leads(search_keyword);

-- Accumulated performance metrics per keyword+source pair
CREATE TABLE keyword_performance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    keyword VARCHAR(200) NOT NULL,
    source VARCHAR(50) NOT NULL,
    leads_found INTEGER DEFAULT 0,
    leads_qualified INTEGER DEFAULT 0,
    leads_approved INTEGER DEFAULT 0,
    leads_rejected INTEGER DEFAULT 0,
    avg_fit_score NUMERIC(4,2) DEFAULT 0,
    score NUMERIC(6,4) DEFAULT 0.5,
    last_run_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(keyword, source)
);
CREATE INDEX IF NOT EXISTS idx_kp_score ON keyword_performance(score DESC);

-- Auto-update updated_at on keyword_performance
CREATE TRIGGER trg_keyword_performance_updated_at
    BEFORE UPDATE ON keyword_performance
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
