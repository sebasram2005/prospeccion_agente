-- ============================================================
-- Add source and job_url to email_queue
-- Enables source-aware HITL: proposals for Upwork, messages
-- for LinkedIn, emails for direct outreach.
-- ============================================================

ALTER TABLE email_queue ADD COLUMN IF NOT EXISTS source VARCHAR(50);
ALTER TABLE email_queue ADD COLUMN IF NOT EXISTS job_url TEXT;

-- Backfill existing rows from qualified_leads → raw_leads
UPDATE email_queue eq
SET source = rl.source,
    job_url = rl.url
FROM qualified_leads ql
JOIN raw_leads rl ON ql.raw_lead_id = rl.id
WHERE eq.qualified_lead_id = ql.id
  AND eq.source IS NULL;

CREATE INDEX IF NOT EXISTS idx_email_queue_source ON email_queue(source);
