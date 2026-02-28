-- ============================================================
-- Prospecting Agent — Initial Schema
-- Target: Supabase (PostgreSQL 15+)
-- ============================================================

-- Habilitar extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ──────────────────────────────────────────────────────────────
-- Leads crudos extraídos por los scrapers
-- ──────────────────────────────────────────────────────────────
CREATE TABLE raw_leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source VARCHAR(50) NOT NULL,           -- 'upwork', 'linkedin', 'gmaps', 'instagram'
    vertical VARCHAR(20) NOT NULL,         -- 'tech', 'cerrieta'
    url TEXT UNIQUE,                       -- URL del perfil/listing para deduplicar
    raw_data JSONB NOT NULL,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE
);

-- ──────────────────────────────────────────────────────────────
-- Leads calificados por el LLM (Gemini Flash-Lite)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE qualified_leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_lead_id UUID REFERENCES raw_leads(id) ON DELETE CASCADE,
    vertical VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    company_name VARCHAR(200),
    email VARCHAR(200),
    qualification_result JSONB NOT NULL,   -- JSON completo del LLM
    pain_point TEXT,                       -- Para V1 (tech)
    aesthetic_match TEXT,                  -- Para V2 (cerrieta)
    qualified_at TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- Cola de emails pendientes de aprobación HITL
-- ──────────────────────────────────────────────────────────────
CREATE TABLE email_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    qualified_lead_id UUID REFERENCES qualified_leads(id) ON DELETE CASCADE,
    vertical VARCHAR(20) NOT NULL,
    to_email VARCHAR(200) NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',  -- pending | approved | rejected | sent | edited
    telegram_message_id INTEGER,           -- Para trackear el mensaje de Telegram
    edit_instructions TEXT,                -- Si el operador pidió editar
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- Log de auditoría de acciones HITL
-- ──────────────────────────────────────────────────────────────
CREATE TABLE hitl_audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email_queue_id UUID REFERENCES email_queue(id) ON DELETE CASCADE,
    action VARCHAR(20) NOT NULL,           -- 'approve', 'reject', 'edit', 'sent'
    operator_note TEXT,
    acted_at TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- Índices para performance
-- ──────────────────────────────────────────────────────────────
CREATE INDEX idx_raw_leads_url ON raw_leads(url);
CREATE INDEX idx_raw_leads_processed ON raw_leads(processed);
CREATE INDEX idx_raw_leads_source_vertical ON raw_leads(source, vertical);
CREATE INDEX idx_qualified_leads_vertical ON qualified_leads(vertical);
CREATE INDEX idx_qualified_leads_email ON qualified_leads(email);
CREATE INDEX idx_email_queue_status ON email_queue(status);
CREATE INDEX idx_email_queue_telegram_msg ON email_queue(telegram_message_id);
CREATE INDEX idx_hitl_audit_email_queue ON hitl_audit_log(email_queue_id);

-- ──────────────────────────────────────────────────────────────
-- Trigger para actualizar updated_at en email_queue
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_email_queue_updated_at
    BEFORE UPDATE ON email_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
