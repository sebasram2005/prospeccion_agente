# Prospecting Agent — $0 Stack

Sistema autónomo de prospección outbound B2B con costo operativo **$0** en fase inicial.

## Arquitectura

```
GitHub Actions (cron) → Scrapers → Gemini LLM → Supabase → Cloud Run → Telegram HITL → Brevo SMTP
```

### Stack

| Componente | Servicio | Tier |
|---|---|---|
| Base de datos | Supabase | Free (500MB) |
| Scrapers (cron) | GitHub Actions | Free (2,000 min/mes) |
| Gateway Telegram | Google Cloud Run | Free (2M req/mes) |
| LLM | Gemini 2.5 Flash-Lite | Free (1,000 req/día) |
| Email SMTP | Brevo | Free (300 emails/día) |

### Verticales

- **V1 — Tech Services**: Upwork + LinkedIn → Data Analyst / Python Developer outreach
- **V2 — Cerrieta**: Google Maps + Instagram → Luxury pet furniture wholesale outreach

## Setup

### Prerequisitos

- Python 3.12+
- Cuenta de GitHub (para Actions)
- `gcloud` CLI instalado (para Cloud Run)

### Paso 1: Supabase

1. Crear proyecto en [supabase.com](https://supabase.com) (free tier)
2. Ir a SQL Editor y ejecutar el contenido de `supabase/migrations/001_initial_schema.sql`
3. Ir a Settings → API → copiar `Project URL` y `service_role` key

### Paso 2: Gemini API Key

1. Ir a [aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)
2. Create API Key → crear en nuevo proyecto
3. Free tier: 1,000 req/día con Gemini 2.5 Flash-Lite

### Paso 3: Telegram Bot

1. Hablar con [@BotFather](https://t.me/BotFather) en Telegram → `/newbot`
2. Guardar el token
3. Hablar con [@userinfobot](https://t.me/userinfobot) para obtener tu `TELEGRAM_CHAT_ID`

### Paso 4: Brevo SMTP

1. Crear cuenta en [brevo.com](https://brevo.com) (free: 300 emails/día)
2. SMTP & API → SMTP → Generate new SMTP key
3. Host: `smtp-relay.brevo.com`, Port: `587`

### Paso 5: Google Cloud Run (HITL Gateway)

```bash
# Prerequisito: gcloud CLI instalado y autenticado
cd services/hitl_gateway

gcloud run deploy hitl-gateway \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars="SUPABASE_URL=...,SUPABASE_KEY=...,TELEGRAM_BOT_TOKEN=...,TELEGRAM_CHAT_ID=...,GEMINI_API_KEY=...,BREVO_SMTP_HOST=smtp-relay.brevo.com,BREVO_SMTP_PORT=587,BREVO_SMTP_USER=...,BREVO_SMTP_PASSWORD=...,SENDER_V1_NAME=Sebastian Ramirez,SENDER_V1_EMAIL=...,SENDER_V2_NAME=Sebastian | Cerrieta,SENDER_V2_EMAIL=..."

# Copiar la URL del output → será tu HITL_GATEWAY_URL
```

### Paso 6: GitHub Actions Secrets

En tu repo GitHub → Settings → Secrets and variables → Actions:

**Secrets:**
- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `GEMINI_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `HITL_GATEWAY_URL`
- `BREVO_SMTP_PASSWORD`
- `GOOGLE_PLACES_API_KEY`

**Variables (no secrets):**
- `TARGET_CITIES` = `New York,Miami,Los Angeles,London,Paris,Barcelona`

### Paso 7: DNS y warm-up del dominio

Antes de enviar emails, configurar en tu DNS:

```
SPF:   v=spf1 include:sendinblue.com ~all
DKIM:  (Brevo lo genera automáticamente en Settings → Senders)
DMARC: v=DMARC1; p=quarantine; rua=mailto:dmarc@tudominio.com
```

**Warm-up schedule:**
- Semana 1: máximo 20 emails/día
- Semana 2: máximo 50/día
- Semana 3+: hasta 300/día (límite free tier)

## Desarrollo local

```bash
# Copiar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# Instalar dependencias de un vertical
pip install -r services/vertical1_tech/requirements.txt

# Ejecutar un scraper manualmente
cd services/vertical1_tech
python -m src.main --source upwork

# Ejecutar con Instagram en mock mode
INSTAGRAM_MOCK=true python -m services.vertical2_cerrieta.src.main --source instagram
```

## Flujo end-to-end

```
GitHub Actions (cron) → Scraper → Dedup check → Supabase (raw_leads)
    → Gemini qualification → Supabase (qualified_leads)
    → Email draft (Jinja2) → Supabase (email_queue)
    → HTTP POST to Cloud Run → Telegram notification
    → Sebastian toca ✅ → Cloud Run sends via Brevo SMTP
    → Supabase (hitl_audit_log)
```

## Estructura del proyecto

```
prospecting-agent/
├── .github/workflows/          # Cron jobs (GitHub Actions)
├── services/
│   ├── vertical1_tech/         # Upwork + LinkedIn scraper
│   ├── vertical2_cerrieta/     # Google Maps + Instagram scraper
│   └── hitl_gateway/           # Cloud Run: Telegram bot + email sender
├── shared/
│   ├── prompts/                # System prompts para Gemini
│   └── utils/                  # Rate limiters, dedup checker
├── supabase/migrations/        # SQL schema
├── .env.example
└── README.md
```
