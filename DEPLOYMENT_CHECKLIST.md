# Deployment Checklist

## 1. Supabase
- [ ] Crear proyecto en supabase.com (free tier)
- [ ] Ejecutar `supabase/migrations/001_initial_schema.sql` en SQL Editor
- [ ] Copiar `Project URL` → `SUPABASE_URL`
- [ ] Copiar `anon` key → `SUPABASE_KEY`
- [ ] Copiar `service_role` key → `SUPABASE_SERVICE_KEY`
- [ ] Verificar que las 4 tablas existen: `raw_leads`, `qualified_leads`, `email_queue`, `hitl_audit_log`

## 2. Gemini API
- [ ] Crear API key en aistudio.google.com/app/apikey
- [ ] Verificar acceso al modelo `gemini-2.5-flash-lite-preview-06-17`
- [ ] Copiar key → `GEMINI_API_KEY`

## 3. Telegram Bot
- [ ] Crear bot con @BotFather → `/newbot`
- [ ] Copiar token → `TELEGRAM_BOT_TOKEN`
- [ ] Obtener chat_id con @userinfobot → `TELEGRAM_CHAT_ID`
- [ ] Verificar que el bot puede enviarte mensajes

## 4. Brevo SMTP
- [ ] Crear cuenta en brevo.com
- [ ] Generar SMTP key en SMTP & API → SMTP
- [ ] Copiar SMTP key → `BREVO_SMTP_PASSWORD`
- [ ] Configurar sender email en Settings → Senders & IPs
- [ ] Verificar sender domain (SPF + DKIM)

## 5. Google Places API (solo Vertical 2)
- [ ] Habilitar Places API en Google Cloud Console
- [ ] Crear API key → `GOOGLE_PLACES_API_KEY`
- [ ] Verificar que el billing tiene los $200/mes de crédito gratis

## 6. Google Cloud Run
- [ ] Instalar y autenticar `gcloud` CLI
- [ ] Desplegar HITL Gateway: `gcloud run deploy hitl-gateway --source . --region us-central1 --allow-unauthenticated`
- [ ] Configurar env vars en Cloud Run
- [ ] Copiar URL del servicio → `HITL_GATEWAY_URL`
- [ ] Verificar health check: `curl https://hitl-gateway-xxx.a.run.app/health`
- [ ] Verificar que el webhook de Telegram se configuró correctamente

## 7. GitHub Repository
- [ ] Crear repo (privado recomendado)
- [ ] Push del código
- [ ] Configurar Secrets en Settings → Secrets → Actions:
  - [ ] `SUPABASE_URL`
  - [ ] `SUPABASE_SERVICE_KEY`
  - [ ] `GEMINI_API_KEY`
  - [ ] `TELEGRAM_BOT_TOKEN`
  - [ ] `TELEGRAM_CHAT_ID`
  - [ ] `HITL_GATEWAY_URL`
  - [ ] `BREVO_SMTP_PASSWORD`
  - [ ] `GOOGLE_PLACES_API_KEY`
- [ ] Configurar Variables en Settings → Variables → Actions:
  - [ ] `TARGET_CITIES`
- [ ] Ejecutar workflow manual para cada vertical y verificar logs

## 8. DNS & Email Deliverability
- [ ] Configurar SPF: `v=spf1 include:sendinblue.com ~all`
- [ ] Configurar DKIM (generado automáticamente por Brevo)
- [ ] Configurar DMARC: `v=DMARC1; p=quarantine; rua=mailto:dmarc@tudominio.com`
- [ ] Enviar email de prueba y verificar que llega a inbox (no spam)

## 9. Warm-up Plan
- [ ] Semana 1: Aprobar máximo 20 emails/día
- [ ] Semana 2: Subir a 50 emails/día
- [ ] Semana 3+: Hasta 300 emails/día (límite free tier Brevo)

## 10. Validación End-to-End
- [ ] Ejecutar scraper manual (workflow_dispatch) → verificar raw_leads en Supabase
- [ ] Verificar que Gemini califica correctamente → qualified_leads
- [ ] Verificar que el email draft aparece en email_queue
- [ ] Verificar que llega la notificación a Telegram con botones
- [ ] Aprobar un lead → verificar que el email se envía via Brevo
- [ ] Rechazar un lead → verificar que el status cambia a 'rejected'
- [ ] Editar un lead → verificar que el LLM re-redacta y re-envía notificación
- [ ] Verificar hitl_audit_log tiene los registros correctos
