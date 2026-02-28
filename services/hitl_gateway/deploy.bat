@echo off
cd /d "C:\Agente_prospección\services\hitl_gateway"
call "C:\Users\sebas\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" run deploy hitl-gateway ^
  --source . ^
  --region us-central1 ^
  --allow-unauthenticated ^
  --quiet ^
  --set-env-vars="SUPABASE_URL=https://ewduhtrnnzlsakyedfto.supabase.co,SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV3ZHVodHJubnpsc2FreWVkZnRvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjIzODQ4MCwiZXhwIjoyMDg3ODE0NDgwfQ.r-BPRifMfpX-3JLPvQlvsccwHEz5XX_4ZkLYE1OixOk,TELEGRAM_BOT_TOKEN=8519492915:AAHarmnNyUU4w16t07CVNX-CtKJIlQxnsl8,TELEGRAM_CHAT_ID=8738207062,GEMINI_API_KEY=AIzaSyC5sGn0gmJUIOB6uaZZWh8sMCKVowtJvw0,HITL_GATEWAY_URL=https://hitl-gateway-649988689857.us-central1.run.app,BREVO_SMTP_HOST=smtp-relay.brevo.com,BREVO_SMTP_PORT=587,BREVO_SMTP_USER=sebas200577@gmail.com,BREVO_SMTP_PASSWORD=xsmtpsib-d9be663874f8c316ececec64bcbcaca4a4e02b79f24622fef11c93486c6fc1cb-9adiABcEp0cSSXj6,SENDER_V1_NAME=Sebastian Ramirez,SENDER_V1_EMAIL=sebastian@sebastianramirezanalytics.com,SENDER_V2_NAME=Sebastian,SENDER_V2_EMAIL=hello@cerrieta.com,LOG_LEVEL=INFO"
echo.
echo Deploy finished with exit code %ERRORLEVEL%
pause
