@echo off
call "C:\Users\sebas\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" logging read "resource.type=cloud_run_revision AND resource.labels.service_name=hitl-gateway" --limit=30 --project=project-1b7f9136-5ccc-49d9-ba3 --format="table(timestamp,textPayload)"
pause
