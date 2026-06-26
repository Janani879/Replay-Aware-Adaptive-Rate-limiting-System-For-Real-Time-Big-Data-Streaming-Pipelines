@echo off
cd /d C:\Users\janan\radar-streaming-platform\backend-go\backend
call .venv\Scripts\activate
uvicorn main:app --reload --port 8000
