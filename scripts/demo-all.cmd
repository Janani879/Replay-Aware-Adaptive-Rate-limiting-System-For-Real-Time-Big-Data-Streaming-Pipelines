@echo off
cd /d C:\Users\janan\radar-streaming-platform
call scripts\demo-reset.cmd
start "RADAR Backend" cmd /k scripts\run-backend.cmd
start "RADAR Spark" cmd /k scripts\run-spark.cmd
start "RADAR Frontend" cmd /k scripts\run-frontend.cmd

echo.
echo Open:
echo   Frontend:    http://localhost:5173
echo   Kafka UI:    http://localhost:8085
echo   Prometheus:  http://localhost:9090
echo   Grafana:     http://localhost:3000
