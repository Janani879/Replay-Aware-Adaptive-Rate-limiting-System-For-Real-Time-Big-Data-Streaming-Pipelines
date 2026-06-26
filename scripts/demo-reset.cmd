@echo off
setlocal
cd /d C:\Users\janan\radar-streaming-platform

echo [RADAR] Stopping old containers and clearing demo state...
docker compose down

if exist spark\checkpoints\clean_events_v3 rmdir /s /q spark\checkpoints\clean_events_v3
if exist spark\checkpoints\client_event_metrics_v3 rmdir /s /q spark\checkpoints\client_event_metrics_v3
if exist spark\data\clean_events rmdir /s /q spark\data\clean_events

echo [RADAR] Starting Docker services...
docker compose up -d

echo [RADAR] Waiting for services to become ready...
timeout /t 18 /nobreak >nul

echo [RADAR] Creating ClickHouse user/table for demo...
docker exec radar-clickhouse clickhouse-client --query "CREATE USER IF NOT EXISTS radar_user IDENTIFIED WITH plaintext_password BY 'radar_pass'"
docker exec radar-clickhouse clickhouse-client --query "ALTER USER radar_user IDENTIFIED WITH plaintext_password BY 'radar_pass'"
docker exec radar-clickhouse clickhouse-client --query "CREATE DATABASE IF NOT EXISTS radar"
docker exec radar-clickhouse clickhouse-client --query "GRANT ALL ON radar.* TO radar_user"
docker exec radar-clickhouse clickhouse-client --database radar --query "DROP TABLE IF EXISTS client_event_metrics"
docker exec radar-clickhouse clickhouse-client --database radar --query "CREATE TABLE client_event_metrics (batch_time DateTime, use_case_topic String, client_id String, raw_events UInt64, unique_events UInt64, duplicate_events UInt64, duplicate_ratio Float64) ENGINE = MergeTree ORDER BY (use_case_topic, client_id, batch_time)"

echo [RADAR] Flushing Redis demo state...
docker exec radar-redis redis-cli FLUSHALL

echo [RADAR] Installing Spark ClickHouse dependency if needed...
docker exec -u 0 radar-spark python3 -m pip install clickhouse-connect==0.7.19

echo.
echo [RADAR] Demo state is clean.
echo Next terminals:
echo   scripts\run-backend.cmd
echo   scripts\run-spark.cmd
echo   scripts\run-frontend.cmd
echo.
endlocal
