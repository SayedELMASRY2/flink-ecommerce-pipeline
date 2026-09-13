# ==============================================================================
# CartStream Analytics - Simple Flink Pipeline Runner (PowerShell)
# Task #26 (Samsung Innovation Campus)
# ==============================================================================

Write-Host "🚀 Launching CartStream Analytics Flink Pipeline..." -ForegroundColor Cyan

# 1. Start Flink Cluster
Write-Host "[1/3] Starting Flink Cluster containers..." -ForegroundColor Yellow
docker compose up -d jobmanager taskmanager

# 2. Run Flink SQL Pipeline Script
Write-Host "[2/3] Submitting Flink SQL Script..." -ForegroundColor Yellow
docker exec flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/flink_ecommerce_pipeline.sql

# 3. Display TaskManager output logs
Write-Host "[3/3] Displaying Tumbling Window Aggregation Output..." -ForegroundColor Green
docker logs --tail 100 flink-taskmanager-1 | Select-String "\+I" | Select-Object -Last 30
