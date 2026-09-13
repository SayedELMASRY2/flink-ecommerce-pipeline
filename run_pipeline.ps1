# ==============================================================================
# CartStream Analytics - End-to-End Automated Flink Pipeline Execution (PowerShell)
# Task #26 (Samsung Innovation Campus)
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "🚀 Starting CartStream Analytics Real-Time Streaming Pipeline Automation" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan

# 1. Ensure sample data exists
if (-not (Test-Path "data\events_sample.csv")) {
    Write-Host "[*] Sample data missing. Extracting 50,000 records..." -ForegroundColor Yellow
    if (Test-Path "2019-Oct.csv\2019-Oct.csv") {
        python extract_sample.py "2019-Oct.csv/2019-Oct.csv" 50000 "data/events_sample.csv"
    } else {
        Write-Error "[!] Error: Neither data/events_sample.csv nor 2019-Oct.csv source file was found!"
        exit 1
    }
} else {
    Write-Host "[+] Found sample dataset: data/events_sample.csv" -ForegroundColor Green
}

# Ensure header starts with # for Flink CSV comment parser
python -c "
with open('data/events_sample.csv', 'r', encoding='utf-8') as f:
    content = f.read()
if not content.startswith('#'):
    with open('data/events_sample.csv', 'w', encoding='utf-8') as f:
        f.write('#' + content)
"

# 2. Check Docker daemon
try {
    docker info 2>&1 | Out-Null
} catch {
    Write-Error "[!] Error: Docker daemon is not running. Please start Docker Desktop first."
    exit 1
}

# 3. Start Flink Cluster containers
Write-Host "[*] Ensuring Apache Flink Cluster (JobManager & TaskManager) is running..." -ForegroundColor Yellow
docker compose up -d jobmanager taskmanager

# 4. Wait for JobManager REST API readiness
Write-Host "[*] Waiting for JobManager REST API on port 8081..." -ForegroundColor Yellow
for ($i = 1; $i -le 15; $i++) {
    try {
        $res = Invoke-WebRequest -Uri "http://localhost:8081/overview" -UseBasicParsing -ErrorAction SilentlyContinue
        if ($res.StatusCode -eq 200) {
            Write-Host "[+] Flink JobManager is ready!" -ForegroundColor Green
            break
        }
    } catch {
        # continue waiting
    }
    Start-Sleep -Seconds 2
}

# 5. Execute Flink SQL Pipeline Script
Write-Host "[*] Submitting Flink SQL pipeline job..." -ForegroundColor Yellow
docker exec flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/flink_ecommerce_pipeline.sql

# 6. Wait briefly for streaming job processing
Write-Host "[*] Processing streaming window aggregations (awaiting 5s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# 7. Extract live TaskManager output logs
Write-Host "[*] Exporting execution logs to output/execution_verification.log..." -ForegroundColor Yellow
if (-not (Test-Path "output")) { New-Item -ItemType Directory -Path "output" | Out-Null }

python -c "
import subprocess
p = subprocess.run(['docker', 'logs', 'flink-taskmanager-1'], capture_output=True, text=True, errors='replace')
lines = p.stdout.splitlines()
sink_records = [l for l in lines if l.startswith('+I[')]

with open('output/execution_verification.log', 'w', encoding='utf-8') as f:
    f.write('=' * 80 + '\n')
    f.write('CartStream Analytics - Automated Flink SQL Streaming Verification Log\n')
    f.write('=' * 80 + '\n\n')
    f.write(f'Total Output Window Aggregations Emitted: {len(sink_records)}\n\n')
    f.write('--- LATEST WINDOW AGGREGATIONS ---\n')
    for r in sink_records[-25:]:
        f.write(r + '\n')

print(f'[+] Captured {len(sink_records)} window records into output/execution_verification.log')
"

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "🎉 Pipeline execution completed successfully!" -ForegroundColor Green
Write-Host "👉 Flink Web UI Dashboard : http://localhost:8081" -ForegroundColor Yellow
Write-Host "👉 Verification Output Log: output/execution_verification.log" -ForegroundColor Yellow
Write-Host "========================================================================" -ForegroundColor Cyan
