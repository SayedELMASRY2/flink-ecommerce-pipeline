#!/usr/bin/env bash
# ==============================================================================
# CartStream Analytics - End-to-End Automated Flink Pipeline Execution
# Task #26 (Samsung Innovation Campus)
# ==============================================================================

set -e

echo "========================================================================"
echo "🚀 Starting CartStream Analytics Real-Time Streaming Pipeline Automation"
echo "========================================================================"

# 1. Ensure sample data exists
if [ ! -f "data/events_sample.csv" ]; then
    echo "[!] Error: Dataset data/events_sample.csv not found!"
    exit 1
else
    echo "[+] Found sample dataset: data/events_sample.csv"
fi

# Ensure header starts with # for Flink CSV comment parser
python -c "
with open('data/events_sample.csv', 'r', encoding='utf-8') as f:
    content = f.read()
if not content.startswith('#'):
    with open('data/events_sample.csv', 'w', encoding='utf-8') as f:
        f.write('#' + content)
"

# 2. Check Docker daemon
if ! docker info > /dev/null 2>&1; then
    echo "[!] Error: Docker daemon is not running. Please start Docker Desktop first."
    exit 1
fi

# 3. Start Flink Cluster containers
echo "[*] Ensuring Apache Flink Cluster (JobManager & TaskManager) is running..."
docker compose up -d jobmanager taskmanager

# 4. Wait for JobManager REST API readiness
echo "[*] Waiting for JobManager REST API on port 8081..."
for i in {1..15}; do
    if curl -s http://localhost:8081/overview > /dev/null 2>&1; then
        echo "[+] Flink JobManager is ready!"
        break
    fi
    sleep 2
done

# 5. Execute Flink SQL Pipeline Script
echo "[*] Submitting Flink SQL pipeline job..."
docker exec flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/flink_ecommerce_pipeline.sql

# 6. Wait briefly for streaming job processing
echo "[*] Processing streaming window aggregations (awaiting 5s)..."
sleep 5

# 7. Extract live TaskManager output logs
echo "[*] Exporting execution logs to output/execution_verification.log..."
mkdir -p output
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

echo "========================================================================"
echo "🎉 Pipeline execution completed successfully!"
echo "👉 Flink Web UI Dashboard : http://localhost:8081"
echo "👉 Verification Output Log: output/execution_verification.log"
echo "========================================================================"
