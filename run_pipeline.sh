#!/usr/bin/env bash
# ==============================================================================
# CartStream Analytics - Simple Flink Pipeline Runner (Bash)
# Task #26 (Samsung Innovation Campus)
# ==============================================================================

echo "🚀 Launching CartStream Analytics Flink Pipeline..."

# 1. Start Flink Cluster
echo "[1/3] Starting Flink Cluster containers..."
docker compose up -d jobmanager taskmanager

# 2. Run Flink SQL Pipeline Script
echo "[2/3] Submitting Flink SQL Script..."
docker exec flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/flink_ecommerce_pipeline.sql

# 3. Display TaskManager output logs
echo -e "\033[0;32m[3/3] Displaying Tumbling Window Aggregation Output...\033[0m"
docker logs --tail 100 flink-taskmanager-1 | grep "\+I" | tail -n 30
