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
echo "[3/3] Displaying Tumbling Window Aggregation Output..."
docker logs --tail 30 flink-taskmanager-1
