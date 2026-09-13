"""
CartStream Analytics - PyFlink Streaming Application
Task #26: Samsung Innovation Campus
Real-Time E-Commerce Revenue Stream Processing
"""
import os
import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

def run_ecommerce_pipeline():
    # 1. Initialize Flink Streaming Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Resolve path to sample data
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, "data", "events_sample.csv").replace("\\", "/")

    print(f"[*] Reading streaming data from: {data_path}")

    # 2. Define Streaming Source Table with Watermark
    source_ddl = f"""
    CREATE TABLE ecommerce_events (
        event_time_str  VARCHAR,
        event_type      VARCHAR,
        product_id      BIGINT,
        category_id     BIGINT,
        category_code   VARCHAR,
        brand           VARCHAR,
        price           DECIMAL(10, 2),
        user_id         BIGINT,
        user_session    VARCHAR,
        event_time AS COALESCE(TO_TIMESTAMP(event_time_str, 'yyyy-MM-dd HH:mm:ss z'), TIMESTAMP '1970-01-01 00:00:00'),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{data_path}',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'
    )
    """
    table_env.execute_sql(source_ddl)
    print("[+] Source table 'ecommerce_events' registered successfully.")

    # 3. Define Output Sink Table (Print Connector)
    sink_ddl = """
    CREATE TABLE brand_window_sales (
        window_start      TIMESTAMP(3),
        window_end        TIMESTAMP(3),
        brand             VARCHAR,
        total_orders      BIGINT,
        gross_revenue     DECIMAL(10, 2),
        avg_order_value   DECIMAL(10, 2),
        unique_buyers     BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """
    table_env.execute_sql(sink_ddl)
    print("[+] Sink table 'brand_window_sales' registered successfully.")

    # 4. Windowing TVF Aggregation Query (5-minute Tumbling Window)
    query = """
    INSERT INTO brand_window_sales
    SELECT
        window_start,
        window_end,
        COALESCE(NULLIF(TRIM(brand), ''), 'UNKNOWN') AS brand,
        COUNT(1) AS total_orders,
        ROUND(SUM(price), 2) AS gross_revenue,
        ROUND(AVG(price), 2) AS avg_order_value,
        COUNT(DISTINCT user_id) AS unique_buyers
    FROM TABLE(
        TUMBLE(
            TABLE ecommerce_events,
            DESCRIPTOR(event_time),
            INTERVAL '5' MINUTE
        )
    )
    WHERE event_type = 'purchase'
    GROUP BY 
        window_start, 
        window_end, 
        COALESCE(NULLIF(TRIM(brand), ''), 'UNKNOWN')
    """
    print("[*] Submitting Flink SQL Windowing TVF streaming job...")
    table_result = table_env.execute_sql(query)
    print("[+] Job submitted! Awaiting streaming output (press Ctrl+C to stop)...")
    try:
        table_result.wait()
    except KeyboardInterrupt:
        print("\n[!] Execution stopped by user.")

if __name__ == "__main__":
    run_ecommerce_pipeline()
