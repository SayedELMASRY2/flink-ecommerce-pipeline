-- ====================================================================
-- CartStream Analytics: Real-Time E-Commerce Revenue Stream Processing
-- Apache Flink SQL Pipeline
-- Task #26 (Samsung Innovation Campus)
-- ====================================================================

-- 1. Streaming Source Table DDL
-- Reads continuous e-commerce activity records formatted in CSV
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
    -- Native event timestamp converted from original string (safeguarded against null parse errors)
    event_time AS COALESCE(TO_TIMESTAMP(event_time_str, 'yyyy-MM-dd HH:mm:ss z'), TIMESTAMP '1970-01-01 00:00:00'),
    -- Watermark declaration: 5-second bounded out-of-orderness tolerance
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/data/events_sample.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.allow-comments' = 'true'
);

-- 2. Streaming Sink Table DDL
-- Directs the streaming output to an append-only sink using Flink's print connector
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
);

-- 3. Windowing TVF & Real-Time Revenue Aggregation Query
-- 5-minute tumbling window on completed purchase transactions grouped by brand
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
    COALESCE(NULLIF(TRIM(brand), ''), 'UNKNOWN');
