-- Analytical Views for Dashboard
-- Run this in Snowsight after your streaming pipeline is running

USE DATABASE KAFKA_DB;
USE SCHEMA STREAMING;
USE WAREHOUSE COMPUTE_WH;

-- =================================================
-- EVENT FUNNEL VIEW - Overall conversion funnel
-- =================================================
CREATE OR REPLACE VIEW event_funnel AS
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT_IF(event_type = 'PAGE_VIEW') AS page_views,
    COUNT_IF(event_type = 'ADD_TO_CART') AS add_to_cart,
    COUNT_IF(event_type = 'PURCHASE') AS purchases,
    
    -- Conversion rates
    ROUND(
        CASE 
            WHEN COUNT_IF(event_type = 'PAGE_VIEW') > 0 
            THEN (COUNT_IF(event_type = 'ADD_TO_CART') * 100.0) / COUNT_IF(event_type = 'PAGE_VIEW')
            ELSE 0 
        END, 2
    ) AS page_to_cart_rate,
    
    ROUND(
        CASE 
            WHEN COUNT_IF(event_type = 'ADD_TO_CART') > 0 
            THEN (COUNT_IF(event_type = 'PURCHASE') * 100.0) / COUNT_IF(event_type = 'ADD_TO_CART')
            ELSE 0 
        END, 2
    ) AS cart_to_purchase_rate,
    
    ROUND(
        CASE 
            WHEN COUNT_IF(event_type = 'PAGE_VIEW') > 0 
            THEN (COUNT_IF(event_type = 'PURCHASE') * 100.0) / COUNT_IF(event_type = 'PAGE_VIEW')
            ELSE 0 
        END, 2
    ) AS overall_conversion_rate
    
FROM kafka_events_silver
GROUP BY DATE(event_timestamp)
ORDER BY event_date DESC;

-- =================================================
-- DAILY CUSTOMER REVENUE VIEW - Customer-level metrics
-- =================================================
CREATE OR REPLACE VIEW daily_customer_revenue AS
SELECT
    customer_id,
    DATE(event_timestamp) AS event_date,
    SUM(CASE WHEN event_type = 'PURCHASE' THEN amount ELSE 0 END) AS total_revenue,
    COUNT_IF(event_type = 'PURCHASE') AS purchase_count,
    COUNT_IF(event_type = 'PAGE_VIEW') AS page_views,
    COUNT_IF(event_type = 'ADD_TO_CART') AS add_to_cart_count,
    
    -- Average order value
    ROUND(
        CASE 
            WHEN COUNT_IF(event_type = 'PURCHASE') > 0 
            THEN SUM(CASE WHEN event_type = 'PURCHASE' THEN amount ELSE 0 END) / COUNT_IF(event_type = 'PURCHASE')
            ELSE 0 
        END, 2
    ) AS avg_order_value
    
FROM kafka_events_silver
GROUP BY customer_id, DATE(event_timestamp)
ORDER BY event_date DESC, total_revenue DESC;

-- =================================================
-- REAL-TIME GOLD METRICS VIEW - From streaming pipeline
-- =================================================
CREATE OR REPLACE VIEW real_time_customer_metrics AS
SELECT
    customer_id,
    event_date,
    page_views,
    add_to_cart_count,
    purchase_count,
    COALESCE(total_revenue, 0) AS total_revenue,
    
    -- Calculate average order value
    ROUND(
        CASE 
            WHEN purchase_count > 0 
            THEN total_revenue / purchase_count
            ELSE 0 
        END, 2
    ) AS avg_order_value,
    
    -- Customer engagement score (simple formula)
    ROUND(
        (page_views * 1) + (add_to_cart_count * 3) + (purchase_count * 10), 2
    ) AS engagement_score
    
FROM customer_daily_metrics
ORDER BY event_date DESC, total_revenue DESC;

-- =================================================
-- TOP CUSTOMERS VIEW - High-value customer analysis
-- =================================================
CREATE OR REPLACE VIEW top_customers AS
SELECT
    customer_id,
    SUM(total_revenue) AS lifetime_revenue,
    SUM(purchase_count) AS total_purchases,
    SUM(page_views) AS total_page_views,
    SUM(add_to_cart_count) AS total_add_to_cart,
    COUNT(DISTINCT event_date) AS active_days,
    
    -- Metrics
    ROUND(SUM(total_revenue) / NULLIF(SUM(purchase_count), 0), 2) AS avg_order_value,
    ROUND(SUM(total_revenue) / NULLIF(COUNT(DISTINCT event_date), 0), 2) AS revenue_per_day,
    ROUND((SUM(purchase_count) * 100.0) / NULLIF(SUM(add_to_cart_count), 0), 2) AS conversion_rate,
    
    -- Customer tier
    CASE 
        WHEN SUM(total_revenue) >= 1000 THEN 'VIP'
        WHEN SUM(total_revenue) >= 500 THEN 'Premium'
        WHEN SUM(total_revenue) >= 100 THEN 'Regular'
        ELSE 'New'
    END AS customer_tier
    
FROM customer_daily_metrics
GROUP BY customer_id
ORDER BY lifetime_revenue DESC;

-- =================================================
-- HOURLY TRENDS VIEW - For real-time monitoring
-- =================================================
CREATE OR REPLACE VIEW hourly_trends AS
SELECT
    DATE_TRUNC('HOUR', event_timestamp) AS event_hour,
    COUNT_IF(event_type = 'PAGE_VIEW') AS page_views,
    COUNT_IF(event_type = 'ADD_TO_CART') AS add_to_cart,
    COUNT_IF(event_type = 'PURCHASE') AS purchases,
    SUM(CASE WHEN event_type = 'PURCHASE' THEN amount ELSE 0 END) AS revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM kafka_events_silver
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 HOURS'
GROUP BY DATE_TRUNC('HOUR', event_timestamp)
ORDER BY event_hour DESC;

-- =================================================
-- VERIFY VIEWS
-- =================================================
SHOW VIEWS;

-- Sample the views
SELECT 'EVENT_FUNNEL' AS view_name, COUNT(*) AS row_count FROM event_funnel
UNION ALL
SELECT 'DAILY_CUSTOMER_REVENUE', COUNT(*) FROM daily_customer_revenue
UNION ALL
SELECT 'REAL_TIME_CUSTOMER_METRICS', COUNT(*) FROM real_time_customer_metrics
UNION ALL
SELECT 'TOP_CUSTOMERS', COUNT(*) FROM top_customers
UNION ALL
SELECT 'HOURLY_TRENDS', COUNT(*) FROM hourly_trends;

-- Preview data
SELECT * FROM event_funnel LIMIT 5;
SELECT * FROM daily_customer_revenue LIMIT 5;
SELECT * FROM real_time_customer_metrics LIMIT 5;