-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UC3: Silver Layer — Clean, Deduplicate, Enrich
-- MAGIC
-- MAGIC This notebook:
-- MAGIC 1. Deduplicates orders
-- MAGIC 2. Handles nulls (zone inferred from restaurant, partner flagged)
-- MAGIC 3. Enforces types & adds derived columns
-- MAGIC 4. Creates clean Silver tables for all entities
-- MAGIC
-- MAGIC **Run on:** Serverless SQL Warehouse

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA zipdrop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Orders — Deduplicate + Clean + Enrich

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_orders AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp) AS _row_num
    FROM bronze_orders
),
cleaned AS (
    SELECT
        order_id,
        customer_id,
        restaurant_id,
        partner_id,
        -- Infer missing zone from restaurant master
        COALESCE(
            NULLIF(d.zone, ''),
            r.zone
        ) AS zone,
        CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
        CAST(delivery_timestamp AS TIMESTAMP) AS delivery_timestamp,
        CAST(order_value_inr AS DOUBLE) AS order_value_inr,
        CAST(discount_inr AS DOUBLE) AS discount_inr,
        payment_mode,
        status,
        cancel_reason,
        CAST(estimated_eta_mins AS INT) AS estimated_eta_mins,
        CAST(actual_delivery_mins AS INT) AS actual_delivery_mins,
        CAST(surge_multiplier AS DOUBLE) AS surge_multiplier,
        CAST(rain_at_order_time AS INT) AS rain_at_order_time,
        CAST(event_active AS INT) AS event_active
    FROM deduped d
    LEFT JOIN bronze_restaurants r ON d.restaurant_id = r.restaurant_id AND (d.zone IS NULL OR d.zone = '')
    WHERE d._row_num = 1
)
SELECT
    *,
    -- Derived columns
    CASE WHEN partner_id IS NULL OR partner_id = '' THEN 'unassigned' ELSE 'assigned' END AS partner_status,
    DATE(order_timestamp) AS order_date,
    HOUR(order_timestamp) AS order_hour,
    DAYOFWEEK(order_timestamp) AS day_of_week,
    CASE WHEN DAYOFWEEK(order_timestamp) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN actual_delivery_mins IS NOT NULL
         THEN actual_delivery_mins - estimated_eta_mins
         ELSE NULL END AS eta_overshoot_mins,
    CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END AS is_cancelled,
    COALESCE(order_value_inr, 0) - COALESCE(discount_inr, 0) AS net_order_value
FROM cleaned;

-- COMMAND ----------

SELECT
    count(*) AS total_orders,
    count(DISTINCT order_id) AS unique_orders,
    SUM(CASE WHEN zone IS NULL THEN 1 ELSE 0 END) AS null_zones_remaining,
    SUM(CASE WHEN partner_status = 'unassigned' THEN 1 ELSE 0 END) AS unassigned_orders,
    ROUND(AVG(eta_overshoot_mins), 1) AS avg_eta_overshoot,
    ROUND(SUM(is_cancelled) * 100.0 / count(*), 1) AS cancel_rate_pct
FROM silver_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Restaurants — Silver

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_restaurants AS
SELECT
    restaurant_id,
    restaurant_name,
    zone,
    cuisine_type,
    CAST(avg_prep_time_mins AS INT) AS avg_prep_time_mins,
    CAST(rating AS DOUBLE) AS rating,
    CAST(is_premium AS INT) AS is_premium,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lng AS DOUBLE) AS lng
FROM bronze_restaurants;

SELECT 'silver_restaurants' AS tbl, count(*) AS rows FROM silver_restaurants;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Delivery Partners — Silver

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_delivery_partners AS
SELECT
    partner_id,
    partner_name,
    zone,
    vehicle_type,
    CAST(avg_rating AS DOUBLE) AS avg_rating,
    CAST(total_deliveries AS INT) AS total_deliveries,
    status,
    CAST(experience_months AS INT) AS experience_months,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lng AS DOUBLE) AS lng,
    CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active
FROM bronze_delivery_partners;

SELECT 'silver_delivery_partners' AS tbl, count(*) AS rows FROM silver_delivery_partners;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Weather — Silver

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_weather AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    zone,
    DATE(CAST(timestamp AS TIMESTAMP)) AS weather_date,
    HOUR(CAST(timestamp AS TIMESTAMP)) AS weather_hour,
    CAST(temperature_c AS DOUBLE) AS temperature_c,
    CAST(humidity_pct AS INT) AS humidity_pct,
    condition,
    CAST(wind_speed_kmh AS DOUBLE) AS wind_speed_kmh,
    CAST(rain_mm AS DOUBLE) AS rain_mm,
    CAST(visibility_km AS DOUBLE) AS visibility_km,
    CASE WHEN condition IN ('Light Rain', 'Heavy Rain', 'Thunderstorm', 'Drizzle') THEN 1 ELSE 0 END AS is_rainy
FROM bronze_weather_hourly;

SELECT 'silver_weather' AS tbl, count(*) AS rows FROM silver_weather;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Local Events — Silver (Explode multi-zone events)

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_local_events AS
SELECT
    event_id,
    event_name,
    event_type,
    CAST(date AS DATE) AS date,
    start_time,
    end_time,
    CAST(expected_crowd AS INT) AS expected_crowd,
    TRIM(zone_exploded) AS zone,
    CAST(demand_multiplier AS DOUBLE) AS demand_multiplier
FROM bronze_local_events
LATERAL VIEW EXPLODE(SPLIT(zones_impacted, ',')) AS zone_exploded;

SELECT 'silver_local_events' AS tbl, count(*) AS rows FROM silver_local_events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer Summary

-- COMMAND ----------

SELECT 'silver_orders' AS tbl, count(*) AS rows FROM silver_orders
UNION ALL SELECT 'silver_restaurants', count(*) FROM silver_restaurants
UNION ALL SELECT 'silver_delivery_partners', count(*) FROM silver_delivery_partners
UNION ALL SELECT 'silver_weather', count(*) FROM silver_weather
UNION ALL SELECT 'silver_local_events', count(*) FROM silver_local_events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver layer complete! Run notebook `03_gold_aggregations` next.
