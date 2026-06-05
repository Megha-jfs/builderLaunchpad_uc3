-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UC3: Gold Layer — Analytics-Ready Aggregations
-- MAGIC
-- MAGIC Creates 4 Gold tables:
-- MAGIC 1. `gold_zone_demand_hourly` — Orders/hour per zone enriched with weather & events
-- MAGIC 2. `gold_partner_availability` — Active partners per zone
-- MAGIC 3. `gold_demand_supply_ratio` — Demand vs supply gap with zone health status
-- MAGIC 4. `gold_customer_360` — Customer-level profile
-- MAGIC
-- MAGIC **Run on:** Serverless SQL Warehouse

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA zipdrop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Zone Demand Hourly (Core Table — enriched with weather + events)

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_zone_demand_hourly AS
SELECT
    o.order_date,
    o.order_hour,
    o.zone,
    COUNT(o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(AVG(o.order_value_inr), 2) AS avg_order_value,
    ROUND(SUM(o.net_order_value), 2) AS total_revenue,
    ROUND(AVG(o.actual_delivery_mins), 1) AS avg_delivery_mins,
    ROUND(AVG(o.estimated_eta_mins), 1) AS avg_estimated_eta,
    ROUND(AVG(o.eta_overshoot_mins), 1) AS avg_eta_overshoot,
    SUM(o.is_cancelled) AS total_cancellations,
    ROUND(SUM(o.is_cancelled) * 100.0 / COUNT(o.order_id), 1) AS cancellation_rate_pct,
    ROUND(AVG(o.surge_multiplier), 2) AS avg_surge,
    MAX(o.rain_at_order_time) AS rain_flag,
    MAX(o.event_active) AS event_flag,
    SUM(CASE WHEN o.partner_status = 'unassigned' THEN 1 ELSE 0 END) AS unassigned_orders,
    COUNT(DISTINCT o.restaurant_id) AS active_restaurants,
    -- Weather enrichment
    MAX(w.temperature_c) AS temperature_c,
    MAX(w.humidity_pct) AS humidity_pct,
    MAX(w.condition) AS weather_condition,
    MAX(w.rain_mm) AS rain_mm,
    MAX(w.is_rainy) AS is_rainy,
    -- Event enrichment
    COALESCE(MAX(e.event_name), 'None') AS event_name,
    COALESCE(MAX(e.event_type), 'None') AS event_type,
    COALESCE(MAX(e.demand_multiplier), 1.0) AS demand_multiplier
FROM silver_orders o
LEFT JOIN silver_weather w
    ON o.zone = w.zone AND o.order_date = w.weather_date AND o.order_hour = w.weather_hour
LEFT JOIN silver_local_events e
    ON o.zone = e.zone AND o.order_date = e.date
GROUP BY o.order_date, o.order_hour, o.zone;

SELECT 'gold_zone_demand_hourly' AS tbl, count(*) AS rows FROM gold_zone_demand_hourly;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Partner Availability by Zone

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_partner_availability AS
SELECT
    zone,
    COUNT(partner_id) AS total_partners,
    SUM(is_active) AS active_partners,
    SUM(CASE WHEN status = 'on_break' THEN 1 ELSE 0 END) AS on_break_partners,
    SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) AS inactive_partners,
    ROUND(AVG(avg_rating), 2) AS avg_partner_rating,
    ROUND(AVG(total_deliveries), 0) AS avg_lifetime_deliveries,
    ROUND(AVG(experience_months), 1) AS avg_experience_months,
    SUM(CASE WHEN vehicle_type = 'ev_scooter' THEN 1 ELSE 0 END) AS ev_partners,
    ROUND(SUM(is_active) * 100.0 / COUNT(partner_id), 1) AS active_pct
FROM silver_delivery_partners
GROUP BY zone;

SELECT 'gold_partner_availability' AS tbl, count(*) AS rows FROM gold_partner_availability;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Demand-Supply Ratio with Zone Health Status

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_demand_supply_ratio AS
SELECT
    d.order_date,
    d.order_hour,
    d.zone,
    d.total_orders,
    d.total_cancellations,
    d.cancellation_rate_pct,
    d.avg_surge,
    d.avg_eta_overshoot,
    d.rain_flag,
    d.event_flag,
    d.event_name,
    d.is_rainy,
    p.active_partners,
    ROUND(d.total_orders / NULLIF(p.active_partners, 0), 2) AS demand_supply_ratio,
    CASE
        WHEN d.total_orders / NULLIF(p.active_partners, 0) > 3 THEN 'CRITICAL'
        WHEN d.total_orders / NULLIF(p.active_partners, 0) > 2 THEN 'STRESSED'
        WHEN d.total_orders / NULLIF(p.active_partners, 0) > 1 THEN 'MODERATE'
        ELSE 'HEALTHY'
    END AS zone_status,
    CASE
        WHEN d.total_orders / NULLIF(p.active_partners, 0) > 2 THEN
            ROUND(LEAST(d.total_orders / NULLIF(p.active_partners, 0) * 0.3 + 1.0, 3.0), 2)
        WHEN d.total_orders / NULLIF(p.active_partners, 0) > 1.5 THEN
            ROUND(d.total_orders / NULLIF(p.active_partners, 0) * 0.2 + 1.0, 2)
        ELSE 1.0
    END AS recommended_surge
FROM gold_zone_demand_hourly d
LEFT JOIN gold_partner_availability p ON d.zone = p.zone
ORDER BY d.order_date, d.order_hour, demand_supply_ratio DESC;

SELECT 'gold_demand_supply_ratio' AS tbl, count(*) AS rows FROM gold_demand_supply_ratio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Customer 360 Profile

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_customer_360 AS
SELECT
    customer_id,
    count(*) AS total_orders,
    count(DISTINCT zone) AS zones_ordered_from,
    count(DISTINCT restaurant_id) AS unique_restaurants,
    ROUND(SUM(net_order_value), 2) AS total_spend,
    ROUND(AVG(net_order_value), 2) AS avg_order_value,
    ROUND(AVG(actual_delivery_mins), 1) AS avg_delivery_time,
    SUM(is_cancelled) AS total_cancellations,
    ROUND(SUM(is_cancelled) * 100.0 / count(*), 1) AS cancel_rate_pct,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_tenure_days,
    SUM(CASE WHEN is_weekend = 1 THEN 1 ELSE 0 END) AS weekend_orders,
    SUM(CASE WHEN order_hour BETWEEN 11 AND 14 THEN 1 ELSE 0 END) AS lunch_orders,
    SUM(CASE WHEN order_hour BETWEEN 19 AND 22 THEN 1 ELSE 0 END) AS dinner_orders,
    SUM(CASE WHEN rain_at_order_time = 1 THEN 1 ELSE 0 END) AS rainy_day_orders
FROM silver_orders
GROUP BY customer_id;

SELECT 'gold_customer_360' AS tbl, count(*) AS rows FROM gold_customer_360;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer Summary

-- COMMAND ----------

SELECT 'gold_zone_demand_hourly' AS tbl, count(*) AS rows FROM gold_zone_demand_hourly
UNION ALL SELECT 'gold_partner_availability', count(*) FROM gold_partner_availability
UNION ALL SELECT 'gold_demand_supply_ratio', count(*) FROM gold_demand_supply_ratio
UNION ALL SELECT 'gold_customer_360', count(*) FROM gold_customer_360;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold layer complete! Run notebook `04_analysis_dashboard` next.
