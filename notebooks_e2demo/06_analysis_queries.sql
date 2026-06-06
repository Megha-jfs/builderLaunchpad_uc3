-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ZipDrop — Analytics Queries for Lakeview Dashboard
-- MAGIC
-- MAGIC These queries back the Lakeview Dashboard tiles. Run each cell to validate,
-- MAGIC then create a Lakeview Dashboard: **New** → **Dashboard** → add datasets from these queries.

-- COMMAND ----------

USE CATALOG zipdrop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI Cards (Top Row)

-- COMMAND ----------

SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    CONCAT('₹', FORMAT_NUMBER(SUM(net_order_value), 0)) AS total_revenue,
    ROUND(AVG(actual_delivery_mins), 1) AS avg_delivery_mins,
    ROUND(SUM(is_cancelled) * 100.0 / COUNT(*), 1) AS cancel_rate_pct,
    ROUND(AVG(eta_overshoot_mins), 1) AS avg_eta_overshoot
FROM zipdrop.silver.orders_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Zone Leaderboard

-- COMMAND ----------

SELECT
    zone,
    SUM(total_orders) AS total_orders,
    CONCAT('₹', FORMAT_NUMBER(SUM(total_revenue), 0)) AS total_revenue,
    ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate_pct,
    ROUND(AVG(avg_eta_overshoot), 1) AS avg_eta_overshoot,
    ROUND(AVG(avg_surge), 2) AS avg_surge
FROM zipdrop.gold.zone_demand_hourly
WHERE zone IS NOT NULL
GROUP BY zone
ORDER BY total_orders DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Hourly Demand — Weekday vs Weekend

-- COMMAND ----------

SELECT
    order_hour,
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(AVG(total_orders), 0) AS avg_orders,
    ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate
FROM zipdrop.gold.zone_demand_hourly
GROUP BY order_hour, day_type
ORDER BY order_hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rain & Event Impact Comparison

-- COMMAND ----------

SELECT 'Clear' AS condition, ROUND(AVG(total_orders), 0) AS avg_orders,
    ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate,
    ROUND(AVG(avg_eta_overshoot), 1) AS eta_overshoot
FROM zipdrop.gold.zone_demand_hourly WHERE is_rainy = 0
UNION ALL
SELECT 'Rainy', ROUND(AVG(total_orders), 0),
    ROUND(AVG(cancellation_rate_pct), 1), ROUND(AVG(avg_eta_overshoot), 1)
FROM zipdrop.gold.zone_demand_hourly WHERE is_rainy = 1
UNION ALL
SELECT 'No Event', ROUND(AVG(total_orders), 0),
    ROUND(AVG(cancellation_rate_pct), 1), ROUND(AVG(avg_eta_overshoot), 1)
FROM zipdrop.gold.zone_demand_hourly WHERE event_flag = 0
UNION ALL
SELECT 'Event Active', ROUND(AVG(total_orders), 0),
    ROUND(AVG(cancellation_rate_pct), 1), ROUND(AVG(avg_eta_overshoot), 1)
FROM zipdrop.gold.zone_demand_hourly WHERE event_flag = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Demand-Supply Heatmap (Zone x Hour)

-- COMMAND ----------

SELECT
    zone,
    order_hour,
    ROUND(AVG(demand_supply_ratio), 2) AS ds_ratio,
    MAX(zone_status) AS status
FROM zipdrop.gold.demand_supply_ratio
GROUP BY zone, order_hour
ORDER BY zone, order_hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Critical Zone Alerts

-- COMMAND ----------

SELECT
    zone, order_date, order_hour,
    total_orders, active_partners, demand_supply_ratio,
    zone_status, cancellation_rate_pct, avg_eta_overshoot,
    recommended_surge, partner_rebalance_count,
    CASE WHEN is_rainy = 1 THEN 'Yes' ELSE 'No' END AS raining,
    event_name
FROM zipdrop.gold.demand_supply_ratio
WHERE zone_status = 'CRITICAL'
ORDER BY demand_supply_ratio DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Surge Analysis — Current vs Recommended

-- COMMAND ----------

SELECT
    zone,
    ROUND(AVG(avg_surge), 2) AS current_avg_surge,
    ROUND(AVG(recommended_surge), 2) AS recommended_avg_surge,
    ROUND(AVG(recommended_surge) - AVG(avg_surge), 2) AS surge_gap,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate
FROM zipdrop.gold.demand_supply_ratio
GROUP BY zone
HAVING AVG(recommended_surge) > 1.0
ORDER BY surge_gap DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Partner Rebalancing — Who Goes Where?

-- COMMAND ----------

WITH zone_load AS (
    SELECT
        d.zone,
        ROUND(AVG(d.demand_supply_ratio), 2) AS avg_ds_ratio,
        p.active_partners,
        ROUND(AVG(d.total_orders), 0) AS avg_hourly_orders,
        ROUND(AVG(d.cancellation_rate_pct), 1) AS cancel_rate,
        SUM(d.partner_rebalance_count) AS total_rebalance_needed
    FROM zipdrop.gold.demand_supply_ratio d
    JOIN zipdrop.gold.partner_availability p ON d.zone = p.zone
    GROUP BY d.zone, p.active_partners
)
SELECT *, CASE
    WHEN avg_ds_ratio > 2.5 THEN CONCAT('🔴 NEEDS +', total_rebalance_needed, ' partners')
    WHEN avg_ds_ratio < 0.5 THEN CONCAT('🟢 EXCESS: move ', ABS(total_rebalance_needed), ' out')
    ELSE '🟡 Balanced'
END AS action
FROM zone_load
ORDER BY avg_ds_ratio DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Daily Trend (30 days)

-- COMMAND ----------

SELECT
    order_date,
    SUM(total_orders) AS daily_orders,
    ROUND(SUM(total_revenue), 0) AS daily_revenue,
    ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate,
    MAX(CASE WHEN event_name != 'None' THEN event_name ELSE NULL END) AS event
FROM zipdrop.gold.zone_demand_hourly
GROUP BY order_date
ORDER BY order_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cuisine Performance

-- COMMAND ----------

SELECT
    cuisine_type,
    COUNT(*) AS restaurants,
    SUM(total_orders) AS total_orders,
    CONCAT('₹', FORMAT_NUMBER(SUM(total_revenue), 0)) AS total_revenue,
    ROUND(AVG(avg_order_value), 0) AS avg_order_value,
    ROUND(AVG(cancel_rate_pct), 1) AS cancel_rate,
    ROUND(AVG(avg_delivery_mins), 1) AS avg_delivery_mins
FROM zipdrop.gold.restaurant_performance
GROUP BY cuisine_type
ORDER BY total_orders DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customer Segments

-- COMMAND ----------

SELECT
    customer_segment,
    COUNT(*) AS customers,
    ROUND(AVG(total_spend), 0) AS avg_spend,
    ROUND(AVG(avg_order_value), 0) AS avg_order_value,
    ROUND(AVG(total_orders), 1) AS avg_orders,
    ROUND(AVG(cancel_rate_pct), 1) AS cancel_rate,
    ROUND(AVG(dinner_orders) * 100.0 / NULLIF(AVG(total_orders), 0), 0) AS dinner_pct
FROM zipdrop.gold.customer_360
GROUP BY customer_segment
ORDER BY avg_spend DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ML Predictions — Tonight's Forecast

-- COMMAND ----------

SELECT * FROM zipdrop.gold.demand_predictions
ORDER BY ds_ratio DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### All dashboard queries validated!
-- MAGIC
-- MAGIC **Create Lakeview Dashboard:**
-- MAGIC 1. Click **New** → **Dashboard**
-- MAGIC 2. Add datasets pointing to `zipdrop.gold.*` tables
-- MAGIC 3. Build tiles: KPI cards → Zone bar chart → Hourly line → Heatmap → Surge scatter → Daily trend → Predictions
