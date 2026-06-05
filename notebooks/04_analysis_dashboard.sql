-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UC3: Analysis & Dashboard Queries
-- MAGIC
-- MAGIC 10 analytics queries — click the **chart icon** (📊) on each result to create visualizations.
-- MAGIC
-- MAGIC **Visualization tips:**
-- MAGIC - Bar chart: Click chart → select "Bar"
-- MAGIC - Line chart: Click chart → select "Line" → set X as order_hour, Y as value, Group by category
-- MAGIC - Heatmap: Use "Pivot" chart type with zone on rows, hour on columns
-- MAGIC
-- MAGIC **Run on:** Serverless SQL Warehouse

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA zipdrop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI Summary — Overall Platform Health

-- COMMAND ----------

SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(net_order_value), 0) AS total_revenue_inr,
    ROUND(AVG(net_order_value), 0) AS avg_order_value,
    ROUND(AVG(actual_delivery_mins), 1) AS avg_delivery_mins,
    ROUND(SUM(is_cancelled) * 100.0 / COUNT(*), 1) AS overall_cancel_rate_pct,
    ROUND(AVG(eta_overshoot_mins), 1) AS avg_eta_overshoot_mins
FROM silver_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Zone Performance Leaderboard
-- MAGIC 📊 **Chart:** Horizontal Bar → Y: zone, X: total_orders, Color: avg_cancel_rate

-- COMMAND ----------

SELECT
    zone,
    SUM(total_orders) AS total_orders,
    ROUND(SUM(total_revenue), 0) AS total_revenue,
    ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
    ROUND(AVG(avg_eta_overshoot), 1) AS avg_eta_overshoot,
    ROUND(AVG(avg_surge), 2) AS avg_surge
FROM gold_zone_demand_hourly
WHERE zone IS NOT NULL
GROUP BY zone
ORDER BY total_orders DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Hourly Demand Pattern — Weekday vs Weekend
-- MAGIC 📊 **Chart:** Line → X: order_hour, Y: avg_orders, Group by: day_type

-- COMMAND ----------

SELECT
    order_hour,
    CASE WHEN DAYOFWEEK(order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(AVG(total_orders), 0) AS avg_orders,
    ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
    ROUND(AVG(avg_surge), 2) AS avg_surge
FROM gold_zone_demand_hourly
GROUP BY order_hour, day_type
ORDER BY order_hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Rain Impact Analysis
-- MAGIC 📊 **Chart:** Grouped Bar → X: weather, Y: avg_orders + avg_cancel_rate

-- COMMAND ----------

SELECT
    CASE WHEN is_rainy = 1 THEN 'Rainy' ELSE 'Clear' END AS weather,
    ROUND(AVG(total_orders), 0) AS avg_orders,
    ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
    ROUND(AVG(avg_eta_overshoot), 1) AS avg_eta_overshoot,
    ROUND(AVG(avg_surge), 2) AS avg_surge
FROM gold_zone_demand_hourly
WHERE is_rainy IS NOT NULL
GROUP BY weather;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Event Days vs Normal Days
-- MAGIC 📊 **Chart:** Grouped Bar → X: event_status, Y: metrics

-- COMMAND ----------

SELECT
    CASE WHEN event_flag = 1 THEN 'Event Active' ELSE 'No Event' END AS event_status,
    ROUND(AVG(total_orders), 0) AS avg_orders,
    ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
    ROUND(AVG(avg_eta_overshoot), 1) AS avg_eta_overshoot,
    ROUND(AVG(avg_surge), 2) AS avg_surge,
    ROUND(AVG(total_revenue), 0) AS avg_revenue
FROM gold_zone_demand_hourly
GROUP BY event_status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Demand-Supply Heatmap by Zone x Hour
-- MAGIC 📊 **Chart:** Pivot → Rows: zone, Columns: order_hour, Values: avg_ds_ratio, Color: Red-Yellow-Green

-- COMMAND ----------

SELECT
    zone,
    order_hour,
    ROUND(AVG(demand_supply_ratio), 2) AS avg_ds_ratio,
    ROUND(AVG(total_orders), 0) AS avg_orders,
    MAX(zone_status) AS worst_status
FROM gold_demand_supply_ratio
GROUP BY zone, order_hour
ORDER BY zone, order_hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Surge Pricing — Current vs Recommended
-- MAGIC 📊 **Chart:** Scatter → X: current_surge, Y: recommended_surge, Size: total_orders, Color: zone

-- COMMAND ----------

SELECT
    zone,
    order_hour,
    ROUND(AVG(avg_surge), 2) AS current_surge,
    ROUND(AVG(recommended_surge), 2) AS recommended_surge,
    ROUND(AVG(demand_supply_ratio), 2) AS ds_ratio,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate
FROM gold_demand_supply_ratio
GROUP BY zone, order_hour
HAVING AVG(recommended_surge) > 1.0
ORDER BY recommended_surge DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Partner Rebalancing Recommendations

-- COMMAND ----------

WITH zone_load AS (
    SELECT
        d.zone,
        ROUND(AVG(d.demand_supply_ratio), 2) AS avg_ds_ratio,
        p.active_partners,
        ROUND(AVG(d.total_orders), 0) AS avg_hourly_orders,
        ROUND(AVG(d.cancellation_rate_pct), 1) AS avg_cancel_rate
    FROM gold_demand_supply_ratio d
    JOIN gold_partner_availability p ON d.zone = p.zone
    GROUP BY d.zone, p.active_partners
)
SELECT
    zone,
    active_partners,
    avg_hourly_orders,
    avg_ds_ratio,
    avg_cancel_rate,
    CASE
        WHEN avg_ds_ratio > 2.5 THEN CONCAT('NEEDS +', CAST(ROUND(avg_hourly_orders / 2 - active_partners * 0.3) AS INT), ' partners')
        WHEN avg_ds_ratio < 0.5 THEN CONCAT('EXCESS: move ', CAST(ROUND(active_partners * 0.2) AS INT), ' partners out')
        ELSE 'Balanced'
    END AS recommendation,
    CASE
        WHEN avg_ds_ratio > 2.5 THEN 'CRITICAL'
        WHEN avg_ds_ratio > 1.5 THEN 'STRESSED'
        WHEN avg_ds_ratio < 0.5 THEN 'OVERSTAFFED'
        ELSE 'BALANCED'
    END AS status
FROM zone_load
ORDER BY avg_ds_ratio DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Cancellation Reasons Breakdown
-- MAGIC 📊 **Chart:** Pie → Keys: reason, Values: count

-- COMMAND ----------

SELECT
    CASE WHEN cancel_reason = '' OR cancel_reason IS NULL THEN 'unknown' ELSE cancel_reason END AS reason,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM silver_orders
WHERE is_cancelled = 1
GROUP BY reason
ORDER BY count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Daily Trend — Orders, Revenue, Cancellations (30 days)
-- MAGIC 📊 **Chart:** Line → X: order_date, Y: daily_orders (left axis) + cancel_rate (right axis)

-- COMMAND ----------

SELECT
    order_date,
    SUM(total_orders) AS daily_orders,
    ROUND(SUM(total_revenue), 0) AS daily_revenue,
    ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate,
    ROUND(AVG(avg_eta_overshoot), 1) AS eta_overshoot,
    MAX(CASE WHEN event_name != 'None' THEN event_name ELSE NULL END) AS event_of_day
FROM gold_zone_demand_hourly
GROUP BY order_date
ORDER BY order_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Customer Segments
-- MAGIC 📊 **Chart:** Bar → X: segment, Y: customers, Color: avg_cancel_rate

-- COMMAND ----------

SELECT
    CASE
        WHEN total_orders >= 15 THEN '1. Power User (15+)'
        WHEN total_orders >= 8 THEN '2. Regular (8-14)'
        WHEN total_orders >= 3 THEN '3. Occasional (3-7)'
        ELSE '4. New (1-2)'
    END AS segment,
    COUNT(*) AS customers,
    ROUND(AVG(total_spend), 0) AS avg_total_spend,
    ROUND(AVG(avg_order_value), 0) AS avg_order_value,
    ROUND(AVG(cancel_rate_pct), 1) AS avg_cancel_rate,
    ROUND(AVG(dinner_orders) * 100.0 / NULLIF(AVG(total_orders), 0), 0) AS dinner_pct,
    ROUND(AVG(rainy_day_orders) * 100.0 / NULLIF(AVG(total_orders), 0), 0) AS rainy_order_pct
FROM gold_customer_360
GROUP BY segment
ORDER BY segment;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bonus: Worst Hours — When Does Everything Break?

-- COMMAND ----------

SELECT
    zone,
    order_date,
    order_hour,
    total_orders,
    active_partners,
    demand_supply_ratio,
    zone_status,
    cancellation_rate_pct,
    avg_eta_overshoot,
    recommended_surge,
    CASE WHEN is_rainy = 1 THEN 'Yes' ELSE 'No' END AS raining,
    event_name
FROM gold_demand_supply_ratio
WHERE zone_status = 'CRITICAL'
ORDER BY demand_supply_ratio DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analysis complete!
-- MAGIC
-- MAGIC **Key findings to look for:**
-- MAGIC - IPL match days (5th, 12th, 18th, 25th) show 1.8x order spikes
-- MAGIC - Rain + dinner rush = highest cancellation rates
-- MAGIC - Weekends have ~25% more demand than weekdays
-- MAGIC - Koramangala & Indiranagar are consistently understaffed
-- MAGIC - Late night (post 11 PM) has excess partners in most zones
