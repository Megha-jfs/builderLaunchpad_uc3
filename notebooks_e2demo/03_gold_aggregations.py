# Databricks notebook source
# MAGIC %md
# MAGIC # ZipDrop — Gold Layer: Analytics-Ready Tables
# MAGIC
# MAGIC Creates 5 Gold tables powering the Lakeview Dashboard, Genie Space, and ML model.

# COMMAND ----------

spark.sql("USE CATALOG zipdrop")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Zone Demand Hourly — Core Fact Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zipdrop.gold.zone_demand_hourly AS
# MAGIC SELECT
# MAGIC     o.order_date,
# MAGIC     o.order_hour,
# MAGIC     o.zone,
# MAGIC     o.is_weekend,
# MAGIC     COUNT(o.order_id) AS total_orders,
# MAGIC     COUNT(DISTINCT o.customer_id) AS unique_customers,
# MAGIC     ROUND(AVG(o.order_value_inr), 2) AS avg_order_value,
# MAGIC     ROUND(SUM(o.net_order_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(o.actual_delivery_mins), 1) AS avg_delivery_mins,
# MAGIC     ROUND(AVG(o.estimated_eta_mins), 1) AS avg_estimated_eta,
# MAGIC     ROUND(AVG(o.eta_overshoot_mins), 1) AS avg_eta_overshoot,
# MAGIC     SUM(o.is_cancelled) AS total_cancellations,
# MAGIC     ROUND(SUM(o.is_cancelled) * 100.0 / COUNT(o.order_id), 1) AS cancellation_rate_pct,
# MAGIC     ROUND(AVG(o.surge_multiplier), 2) AS avg_surge,
# MAGIC     MAX(o.rain_at_order_time) AS rain_flag,
# MAGIC     MAX(o.event_active) AS event_flag,
# MAGIC     SUM(CASE WHEN o.partner_status = 'unassigned' THEN 1 ELSE 0 END) AS unassigned_orders,
# MAGIC     COUNT(DISTINCT o.restaurant_id) AS active_restaurants,
# MAGIC     -- Weather enrichment
# MAGIC     MAX(w.temperature_c) AS temperature_c,
# MAGIC     MAX(w.humidity_pct) AS humidity_pct,
# MAGIC     MAX(w.condition) AS weather_condition,
# MAGIC     COALESCE(MAX(w.rain_mm), 0) AS rain_mm,
# MAGIC     COALESCE(MAX(w.is_rainy), 0) AS is_rainy,
# MAGIC     -- Event enrichment
# MAGIC     COALESCE(MAX(e.event_name), 'None') AS event_name,
# MAGIC     COALESCE(MAX(e.event_type), 'None') AS event_type,
# MAGIC     COALESCE(MAX(e.demand_multiplier), 1.0) AS demand_multiplier
# MAGIC FROM zipdrop.silver.orders_clean o
# MAGIC LEFT JOIN zipdrop.silver.weather_clean w
# MAGIC     ON o.zone = w.zone AND o.order_date = w.weather_date AND o.order_hour = w.weather_hour
# MAGIC LEFT JOIN zipdrop.silver.local_events_clean e
# MAGIC     ON o.zone = e.zone AND o.order_date = e.date
# MAGIC GROUP BY o.order_date, o.order_hour, o.zone, o.is_weekend;

# COMMAND ----------

select_count = spark.sql("SELECT count(*) FROM zipdrop.gold.zone_demand_hourly").first()[0]
print(f"gold.zone_demand_hourly: {select_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Partner Availability by Zone

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zipdrop.gold.partner_availability AS
# MAGIC SELECT
# MAGIC     zone,
# MAGIC     COUNT(partner_id) AS total_partners,
# MAGIC     SUM(is_active) AS active_partners,
# MAGIC     SUM(CASE WHEN status = 'on_break' THEN 1 ELSE 0 END) AS on_break_partners,
# MAGIC     SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) AS inactive_partners,
# MAGIC     ROUND(AVG(avg_rating), 2) AS avg_partner_rating,
# MAGIC     ROUND(AVG(total_deliveries), 0) AS avg_lifetime_deliveries,
# MAGIC     ROUND(AVG(experience_months), 1) AS avg_experience_months,
# MAGIC     SUM(CASE WHEN vehicle_type = 'ev_scooter' THEN 1 ELSE 0 END) AS ev_partners,
# MAGIC     ROUND(SUM(is_active) * 100.0 / COUNT(partner_id), 1) AS active_pct
# MAGIC FROM zipdrop.silver.delivery_partners_clean
# MAGIC GROUP BY zone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Demand-Supply Ratio with Zone Health & Surge Recommendations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zipdrop.gold.demand_supply_ratio AS
# MAGIC SELECT
# MAGIC     d.order_date,
# MAGIC     d.order_hour,
# MAGIC     d.zone,
# MAGIC     d.is_weekend,
# MAGIC     d.total_orders,
# MAGIC     d.total_cancellations,
# MAGIC     d.cancellation_rate_pct,
# MAGIC     d.avg_surge,
# MAGIC     d.avg_eta_overshoot,
# MAGIC     d.avg_delivery_mins,
# MAGIC     d.total_revenue,
# MAGIC     d.rain_flag,
# MAGIC     d.event_flag,
# MAGIC     d.event_name,
# MAGIC     d.is_rainy,
# MAGIC     d.weather_condition,
# MAGIC     d.demand_multiplier,
# MAGIC     p.active_partners,
# MAGIC     p.total_partners,
# MAGIC     ROUND(d.total_orders / NULLIF(p.active_partners, 0), 2) AS demand_supply_ratio,
# MAGIC     CASE
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 3 THEN 'CRITICAL'
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 2 THEN 'STRESSED'
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 1 THEN 'MODERATE'
# MAGIC         ELSE 'HEALTHY'
# MAGIC     END AS zone_status,
# MAGIC     CASE
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 2.5 THEN
# MAGIC             ROUND(LEAST(d.total_orders / NULLIF(p.active_partners, 0) * 0.3 + 1.0, 3.0), 2)
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 1.5 THEN
# MAGIC             ROUND(d.total_orders / NULLIF(p.active_partners, 0) * 0.2 + 1.0, 2)
# MAGIC         ELSE 1.0
# MAGIC     END AS recommended_surge,
# MAGIC     CASE
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 2.5 THEN
# MAGIC             CAST(ROUND(d.total_orders / 2 - p.active_partners * 0.3) AS INT)
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) < 0.5 THEN
# MAGIC             -1 * CAST(ROUND(p.active_partners * 0.2) AS INT)
# MAGIC         ELSE 0
# MAGIC     END AS partner_rebalance_count
# MAGIC FROM zipdrop.gold.zone_demand_hourly d
# MAGIC LEFT JOIN zipdrop.gold.partner_availability p ON d.zone = p.zone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Customer 360

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zipdrop.gold.customer_360 AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     count(*) AS total_orders,
# MAGIC     count(DISTINCT zone) AS zones_ordered_from,
# MAGIC     count(DISTINCT restaurant_id) AS unique_restaurants,
# MAGIC     ROUND(SUM(net_order_value), 2) AS total_spend,
# MAGIC     ROUND(AVG(net_order_value), 2) AS avg_order_value,
# MAGIC     ROUND(AVG(actual_delivery_mins), 1) AS avg_delivery_time,
# MAGIC     SUM(is_cancelled) AS total_cancellations,
# MAGIC     ROUND(SUM(is_cancelled) * 100.0 / count(*), 1) AS cancel_rate_pct,
# MAGIC     MIN(order_date) AS first_order_date,
# MAGIC     MAX(order_date) AS last_order_date,
# MAGIC     DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_tenure_days,
# MAGIC     SUM(is_weekend) AS weekend_orders,
# MAGIC     SUM(is_lunch_hour) AS lunch_orders,
# MAGIC     SUM(is_dinner_hour) AS dinner_orders,
# MAGIC     SUM(is_late_night) AS late_night_orders,
# MAGIC     SUM(rain_at_order_time) AS rainy_day_orders,
# MAGIC     CASE
# MAGIC         WHEN count(*) >= 15 THEN 'Power User'
# MAGIC         WHEN count(*) >= 8 THEN 'Regular'
# MAGIC         WHEN count(*) >= 3 THEN 'Occasional'
# MAGIC         ELSE 'New'
# MAGIC     END AS customer_segment
# MAGIC FROM zipdrop.silver.orders_clean
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Restaurant Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zipdrop.gold.restaurant_performance AS
# MAGIC SELECT
# MAGIC     o.restaurant_id,
# MAGIC     r.restaurant_name,
# MAGIC     r.zone,
# MAGIC     r.cuisine_type,
# MAGIC     r.avg_prep_time_mins,
# MAGIC     r.rating AS restaurant_rating,
# MAGIC     r.is_premium,
# MAGIC     COUNT(o.order_id) AS total_orders,
# MAGIC     ROUND(SUM(o.net_order_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(o.net_order_value), 2) AS avg_order_value,
# MAGIC     ROUND(AVG(o.actual_delivery_mins), 1) AS avg_delivery_mins,
# MAGIC     SUM(o.is_cancelled) AS total_cancellations,
# MAGIC     ROUND(SUM(o.is_cancelled) * 100.0 / COUNT(*), 1) AS cancel_rate_pct,
# MAGIC     COUNT(DISTINCT o.customer_id) AS unique_customers,
# MAGIC     COUNT(DISTINCT o.order_date) AS active_days
# MAGIC FROM zipdrop.silver.orders_clean o
# MAGIC JOIN zipdrop.silver.restaurants_clean r ON o.restaurant_id = r.restaurant_id
# MAGIC GROUP BY o.restaurant_id, r.restaurant_name, r.zone, r.cuisine_type,
# MAGIC          r.avg_prep_time_mins, r.rating, r.is_premium;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'zone_demand_hourly' AS tbl, count(*) AS rows FROM zipdrop.gold.zone_demand_hourly
# MAGIC UNION ALL SELECT 'partner_availability', count(*) FROM zipdrop.gold.partner_availability
# MAGIC UNION ALL SELECT 'demand_supply_ratio', count(*) FROM zipdrop.gold.demand_supply_ratio
# MAGIC UNION ALL SELECT 'customer_360', count(*) FROM zipdrop.gold.customer_360
# MAGIC UNION ALL SELECT 'restaurant_performance', count(*) FROM zipdrop.gold.restaurant_performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer complete! Run `04_ml_demand_forecast` next.
