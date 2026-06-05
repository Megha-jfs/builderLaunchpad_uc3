# Databricks notebook source
# MAGIC %md
# MAGIC # UC3: Gold Layer — Analytics-Ready Aggregations
# MAGIC
# MAGIC This notebook creates 4 Gold tables:
# MAGIC 1. `zone_demand_hourly` — Orders/hour per zone enriched with weather & events
# MAGIC 2. `partner_availability` — Active partners per zone
# MAGIC 3. `demand_supply_ratio` — Demand vs supply gap per zone/hour
# MAGIC 4. `customer_360` — Customer-level aggregated profile

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Zone Demand Hourly (Core Table)

# COMMAND ----------

df_orders = spark.table("zipdrop_silver.orders_clean")

gold_zone_demand = (df_orders
    .groupBy("order_date", "order_hour", "zone")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        round(avg("order_value_inr"), 2).alias("avg_order_value"),
        round(sum("net_order_value"), 2).alias("total_revenue"),
        round(avg("actual_delivery_mins"), 1).alias("avg_delivery_mins"),
        round(avg("estimated_eta_mins"), 1).alias("avg_estimated_eta"),
        round(avg("eta_overshoot_mins"), 1).alias("avg_eta_overshoot"),
        sum("is_cancelled").alias("total_cancellations"),
        round(sum("is_cancelled") / count("order_id") * 100, 1).alias("cancellation_rate_pct"),
        round(avg("surge_multiplier"), 2).alias("avg_surge"),
        max("rain_at_order_time").alias("rain_flag"),
        max("event_active").alias("event_flag"),
        sum(when(col("partner_status") == "unassigned", 1).otherwise(0)).alias("unassigned_orders"),
        countDistinct("restaurant_id").alias("active_restaurants")
    ))

# Enrich with weather
df_weather = spark.table("zipdrop_silver.weather_clean")

gold_zone_demand_enriched = (gold_zone_demand
    .join(
        df_weather.select("zone", "weather_date", "weather_hour", "temperature_c",
                          "humidity_pct", "condition", "rain_mm", "is_rainy"),
        (gold_zone_demand.zone == df_weather.zone) &
        (gold_zone_demand.order_date == df_weather.weather_date) &
        (gold_zone_demand.order_hour == df_weather.weather_hour),
        "left")
    .drop(df_weather.zone))

# Enrich with events
df_events = (spark.table("zipdrop_silver.local_events_clean")
    .select("zone", "date", "event_name", "event_type", "expected_crowd", "demand_multiplier")
    .withColumnRenamed("zone", "event_zone"))

gold_final = (gold_zone_demand_enriched
    .join(
        df_events,
        (gold_zone_demand_enriched.zone == df_events.event_zone) &
        (gold_zone_demand_enriched.order_date == df_events.date),
        "left")
    .drop("event_zone")
    .withColumn("event_name", coalesce(col("event_name"), lit("None")))
    .withColumn("demand_multiplier", coalesce(col("demand_multiplier"), lit(1.0))))

gold_final.write.mode("overwrite").format("delta").saveAsTable("zipdrop_gold.zone_demand_hourly")
print(f"zipdrop_gold.zone_demand_hourly: {gold_final.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Partner Availability by Zone

# COMMAND ----------

df_partners = spark.table("zipdrop_silver.delivery_partners_clean")

gold_partner_avail = (df_partners
    .groupBy("zone")
    .agg(
        count("partner_id").alias("total_partners"),
        sum("is_active").alias("active_partners"),
        sum(when(col("status") == "on_break", 1).otherwise(0)).alias("on_break_partners"),
        sum(when(col("status") == "inactive", 1).otherwise(0)).alias("inactive_partners"),
        round(avg("avg_rating"), 2).alias("avg_partner_rating"),
        round(avg("total_deliveries"), 0).alias("avg_lifetime_deliveries"),
        round(avg("experience_months"), 1).alias("avg_experience_months"),
        sum(when(col("vehicle_type") == "ev_scooter", 1).otherwise(0)).alias("ev_partners")
    )
    .withColumn("active_pct", round(col("active_partners") / col("total_partners") * 100, 1)))

gold_partner_avail.write.mode("overwrite").format("delta").saveAsTable("zipdrop_gold.partner_availability")
print(f"zipdrop_gold.partner_availability: {gold_partner_avail.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Demand-Supply Ratio per Zone-Hour

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zipdrop_gold.demand_supply_ratio AS
# MAGIC SELECT
# MAGIC     d.order_date,
# MAGIC     d.order_hour,
# MAGIC     d.zone,
# MAGIC     d.total_orders,
# MAGIC     d.total_cancellations,
# MAGIC     d.cancellation_rate_pct,
# MAGIC     d.avg_surge,
# MAGIC     d.avg_eta_overshoot,
# MAGIC     d.rain_flag,
# MAGIC     d.event_flag,
# MAGIC     d.event_name,
# MAGIC     p.active_partners,
# MAGIC     ROUND(d.total_orders / NULLIF(p.active_partners, 0), 2) AS demand_supply_ratio,
# MAGIC     CASE
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 3 THEN 'CRITICAL'
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 2 THEN 'STRESSED'
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 1 THEN 'MODERATE'
# MAGIC         ELSE 'HEALTHY'
# MAGIC     END AS zone_status,
# MAGIC     CASE
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 2 THEN
# MAGIC             ROUND(d.total_orders / NULLIF(p.active_partners, 0) * 0.3 + 1.0, 2)
# MAGIC         WHEN d.total_orders / NULLIF(p.active_partners, 0) > 1.5 THEN
# MAGIC             ROUND(d.total_orders / NULLIF(p.active_partners, 0) * 0.2 + 1.0, 2)
# MAGIC         ELSE 1.0
# MAGIC     END AS recommended_surge
# MAGIC FROM zipdrop_gold.zone_demand_hourly d
# MAGIC LEFT JOIN zipdrop_gold.partner_availability p ON d.zone = p.zone
# MAGIC ORDER BY d.order_date, d.order_hour, demand_supply_ratio DESC

# COMMAND ----------

print(f"zipdrop_gold.demand_supply_ratio: {spark.table('zipdrop_gold.demand_supply_ratio').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Customer 360 Profile

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zipdrop_gold.customer_360 AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     count(*) AS total_orders,
# MAGIC     count(DISTINCT zone) AS zones_ordered_from,
# MAGIC     count(DISTINCT restaurant_id) AS unique_restaurants,
# MAGIC     ROUND(SUM(net_order_value), 2) AS total_spend,
# MAGIC     ROUND(AVG(net_order_value), 2) AS avg_order_value,
# MAGIC     ROUND(AVG(actual_delivery_mins), 1) AS avg_delivery_time,
# MAGIC     SUM(is_cancelled) AS total_cancellations,
# MAGIC     ROUND(SUM(is_cancelled) / count(*) * 100, 1) AS cancel_rate_pct,
# MAGIC     MIN(order_date) AS first_order_date,
# MAGIC     MAX(order_date) AS last_order_date,
# MAGIC     DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_tenure_days,
# MAGIC     SUM(CASE WHEN is_weekend = 1 THEN 1 ELSE 0 END) AS weekend_orders,
# MAGIC     SUM(CASE WHEN order_hour BETWEEN 11 AND 14 THEN 1 ELSE 0 END) AS lunch_orders,
# MAGIC     SUM(CASE WHEN order_hour BETWEEN 19 AND 22 THEN 1 ELSE 0 END) AS dinner_orders,
# MAGIC     SUM(CASE WHEN rain_at_order_time = 1 THEN 1 ELSE 0 END) AS rainy_day_orders
# MAGIC FROM zipdrop_silver.orders_clean
# MAGIC GROUP BY customer_id

# COMMAND ----------

print(f"zipdrop_gold.customer_360: {spark.table('zipdrop_gold.customer_360').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'zone_demand_hourly' as table_name, count(*) as rows FROM zipdrop_gold.zone_demand_hourly
# MAGIC UNION ALL
# MAGIC SELECT 'partner_availability', count(*) FROM zipdrop_gold.partner_availability
# MAGIC UNION ALL
# MAGIC SELECT 'demand_supply_ratio', count(*) FROM zipdrop_gold.demand_supply_ratio
# MAGIC UNION ALL
# MAGIC SELECT 'customer_360', count(*) FROM zipdrop_gold.customer_360

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer complete! Run notebook `04_analysis_dashboard` next.
