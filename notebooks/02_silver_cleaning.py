# Databricks notebook source
# MAGIC %md
# MAGIC # UC3: Silver Layer — Clean, Deduplicate, Enrich
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Deduplicates orders
# MAGIC 2. Handles nulls (zone inferred from restaurant, partner flagged)
# MAGIC 3. Enforces schema & types
# MAGIC 4. Creates clean Silver tables for all entities

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Orders — Deduplication

# COMMAND ----------

df_orders_raw = spark.table("zipdrop_bronze.orders_raw")

window_dedup = Window.partitionBy("order_id").orderBy("order_timestamp")
df_deduped = (df_orders_raw
    .withColumn("_row_num", row_number().over(window_dedup))
    .filter(col("_row_num") == 1)
    .drop("_row_num"))

removed = df_orders_raw.count() - df_deduped.count()
print(f"Removed {removed:,} duplicate rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Orders — Null Handling

# COMMAND ----------

df_restaurants = spark.table("zipdrop_bronze.restaurants_raw").select(
    col("restaurant_id"),
    col("zone").alias("restaurant_zone")
)

df_zone_fixed = (df_deduped
    .join(df_restaurants, on="restaurant_id", how="left")
    .withColumn("zone_clean",
        when((col("zone").isNull()) | (col("zone") == ""), col("restaurant_zone"))
        .otherwise(col("zone")))
    .drop("restaurant_zone"))

df_partner_flagged = df_zone_fixed.withColumn(
    "partner_status",
    when((col("partner_id").isNull()) | (col("partner_id") == ""), lit("unassigned"))
    .otherwise(lit("assigned")))

null_zones_remaining = df_partner_flagged.filter(col("zone_clean").isNull()).count()
print(f"Null zones remaining after restaurant lookup: {null_zones_remaining}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Orders — Schema Enforcement & Derived Columns

# COMMAND ----------

df_orders_silver = (df_partner_flagged
    .withColumn("order_timestamp", to_timestamp("order_timestamp"))
    .withColumn("delivery_timestamp", to_timestamp("delivery_timestamp"))
    .withColumn("order_value_inr", col("order_value_inr").cast("double"))
    .withColumn("discount_inr", col("discount_inr").cast("double"))
    .withColumn("surge_multiplier", col("surge_multiplier").cast("double"))
    .withColumn("estimated_eta_mins", col("estimated_eta_mins").cast("int"))
    .withColumn("actual_delivery_mins", col("actual_delivery_mins").cast("int"))
    .withColumn("rain_at_order_time", col("rain_at_order_time").cast("int"))
    .withColumn("event_active", col("event_active").cast("int"))
    # Derived columns
    .withColumn("order_date", to_date("order_timestamp"))
    .withColumn("order_hour", hour("order_timestamp"))
    .withColumn("order_day_of_week", dayofweek("order_timestamp"))
    .withColumn("is_weekend", when(dayofweek("order_timestamp").isin(1, 7), 1).otherwise(0))
    .withColumn("eta_overshoot_mins",
        when(col("actual_delivery_mins").isNotNull(),
             col("actual_delivery_mins") - col("estimated_eta_mins"))
        .otherwise(lit(None)))
    .withColumn("is_cancelled", when(col("status") == "cancelled", 1).otherwise(0))
    .withColumn("net_order_value", col("order_value_inr") - col("discount_inr"))
    .drop("zone")
    .withColumnRenamed("zone_clean", "zone"))

df_orders_silver.write.mode("overwrite").format("delta").saveAsTable("zipdrop_silver.orders_clean")
print(f"zipdrop_silver.orders_clean: {df_orders_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Restaurants — Silver

# COMMAND ----------

df_rest_silver = (spark.table("zipdrop_bronze.restaurants_raw")
    .withColumn("avg_prep_time_mins", col("avg_prep_time_mins").cast("int"))
    .withColumn("rating", col("rating").cast("double"))
    .withColumn("is_premium", col("is_premium").cast("int"))
    .withColumn("lat", col("lat").cast("double"))
    .withColumn("lng", col("lng").cast("double")))

df_rest_silver.write.mode("overwrite").format("delta").saveAsTable("zipdrop_silver.restaurants_clean")
print(f"zipdrop_silver.restaurants_clean: {df_rest_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delivery Partners — Silver

# COMMAND ----------

df_partners_silver = (spark.table("zipdrop_bronze.delivery_partners_raw")
    .withColumn("avg_rating", col("avg_rating").cast("double"))
    .withColumn("total_deliveries", col("total_deliveries").cast("int"))
    .withColumn("experience_months", col("experience_months").cast("int"))
    .withColumn("lat", col("lat").cast("double"))
    .withColumn("lng", col("lng").cast("double"))
    .withColumn("is_active", when(col("status") == "active", 1).otherwise(0)))

df_partners_silver.write.mode("overwrite").format("delta").saveAsTable("zipdrop_silver.delivery_partners_clean")
print(f"zipdrop_silver.delivery_partners_clean: {df_partners_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Weather — Silver

# COMMAND ----------

df_weather_silver = (spark.table("zipdrop_bronze.weather_hourly_raw")
    .withColumn("timestamp", to_timestamp("timestamp"))
    .withColumn("weather_date", to_date("timestamp"))
    .withColumn("weather_hour", hour("timestamp"))
    .withColumn("temperature_c", col("temperature_c").cast("double"))
    .withColumn("humidity_pct", col("humidity_pct").cast("int"))
    .withColumn("wind_speed_kmh", col("wind_speed_kmh").cast("double"))
    .withColumn("rain_mm", col("rain_mm").cast("double"))
    .withColumn("visibility_km", col("visibility_km").cast("double"))
    .withColumn("is_rainy", when(col("condition").isin("Light Rain", "Heavy Rain", "Thunderstorm", "Drizzle"), 1).otherwise(0)))

df_weather_silver.write.mode("overwrite").format("delta").saveAsTable("zipdrop_silver.weather_clean")
print(f"zipdrop_silver.weather_clean: {df_weather_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Local Events — Silver (Explode multi-zone events)

# COMMAND ----------

df_events_silver = (spark.table("zipdrop_bronze.local_events_raw")
    .withColumn("date", to_date("date"))
    .withColumn("expected_crowd", col("expected_crowd").cast("int"))
    .withColumn("demand_multiplier", col("demand_multiplier").cast("double"))
    .withColumn("zone", explode(split(col("zones_impacted"), ",")))
    .withColumn("zone", trim(col("zone"))))

df_events_silver.write.mode("overwrite").format("delta").saveAsTable("zipdrop_silver.local_events_clean")
print(f"zipdrop_silver.local_events_clean: {df_events_silver.count():,} rows (exploded by zone)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Silver Layer Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders_clean' as table_name, count(*) as rows FROM zipdrop_silver.orders_clean
# MAGIC UNION ALL
# MAGIC SELECT 'restaurants_clean', count(*) FROM zipdrop_silver.restaurants_clean
# MAGIC UNION ALL
# MAGIC SELECT 'delivery_partners_clean', count(*) FROM zipdrop_silver.delivery_partners_clean
# MAGIC UNION ALL
# MAGIC SELECT 'weather_clean', count(*) FROM zipdrop_silver.weather_clean
# MAGIC UNION ALL
# MAGIC SELECT 'local_events_clean', count(*) FROM zipdrop_silver.local_events_clean

# COMMAND ----------

# Quick quality check on orders
display(spark.sql("""
    SELECT
        count(*) as total_orders,
        count(DISTINCT order_id) as unique_orders,
        sum(CASE WHEN zone IS NULL THEN 1 ELSE 0 END) as null_zones,
        sum(CASE WHEN partner_status = 'unassigned' THEN 1 ELSE 0 END) as unassigned_orders,
        round(avg(eta_overshoot_mins), 1) as avg_eta_overshoot,
        round(sum(is_cancelled) / count(*) * 100, 1) as cancel_rate_pct
    FROM zipdrop_silver.orders_clean
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer complete! Run notebook `03_gold_aggregations` next.
