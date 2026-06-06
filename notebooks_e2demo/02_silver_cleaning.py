# Databricks notebook source
# MAGIC %md
# MAGIC # ZipDrop — Silver Layer: Clean, Deduplicate, Enrich
# MAGIC
# MAGIC Transforms raw Bronze data into clean, typed, enriched Silver tables.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG = "zipdrop"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Orders — Deduplicate

# COMMAND ----------

df_raw = spark.table("zipdrop.bronze.orders_raw")
before = df_raw.count()

window_dedup = Window.partitionBy("order_id").orderBy("order_timestamp")
df_deduped = (df_raw
    .withColumn("_rn", row_number().over(window_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn"))

after = df_deduped.count()
print(f"Deduplication: {before:,} → {after:,} (removed {before - after:,} duplicates)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Orders — Null Handling & Enrichment

# COMMAND ----------

df_restaurants = spark.table("zipdrop.bronze.restaurants_raw").select(
    col("restaurant_id"), col("zone").alias("restaurant_zone"))

df_enriched = (df_deduped
    .join(df_restaurants, on="restaurant_id", how="left")
    # Fix missing zones from restaurant master
    .withColumn("zone", coalesce(
        when((col("zone").isNull()) | (col("zone") == ""), None).otherwise(col("zone")),
        col("restaurant_zone")))
    .drop("restaurant_zone")
    # Flag unassigned partners
    .withColumn("partner_status",
        when((col("partner_id").isNull()) | (col("partner_id") == ""), lit("unassigned"))
        .otherwise(lit("assigned")))
    # Derived time columns
    .withColumn("order_date", to_date("order_timestamp"))
    .withColumn("order_hour", hour("order_timestamp"))
    .withColumn("day_of_week", dayofweek("order_timestamp"))
    .withColumn("is_weekend", when(dayofweek("order_timestamp").isin(1, 7), 1).otherwise(0))
    .withColumn("is_lunch_hour", when(col("order_hour").between(11, 14), 1).otherwise(0))
    .withColumn("is_dinner_hour", when(col("order_hour").between(19, 22), 1).otherwise(0))
    .withColumn("is_late_night", when((col("order_hour") >= 23) | (col("order_hour") <= 5), 1).otherwise(0))
    # Derived metrics
    .withColumn("eta_overshoot_mins",
        when(col("actual_delivery_mins").isNotNull(),
             col("actual_delivery_mins") - col("estimated_eta_mins")))
    .withColumn("is_cancelled", when(col("status") == "cancelled", 1).otherwise(0))
    .withColumn("net_order_value",
        coalesce(col("order_value_inr"), lit(0)) - coalesce(col("discount_inr"), lit(0))))

df_enriched.write.mode("overwrite").format("delta").saveAsTable("zipdrop.silver.orders_clean")

remaining_nulls = df_enriched.filter(col("zone").isNull()).count()
print(f"silver.orders_clean: {df_enriched.count():,} rows | Null zones remaining: {remaining_nulls}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Restaurants — Silver

# COMMAND ----------

(spark.table("zipdrop.bronze.restaurants_raw")
    .withColumn("avg_prep_time_mins", col("avg_prep_time_mins").cast("int"))
    .withColumn("rating", col("rating").cast("double"))
    .withColumn("is_premium", col("is_premium").cast("int"))
    .withColumn("lat", col("lat").cast("double"))
    .withColumn("lng", col("lng").cast("double"))
    .write.mode("overwrite").format("delta").saveAsTable("zipdrop.silver.restaurants_clean"))
print(f"silver.restaurants_clean: {spark.table('zipdrop.silver.restaurants_clean').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delivery Partners — Silver

# COMMAND ----------

(spark.table("zipdrop.bronze.delivery_partners_raw")
    .withColumn("avg_rating", col("avg_rating").cast("double"))
    .withColumn("total_deliveries", col("total_deliveries").cast("int"))
    .withColumn("experience_months", col("experience_months").cast("int"))
    .withColumn("is_active", when(col("status") == "active", 1).otherwise(0))
    .write.mode("overwrite").format("delta").saveAsTable("zipdrop.silver.delivery_partners_clean"))
print(f"silver.delivery_partners_clean: {spark.table('zipdrop.silver.delivery_partners_clean').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Weather — Silver

# COMMAND ----------

(spark.table("zipdrop.bronze.weather_hourly_raw")
    .withColumn("timestamp", to_timestamp("timestamp"))
    .withColumn("weather_date", to_date("timestamp"))
    .withColumn("weather_hour", hour("timestamp"))
    .withColumn("is_rainy", when(col("condition").isin("Light Rain", "Heavy Rain", "Thunderstorm", "Drizzle"), 1).otherwise(0))
    .write.mode("overwrite").format("delta").saveAsTable("zipdrop.silver.weather_clean"))
print(f"silver.weather_clean: {spark.table('zipdrop.silver.weather_clean').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Local Events — Silver (Explode multi-zone)

# COMMAND ----------

(spark.table("zipdrop.bronze.local_events_raw")
    .withColumn("date", to_date("date"))
    .withColumn("expected_crowd", col("expected_crowd").cast("int"))
    .withColumn("demand_multiplier", col("demand_multiplier").cast("double"))
    .withColumn("zone", explode(split(col("zones_impacted"), ",")))
    .withColumn("zone", trim(col("zone")))
    .write.mode("overwrite").format("delta").saveAsTable("zipdrop.silver.local_events_clean"))
print(f"silver.local_events_clean: {spark.table('zipdrop.silver.local_events_clean').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders_clean' AS tbl, count(*) AS rows FROM zipdrop.silver.orders_clean
# MAGIC UNION ALL SELECT 'restaurants_clean', count(*) FROM zipdrop.silver.restaurants_clean
# MAGIC UNION ALL SELECT 'delivery_partners_clean', count(*) FROM zipdrop.silver.delivery_partners_clean
# MAGIC UNION ALL SELECT 'weather_clean', count(*) FROM zipdrop.silver.weather_clean
# MAGIC UNION ALL SELECT 'local_events_clean', count(*) FROM zipdrop.silver.local_events_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer complete! Run `03_gold_aggregations` next.
