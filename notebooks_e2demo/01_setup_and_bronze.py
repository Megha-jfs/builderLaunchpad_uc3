# Databricks notebook source
# MAGIC %md
# MAGIC # ZipDrop — Setup & Bronze Layer
# MAGIC **Real-Time Food Delivery Demand Forecasting & Surge Optimizer**
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Validates raw CSVs in the UC Volume
# MAGIC 2. Ingests into Bronze Delta tables with schema enforcement
# MAGIC 3. Runs a data quality audit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "zipdrop"
SCHEMA_BRONZE = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/raw_data"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Raw Files

# COMMAND ----------

files = dbutils.fs.ls(VOLUME_PATH)
for f in files:
    print(f"{f.name:30s} {f.size / 1024 / 1024:.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion — Orders (~410K rows, intentionally messy)

# COMMAND ----------

from pyspark.sql.types import *

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("partner_id", StringType()),
    StructField("zone", StringType()),
    StructField("order_timestamp", TimestampType()),
    StructField("delivery_timestamp", TimestampType()),
    StructField("order_value_inr", DoubleType()),
    StructField("discount_inr", DoubleType()),
    StructField("payment_mode", StringType()),
    StructField("status", StringType()),
    StructField("cancel_reason", StringType()),
    StructField("estimated_eta_mins", IntegerType()),
    StructField("actual_delivery_mins", IntegerType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("rain_at_order_time", IntegerType()),
    StructField("event_active", IntegerType()),
])

df_orders = (spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(f"{VOLUME_PATH}/orders.csv"))

df_orders.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.orders_raw")
print(f"bronze.orders_raw: {df_orders.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion — Reference Tables

# COMMAND ----------

# Restaurants
df_rest = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{VOLUME_PATH}/restaurants.csv")
df_rest.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.restaurants_raw")
print(f"bronze.restaurants_raw: {df_rest.count():,} rows")

# Delivery Partners
df_dp = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{VOLUME_PATH}/delivery_partners.csv")
df_dp.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.delivery_partners_raw")
print(f"bronze.delivery_partners_raw: {df_dp.count():,} rows")

# Weather
df_weather = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{VOLUME_PATH}/weather_hourly.csv")
df_weather.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.weather_hourly_raw")
print(f"bronze.weather_hourly_raw: {df_weather.count():,} rows")

# Local Events
df_events = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{VOLUME_PATH}/local_events.csv")
df_events.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.local_events_raw")
print(f"bronze.local_events_raw: {df_events.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Audit

# COMMAND ----------

from pyspark.sql.functions import col, count, when, lit, round as spark_round

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.orders_raw")
total = df.count()

quality_report = spark.createDataFrame([
    ("Total rows", total, 100.0),
    ("Duplicate order_ids", total - df.dropDuplicates(["order_id"]).count(),
     (total - df.dropDuplicates(["order_id"]).count()) / total * 100),
    ("Missing zone", df.filter((col("zone").isNull()) | (col("zone") == "")).count(),
     df.filter((col("zone").isNull()) | (col("zone") == "")).count() / total * 100),
    ("Missing partner_id", df.filter((col("partner_id").isNull()) | (col("partner_id") == "")).count(),
     df.filter((col("partner_id").isNull()) | (col("partner_id") == "")).count() / total * 100),
    ("Missing order_value", df.filter(col("order_value_inr").isNull()).count(),
     df.filter(col("order_value_inr").isNull()).count() / total * 100),
], ["metric", "count", "percentage"])

display(quality_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate All Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders_raw' AS tbl, count(*) AS rows FROM zipdrop.bronze.orders_raw
# MAGIC UNION ALL SELECT 'restaurants_raw', count(*) FROM zipdrop.bronze.restaurants_raw
# MAGIC UNION ALL SELECT 'delivery_partners_raw', count(*) FROM zipdrop.bronze.delivery_partners_raw
# MAGIC UNION ALL SELECT 'weather_hourly_raw', count(*) FROM zipdrop.bronze.weather_hourly_raw
# MAGIC UNION ALL SELECT 'local_events_raw', count(*) FROM zipdrop.bronze.local_events_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze layer complete! Run `02_silver_cleaning` next.
