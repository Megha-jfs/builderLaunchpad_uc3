# Databricks notebook source
# MAGIC %md
# MAGIC # UC3: Food Delivery Demand Forecasting — Setup & Bronze Layer
# MAGIC **ZipDrop** — Real-time demand surge prediction for Bangalore food delivery
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Downloads datasets from GitHub
# MAGIC 2. Creates the Bronze Delta tables (raw ingestion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download Datasets from GitHub

# COMMAND ----------

import urllib.request
import os

base_url = "https://raw.githubusercontent.com/Megha-jfs/builderLaunchpad_uc3/main/data"
local_dir = "/tmp/builder_launchpad"

os.makedirs(local_dir, exist_ok=True)

files = ["orders.csv", "restaurants.csv", "delivery_partners.csv", "weather_hourly.csv", "local_events.csv"]

for f in files:
    url = f"{base_url}/{f}"
    dest = f"{local_dir}/{f}"
    print(f"Downloading {f}...")
    urllib.request.urlretrieve(url, dest)
    print(f"  Saved to {dest}")

print("\nAll files downloaded!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Copy to DBFS (Volumes)

# COMMAND ----------

dbfs_path = "dbfs:/builder_launchpad/raw"

for f in files:
    dbutils.fs.cp(f"file:/tmp/builder_launchpad/{f}", f"{dbfs_path}/{f}")
    print(f"Copied {f} to DBFS")

print("\nAll files in DBFS:")
display(dbutils.fs.ls(dbfs_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS zipdrop_bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS zipdrop_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS zipdrop_gold")
print("Databases created: zipdrop_bronze, zipdrop_silver, zipdrop_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Bronze Tables — Raw Ingestion

# COMMAND ----------

# Orders (largest — ~410K rows)
df_orders = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{dbfs_path}/orders.csv"))

df_orders.write.mode("overwrite").format("delta").saveAsTable("zipdrop_bronze.orders_raw")
print(f"zipdrop_bronze.orders_raw: {df_orders.count()} rows")

# COMMAND ----------

# Restaurants
df_restaurants = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{dbfs_path}/restaurants.csv"))

df_restaurants.write.mode("overwrite").format("delta").saveAsTable("zipdrop_bronze.restaurants_raw")
print(f"zipdrop_bronze.restaurants_raw: {df_restaurants.count()} rows")

# COMMAND ----------

# Delivery Partners
df_partners = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{dbfs_path}/delivery_partners.csv"))

df_partners.write.mode("overwrite").format("delta").saveAsTable("zipdrop_bronze.delivery_partners_raw")
print(f"zipdrop_bronze.delivery_partners_raw: {df_partners.count()} rows")

# COMMAND ----------

# Weather Hourly
df_weather = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{dbfs_path}/weather_hourly.csv"))

df_weather.write.mode("overwrite").format("delta").saveAsTable("zipdrop_bronze.weather_hourly_raw")
print(f"zipdrop_bronze.weather_hourly_raw: {df_weather.count()} rows")

# COMMAND ----------

# Local Events
df_events = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{dbfs_path}/local_events.csv"))

df_events.write.mode("overwrite").format("delta").saveAsTable("zipdrop_bronze.local_events_raw")
print(f"zipdrop_bronze.local_events_raw: {df_events.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Quick Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate row counts
# MAGIC SELECT 'orders_raw' as table_name, count(*) as row_count FROM zipdrop_bronze.orders_raw
# MAGIC UNION ALL
# MAGIC SELECT 'restaurants_raw', count(*) FROM zipdrop_bronze.restaurants_raw
# MAGIC UNION ALL
# MAGIC SELECT 'delivery_partners_raw', count(*) FROM zipdrop_bronze.delivery_partners_raw
# MAGIC UNION ALL
# MAGIC SELECT 'weather_hourly_raw', count(*) FROM zipdrop_bronze.weather_hourly_raw
# MAGIC UNION ALL
# MAGIC SELECT 'local_events_raw', count(*) FROM zipdrop_bronze.local_events_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Check — What's Messy?

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

df = spark.table("zipdrop_bronze.orders_raw")

total = df.count()
dupes = total - df.dropDuplicates(["order_id"]).count()
null_zone = df.filter(col("zone").isNull() | (col("zone") == "")).count()
null_partner = df.filter(col("partner_id").isNull() | (col("partner_id") == "")).count()
null_value = df.filter(col("order_value_inr").isNull()).count()

print(f"""
DATA QUALITY REPORT — orders_raw
{'='*45}
Total rows:           {total:,}
Duplicate order_ids:  {dupes:,} ({dupes/total*100:.2f}%)
Missing zone:         {null_zone:,} ({null_zone/total*100:.2f}%)
Missing partner_id:   {null_partner:,} ({null_partner/total*100:.2f}%)
Missing order_value:  {null_value:,} ({null_value/total*100:.2f}%)
{'='*45}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze layer complete! Run notebook `02_silver_cleaning` next.
