# Starter Notebook Guide

## Suggested Notebook Structure

```
notebooks/
├── 01_bronze_ingestion.py        # Raw CSV → Bronze Delta tables
├── 02_silver_cleaning.py         # Dedup, null handling, schema enforcement
├── 03_gold_aggregations.py       # Zone-demand-hourly, supply-demand ratio
├── 04_dashboard_queries.sql      # SQL queries backing the Lakeview dashboard
└── 05_genie_semantic_layer.sql   # Metric definitions for Genie Space
```

## Notebook 1: Bronze Ingestion (Starter Code)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC Ingest all 5 CSV files into Delta Bronze tables with minimal transformation.

catalog = "zipdrop"
schema = "bronze"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Ingest orders (largest table)
df_orders = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/{catalog}/{schema}/raw/orders.csv"))

df_orders.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.orders_raw")

# Repeat for other tables...
# restaurants, delivery_partners, weather_hourly, local_events
```

## Notebook 2: Silver Cleaning (Key Logic)

```python
# MAGIC %md
# MAGIC # Silver Layer — Clean & Deduplicate

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# --- Deduplication ---
window_dedup = Window.partitionBy("order_id").orderBy("order_timestamp")
df_deduped = (df_orders_raw
    .withColumn("row_num", row_number().over(window_dedup))
    .filter(col("row_num") == 1)
    .drop("row_num"))

# --- Null Handling ---
# Infer missing zone from restaurant master
df_with_zone = (df_deduped
    .alias("o")
    .join(df_restaurants.select("restaurant_id", col("zone").alias("rest_zone")).alias("r"),
          on="restaurant_id", how="left")
    .withColumn("zone_clean", coalesce(col("o.zone"), col("rest_zone")))
    .drop("rest_zone"))

# Flag unassigned orders
df_clean = df_with_zone.withColumn(
    "partner_status",
    when(col("partner_id").isNull(), "unassigned").otherwise("assigned"))

# --- Schema Enforcement ---
df_typed = (df_clean
    .withColumn("order_timestamp", to_timestamp("order_timestamp"))
    .withColumn("delivery_timestamp", to_timestamp("delivery_timestamp"))
    .withColumn("order_value_inr", col("order_value_inr").cast("double"))
    .withColumn("surge_multiplier", col("surge_multiplier").cast("double")))
```

## Notebook 3: Gold Aggregations (Core Tables)

```python
# MAGIC %md
# MAGIC # Gold Layer — Analytics-Ready Tables

# --- Zone Demand Hourly ---
gold_zone_demand = (df_silver_orders
    .withColumn("order_hour", hour("order_timestamp"))
    .withColumn("order_date", to_date("order_timestamp"))
    .groupBy("order_date", "order_hour", "zone_clean")
    .agg(
        count("order_id").alias("total_orders"),
        avg("order_value_inr").alias("avg_order_value"),
        avg("actual_delivery_mins").alias("avg_delivery_mins"),
        sum(when(col("status") == "cancelled", 1).otherwise(0)).alias("cancellations"),
        avg("surge_multiplier").alias("avg_surge"),
        max("rain_at_order_time").alias("rain_flag"),
        max("event_active").alias("event_flag")
    ))

# --- Partner Availability per Zone-Hour ---
# (Simplified — in production this would be streaming GPS data)
gold_partner_supply = (df_silver_partners
    .filter(col("status") == "active")
    .groupBy("zone")
    .count()
    .withColumnRenamed("count", "available_partners"))

# --- Demand-Supply Ratio ---
gold_ratio = (gold_zone_demand
    .join(gold_partner_supply, gold_zone_demand.zone_clean == gold_partner_supply.zone)
    .withColumn("demand_supply_ratio",
        col("total_orders") / col("available_partners"))
    .withColumn("zone_status",
        when(col("demand_supply_ratio") > 3, "critical")
        .when(col("demand_supply_ratio") > 2, "stressed")
        .otherwise("healthy")))
```

## Notebook 4: Dashboard Queries

```sql
-- Zone performance KPIs
SELECT
    zone_clean as zone,
    SUM(total_orders) as total_orders,
    ROUND(AVG(avg_delivery_mins), 1) as avg_eta_mins,
    ROUND(SUM(cancellations) / SUM(total_orders) * 100, 1) as cancel_rate_pct,
    ROUND(AVG(avg_surge), 2) as avg_surge_applied,
    ROUND(AVG(demand_supply_ratio), 1) as avg_ds_ratio
FROM gold.zone_demand_hourly
GROUP BY zone_clean
ORDER BY total_orders DESC;

-- Spike detection: hours where demand > 2x baseline
WITH baseline AS (
    SELECT zone_clean, AVG(total_orders) as baseline_orders
    FROM gold.zone_demand_hourly
    GROUP BY zone_clean
)
SELECT z.*, b.baseline_orders,
    ROUND(z.total_orders / b.baseline_orders, 1) as spike_factor
FROM gold.zone_demand_hourly z
JOIN baseline b ON z.zone_clean = b.zone_clean
WHERE z.total_orders > 2 * b.baseline_orders
ORDER BY spike_factor DESC;
```

## Notebook 5: Genie Space Semantic Layer

```sql
-- Metric definitions for Genie Space natural-language queries
-- These help Genie understand the domain vocabulary

-- "demand" = total_orders per zone per hour
-- "supply" = available active partners in a zone
-- "surge" = surge_multiplier applied to pricing
-- "spike" = when demand > 2x the zone's 30-day hourly average
-- "stressed zone" = demand_supply_ratio > 2
-- "critical zone" = demand_supply_ratio > 3
-- "cancellation rate" = cancellations / total_orders * 100
-- "ETA overshoot" = actual_delivery_mins - estimated_eta_mins
-- "rain impact" = compare metrics where rain_flag=1 vs rain_flag=0
-- "event impact" = compare metrics where event_flag=1 vs event_flag=0
```

## Vibe Coding Prompts (for AI-assisted development)

Use these prompts with Claude/Cursor/GitHub Copilot to accelerate:

1. "Generate a DLT pipeline that reads orders.csv, deduplicates on order_id, fills missing zones from restaurant master, and outputs a clean Silver table"

2. "Create a Lakeview dashboard JSON with 4 tiles: zone heatmap by demand, line chart of hourly orders, bar chart of cancellation rate by zone, and KPI cards for avg ETA and surge"

3. "Write a Genie Space instruction set that teaches the model about food delivery domain — zones, demand, supply, surge, spike detection"

4. "Build a simple demand forecasting function using weighted moving average: 70% same-hour-last-week + 20% yesterday-same-hour + 10% rolling-7-day-average, with multipliers for rain (+40%) and active events (+80%)"
