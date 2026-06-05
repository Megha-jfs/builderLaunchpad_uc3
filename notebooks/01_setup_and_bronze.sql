-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UC3: Food Delivery Demand Forecasting — Setup & Bronze Layer
-- MAGIC **ZipDrop** — Real-time demand surge prediction for Bangalore food delivery
-- MAGIC
-- MAGIC This notebook creates Bronze Delta tables from raw CSVs in the Unity Catalog Volume.
-- MAGIC
-- MAGIC **Run on:** Serverless SQL Warehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Use the catalog and schema

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA zipdrop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Verify raw files are in the volume

-- COMMAND ----------

LIST '/Volumes/workspace/zipdrop/raw_data/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Create Bronze Tables

-- COMMAND ----------

-- Orders (~410K rows — intentionally messy)
CREATE OR REPLACE TABLE bronze_orders
USING CSV
OPTIONS (
  path '/Volumes/workspace/zipdrop/raw_data/orders.csv',
  header 'true',
  inferSchema 'true'
);

SELECT 'bronze_orders' AS table_name, count(*) AS row_count FROM bronze_orders;

-- COMMAND ----------

-- Restaurants (2,000 rows)
CREATE OR REPLACE TABLE bronze_restaurants
USING CSV
OPTIONS (
  path '/Volumes/workspace/zipdrop/raw_data/restaurants.csv',
  header 'true',
  inferSchema 'true'
);

SELECT 'bronze_restaurants' AS table_name, count(*) AS row_count FROM bronze_restaurants;

-- COMMAND ----------

-- Delivery Partners (5,000 rows)
CREATE OR REPLACE TABLE bronze_delivery_partners
USING CSV
OPTIONS (
  path '/Volumes/workspace/zipdrop/raw_data/delivery_partners.csv',
  header 'true',
  inferSchema 'true'
);

SELECT 'bronze_delivery_partners' AS table_name, count(*) AS row_count FROM bronze_delivery_partners;

-- COMMAND ----------

-- Weather Hourly (720 rows)
CREATE OR REPLACE TABLE bronze_weather_hourly
USING CSV
OPTIONS (
  path '/Volumes/workspace/zipdrop/raw_data/weather_hourly.csv',
  header 'true',
  inferSchema 'true'
);

SELECT 'bronze_weather_hourly' AS table_name, count(*) AS row_count FROM bronze_weather_hourly;

-- COMMAND ----------

-- Local Events (50 rows)
CREATE OR REPLACE TABLE bronze_local_events
USING CSV
OPTIONS (
  path '/Volumes/workspace/zipdrop/raw_data/local_events.csv',
  header 'true',
  inferSchema 'true'
);

SELECT 'bronze_local_events' AS table_name, count(*) AS row_count FROM bronze_local_events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Validate Bronze Layer

-- COMMAND ----------

SELECT 'bronze_orders' AS tbl, count(*) AS rows FROM bronze_orders
UNION ALL SELECT 'bronze_restaurants', count(*) FROM bronze_restaurants
UNION ALL SELECT 'bronze_delivery_partners', count(*) FROM bronze_delivery_partners
UNION ALL SELECT 'bronze_weather_hourly', count(*) FROM bronze_weather_hourly
UNION ALL SELECT 'bronze_local_events', count(*) FROM bronze_local_events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Data Quality Check — What's Messy?

-- COMMAND ----------

SELECT
    count(*) AS total_rows,
    count(*) - count(DISTINCT order_id) AS duplicate_orders,
    ROUND((count(*) - count(DISTINCT order_id)) / count(*) * 100, 2) AS duplicate_pct,
    SUM(CASE WHEN zone IS NULL OR zone = '' THEN 1 ELSE 0 END) AS null_zones,
    ROUND(SUM(CASE WHEN zone IS NULL OR zone = '' THEN 1 ELSE 0 END) / count(*) * 100, 2) AS null_zone_pct,
    SUM(CASE WHEN partner_id IS NULL OR partner_id = '' THEN 1 ELSE 0 END) AS null_partners,
    ROUND(SUM(CASE WHEN partner_id IS NULL OR partner_id = '' THEN 1 ELSE 0 END) / count(*) * 100, 2) AS null_partner_pct,
    SUM(CASE WHEN order_value_inr IS NULL THEN 1 ELSE 0 END) AS null_order_value,
    ROUND(SUM(CASE WHEN order_value_inr IS NULL THEN 1 ELSE 0 END) / count(*) * 100, 2) AS null_value_pct
FROM bronze_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze layer complete! Run notebook `02_silver_cleaning` next.
