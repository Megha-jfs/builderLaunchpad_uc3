# Starter Notebook Guide (Databricks Community Edition)

## Suggested Notebook Structure

```
builder_launchpad/
├── 01_setup_and_bronze        # Download CSVs from GitHub → DBFS → Bronze Delta tables
├── 02_silver_cleaning         # Dedup, null handling, schema enforcement, derived columns
├── 03_gold_aggregations       # Zone-demand-hourly, supply-demand ratio, customer 360
├── 04_analysis_dashboard      # 10 analytics queries with visualizations
└── 05_ml_demand_forecast      # (Tier 2) MLflow-tracked demand prediction model
```

## Notebook 1: Setup & Bronze Ingestion

```python
# Download data from GitHub → DBFS → Delta tables
import urllib.request, os

base_url = "https://raw.githubusercontent.com/Megha-jfs/builderLaunchpad_uc3/main/data"
local_dir = "/tmp/builder_launchpad"
os.makedirs(local_dir, exist_ok=True)

files = ["orders.csv", "restaurants.csv", "delivery_partners.csv", "weather_hourly.csv", "local_events.csv"]
for f in files:
    urllib.request.urlretrieve(f"{base_url}/{f}", f"{local_dir}/{f}")

# Copy to DBFS and create Bronze Delta tables
spark.sql("CREATE DATABASE IF NOT EXISTS zipdrop_bronze")
df_orders = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/builder_launchpad/raw/orders.csv")
df_orders.write.mode("overwrite").format("delta").saveAsTable("zipdrop_bronze.orders_raw")
```

## Notebook 2: Silver Cleaning (Key Logic)

```python
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Deduplication
window_dedup = Window.partitionBy("order_id").orderBy("order_timestamp")
df_deduped = (df_orders_raw
    .withColumn("row_num", row_number().over(window_dedup))
    .filter(col("row_num") == 1)
    .drop("row_num"))

# Infer missing zone from restaurant master
df_with_zone = (df_deduped
    .join(df_restaurants.select("restaurant_id", col("zone").alias("rest_zone")),
          on="restaurant_id", how="left")
    .withColumn("zone_clean", coalesce(col("zone"), col("rest_zone"))))

# Derived columns
df_silver = (df_with_zone
    .withColumn("order_date", to_date("order_timestamp"))
    .withColumn("order_hour", hour("order_timestamp"))
    .withColumn("is_weekend", when(dayofweek("order_timestamp").isin(1, 7), 1).otherwise(0))
    .withColumn("eta_overshoot_mins", col("actual_delivery_mins") - col("estimated_eta_mins"))
    .withColumn("is_cancelled", when(col("status") == "cancelled", 1).otherwise(0)))
```

## Notebook 3: Gold Aggregations

```python
# Zone demand hourly — enriched with weather + events
gold_zone_demand = (df_orders_silver
    .groupBy("order_date", "order_hour", "zone")
    .agg(
        count("order_id").alias("total_orders"),
        avg("order_value_inr").alias("avg_order_value"),
        avg("actual_delivery_mins").alias("avg_delivery_mins"),
        sum("is_cancelled").alias("total_cancellations"),
        avg("surge_multiplier").alias("avg_surge"),
        max("rain_at_order_time").alias("rain_flag"),
        max("event_active").alias("event_flag")))

# Demand-supply ratio with zone health status
# CRITICAL (>3), STRESSED (>2), MODERATE (>1), HEALTHY (<=1)
```

## Notebook 4: Analytics & Visualizations

Use `display()` for quick Databricks built-in charts, or matplotlib/plotly for richer visuals:

```python
import plotly.express as px

# Zone heatmap — demand by zone x hour
pdf = spark.table("zipdrop_gold.demand_supply_ratio").toPandas()
fig = px.density_heatmap(pdf, x="order_hour", y="zone",
    z="demand_supply_ratio", color_continuous_scale="RdYlGn_r",
    title="Demand-Supply Ratio by Zone & Hour")
fig.show()

# Rain impact comparison
pdf_rain = spark.sql("SELECT ... GROUP BY weather").toPandas()
fig = px.bar(pdf_rain, x="weather", y=["avg_orders", "avg_cancel_rate"],
    barmode="group", title="Rain Impact on Orders & Cancellations")
fig.show()
```

## Notebook 5: ML Demand Forecasting (Tier 2)

```python
import mlflow
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split

# Feature engineering
features = ["order_hour", "day_of_week", "is_weekend", "rain_flag",
            "event_flag", "active_partners", "avg_prep_time"]
X = pdf_features[features]
y = pdf_features["total_orders"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Train with MLflow tracking
with mlflow.start_run(run_name="demand_forecast_gbr"):
    model = GradientBoostingRegressor(n_estimators=200, max_depth=5)
    model.fit(X_train, y_train)

    mlflow.log_params({"n_estimators": 200, "max_depth": 5})
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2_score)
    mlflow.sklearn.log_model(model, "demand_model")
```

## Vibe Coding Prompts (for AI-assisted development)

Use these prompts with your AI coding assistant to accelerate:

1. "Generate a PySpark pipeline that reads orders.csv, deduplicates on order_id, fills missing zones from restaurant master, and saves as a Silver Delta table on hive_metastore"

2. "Create a plotly dashboard in a Databricks notebook with 4 charts: zone heatmap by demand, hourly order pattern line chart, rain impact bar chart, and cancellation rate by zone"

3. "Build a scikit-learn demand forecasting model with these features: hour, day_of_week, is_weekend, rain, event_active, zone. Track the experiment in MLflow with parameters and metrics logged"

4. "Write a demand prediction function using weighted moving average: 70% same-hour-last-week + 20% yesterday-same-hour + 10% rolling-7-day-average, with multipliers for rain (+40%) and active events (+80%)"

5. "Create an interactive Databricks notebook with widgets for zone and date_range that dynamically shows demand forecast, recommended surge, and partner rebalancing for the selected inputs"
