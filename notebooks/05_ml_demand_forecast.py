# Databricks notebook source
# MAGIC %md
# MAGIC # UC3: Tier 2 — ML Demand Forecasting with MLflow
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Engineers features from Gold tables
# MAGIC 2. Trains a demand prediction model (scikit-learn)
# MAGIC 3. Tracks experiments in MLflow
# MAGIC 4. Generates zone-level demand predictions

# COMMAND ----------

# MAGIC %pip install plotly
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import plotly.express as px
import plotly.graph_objects as go

mlflow.set_experiment("/Users/rnu.megha@gmail.com/zipdrop_demand_forecast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Feature Engineering

# COMMAND ----------

pdf_features = spark.sql("""
    SELECT
        d.zone,
        d.order_date,
        d.order_hour,
        d.total_orders,
        d.total_cancellations,
        d.avg_order_value,
        d.avg_surge,
        d.rain_flag,
        d.event_flag,
        COALESCE(d.demand_multiplier, 1.0) AS demand_multiplier,
        dayofweek(d.order_date) AS day_of_week,
        CASE WHEN dayofweek(d.order_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
        CASE WHEN d.order_hour BETWEEN 11 AND 14 THEN 1 ELSE 0 END AS is_lunch_hour,
        CASE WHEN d.order_hour BETWEEN 19 AND 22 THEN 1 ELSE 0 END AS is_dinner_hour,
        CASE WHEN d.order_hour >= 23 OR d.order_hour <= 5 THEN 1 ELSE 0 END AS is_late_night,
        p.active_partners,
        p.avg_partner_rating,
        COALESCE(d.rain_mm, 0) AS rain_mm,
        COALESCE(d.temperature_c, 30) AS temperature_c
    FROM zipdrop_gold.zone_demand_hourly d
    LEFT JOIN zipdrop_gold.partner_availability p ON d.zone = p.zone
    WHERE d.zone IS NOT NULL AND d.total_orders IS NOT NULL
""").toPandas()

le_zone = LabelEncoder()
pdf_features["zone_encoded"] = le_zone.fit_transform(pdf_features["zone"])

# Interaction features
pdf_features["rain_x_dinner"] = pdf_features["rain_flag"] * pdf_features["is_dinner_hour"]
pdf_features["event_x_evening"] = pdf_features["event_flag"] * pdf_features["is_dinner_hour"]
pdf_features["weekend_x_lunch"] = pdf_features["is_weekend"] * pdf_features["is_lunch_hour"]

print(f"Feature dataset: {pdf_features.shape[0]:,} rows, {pdf_features.shape[1]} columns")
print(f"Target (total_orders) — mean: {pdf_features['total_orders'].mean():.1f}, std: {pdf_features['total_orders'].std():.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Train-Test Split

# COMMAND ----------

feature_cols = [
    "order_hour", "day_of_week", "is_weekend", "is_lunch_hour", "is_dinner_hour",
    "is_late_night", "rain_flag", "event_flag", "demand_multiplier", "active_partners",
    "rain_mm", "temperature_c", "zone_encoded",
    "rain_x_dinner", "event_x_evening", "weekend_x_lunch"
]

X = pdf_features[feature_cols]
y = pdf_features["total_orders"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Train: {X_train.shape[0]:,} rows | Test: {X_test.shape[0]:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Model Training — Gradient Boosting (MLflow Tracked)

# COMMAND ----------

with mlflow.start_run(run_name="gradient_boosting_v1"):
    params = {"n_estimators": 300, "max_depth": 6, "learning_rate": 0.1, "min_samples_split": 10}

    model_gb = GradientBoostingRegressor(**params, random_state=42)
    model_gb.fit(X_train, y_train)

    y_pred = model_gb.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    mlflow.log_params(params)
    mlflow.log_params({"features": len(feature_cols), "train_size": len(X_train), "test_size": len(X_test)})
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)
    mlflow.sklearn.log_model(model_gb, "demand_model")

    print(f"Gradient Boosting — RMSE: {rmse:.2f}, MAE: {mae:.2f}, R²: {r2:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model Training — Random Forest (Comparison)

# COMMAND ----------

with mlflow.start_run(run_name="random_forest_v1"):
    params_rf = {"n_estimators": 200, "max_depth": 8, "min_samples_split": 10}

    model_rf = RandomForestRegressor(**params_rf, random_state=42)
    model_rf.fit(X_train, y_train)

    y_pred_rf = model_rf.predict(X_test)
    rmse_rf = np.sqrt(mean_squared_error(y_test, y_pred_rf))
    mae_rf = mean_absolute_error(y_test, y_pred_rf)
    r2_rf = r2_score(y_test, y_pred_rf)

    mlflow.log_params(params_rf)
    mlflow.log_metric("rmse", rmse_rf)
    mlflow.log_metric("mae", mae_rf)
    mlflow.log_metric("r2", r2_rf)
    mlflow.sklearn.log_model(model_rf, "demand_model")

    print(f"Random Forest — RMSE: {rmse_rf:.2f}, MAE: {mae_rf:.2f}, R²: {r2_rf:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Feature Importance

# COMMAND ----------

importance = pd.DataFrame({
    "feature": feature_cols,
    "importance": model_gb.feature_importances_
}).sort_values("importance", ascending=True)

fig = px.bar(importance, x="importance", y="feature", orientation="h",
             title="Feature Importance — What Drives Demand?",
             color="importance", color_continuous_scale="Blues")
fig.update_layout(height=500)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Prediction vs Actual — Scatter

# COMMAND ----------

pdf_pred = pd.DataFrame({"actual": y_test, "predicted": y_pred})

fig = px.scatter(pdf_pred, x="actual", y="predicted", opacity=0.3,
                 title=f"Predicted vs Actual Demand (R² = {r2:.4f})",
                 labels={"actual": "Actual Orders", "predicted": "Predicted Orders"})
fig.add_shape(type="line", x0=0, y0=0, x1=pdf_pred["actual"].max(), y1=pdf_pred["actual"].max(),
              line=dict(dash="dash", color="red"))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Zone-Level Demand Predictions — Next Hour Simulation

# COMMAND ----------

# Simulate "next hour" prediction for all zones
zones = pdf_features["zone"].unique()
next_hour_data = []

sim_hour = 20  # 8 PM dinner rush
sim_rain = 1   # raining
sim_event = 1  # IPL match on

for zone in zones:
    zone_data = pdf_features[pdf_features["zone"] == zone].iloc[-1:].copy()
    zone_data["order_hour"] = sim_hour
    zone_data["is_dinner_hour"] = 1
    zone_data["rain_flag"] = sim_rain
    zone_data["event_flag"] = sim_event
    zone_data["rain_x_dinner"] = sim_rain * 1
    zone_data["event_x_evening"] = sim_event * 1
    pred = model_gb.predict(zone_data[feature_cols])[0]
    next_hour_data.append({
        "zone": zone,
        "predicted_orders": round(pred),
        "active_partners": zone_data["active_partners"].values[0],
        "predicted_ds_ratio": round(pred / max(zone_data["active_partners"].values[0], 1), 2)
    })

pdf_next = pd.DataFrame(next_hour_data).sort_values("predicted_ds_ratio", ascending=False)

pdf_next["status"] = pdf_next["predicted_ds_ratio"].apply(
    lambda x: "CRITICAL" if x > 3 else ("STRESSED" if x > 2 else ("MODERATE" if x > 1 else "HEALTHY")))
pdf_next["recommended_surge"] = pdf_next["predicted_ds_ratio"].apply(
    lambda x: round(min(x * 0.3 + 1.0, 3.0), 2) if x > 1.5 else 1.0)

print(f"\nSIMULATION: 8 PM | Rain: YES | IPL Match: YES\n{'='*60}")
display(spark.createDataFrame(pdf_next))

# COMMAND ----------

fig = px.bar(pdf_next, x="zone", y="predicted_orders", color="status",
             color_discrete_map={"CRITICAL": "#d32f2f", "STRESSED": "#f57c00", "MODERATE": "#fbc02d", "HEALTHY": "#388e3c"},
             title="Predicted Demand by Zone — 8 PM, Rainy IPL Night",
             text="predicted_orders")
fig.update_layout(xaxis_tickangle=-45, height=500)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. MLflow Experiment Summary
# MAGIC
# MAGIC View all runs: Click **Experiments** in the left sidebar → `zipdrop_demand_forecast`
# MAGIC
# MAGIC Compare model metrics, parameters, and artifacts directly in the MLflow UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ML Demand Forecasting complete!
# MAGIC
# MAGIC **What was built:**
# MAGIC - Feature engineering with interaction terms (rain×dinner, event×evening)
# MAGIC - 2 models compared (Gradient Boosting vs Random Forest) tracked in MLflow
# MAGIC - Feature importance analysis
# MAGIC - Zone-level next-hour demand simulation
# MAGIC - Actionable outputs: predicted orders, D/S ratio, surge recommendation, zone status
