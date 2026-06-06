# Databricks notebook source
# MAGIC %md
# MAGIC # ZipDrop — ML Demand Forecasting with MLflow
# MAGIC
# MAGIC Trains a demand prediction model, tracks experiments in MLflow, and registers the best model to Unity Catalog.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder

mlflow.set_registry_uri("databricks-uc")
experiment_path = "/Users/megha.upadhyay@databricks.com/zipdrop_demand_forecast"
mlflow.set_experiment(experiment_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Feature Engineering

# COMMAND ----------

pdf = spark.sql("""
    SELECT
        d.zone, d.order_date, d.order_hour, d.total_orders,
        d.avg_order_value, d.avg_surge, d.rain_flag, d.event_flag,
        d.is_rainy, d.is_weekend,
        COALESCE(d.demand_multiplier, 1.0) AS demand_multiplier,
        COALESCE(d.rain_mm, 0) AS rain_mm,
        COALESCE(d.temperature_c, 30) AS temperature_c,
        DAYOFWEEK(d.order_date) AS day_of_week,
        CASE WHEN d.order_hour BETWEEN 11 AND 14 THEN 1 ELSE 0 END AS is_lunch,
        CASE WHEN d.order_hour BETWEEN 19 AND 22 THEN 1 ELSE 0 END AS is_dinner,
        CASE WHEN d.order_hour >= 23 OR d.order_hour <= 5 THEN 1 ELSE 0 END AS is_late_night,
        p.active_partners
    FROM zipdrop.gold.zone_demand_hourly d
    LEFT JOIN zipdrop.gold.partner_availability p ON d.zone = p.zone
    WHERE d.zone IS NOT NULL AND d.total_orders IS NOT NULL
""").toPandas()

le = LabelEncoder()
pdf["zone_encoded"] = le.fit_transform(pdf["zone"])

# Interaction features
pdf["rain_x_dinner"] = pdf["rain_flag"] * pdf["is_dinner"]
pdf["event_x_evening"] = pdf["event_flag"] * pdf["is_dinner"]
pdf["weekend_x_lunch"] = pdf["is_weekend"] * pdf["is_lunch"]

print(f"Feature dataset: {pdf.shape[0]:,} rows, {pdf.shape[1]} columns")
print(f"Target mean: {pdf['total_orders'].mean():.1f}, std: {pdf['total_orders'].std():.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Train-Test Split

# COMMAND ----------

feature_cols = [
    "order_hour", "day_of_week", "is_weekend", "is_lunch", "is_dinner",
    "is_late_night", "rain_flag", "event_flag", "demand_multiplier",
    "active_partners", "rain_mm", "temperature_c", "zone_encoded",
    "rain_x_dinner", "event_x_evening", "weekend_x_lunch"
]

X = pdf[feature_cols]
y = pdf["total_orders"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Train: {X_train.shape[0]:,} | Test: {X_test.shape[0]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Model 1 — Gradient Boosting

# COMMAND ----------

with mlflow.start_run(run_name="gradient_boosting_v1") as run:
    params = {"n_estimators": 300, "max_depth": 6, "learning_rate": 0.1, "min_samples_split": 10}
    model_gb = GradientBoostingRegressor(**params, random_state=42)
    model_gb.fit(X_train, y_train)

    y_pred = model_gb.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    mlflow.log_params(params)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)
    mlflow.sklearn.log_model(model_gb, "demand_model",
                              input_example=X_test.iloc[:5],
                              registered_model_name="zipdrop.gold.demand_forecast_model")

    gb_run_id = run.info.run_id
    print(f"Gradient Boosting — RMSE: {rmse:.2f}, MAE: {mae:.2f}, R²: {r2:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model 2 — Random Forest

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

import matplotlib.pyplot as plt

importance = pd.DataFrame({
    "feature": feature_cols,
    "importance": model_gb.feature_importances_
}).sort_values("importance", ascending=True)

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(importance["feature"], importance["importance"], color="#FF3621")
ax.set_xlabel("Importance")
ax.set_title("What Drives Demand? — Feature Importance (Gradient Boosting)")
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Predicted vs Actual

# COMMAND ----------

fig, ax = plt.subplots(figsize=(8, 8))
ax.scatter(y_test, y_pred, alpha=0.3, s=10, color="#1B3139")
max_val = max(y_test.max(), y_pred.max())
ax.plot([0, max_val], [0, max_val], "r--", linewidth=2, label="Perfect prediction")
ax.set_xlabel("Actual Orders")
ax.set_ylabel("Predicted Orders")
ax.set_title(f"Demand Prediction Accuracy (R² = {r2:.4f})")
ax.legend()
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Zone-Level Next Hour Simulation
# MAGIC *Scenario: 8 PM, Rainy IPL Night*

# COMMAND ----------

zones = pdf["zone"].unique()
predictions = []

for zone in zones:
    zone_data = pdf[pdf["zone"] == zone].iloc[-1:].copy()
    zone_data["order_hour"] = 20
    zone_data["is_dinner"] = 1
    zone_data["rain_flag"] = 1
    zone_data["event_flag"] = 1
    zone_data["rain_x_dinner"] = 1
    zone_data["event_x_evening"] = 1
    pred = model_gb.predict(zone_data[feature_cols])[0]
    active = zone_data["active_partners"].values[0]
    ds_ratio = round(pred / max(active, 1), 2)
    predictions.append({
        "zone": zone,
        "predicted_orders": round(pred),
        "active_partners": int(active),
        "ds_ratio": ds_ratio,
        "status": "CRITICAL" if ds_ratio > 3 else ("STRESSED" if ds_ratio > 2 else ("MODERATE" if ds_ratio > 1 else "HEALTHY")),
        "recommended_surge": round(min(ds_ratio * 0.3 + 1.0, 3.0), 2) if ds_ratio > 1.5 else 1.0,
        "partners_needed": max(0, round(pred / 2 - active * 0.3)) if ds_ratio > 2 else 0
    })

pdf_pred = pd.DataFrame(predictions).sort_values("ds_ratio", ascending=False)
display(spark.createDataFrame(pdf_pred))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Save Predictions as Gold Table

# COMMAND ----------

spark.createDataFrame(pdf_pred).write.mode("overwrite").format("delta").saveAsTable("zipdrop.gold.demand_predictions")
print("Predictions saved to zipdrop.gold.demand_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ML pipeline complete!
# MAGIC
# MAGIC - **MLflow experiment:** Check the Experiments tab for run comparison
# MAGIC - **Registered model:** `zipdrop.gold.demand_forecast_model` in Unity Catalog
# MAGIC - **Predictions table:** `zipdrop.gold.demand_predictions` — ready for Lakeview Dashboard
