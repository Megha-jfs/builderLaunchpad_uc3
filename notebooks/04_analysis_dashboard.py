# Databricks notebook source
# MAGIC %md
# MAGIC # UC3: Analysis & Visualizations
# MAGIC
# MAGIC 10 analytics views with charts using `display()` (Databricks built-in) and `plotly`.
# MAGIC
# MAGIC **Tip:** Click the chart icon on any `display()` output to switch between table/bar/line/scatter views.

# COMMAND ----------

# MAGIC %pip install plotly
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI Summary — Overall Platform Health

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     ROUND(SUM(net_order_value), 0) AS total_revenue_inr,
# MAGIC     ROUND(AVG(net_order_value), 0) AS avg_order_value,
# MAGIC     ROUND(AVG(actual_delivery_mins), 1) AS avg_delivery_mins,
# MAGIC     ROUND(SUM(is_cancelled) / COUNT(*) * 100, 1) AS overall_cancel_rate_pct,
# MAGIC     ROUND(AVG(eta_overshoot_mins), 1) AS avg_eta_overshoot_mins
# MAGIC FROM zipdrop_silver.orders_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Zone Performance Leaderboard

# COMMAND ----------

pdf_zone = spark.sql("""
    SELECT zone,
        SUM(total_orders) AS total_orders,
        ROUND(SUM(total_revenue), 0) AS total_revenue,
        ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
        ROUND(AVG(avg_eta_overshoot), 1) AS avg_eta_overshoot
    FROM zipdrop_gold.zone_demand_hourly
    WHERE zone IS NOT NULL
    GROUP BY zone
    ORDER BY total_orders DESC
""").toPandas()

fig = px.bar(pdf_zone, x="total_orders", y="zone", orientation="h",
             color="avg_cancel_rate", color_continuous_scale="RdYlGn_r",
             title="Zone Performance: Orders vs Cancellation Rate",
             labels={"total_orders": "Total Orders", "avg_cancel_rate": "Cancel Rate %"})
fig.update_layout(yaxis=dict(autorange="reversed"), height=600)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hourly Demand Pattern — Weekday vs Weekend

# COMMAND ----------

pdf_hourly = spark.sql("""
    SELECT order_hour,
        CASE WHEN dayofweek(order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        ROUND(AVG(total_orders), 0) AS avg_orders
    FROM zipdrop_gold.zone_demand_hourly
    GROUP BY order_hour, day_type
    ORDER BY order_hour
""").toPandas()

fig = px.line(pdf_hourly, x="order_hour", y="avg_orders", color="day_type",
              title="Hourly Demand Pattern: Weekday vs Weekend",
              labels={"order_hour": "Hour of Day", "avg_orders": "Avg Orders per Zone"})
fig.update_layout(xaxis=dict(dtick=1))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rain Impact Analysis

# COMMAND ----------

pdf_rain = spark.sql("""
    SELECT
        CASE WHEN is_rainy = 1 THEN 'Rainy' ELSE 'Clear' END AS weather,
        ROUND(AVG(total_orders), 0) AS avg_orders,
        ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
        ROUND(AVG(avg_eta_overshoot), 1) AS avg_eta_overshoot
    FROM zipdrop_gold.zone_demand_hourly
    WHERE is_rainy IS NOT NULL
    GROUP BY weather
""").toPandas()

fig = make_subplots(rows=1, cols=3, subplot_titles=("Avg Orders", "Cancel Rate %", "ETA Overshoot (mins)"))
fig.add_trace(go.Bar(x=pdf_rain["weather"], y=pdf_rain["avg_orders"], marker_color=["#2196F3", "#FF9800"]), row=1, col=1)
fig.add_trace(go.Bar(x=pdf_rain["weather"], y=pdf_rain["avg_cancel_rate"], marker_color=["#2196F3", "#FF9800"]), row=1, col=2)
fig.add_trace(go.Bar(x=pdf_rain["weather"], y=pdf_rain["avg_eta_overshoot"], marker_color=["#2196F3", "#FF9800"]), row=1, col=3)
fig.update_layout(title="Rain Impact on Delivery Metrics", showlegend=False, height=400)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Event Days vs Normal Days

# COMMAND ----------

pdf_event = spark.sql("""
    SELECT
        CASE WHEN event_flag = 1 THEN 'Event Active' ELSE 'No Event' END AS event_status,
        ROUND(AVG(total_orders), 0) AS avg_orders,
        ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
        ROUND(AVG(avg_eta_overshoot), 1) AS avg_eta_overshoot,
        ROUND(AVG(avg_surge), 2) AS avg_surge
    FROM zipdrop_gold.zone_demand_hourly
    GROUP BY event_status
""").toPandas()

display(spark.createDataFrame(pdf_event))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Demand-Supply Heatmap by Zone x Hour

# COMMAND ----------

pdf_heatmap = spark.sql("""
    SELECT zone, order_hour,
        ROUND(AVG(demand_supply_ratio), 2) AS avg_ds_ratio
    FROM zipdrop_gold.demand_supply_ratio
    GROUP BY zone, order_hour
    ORDER BY zone, order_hour
""").toPandas()

pivot = pdf_heatmap.pivot(index="zone", columns="order_hour", values="avg_ds_ratio")

fig = px.imshow(pivot, color_continuous_scale="RdYlGn_r",
                title="Demand-Supply Ratio Heatmap (Red = Critical, Green = Healthy)",
                labels={"x": "Hour of Day", "y": "Zone", "color": "D/S Ratio"},
                aspect="auto")
fig.update_layout(height=700)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Surge Pricing — Current vs Recommended

# COMMAND ----------

pdf_surge = spark.sql("""
    SELECT zone, order_hour,
        ROUND(AVG(avg_surge), 2) AS current_surge,
        ROUND(AVG(recommended_surge), 2) AS recommended_surge,
        SUM(total_orders) AS total_orders
    FROM zipdrop_gold.demand_supply_ratio
    GROUP BY zone, order_hour
    HAVING AVG(recommended_surge) > 1.0
    ORDER BY recommended_surge DESC
    LIMIT 50
""").toPandas()

fig = px.scatter(pdf_surge, x="current_surge", y="recommended_surge",
                 size="total_orders", color="zone", hover_data=["order_hour"],
                 title="Surge Pricing: Current vs AI-Recommended",
                 labels={"current_surge": "Current Avg Surge", "recommended_surge": "Recommended Surge"})
fig.add_shape(type="line", x0=1, y0=1, x1=2.5, y1=2.5, line=dict(dash="dash", color="gray"))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Partner Rebalancing Recommendations

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH zone_load AS (
# MAGIC     SELECT
# MAGIC         d.zone,
# MAGIC         ROUND(AVG(d.demand_supply_ratio), 2) AS avg_ds_ratio,
# MAGIC         p.active_partners,
# MAGIC         ROUND(AVG(d.total_orders), 0) AS avg_hourly_orders,
# MAGIC         ROUND(AVG(d.cancellation_rate_pct), 1) AS avg_cancel_rate
# MAGIC     FROM zipdrop_gold.demand_supply_ratio d
# MAGIC     JOIN zipdrop_gold.partner_availability p ON d.zone = p.zone
# MAGIC     GROUP BY d.zone, p.active_partners
# MAGIC )
# MAGIC SELECT
# MAGIC     zone,
# MAGIC     active_partners,
# MAGIC     avg_hourly_orders,
# MAGIC     avg_ds_ratio,
# MAGIC     avg_cancel_rate,
# MAGIC     CASE
# MAGIC         WHEN avg_ds_ratio > 2.5 THEN CONCAT('NEEDS +', CAST(ROUND(avg_hourly_orders / 2 - active_partners * 0.3) AS INT), ' partners')
# MAGIC         WHEN avg_ds_ratio < 0.5 THEN CONCAT('EXCESS: move ', CAST(ROUND(active_partners * 0.2) AS INT), ' partners out')
# MAGIC         ELSE 'Balanced'
# MAGIC     END AS recommendation
# MAGIC FROM zone_load
# MAGIC ORDER BY avg_ds_ratio DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cancellation Reasons Breakdown

# COMMAND ----------

pdf_cancel = spark.sql("""
    SELECT
        CASE WHEN cancel_reason = '' OR cancel_reason IS NULL THEN 'unknown' ELSE cancel_reason END AS reason,
        COUNT(*) AS count
    FROM zipdrop_silver.orders_clean
    WHERE is_cancelled = 1
    GROUP BY reason
    ORDER BY count DESC
""").toPandas()

fig = px.pie(pdf_cancel, values="count", names="reason",
             title="Order Cancellation Reasons", hole=0.4)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Daily Trend — Orders, Revenue, Cancellations (30 days)

# COMMAND ----------

pdf_daily = spark.sql("""
    SELECT order_date,
        SUM(total_orders) AS daily_orders,
        ROUND(SUM(total_revenue), 0) AS daily_revenue,
        ROUND(AVG(cancellation_rate_pct), 1) AS cancel_rate,
        MAX(CASE WHEN event_name != 'None' THEN event_name ELSE NULL END) AS event_of_day
    FROM zipdrop_gold.zone_demand_hourly
    GROUP BY order_date
    ORDER BY order_date
""").toPandas()

fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                    subplot_titles=("Daily Orders (spikes = event days)", "Cancellation Rate %"))
fig.add_trace(go.Scatter(x=pdf_daily["order_date"], y=pdf_daily["daily_orders"],
                         mode="lines+markers", name="Orders",
                         text=pdf_daily["event_of_day"], hovertemplate="%{x}<br>Orders: %{y}<br>Event: %{text}"),
              row=1, col=1)
fig.add_trace(go.Scatter(x=pdf_daily["order_date"], y=pdf_daily["cancel_rate"],
                         mode="lines", name="Cancel Rate %", line=dict(color="red")),
              row=2, col=1)
fig.update_layout(height=500, title="30-Day Platform Trend")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Customer Segments

# COMMAND ----------

pdf_seg = spark.sql("""
    SELECT
        CASE
            WHEN total_orders >= 15 THEN 'Power User (15+)'
            WHEN total_orders >= 8 THEN 'Regular (8-14)'
            WHEN total_orders >= 3 THEN 'Occasional (3-7)'
            ELSE 'New (1-2)'
        END AS segment,
        COUNT(*) AS customers,
        ROUND(AVG(total_spend), 0) AS avg_total_spend,
        ROUND(AVG(cancel_rate_pct), 1) AS avg_cancel_rate
    FROM zipdrop_gold.customer_360
    GROUP BY segment
    ORDER BY customers DESC
""").toPandas()

fig = px.bar(pdf_seg, x="segment", y="customers", color="avg_cancel_rate",
             color_continuous_scale="RdYlGn_r", text="customers",
             title="Customer Segments by Order Frequency",
             labels={"customers": "# Customers", "avg_cancel_rate": "Cancel Rate %"})
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis complete!
# MAGIC
# MAGIC **Next step (Tier 2):** Run `05_ml_demand_forecast` for MLflow-tracked demand prediction.
