# Databricks notebook source
# MAGIC %md
# MAGIC # ZipDrop — Genie Space Setup
# MAGIC
# MAGIC This notebook sets up the tables and instructions for a Genie Space
# MAGIC that lets ops teams ask natural-language questions about demand, supply, and surge.
# MAGIC
# MAGIC ## How to Create the Genie Space
# MAGIC
# MAGIC 1. In the Databricks sidebar, click **New** → **Genie space**
# MAGIC 2. Name it: **ZipDrop Ops Intelligence**
# MAGIC 3. Add the following tables:
# MAGIC    - `zipdrop.gold.zone_demand_hourly`
# MAGIC    - `zipdrop.gold.demand_supply_ratio`
# MAGIC    - `zipdrop.gold.customer_360`
# MAGIC    - `zipdrop.gold.partner_availability`
# MAGIC    - `zipdrop.gold.restaurant_performance`
# MAGIC    - `zipdrop.gold.demand_predictions`
# MAGIC 4. Paste the **General Instructions** below into the Genie Space instructions field
# MAGIC 5. Test with the sample questions listed below

# COMMAND ----------

# MAGIC %md
# MAGIC ## General Instructions (Copy into Genie Space)
# MAGIC
# MAGIC ```
# MAGIC You are the ZipDrop Ops Intelligence assistant for a food delivery platform in Bangalore.
# MAGIC
# MAGIC DOMAIN VOCABULARY:
# MAGIC - "demand" = total_orders per zone per hour
# MAGIC - "supply" = active_partners in a zone
# MAGIC - "D/S ratio" or "demand-supply ratio" = total_orders / active_partners
# MAGIC - "spike" = when demand > 2x the zone's average hourly orders
# MAGIC - "surge" = surge_multiplier applied to pricing (1.0 = no surge)
# MAGIC - "CRITICAL zone" = demand_supply_ratio > 3
# MAGIC - "STRESSED zone" = demand_supply_ratio > 2
# MAGIC - "ETA overshoot" = actual_delivery_mins - estimated_eta_mins (positive = late)
# MAGIC - "cancellation rate" = total_cancellations / total_orders * 100
# MAGIC - "rain impact" = compare metrics where is_rainy=1 vs is_rainy=0
# MAGIC - "event impact" = compare metrics where event_flag=1 vs event_flag=0
# MAGIC - "IPL days" = dates: May 5, 12, 18, 25
# MAGIC - "power user" = customer with >= 15 orders
# MAGIC
# MAGIC ZONES: Koramangala, Indiranagar, HSR Layout, Whitefield, Electronic City,
# MAGIC Marathahalli, Jayanagar, BTM Layout, Yelahanka, Banashankari,
# MAGIC JP Nagar, Malleshwaram, Rajajinagar, Hebbal, Bellandur,
# MAGIC Sarjapur Road, Bommanahalli, Basavanagudi, MG Road, Brigade Road
# MAGIC
# MAGIC IMPORTANT:
# MAGIC - Always include zone in GROUP BY when comparing across zones
# MAGIC - Use demand_supply_ratio table for zone health questions
# MAGIC - Use zone_demand_hourly for time-based trend questions
# MAGIC - Use customer_360 for customer behavior questions
# MAGIC - Use demand_predictions for forecasting questions
# MAGIC - When asked about "worst" or "best", order by the relevant metric DESC/ASC and LIMIT
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Questions to Test
# MAGIC
# MAGIC **Demand & Trends:**
# MAGIC - Which zones have the highest demand right now?
# MAGIC - Show me the hourly order trend for Koramangala
# MAGIC - Compare weekday vs weekend demand across all zones
# MAGIC - What time of day has the most orders?
# MAGIC
# MAGIC **Supply & Operations:**
# MAGIC - Which zones are CRITICAL right now?
# MAGIC - Where do we need more delivery partners?
# MAGIC - Show partner availability by zone
# MAGIC - What's the demand-supply ratio for Indiranagar during dinner hours?
# MAGIC
# MAGIC **Weather & Events:**
# MAGIC - How does rain affect cancellation rates?
# MAGIC - Compare metrics on IPL match days vs normal days
# MAGIC - What happens to ETAs when it rains during dinner?
# MAGIC - Which zones are most affected by weather?
# MAGIC
# MAGIC **Surge & Pricing:**
# MAGIC - What's the current vs recommended surge for each zone?
# MAGIC - Where is surge pricing too low?
# MAGIC - Show zones where recommended surge > 2x
# MAGIC
# MAGIC **Customer Insights:**
# MAGIC - How many power users do we have?
# MAGIC - What's the average spend per customer segment?
# MAGIC - Which segment has the highest cancellation rate?
# MAGIC
# MAGIC **Predictions:**
# MAGIC - What's the predicted demand for tonight?
# MAGIC - Which zones will need rebalancing?
# MAGIC - Show predicted vs actual for last week

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Table Comments for Better Genie Understanding

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE zipdrop.gold.zone_demand_hourly IS 'Hourly order demand aggregated by zone, enriched with weather and event data. Use for trend analysis, zone comparison, and time-based patterns.';
# MAGIC
# MAGIC COMMENT ON TABLE zipdrop.gold.demand_supply_ratio IS 'Demand vs supply gap per zone per hour. Includes zone health status (CRITICAL/STRESSED/MODERATE/HEALTHY), recommended surge pricing, and partner rebalancing counts.';
# MAGIC
# MAGIC COMMENT ON TABLE zipdrop.gold.customer_360 IS 'Customer-level profile with order history, spend, preferences, and segment classification (Power User/Regular/Occasional/New).';
# MAGIC
# MAGIC COMMENT ON TABLE zipdrop.gold.partner_availability IS 'Delivery partner counts by zone with status breakdown (active/on_break/inactive), ratings, and experience.';
# MAGIC
# MAGIC COMMENT ON TABLE zipdrop.gold.restaurant_performance IS 'Restaurant-level KPIs including order volume, revenue, delivery times, cancellation rates, and customer reach.';
# MAGIC
# MAGIC COMMENT ON TABLE zipdrop.gold.demand_predictions IS 'ML-generated demand predictions per zone for a simulated scenario (8 PM rainy IPL night). Includes predicted orders, D/S ratio, zone status, recommended surge, and partner rebalancing needs.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Column Comments for Key Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE zipdrop.gold.demand_supply_ratio ALTER COLUMN demand_supply_ratio COMMENT 'Ratio of orders to active partners. >3 = CRITICAL, >2 = STRESSED, >1 = MODERATE, <=1 = HEALTHY';
# MAGIC ALTER TABLE zipdrop.gold.demand_supply_ratio ALTER COLUMN recommended_surge COMMENT 'AI-recommended surge multiplier based on demand-supply ratio. 1.0 = no surge needed';
# MAGIC ALTER TABLE zipdrop.gold.demand_supply_ratio ALTER COLUMN partner_rebalance_count COMMENT 'Number of partners to add (positive) or remove (negative) from this zone';
# MAGIC ALTER TABLE zipdrop.gold.demand_supply_ratio ALTER COLUMN zone_status COMMENT 'Zone health: CRITICAL (>3 D/S), STRESSED (>2), MODERATE (>1), HEALTHY (<=1)';
# MAGIC ALTER TABLE zipdrop.gold.zone_demand_hourly ALTER COLUMN cancellation_rate_pct COMMENT 'Percentage of orders cancelled in this zone-hour window';
# MAGIC ALTER TABLE zipdrop.gold.zone_demand_hourly ALTER COLUMN avg_eta_overshoot COMMENT 'Average minutes delivery was late vs estimated ETA. Positive = late delivery';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie Space ready!
# MAGIC
# MAGIC Go create it: **New** → **Genie space** → Add tables → Paste instructions → Start asking questions!
