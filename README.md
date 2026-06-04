# Use Case 3: Real-Time Food Delivery Demand Forecasting & Surge Optimizer

## Problem Statement

Food delivery platforms (Swiggy/Zomato-style) lose revenue and customer trust during demand spikes — festivals, IPL matches, rain, office-hour rushes. Delivery partners are unevenly distributed, ETAs blow up, and surge pricing kicks in too late (or too aggressively). Currently, ops teams react **after** the spike hits rather than preparing ahead.

### The Goal

Build an AI-powered **demand intelligence platform** on Databricks that ingests historical order data, weather forecasts, local events calendar, and delivery partner GPS pings to:

1. **Predict demand surges** 30-60 mins in advance at a zone level
2. **Recommend dynamic pricing** tiers that balance supply/demand without alienating customers
3. **Optimize delivery partner rebalancing** across zones before the spike hits

---

## Company Context (Fictional)

**ZipDrop** — a cloud-native food delivery platform operating in Bangalore with:
- 200K active customers
- 2,000 restaurant partners across 20 zones
- 5,000 delivery partners
- ~400K orders/month

ZipDrop's ops team currently relies on static time-based surge rules (lunch = 1.3x, dinner = 1.5x) that don't account for weather, events, or real-time partner availability — resulting in:
- 18% order cancellation rate during spikes
- Average ETA overshoot of 12 mins on rainy evenings
- 22% of partners idle in low-demand zones while adjacent zones are starved

---

## Datasets

| File | Rows | Description |
|------|------|-------------|
| `data/orders.csv` | ~410K | 30 days of order data (intentionally messy — duplicates, nulls, late events) |
| `data/restaurants.csv` | 2,000 | Restaurant master with zone, cuisine, prep time, rating |
| `data/delivery_partners.csv` | 5,000 | Partner master with zone, vehicle, rating, status |
| `data/weather_hourly.csv` | 720 | Hourly weather for 10 zones over 30 days |
| `data/local_events.csv` | 50 | Events (IPL, festivals, concerts) with demand multiplier |

### Data Quality Issues (Intentional — Part of the Challenge)

The `orders.csv` dataset has deliberate messiness that participants must clean:
- ~0.5% duplicate orders (same order_id appearing twice)
- ~2% missing `partner_id` (unassigned orders)
- ~3% missing `zone` field
- ~1% missing `order_value_inr`
- Out-of-order timestamps on some late-arriving events
- Inconsistent surge multiplier application

---

## What to Build (Tiered by Difficulty)

### Tier 0 — Foundation (Required, ~1.5 hrs)

**Bronze → Silver → Gold pipeline:**
- Ingest raw CSVs into Bronze (Delta)
- Clean & deduplicate in Silver: resolve duplicates, fill/flag nulls, sessionize delivery trips
- Build Gold tables:
  - `gold_zone_demand_hourly` — orders/hour per zone with weather & event enrichment
  - `gold_partner_availability` — active partners per zone per hour
  - `gold_demand_supply_ratio` — demand vs supply gap per zone/hour

### Tier 1 — AI/BI Dashboard (Required, ~1 hr)

Build a Lakeview Dashboard with:
- **Zone Heatmap**: Predicted demand intensity by zone (color-coded)
- **Supply-Demand Gap Chart**: Where partners are vs. where orders are
- **Surge Pricing Simulator**: What-if analysis — "If surge = 1.8x in Koramangala, what's the predicted cancellation rate?"
- **KPIs**: Avg ETA overshoot, cancellation rate, revenue/zone, partner utilization %

### Tier 2 — Genie Space (Required, ~30 mins)

Natural-language Q&A on the Gold layer:
- "Which zones will spike in the next hour?"
- "What's the optimal surge multiplier for Koramangala right now?"
- "Show me partner utilization across zones during last IPL match"
- "What was the cancellation rate during heavy rain in Indiranagar?"

### Tier 3 — Databricks App (Bonus, if time permits)

An interactive **Ops Command Console**:
- Input: Select zone + time window
- Output: Predicted demand curve, recommended surge tier, partner rebalancing suggestions
- Bonus: Alert rules — "Notify when predicted demand > 2x available partners"

---

## Schema Reference

### orders.csv

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Unique order identifier (ORD_XXXXXXX) |
| customer_id | string | Customer identifier |
| restaurant_id | string | FK to restaurants |
| partner_id | string | FK to delivery_partners (nullable) |
| zone | string | Delivery zone (nullable — data quality issue) |
| order_timestamp | timestamp | When order was placed |
| delivery_timestamp | timestamp | When order was delivered (empty if not delivered) |
| order_value_inr | float | Order value in INR (nullable) |
| discount_inr | float | Discount applied |
| payment_mode | string | UPI/Credit Card/Debit Card/Cash/Wallet |
| status | string | delivered/cancelled/in_transit/preparing |
| cancel_reason | string | Reason for cancellation (if applicable) |
| estimated_eta_mins | int | Estimated delivery time at order placement |
| actual_delivery_mins | int | Actual delivery time (empty if not delivered) |
| surge_multiplier | float | Surge pricing applied (1.0 = no surge) |
| rain_at_order_time | int | 1 if raining when order placed, 0 otherwise |
| event_active | int | 1 if a major event was active in the zone |

### restaurants.csv

| Column | Type | Description |
|--------|------|-------------|
| restaurant_id | string | Primary key |
| restaurant_name | string | Name |
| zone | string | Operating zone |
| cuisine_type | string | Primary cuisine |
| avg_prep_time_mins | int | Average food prep time |
| rating | float | Customer rating (3.0-5.0) |
| is_premium | int | Premium partner flag |
| lat/lng | float | Location coordinates |

### delivery_partners.csv

| Column | Type | Description |
|--------|------|-------------|
| partner_id | string | Primary key |
| partner_name | string | Name |
| zone | string | Assigned zone |
| vehicle_type | string | bike/scooter/bicycle/ev_scooter |
| avg_rating | float | Partner rating |
| total_deliveries | int | Lifetime deliveries |
| status | string | active/inactive/on_break |
| experience_months | int | Months on platform |
| lat/lng | float | Last known location |

### weather_hourly.csv

| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp | Hour timestamp |
| zone | string | Zone |
| temperature_c | float | Temperature |
| humidity_pct | int | Humidity % |
| condition | string | Clear/Cloudy/Light Rain/Heavy Rain/Thunderstorm/Foggy/Drizzle |
| wind_speed_kmh | float | Wind speed |
| rain_mm | float | Rainfall amount |
| visibility_km | float | Visibility |

### local_events.csv

| Column | Type | Description |
|--------|------|-------------|
| event_id | string | Primary key |
| event_name | string | Event name |
| event_type | string | IPL Match/Festival/Concert/etc. |
| date | date | Event date |
| start_time/end_time | time | Duration |
| expected_crowd | int | Expected attendance |
| zones_impacted | string | Comma-separated zones affected |
| demand_multiplier | float | Expected demand increase factor |

---

## Key Insights Hidden in the Data

Participants who explore the data deeply will find:
1. **IPL match days** (5th, 12th, 18th, 25th) have ~1.8x order volume spikes between 5-11 PM
2. **Rain + dinner rush** combo causes the highest cancellation rates
3. **Weekend vs weekday** patterns: weekends have 25% higher baseline demand
4. **Zone imbalance**: Koramangala and Indiranagar consistently have demand > supply
5. **Late-night dead zones**: After 11 PM, only 3-4 zones have meaningful demand but partners are spread across all 20

---

## Evaluation Rubric

| Criteria | Weight | What Judges Look For |
|----------|--------|---------------------|
| Data Engineering | 30% | Clean pipeline, proper dedup, null handling, schema enforcement |
| Analytics & Insights | 25% | Meaningful KPIs, actionable dashboard, correct aggregations |
| AI/ML Layer | 20% | Demand prediction logic, surge optimization, Genie Space quality |
| Demo & Storytelling | 15% | Clear narrative, live demo, business impact articulation |
| Code Quality & Creativity | 10% | Clean notebooks, reusable patterns, creative extensions |

---

## Getting Started

```python
# 1. Upload data to Databricks workspace (Unity Catalog volume or DBFS)
# 2. Create a catalog and schema for the project
# 3. Start with Bronze ingestion notebook

# Quick validation after upload:
df_orders = spark.read.csv("/Volumes/your_catalog/your_schema/raw/orders.csv", header=True, inferSchema=True)
print(f"Orders: {df_orders.count()} rows")
print(f"Duplicates: {df_orders.count() - df_orders.dropDuplicates(['order_id']).count()}")
print(f"Null zones: {df_orders.filter(df_orders.zone.isNull()).count()}")
```

---

## Tips for Vibe Coding with AI

- Start with the data quality problem — ask AI to help identify and fix anomalies
- Use AI to generate the DLT pipeline boilerplate, then customize
- For Genie Space, get AI to help craft the semantic layer / metric definitions
- For the dashboard, describe what you want visually and let AI generate the Lakeview JSON
- Don't try to build everything — nail Tier 0 + Tier 1 first, then stretch
