# Real-Time Food Delivery Demand Forecasting & Surge Optimizer

## The Business Problem

Food delivery platforms in India (Swiggy, Zomato, Dunzo) serve millions of orders daily across hundreds of micro-zones. Their #1 operational challenge: **demand is spiky, unpredictable, and hyper-local** — yet supply (delivery partners) is distributed based on static rules.

The result:
- During IPL evenings in Koramangala, ETAs jump from 25 to 55 minutes
- Partners in Yelahanka sit idle while Indiranagar is drowning in orders
- Surge pricing applied reactively angers customers — 1-star reviews spike 3x during poorly-timed surges
- Restaurants prepare food that gets cancelled because no partner is available

## Why Current Approaches Fail

| Approach | Problem |
|----------|---------|
| Static time-based surge (lunch=1.3x, dinner=1.5x) | Ignores weather, events, real-time supply |
| Manual ops intervention | Too slow — by the time ops reacts, damage is done |
| Historical averages | Can't predict one-off events (concert, match, bandh) |
| Zone-level planning | Too coarse — demand varies block-by-block |

## The Opportunity

A platform that fuses **5 signals**:
1. Historical order patterns (time-of-day, day-of-week, zone)
2. Weather data (rain = +40% demand, +60% partner unavailability)
3. Events calendar (IPL match in stadium zone = +80% nearby demand)
4. Partner availability (active partners per zone)
5. Order velocity (rolling order rate vs. baseline)

...to produce **3 actionable outputs**:
1. Zone-level demand forecast (next 30/60 mins)
2. Optimal surge multiplier per zone (maximize revenue without spiking cancellations)
3. Partner rebalancing recommendations (move N partners from zone A to zone B)

## Success Metrics

If built well, this system could deliver:
- 30% reduction in ETA overshoot during spikes
- 15% reduction in spike-hour cancellation rate
- 20% improvement in partner utilization (fewer idle partners)
- 10% revenue uplift from smarter surge (vs. flat surcharge)

## Platform & Tools

This use case runs entirely on **Databricks Community Edition** (free tier):

| What You Use | For What |
|---|---|
| Apache Spark (PySpark + SQL) | Data ingestion, cleaning, aggregation |
| Delta Lake | Bronze → Silver → Gold medallion tables |
| MLflow | Experiment tracking, model logging, metrics |
| matplotlib / plotly | Rich interactive visualizations |
| Databricks widgets | Interactive notebook parameters |
| scikit-learn / pandas | ML model training (demand forecasting) |

**Not required:** Unity Catalog, Delta Live Tables, Genie Space, Lakeview Dashboards, Databricks Apps, SQL Warehouses.

## Candidate's Role

You are a **Data Engineer on the Ops Intelligence team** at ZipDrop. Your job:
- Build the data foundation (ingest, clean, model) using Delta Lake
- Create analytics visualizations (matplotlib/plotly charts in notebooks)
- Optionally: Build a demand prediction model tracked in MLflow
- Optionally: Build an interactive ops console using notebook widgets

You are NOT expected to build a production ML model in 4 hours — a rule-based heuristic (weighted moving average + event multiplier) is perfectly valid for Tier 0/1.
