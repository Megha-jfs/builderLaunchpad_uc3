# Data Dictionary

## Overview

| Dataset | File | Rows | Grain | Update Frequency (Production) |
|---------|------|------|-------|-------------------------------|
| Orders | `data/orders.csv` | ~410K | 1 row per order | Real-time streaming |
| Restaurants | `data/restaurants.csv` | 2,000 | 1 row per restaurant | Daily batch |
| Delivery Partners | `data/delivery_partners.csv` | 5,000 | 1 row per partner | Hourly snapshot |
| Weather | `data/weather_hourly.csv` | 720 | 1 row per zone-hour | Hourly API pull |
| Local Events | `data/local_events.csv` | 50 | 1 row per event | Weekly manual + API |

## Entity Relationships

```
orders.restaurant_id → restaurants.restaurant_id
orders.partner_id → delivery_partners.partner_id
orders.zone → weather_hourly.zone (temporal join on hour)
orders.zone → local_events.zones_impacted (contains check)
```

## Known Data Quality Issues

These are intentional and part of the challenge:

| Issue | Dataset | Column | Rate | Suggested Fix |
|-------|---------|--------|------|---------------|
| Duplicate rows | orders | order_id | ~0.5% | Dedup on order_id, keep first occurrence |
| Missing values | orders | partner_id | ~2% | Flag as "unassigned" — these are failed-to-dispatch orders |
| Missing values | orders | zone | ~3% | Infer from restaurant_id → restaurants.zone |
| Missing values | orders | order_value_inr | ~1% | Impute with zone+cuisine median or exclude |
| Late-arriving events | orders | order_timestamp | scattered | Some records arrive out of sequence — use watermarking |
| Multi-zone events | local_events | zones_impacted | all | Comma-separated — needs exploding for joins |

## Zone Reference

The 20 delivery zones (all in Bangalore):

| Zone | Demand Profile | Typical Peak |
|------|---------------|--------------|
| Koramangala | Very High — young professional hub | Dinner (7-10 PM) |
| Indiranagar | Very High — nightlife + offices | Lunch + Dinner |
| HSR Layout | High — tech worker residential | Dinner (8-11 PM) |
| Whitefield | High — IT corridor | Lunch (12-2 PM) |
| Electronic City | High — IT parks | Lunch (12-2 PM) |
| Marathahalli | Medium-High — residential + offices | Dinner |
| Jayanagar | Medium — family residential | Lunch + Early Dinner |
| BTM Layout | Medium-High — student + young professionals | Late Night (9 PM-12 AM) |
| Yelahanka | Low-Medium — suburban | Lunch only |
| Banashankari | Medium — mixed residential | Dinner |
| JP Nagar | Medium — family residential | Dinner |
| Malleshwaram | Medium — traditional area | Lunch |
| Rajajinagar | Low-Medium — residential | Dinner |
| Hebbal | Medium — growing IT area | Lunch + Dinner |
| Bellandur | High — IT corridor extension | Lunch (12-2 PM) |
| Sarjapur Road | High — new residential + IT | Dinner |
| Bommanahalli | Medium — transit zone | Dinner |
| Basavanagudi | Low — traditional, less delivery culture | Lunch |
| MG Road | Medium — commercial, less residential | Lunch |
| Brigade Road | Medium — evening/nightlife | Late Dinner (9-11 PM) |

## Surge Multiplier Logic (Current — to be improved)

Current static rules in production:
- Lunch (12-2 PM): 1.3x
- Dinner (7-10 PM): 1.5x
- Rain: +0.3x
- Event active: +0.5x

The challenge: build something smarter using the actual demand-supply data.
