# Dashboard

Looker Studio report comparing **Eastern vs Southern European** electricity markets, with Polish detail panels.

## Live link

**[European Electricity Markets: Eastern vs Southern Comparison](https://datastudio.google.com/u/1/reporting/a0ec59d4-bb8b-4b1f-b91c-caba4eb18a7c/page/p_ydjqp8qx2d)** (Looker Studio, public link)

## Known limitation

Fuel-mix columns (`avg_renewable_share`, `avg_fossil_share`, `regional_*_share`, `total_generation_mwh`, `avg_residual_load_mw`) are currently **NULL** for non-PL zones — the ENTSO-E generation endpoint returns payloads `entsoe-py 0.7.11` can't parse for post-2024 data. Build the generation-based panels last so you notice the gap. Price-only comparisons (the Eastern-vs-Southern story) are fully populated.

## BigQuery sources

Project: `de-zoomcamp-energy-mf01`

| Purpose                            | Table                                            | Rows   |
|------------------------------------|--------------------------------------------------|--------|
| Regional headline tiles            | `energy_marts_marts.fct_eu_regional_summary`     | ~1,500 |
| Per-zone daily detail              | `energy_marts_marts.fct_eu_daily_by_region`      | ~10,200 |
| Duck-curve hourly profile          | `energy_marts_marts.fct_eu_duck_curve`           | ~245,600 |
| Polish day-ahead daily             | `energy_marts_marts.fct_daily_prices`            | ~1,200 |
| Commodity drivers (FX + futures)   | `energy_marts_marts.fct_commodity_drivers`       | ~4,200 |
| Real-time Polish demand (streaming)| `energy_marts_marts.fct_intraday_demand`         | grows  |

## Pages & charts

### Page 1 — Regional overview
1. **Scorecard**: `regional_avg_price_eur_mwh` grouped by `region` (Eastern / Southern), last 30 days. Source: `fct_eu_regional_summary`.
2. **Time series (line)**: `regional_avg_price_eur_mwh` over `date`, series = `region`. Shows the Eastern–Southern spread.
3. **Bar**: `regional_negative_price_hours` summed by `region` over last 90 days — highlights Southern duck-curve negative pricing.

### Page 2 — Country drilldown
4. **Stacked bar**: `avg_price_eur_mwh` per `country` per `date` (filtered to last 60 days). Source: `fct_eu_daily_by_region`.
5. **Heatmap**: `avg_price_eur_mwh` by `zone_code` × `date`. Shows IT bidding-zone divergence.

### Page 3 — Duck curve
6. **Line**: `avg_price_eur_mwh` by `hour_utc`, one series per `region`. Source: `fct_eu_duck_curve`, filtered to spring/summer months (solar-heavy). Southern shows characteristic midday dip.

### Page 4 — Poland & commodity drivers
7. **Dual-axis line**: PL daily `avg_price_eur_mwh` (`fct_daily_prices`, date field = `price_date`) vs `close_price` (`fct_commodity_drivers` filtered to `ticker='TTF_GAS'`, date field = `price_date`, blended on date).
8. **Line**: `close_price` by `price_date` for `BRENT`, `NAT_GAS_HH`, `EURPLN` — commodity/FX drivers.

### Page 5 — Real-time demand
9. **Line**: `demand_mw` by `event_ts` from `fct_intraday_demand` (populated by the streaming stack at `docker compose up`).

## Building it

1. Go to https://lookerstudio.google.com/
2. **Create → Data source → BigQuery** — pick `de-zoomcamp-energy-mf01` → `energy_marts_marts` → select tables above as sources.
3. Build pages per the list above.
4. **Share → Public to the web** (or domain-restricted to your grader). Paste the link at the top of this file.
5. Export screenshots of each page into `dashboard/screenshots/` for the submission.

## Screenshots

All five pages are captured under [`screenshots/`](screenshots/):

| File                                                               | Page                        |
|--------------------------------------------------------------------|-----------------------------|
| [`page_1_regional_overview.png`](screenshots/page_1_regional_overview.png) | Regional overview       |
| [`page_2_country_drilldown.png`](screenshots/page_2_country_drilldown.png) | Country drilldown       |
| [`page_3_duck_curve.png`](screenshots/page_3_duck_curve.png)               | Duck curve              |
| [`page_4_poland_and_drivers.png`](screenshots/page_4_poland_and_drivers.png) | Poland & drivers      |
| [`page_5_realtime_demand.png`](screenshots/page_5_realtime_demand.png)     | Real-time demand        |
