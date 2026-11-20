# dbt project: `olist_warehouse`

| Layer | Schema | Materialisation | Models |
|---|---|---|---|
| staging | `staging` | view | one `stg_` model per source table (9): renaming, typing, dedup of reviews, geolocation → one point per zip |
| intermediate | (none) | ephemeral | `int_order_payments`, `int_order_items_agg` |
| core | `core` | table / **incremental** | `fct_orders` (contract enforced), `fct_order_items`, `dim_customers`, `dim_sellers`, `dim_products`, `dim_date` |
| analytics | `analytics` | table | `delivery_performance_monthly`, `seller_scorecard`, `customer_cohorts`, `customer_rfm` |
| snapshots | `snapshots` | SCD2 | `snap_products` |

Grain: `fct_orders` = one row per order · `fct_order_items` = one row per order line · `dim_customers` = one row per
`customer_unique_id` (the person, not the per-order `customer_id`).

```bash
dbt deps --profiles-dir .
dbt build --profiles-dir .                 # models + snapshot + 57 tests
dbt build --profiles-dir . --full-refresh  # rebuild incremental models from scratch
dbt source freshness --profiles-dir .
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8090
```

Connection settings come from environment variables (`WAREHOUSE_HOST`, `WAREHOUSE_PORT`, …). The defaults point at the
Docker Postgres on `localhost:5434`.
