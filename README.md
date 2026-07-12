# Safaricom Intelligence — dbt Project

Transformation layer for the Safaricom Financial Intelligence Pipeline.
Reads from BigQuery `raw` (loaded by `safaricom_seed_dag` and
`safaricom_scraper_dag`), builds `staging`, and serves three analytical
marts consumed by the Looker Studio dashboard.

## Lineage

```
raw.company_overview   ─┐
raw.mpesa_metrics       ├─▶ stg_company_overview
raw.revenue_segments    │   stg_mpesa_metrics
raw.kenya_ethiopia     ─┘   stg_revenue_segments
                             stg_kenya_ethiopia
                                   │
                                   ▼
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
      mart_mpesa_growth_trends  mart_revenue_mix  mart_ke_et_trajectory
         (Looker Page 1)         (Looker Page 2)     (Looker Page 3)
```

## Folder structure

```
dbt/
├── dbt_project.yml
├── macros/
│   ├── generate_period_surrogate_key.sql   # hashed key, no dbt_utils needed
│   ├── pct_of.sql                          # SAFE_DIVIDE-based % share helper
│   └── yoy_growth.sql                      # LAG-based YoY growth helper
├── models/
│   ├── staging/
│   │   ├── _staging__sources.yml           # raw.* source definitions + freshness
│   │   ├── _staging__models.yml            # staging docs + tests
│   │   ├── stg_company_overview.sql
│   │   ├── stg_mpesa_metrics.sql
│   │   ├── stg_revenue_segments.sql
│   │   └── stg_kenya_ethiopia.sql
│   └── marts/
│       ├── _marts__models.yml              # mart docs + tests
│       ├── mart_mpesa_growth_trends.sql
│       ├── mart_revenue_mix.sql
│       └── mart_ke_et_trajectory.sql
└── tests/
    ├── assert_revenue_segments_reconcile.sql
    └── assert_mpesa_revenue_consistency.sql
```

## Design notes

- **No external packages.** Surrogate keys, % shares, and YoY growth are
  all plain BigQuery SQL wrapped in local macros — no `dbt_utils` install,
  no `packages.yml`, no extra download. Keeps `dbt deps` a no-op.
- **`period_key` / `period_geo_key`** are `MD5`-hashed surrogate keys built
  from `period_label + period_type + fiscal_year` (and `+ geography` for
  the Kenya/Ethiopia table). Every staging and mart model carries one of
  these as its primary key, tested `unique` + `not_null`.
- **FY vs HY never mixed.** All window functions (`yoy_growth`, rolling
  averages) partition by `period_type` so a half-year figure is never
  compared against a full-year one.
- **Two singular tests reconcile the source data itself**, not just the
  models: `assert_revenue_segments_reconcile` checks that
  connectivity + M-PESA + fixed/IoT sums to the reported total service
  revenue (small rounding tolerance), and
  `assert_mpesa_revenue_consistency` checks that M-PESA revenue agrees
  between the `mpesa_metrics` and `revenue_segments` raw tables, since both
  are sourced from different pages of the same booklet.
- **Materialization:** staging = `view` (cheap, always fresh), marts =
  `table` (queried directly by Looker Studio, so pre-computed).

## Running

```bash
cd dbt
dbt debug          # verify BigQuery connection
dbt run            # build staging views + mart tables
dbt test           # generic + singular tests
dbt docs generate && dbt docs serve   # browse lineage graph + column docs
```

Triggered in production via `safaricom_seed_dag` (after the historical
CSV load) and `safaricom_scraper_dag` (after each new booklet is scraped
and appended) — both call the dbt Cloud Jobs API as their final task.