# Safaricom Intelligence

A cloud ELT pipeline that converts Safaricom PLC's public financial disclosure PDFs into a versioned, queryable BigQuery dataset, with a dbt (Fusion) transformation layer and a 3-page Looker Studio dashboard built around three analytical lenses: M-PESA growth, revenue mix, and Kenya vs. Ethiopia.

## Table of Contents

- [Problem Description](#problem-description)
- [Solution Overview](#solution-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Infrastructure as Code (Terraform)](#infrastructure-as-code-terraform)
- [Data Ingestion and Orchestration](#data-ingestion-and-orchestration)
- [Transformations (dbt Fusion / dbt Cloud)](#transformations-dbt-fusion--dbt-cloud)
- [Dashboard](#dashboard)
- [Data Quality: What Broke and How It Was Found](#data-quality-what-broke-and-how-it-was-found)
- [Known Data Limitations](#known-data-limitations)
- [Reproducibility: How to Run](#reproducibility-how-to-run)
- [Author](#author)

## Problem Description

Safaricom PLC (Kenya's largest telecom operator and the company behind M-PESA) publishes detailed segment-level KPIs twice a year via Results Booklets (half-year and full-year results), plus narrative Press Releases and full Annual Reports. Together these cover M-PESA sub-segments, connectivity revenue by type, and, since STE's commercial launch in October 2022, a Kenya vs. Ethiopia geographic split.

This is one of the most granular publicly-disclosed telecom and fintech datasets in East Africa. But it exists only as a series of disconnected PDFs, in three different document types with three different levels of structure. Answering a question like *"how has M-PESA's share of total revenue changed since FY15?"* or *"how does Ethiopia's loss trajectory compare to Kenya's early years?"* means manually opening a dozen-plus documents and transcribing numbers by hand, every time.

**Safaricom Intelligence** turns these public disclosures into a versioned, queryable BigQuery dataset spanning FY2015 through FY2026, refreshed automatically as new results are published, with a dbt transformation layer and a Looker Studio dashboard on top.

## Solution Overview

| Dimension | Detail |
|---|---|
| Scope | FY2015 to FY2026, 12 years of financial history |
| Granularity | Segment-level: M-PESA sub-segments, connectivity breakdown, Kenya vs. Ethiopia |
| Refresh cadence | Automatic via weekly Airflow scraper (every Monday, 08:00 EAT) |
| Deployment | Local Docker Compose (Airflow) + GCP (Terraform-provisioned) + dbt Cloud (Fusion engine) |
| Data source | safaricom.co.ke, free, no authentication, no API key |

**Key analytical questions answered:**

- How has M-PESA grown from ~28% to 45.6% of total Kenya service revenue between FY15 and FY26?
- Which connectivity segment (voice, data, or messaging) has driven or dragged revenue year over year, and when did data overtake voice?
- How quickly is Ethiopia's EBIT loss narrowing (it isn't a straight line: it got worse before it got better), and how does its customer growth compare to Kenya's own early years?
- How has the merchant network (Lipa na M-PESA + Pochi La Biashara) scaled since FY22, and what does that say about M-PESA's shift from a payments app toward the rail for Kenya's informal economy?

## Architecture

```
safaricom.co.ke (free, no auth)
        │
        ├── Results Booklets FY20–FY26 (structured template, Sections 1 / 2 / 4A–4C)
        ├── Press Release PDFs FY15–FY19 (narrative prose)
        └── Annual Reports (full audited financials, segment notes)
                    │
    Local: Ubuntu (Docker Compose)
                    │
    Seed CSVs (one-time backfill FY15–FY26) ──▶ Airflow 2.9.2
        safaricom_seed_dag     (manual trigger, one-time)
        safaricom_scraper_dag  (every Monday 08:00 EAT)
                    │
    GCP: safaricom-intelligence (Terraform-provisioned)
                    │
    GCS: safaricom-intel-data-lake
        seed/ | pdfs/ | extracted/ | scraper_state/
                    │
    BigQuery
        raw ──▶ staging (dbt views) ──▶ mart (dbt tables)
                    │
    Looker Studio: 3-page "Safaricom Financial Intelligence Dashboard"
        1. M-PESA Intelligence
        2. Revenue Mix
        3. Kenya vs Ethiopia
```

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| IaC | Terraform (~> 5.0 google provider) | Provisions GCS bucket, BQ datasets, and all 4 raw tables with explicit schemas |
| Storage | Google Cloud Storage | Raw zone: seed CSVs, downloaded PDFs, scraper JSONL |
| Warehouse | BigQuery | `raw` / `staging` / `mart` datasets, YEAR-partitioned on `period_end_date` |
| Orchestration | Apache Airflow 2.9.2 (Docker Compose, LocalExecutor) | `safaricom_seed_dag` + `safaricom_scraper_dag` |
| Extraction | Python (`requests` + `pdfplumber`) | PDF download and layout-aware table extraction |
| Transformation | dbt Cloud, Fusion engine | Staging views, mart tables, YoY window-function macros |
| Visualization | Looker Studio | 3-page public dashboard on the mart tables |
| Secrets | `.env` with `TF_VAR_` prefixes for Terraform, `os.environ` inside Airflow task bodies | No hardcoded values anywhere |

## Data Sources

All data sourced from safaricom.co.ke, free, no authentication, no API key required. Static PDFs served from predictable paths (`/images/Downloads/`, `/images/calendars/`, `/images/Investorrelation/`), so ingestion is polite HTTP GETs, nothing more.

| Source | Coverage | Format | Feeds |
|---|---|---|---|
| Results Booklets | FY20–FY26 | Structured template: Section 1 (Group/KE/ET KPIs), Section 4A–4C (segment detail), Section 6 (KE/ET side-by-side P&L, from FY23 onward) | All 4 raw tables |
| Press Release PDFs | FY15–FY19 | Narrative prose, headline figures embedded in text, no consistent table structure | `company_overview` mainly; some segment figures hand-extracted |
| Annual Reports | All years, as needed for verification | Full audited financials with a proper segment note (Kenya/Ethiopia P&L), the most authoritative source but the longest document by far | Used to verify/correct specific disputed figures, not for routine ingestion |

**Data quality decision:** all YoY% and growth metrics are computed in dbt via `LAG()` window functions on `fiscal_year`, partitioned by `period_type` (so an HY figure is never compared against an FY one). The booklets' own printed percentages are never ingested directly: computing them independently is what makes the reconciliation tests possible in the first place.

## Project Structure

```
safaricom-intelligence/
├── .env.example                    # env var template, copy to .env, never commit .env
├── .gitignore
├── README.md
├── terraform/
│   ├── main.tf                     # GCS bucket + BQ datasets + 4 raw tables
│   ├── variables.tf
│   └── outputs.tf
├── ingestion/
│   ├── requirements.txt
│   ├── seed/
│   │   ├── seed_loader.py
│   │   └── data/
│   │       ├── company_overview.csv    # FY15–FY26, 12 rows, Group level
│   │       ├── mpesa_metrics.csv       # FY15–FY26, 12 rows, Kenya segment
│   │       ├── revenue_segments.csv    # FY15–FY26, 12 rows, Kenya segment
│   │       └── kenya_ethiopia.csv      # FY22–FY26, KE+ET, one row per geography per period
│   └── scraper/
│       ├── safaricom_scraper.py    # standalone scraper utility
│       └── pdf_parser.py           # pdfplumber extraction logic
├── dags/
│   ├── safaricom_seed_dag.py       # one-time historical load
│   └── safaricom_scraper_dag.py    # weekly auto-scraper
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── Staging/
│   │   │   ├── stg_company_overview.sql
│   │   │   ├── stg_mpesa_metrics.sql
│   │   │   ├── stg_revenue_segments.sql
│   │   │   ├── stg_kenya_ethiopia.sql
│   │   │   ├── _staging__sources.yml
│   │   │   └── _staging__models.yml
│   │   └── Mart/
│   │       ├── mart_mpesa_growth_trends.sql
│   │       ├── mart_revenue_mix.sql
│   │       ├── mart_ke_et_trajectory.sql
│   │       └── _marts__models.yml
│   ├── macros/
│   │   ├── generate_period_surrogate_key.sql   # hashed surrogate key, zero external deps
│   │   ├── pct_of.sql                          # SAFE_DIVIDE-based % share helper
│   │   └── yoy_growth.sql                      # LAG-based YoY growth helper
│   └── tests/
│       ├── assert_revenue_segments_reconcile.sql       # connectivity + mpesa + fixed ≈ reported total
│       └── assert_mpesa_revenue_consistency.sql        # mpesa_metrics vs revenue_segments tie-out
├── docker/
│   ├── Dockerfile                  # adds GCP libs + pdfplumber to base Airflow image
│   └── docker-compose.yml         # Airflow (webserver :8085) + Postgres 13 (:5439)
└── logs/                          # Airflow task logs (gitignored)
```

> **Naming note:** the `dbt/models/` folders are `Staging/` and `Mart/` (capitalized, `Mart` singular) rather than the more conventional lowercase `staging/`/`marts/`. This isn't a stylistic choice: it's what the repo ended up with after some IDE-driven folder creation, and `dbt_project.yml`'s model config paths are set to match this exact casing rather than the other way around. If you fork this and rename the folders, update `dbt_project.yml`'s `models.safaricom_intelligence.Staging` / `.Mart` keys to match, or dbt Fusion will silently warn `UnusedResourceConfigPath` and your materialization/schema configs won't apply.

No external dbt packages (`dbt_utils`, etc.) are used: every helper (surrogate keys, % shares, YoY growth) is a hand-written macro using plain BigQuery SQL (`MD5`, `SAFE_DIVIDE`, `LAG() OVER`). This keeps `dbt deps` a no-op, deliberately, given bandwidth constraints on the dev machine.

## Data Model

### Raw Layer (BigQuery dataset: `raw`)

All four tables share four leading columns: `period_label` (e.g. `"FY26"`), `period_type` (`"FY"` or `"HY"`), `fiscal_year` (`INT64`), `period_end_date` (`DATE`, the partition key).

**`raw.company_overview`** (FY15–FY26, Group level)

| Column | Type | Notes |
|---|---|---|
| `total_revenue_kes_bn` | NUMERIC | Group consolidated |
| `service_revenue_kes_bn` | NUMERIC | Group consolidated |
| `ebit_kes_bn` | NUMERIC | |
| `net_income_kes_bn` | NUMERIC | |
| `active_customers_mn` | NUMERIC | One-month active |
| `capex_kes_bn` | NUMERIC | PP&E + intangibles |

**`raw.mpesa_metrics`** (FY15–FY26, Safaricom Kenya)

| Column | Type | Notes |
|---|---|---|
| `mpesa_revenue_kes_bn` | NUMERIC | |
| `mpesa_txn_value_kes_bn` | NUMERIC | KES billions. Blank FY16–FY20 |
| `mpesa_txn_volume_mn` | NUMERIC | Millions of transactions. Blank FY16–FY20 |
| `mpesa_customers_1m_mn` | NUMERIC | One-month active M-PESA customers |
| `merchants_mn` | NUMERIC | **Metric definition changes mid-series**, see limitations below |
| `business_payments_kes_bn` | NUMERIC | Blank FY16–FY21, not broken out that far back |
| `mpesa_global_kes_bn` | NUMERIC | Visa Card + International Money Transfer revenue |
| `merchant_overdraft_customers` | NUMERIC | Fuliza customer count. Blank pre-FY24 (**product launched January 2019**), so blank FY15–FY18 is structural, not a gap |

**`raw.revenue_segments`** (FY15–FY26, Safaricom Kenya)

| Column | Type | Notes |
|---|---|---|
| `voice_kes_bn` | NUMERIC | |
| `mobile_data_kes_bn` | NUMERIC | |
| `messaging_kes_bn` | NUMERIC | |
| `mobile_incoming_kes_bn` | NUMERIC | Blank FY15–FY19 |
| `other_mobile_service_kes_bn` | NUMERIC | |
| `mpesa_kes_bn` | NUMERIC | Ties to `mpesa_metrics.mpesa_revenue_kes_bn` for the same period, checked by a dedicated test |
| `fixed_service_iot_kes_bn` | NUMERIC | |
| `connectivity_total_kes_bn` | NUMERIC | |
| `total_service_revenue_kes_bn` | NUMERIC | Reported total, checked against a computed sum by a dedicated test |

**`raw.kenya_ethiopia`** (FY22–FY26, one row per geography per period)

| Column | Type | Notes |
|---|---|---|
| `geography` | STRING | `KE` or `ET` |
| `service_revenue_kes_bn` | NUMERIC | Blank for ET pre-launch (Oct 2022) |
| `active_customers_1m_mn` | NUMERIC | |
| `active_customers_3m_mn` | NUMERIC | |
| `ebit_kes_bn` | NUMERIC | Negative for ET until breakeven |
| `capex_kes_bn` | NUMERIC | Blank for ET most years, rarely disclosed as a standalone segment figure |

### Staging Layer (dbt views)

One view per raw table: casting, renaming, and derived per-row ratios (margins, % shares, average transaction value). Models: `stg_company_overview`, `stg_mpesa_metrics`, `stg_revenue_segments`, `stg_kenya_ethiopia`.

### Mart Layer (dbt tables)

| Model | Computes |
|---|---|
| `mart_mpesa_growth_trends` | YoY revenue/customer/merchant growth, 3-period trailing average, M-PESA % of service revenue, all via `LAG()` partitioned by `period_type` |
| `mart_revenue_mix` | Every Kenya revenue segment as a % share of total service revenue, plus YoY growth per segment, built for stacked-area charting |
| `mart_ke_et_trajectory` | Kenya and Ethiopia pivoted side by side per period, EBIT margin for both, and an `et_ebit_loss_narrowing_kes_bn` delta showing whether Ethiopia's loss widened or narrowed year over year |

## Infrastructure as Code (Terraform)

Terraform runs once to provision GCP resources; it is not a running service.

```bash
cd terraform
export $(grep -v '^#' ../.env | grep -v '^$' | xargs)
terraform init
terraform apply
```

Terraform provisions:

- GCS bucket `safaricom-intel-data-lake` with four prefixes: `seed/`, `pdfs/`, `extracted/`, `scraper_state/`
- BigQuery dataset `raw`, the actual home of all four ingested tables
- BigQuery datasets `staging` and `mart`, created empty as placeholders; **the real dbt-built tables do not physically live here** (see the callout below)
- Four raw tables with explicit schemas (no autodetect anywhere), YEAR partitioning on `period_end_date`, and clustering on `period_type`/`fiscal_year` (or `geography` for `kenya_ethiopia`)

> **Gotcha worth knowing before you go looking for your mart tables:** dbt's default schema-naming behavior prepends your connection's default schema to any custom `+schema:` config. So even though `dbt_project.yml` sets `+schema: mart` and `+schema: staging`, the tables actually land in `dbt_<your_dbt_cloud_username>_mart` and `dbt_<your_dbt_cloud_username>_staging` (e.g. `dbt_rgiggs_mart`), not in the plain `mart`/`staging` datasets Terraform created. Those Terraform-created datasets sit empty. Point Looker Studio (or anything else querying the mart layer) at the `dbt_<username>_mart` dataset, not `mart`.

`deletion_protection = false` on every table, deliberate since this is dev data corrected from primary sources multiple times over the project's life; a bad schema (see [Data Quality](#data-quality-what-broke-and-how-it-was-found) below) needs to be droppable and rebuildable via `terraform apply -replace`, not something you have to fight Terraform to fix.

## Data Ingestion and Orchestration

### DAG Design

Runs inside a custom Docker image (adds GCP client libraries + `pdfplumber` to a base Airflow image), Airflow 2.9.2 with `LocalExecutor`.

**`safaricom_seed_dag`** (manual trigger, one-time)

```
validate_seed_csvs → upload_to_gcs → load_to_bigquery → trigger_dbt_run
```

- `validate_seed_csvs`: checks all 4 CSVs exist and headers exactly match the expected schema
- `upload_to_gcs`: copies CSVs to `gs://safaricom-intel-data-lake/seed/`
- `load_to_bigquery`: BQ Load Job per table, `WRITE_TRUNCATE`
- `trigger_dbt_run`: calls the dbt Cloud API to trigger a `dbt build` job

**`safaricom_scraper_dag`** (every Monday 05:00 UTC = 08:00 EAT)

```
check_ir_page → download_booklet → extract_tables → load_to_bigquery → trigger_dbt_run
```

- `check_ir_page`: `ShortCircuitOperator`; fetches the IR results page, compares the latest booklet URL against a GCS marker (`scraper_state/last_processed_url.txt`), short-circuits every downstream task if no new booklet is detected
- `download_booklet`: downloads the new PDF to `gs://safaricom-intel-data-lake/pdfs/`
- `extract_tables`: `pdfplumber` extraction of the KPI and income-statement sections, writes JSONL to `extracted/`
- `load_to_bigquery`: BQ Load Job, `WRITE_APPEND`
- `trigger_dbt_run`: dbt Cloud API call + updates the GCS marker

**On the dbt Cloud API call specifically:** the trigger URL is *not* `cloud.getdbt.com` for every account: multi-tenant dbt Cloud accounts live on a region-specific host (e.g. `pz121.us1.dbt.com`). Both DAGs read this from a `DBT_BASE_URL` env var (defaulting to `cloud.getdbt.com` for anyone whose account happens to be on that host), rather than hardcoding it; check your own dbt Cloud URL bar before assuming the default is right for your account.

### PDF Extraction

`pdf_parser.py` uses `pdfplumber.extract_text(layout=True)`, which preserves column spacing well enough for regex-based row matching against the Results Booklet's fairly consistent template (FY20 onward). Pre-FY20 Press Release PDFs are narrative prose without a comparable table structure, and were hand-extracted into the seed CSVs rather than parsed.

## Transformations (dbt Fusion / dbt Cloud)

This project runs on dbt Cloud's **Fusion engine** (the newer Rust-based dbt runtime), which is stricter about a few things than classic dbt Core historically was, worth knowing if you're extending this project:

- **`accepted_values` tests need an `arguments:` wrapper** around `values:` (a v1.10.5+ requirement Fusion enforces):
  ```yaml
  - accepted_values:
      arguments:
        values: ['FY', 'HY']
  ```
- **Source `freshness` and `loaded_at_field` must nest under a `config:` block**, not sit at the top level of the table definition (a v1.9+ requirement):
  ```yaml
  - name: company_overview
    config:
      loaded_at_field: period_end_date
      freshness:
        warn_after: {count: 200, period: day}
  ```

### Mart Models: the actual computation

**`mart_mpesa_growth_trends`**: YoY growth via a shared macro, partitioned so FY and HY periods are never compared against each other:

```sql
{{ yoy_growth('mpesa_revenue_kes_bn', 'period_type', 'fiscal_year') }} as mpesa_revenue_yoy_pct
```

which expands to:

```sql
ROUND(
    SAFE_DIVIDE(
        mpesa_revenue_kes_bn - LAG(mpesa_revenue_kes_bn) OVER (PARTITION BY period_type ORDER BY fiscal_year),
        LAG(mpesa_revenue_kes_bn) OVER (PARTITION BY period_type ORDER BY fiscal_year)
    ) * 100, 2
)
```

**`mart_revenue_mix`**: every segment expressed as a share of total, via the `pct_of` macro:

```sql
{{ pct_of('mpesa_kes_bn', 'total_service_revenue_kes_bn') }} as mpesa_share_pct
```

**`mart_ke_et_trajectory`**: Kenya and Ethiopia pivoted into one row per period, plus a loss-narrowing delta:

```sql
et_ebit_kes_bn - LAG(et_ebit_kes_bn) OVER (PARTITION BY period_type ORDER BY fiscal_year) as et_ebit_loss_narrowing_kes_bn
-- Positive = loss narrowing, negative = loss widening
```

### Tests

- `assert_revenue_segments_reconcile`: fails if `connectivity_total + mpesa + fixed_service_iot` diverges from the reported `total_service_revenue_kes_bn` by more than 1 KES bn
- `assert_mpesa_revenue_consistency`: fails if `mpesa_metrics.mpesa_revenue_kes_bn` doesn't match `revenue_segments.mpesa_kes_bn` for the same period (both describe the same revenue line, sourced from different pages of the same booklet, so they should tie out)
- Generic schema tests: `unique`/`not_null` on every surrogate key, `accepted_values` on `period_type` and `geography`

Both singular tests found **real, confirmed data errors** during development, not false positives; see below.

## Dashboard

[Open the live dashboard](https://datastudio.google.com/reporting/d1679099-7abb-4d6e-bc15-aa8beb9dfa6c)

**Tool:** Looker Studio · **Dataset:** `safaricom-intelligence.dbt_<username>_mart.*` (not the empty `mart` dataset, see the Terraform callout above)

**Page 1 (M-PESA Intelligence):** revenue trend FY15–FY26, M-PESA as % of total service revenue, Fuliza (merchant overdraft) customer growth, active merchant base over time (Lipa na M-PESA + Pochi La Biashara combined).

**Page 2 (Revenue Mix):** stacked-area and 100%-stacked-column charts showing voice, data, messaging, M-PESA, and fixed/IoT as shares of total service revenue, making visible the decade-long structural shift away from voice (which genuinely declined in absolute KES terms during FY20–21, a real COVID-era effect, not a data artifact) toward data and M-PESA.

**Page 3 (Kenya vs Ethiopia):** dual-axis revenue/customer-base chart, EBIT margin comparison, an Ethiopia "path to profitability" chart, and capex comparison, telling the story of a J-curve market entry (Ethiopia's loss got *worse* before it got better: -5.1bn → -30.7bn → -59.6bn → -61.1bn at the trough → -30.1bn, nearly halving in the most recent year).

Data Freshness is set to 12 hours on all three data sources. BigQuery **BI Engine** was evaluated and deliberately not enabled: it's a continuous hourly-billed reservation (~$30/month minimum), which is wildly disproportionate for mart tables with roughly a dozen rows each; the actual latency in this dashboard is per-query overhead from the sheer number of charts, not anything BI Engine would meaningfully improve at this data volume.

## Data Quality: What Broke and How It Was Found

This section exists because several of the fixes below weren't visible from the data alone; they only surfaced through cross-referencing every suspicious number against Safaricom's own primary sources (in order of reliability: press release → results booklet → annual report's segment note). Documenting them here both as an engineering log and as a demonstration that "the pipeline runs" and "the pipeline is correct" are two different claims.

- **BigQuery schema drift on `revenue_segments`.** The live table had two columns (`mobile_incoming_kes_bn`, `other_mobile_service_kes_bn`) appended at the *end* of the schema instead of sitting in their intended positions, because BigQuery only allows appending new columns to an existing table, never inserting mid-schema, and the load job doesn't pass an explicit schema; it defers to whatever the live table already has. Every CSV load was silently mapping columns into the wrong slots as a result. Root-caused via `INFORMATION_SCHEMA.COLUMNS`, fixed with `terraform apply -replace` to force a clean rebuild.
- **FY15 `mpesa_metrics` had three fields copy-pasted from FY26** (`merchants_mn`, `business_payments_kes_bn`, `merchant_overdraft_customers` were all byte-identical to their FY26 counterparts), almost certainly an artifact of using the most recent row as a template while curating the earliest one. Fuliza didn't exist until January 2019, so a non-null FY15 value there was itself a strong tell. Corrected `merchants_mn` to 50,000 (0.05M), sourced from contemporaneous press coverage of the actual FY15 results announcement; the other two fields correctly blanked.
- **FY25 and FY26 `kenya_ethiopia.et_ebit_kes_bn` were both wrong**, not missing, *wrong*. Four independent financial wire services (Reuters/Zawya, CNBC Africa, LSE, Bloomberg), all quoting the same FY26 earnings call, state Ethiopia's EBIT loss narrowed to KES 30.1bn from 61.1bn a year earlier. The seed data had -37.71 and -54.16. Corrected to -30.1 and -61.1.
- **FY23 `kenya_ethiopia` (Ethiopia row) was mostly blank** and initially assumed to be an honest "not yet disclosed" gap (STE launched commercially only 6 months into that fiscal year). On closer inspection, real figures did exist: the FY23 Results Booklet's Section 6 side-by-side Kenya/Ethiopia P&L gives service revenue of KES 562.4Mn and an operating loss of KES 30,721.4Mn for that year. Both were added; capex (KES 55.77bn, from the FY23 press release's narrative) was added separately.
- **FY22 `et_ebit_kes_bn` (-5.12) was independently verified as correct**, not just assumed: the FY22 Annual Report's CEO letter states Group EBIT excluding Ethiopia at 114.3bn vs. including Ethiopia at 109.1bn, and the ~5.2bn gap between those two figures matches.

## Known Data Limitations

| Gap | Reason | Impact |
|---|---|---|
| `merchants_mn` blank FY16–FY21 | Real historical gap: Lipa na M-PESA merchants existed and grew through these years, but per-year figures were never sourced during curation (only FY15, via a press quote, and FY22 onward, via the standard booklet format, were captured) | Merchant growth trend has a 6-year hole between 50K (FY15) and 493K (FY22) |
| `merchants_mn` methodology changes at FY25 | FY22–FY24 figures are Lipa na M-PESA only; FY25–FY26 figures are a *combined* Lipa na M-PESA + Pochi La Biashara metric, per Safaricom's own reporting change | Part of the FY24→FY25 jump in this metric reflects a disclosure definition change, not pure organic growth; don't read it as a single consistent time series without this caveat |
| `merchant_overdraft_customers` blank FY15–FY23 | Fuliza launched January 2019; even after launch, the specific customer-count metric wasn't consistently disclosed until later booklets | Merchant OD customer trend effectively starts FY24 |
| `mobile_incoming_kes_bn` blank FY15–FY19 | Embedded within `voice_kes_bn` before a FY20 reporting reclassification; genuinely not separable without internal Safaricom data | Connectivity sub-segment split only meaningful from FY20 |
| `kenya_ethiopia.capex_kes_bn` blank for ET, most years | Rarely disclosed as a standalone Ethiopia segment figure; FY23 is a rare exception where the press release's narrative happened to state it explicitly | Ethiopia capex trend has only one real data point (FY23) |
| `kenya_ethiopia.ebit_kes_bn` for FY24 (-59.6) | Not yet independently re-verified against a primary source the way FY22/23/25/26 were | Treat with slightly lower confidence than the other years until checked |
| Terraform-created `staging`/`mart` BigQuery datasets are unused | dbt's default schema-prefixing behavior routes actual output to `dbt_<username>_staging`/`_mart` instead | Cosmetic/navigational gotcha only, doesn't affect correctness, just where you look for the tables |

## Reproducibility: How to Run

### Prerequisites

| Requirement | Notes |
|---|---|
| Ubuntu / Linux | 22.04+ |
| Docker Engine + Compose plugin | v2 |
| Terraform | 1.3.0+ |
| gcloud CLI | |
| dbt Cloud account | Free tier sufficient; Fusion engine |
| GCP account with billing enabled | |

### Step 1: Clone and configure

```bash
git clone https://github.com/Derrick-Ryan-Giggs/safaricom-intelligence.git
cd safaricom-intelligence
cp .env.example .env
```

### Step 2: GCP setup

```bash
gcloud projects create safaricom-intelligence --name="Safaricom Intelligence"
gcloud config set project safaricom-intelligence
gcloud billing projects link safaricom-intelligence --billing-account=YOUR_ACCOUNT_ID
gcloud services enable bigquery.googleapis.com storage.googleapis.com iam.googleapis.com

gcloud iam service-accounts create safaricom-intel-sa --display-name="Safaricom Intelligence SA"

gcloud projects add-iam-policy-binding safaricom-intelligence \
  --member="serviceAccount:safaricom-intel-sa@safaricom-intelligence.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding safaricom-intelligence \
  --member="serviceAccount:safaricom-intel-sa@safaricom-intelligence.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

mkdir -p ~/.gcp
gcloud iam service-accounts keys create ~/.gcp/safaricom-intelligence-sa.json \
  --iam-account=safaricom-intel-sa@safaricom-intelligence.iam.gserviceaccount.com
chmod 644 ~/.gcp/safaricom-intelligence-sa.json
```

### Step 3: Provision infrastructure

```bash
cd terraform
export $(grep -v '^#' ../.env | grep -v '^$' | xargs)
terraform init
terraform apply
cd ..
```

### Step 4: Build the Docker image and start Airflow

```bash
mkdir -p logs
cd docker
docker build -t safaricom-intel-airflow:latest .
docker compose --env-file ../.env up airflow-init
# Wait for: "User admin created with role Admin"
docker compose --env-file ../.env up -d
```

Airflow UI at `http://localhost:8085`, login `admin` / `admin`.

> `docker-compose.yml` lives in this `docker/` subfolder while `.env` lives at the project root; always pass `--env-file ../.env` explicitly rather than relying on Compose's auto-discovery, which only looks for a `.env` sitting in the same folder as the compose file itself.

### Step 5: Seed historical data

The four CSVs in `ingestion/seed/data/` are already populated (FY15–FY26), sourced and cross-checked against Safaricom Results Booklets, Press Releases, and Annual Reports as documented above.

In the Airflow UI:
1. Unpause `safaricom_seed_dag`
2. Trigger manually (▶ button)
3. Monitor: `validate_seed_csvs → upload_to_gcs → load_to_bigquery → trigger_dbt_run`

### Step 6: dbt Cloud setup

1. Create a project in dbt Cloud, connect to BigQuery (upload the same service account JSON)
2. Connect your GitHub repo, models path `dbt/`
3. In the IDE: `dbt debug`, then `dbt build`; confirm all models and tests pass
4. Deploy → Environments → create a Production environment (deploy branch `main`)
5. Deploy → Jobs → create a job, command `dbt build`
6. From the job's URL (`.../deploy/<ACCOUNT_ID>/projects/<PROJECT_ID>/jobs/<JOB_ID>`), copy `DBT_ACCOUNT_ID` and `DBT_JOB_ID`
7. Account Settings → Service Tokens → create one (Job Admin scope) for `DBT_API_TOKEN`
8. Note your dbt Cloud host from the browser URL bar (e.g. `pz121.us1.dbt.com`) for `DBT_BASE_URL`; **don't assume it's `cloud.getdbt.com`**, multi-tenant accounts often aren't
9. Add all four values to `.env`, then `docker compose --env-file ../.env down && docker compose --env-file ../.env up -d` to actually load them (editing `.env` alone does nothing until containers restart)

### Step 7: Activate the weekly scraper

Unpause `safaricom_scraper_dag` in the Airflow UI. It runs every Monday at 08:00 EAT and auto-detects new Results Booklets from HY27 (November 2026) onward; no manual intervention needed after this point.

## Author

**Derrick Ryan Giggs**, Data Engineer, Matunda, Kakamega County, Kenya
GitHub: [github.com/Derrick-Ryan-Giggs](https://github.com/Derrick-Ryan-Giggs)
