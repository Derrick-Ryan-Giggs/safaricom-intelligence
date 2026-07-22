> **A cloud ELT pipeline that converts Safaricom PLC's public financial disclosure PDFs into a versioned, queryable BigQuery dataset — with dbt transformations and a Looker Studio dashboard built around three analytical lenses.**

---

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
- [Transformations (dbt Cloud)](#transformations-dbt-cloud)
- [Dashboard](#dashboard)
- [Known Data Limitations](#known-data-limitations)
- [Reproducibility - How to Run](#reproducibility---how-to-run)
- [Author](#author)

---

## Problem Description

Safaricom PLC - Kenya's largest telecom operator and the company behind M-PESA - publishes detailed segment-level KPIs twice a year via Results Booklets (half-year and full-year results). These cover M-PESA sub-segments, connectivity revenue breakdown by type, and since FY2023 a Kenya vs Ethiopia geographic split.

This is one of the most granular publicly-disclosed telecom and fintech datasets in East Africa. But it exists only as a series of disconnected PDFs. Answering questions like "how has M-PESA's share of total revenue changed since FY15?" or "how does Ethiopia's customer growth compare to Kenya's early years?" requires manually opening a dozen-plus documents and transcribing numbers into a spreadsheet every time.

**Safaricom Intelligence** converts these public disclosures into a versioned, queryable BigQuery dataset spanning FY2015 through FY2026, refreshed automatically as new results are published, with a dbt transformation layer and Looker Studio dashboard.

---

## Solution Overview

| Dimension | Detail |
|---|---|
| Scope | FY2015 to FY2026, 12 years of financial history |
| Granularity | Segment-level: M-PESA sub-segments, connectivity breakdown, Kenya vs Ethiopia |
| Refresh cadence | Automatic via weekly Airflow scraper (every Monday 08:00 EAT) |
| Deployment | Local Docker Compose + GCP (Terraform-provisioned) |
| Data source | safaricom.co.ke - free, no authentication, no API key |

**Key analytical questions answered:**

- How has M-PESA grown from 38% to 45.6% of total Kenya service revenue between FY15 and FY26?
- Which connectivity segment - voice, data, or messaging - has driven or dragged revenue year over year?
- How quickly is Ethiopia's EBIT loss narrowing, and what does its customer growth trajectory look like vs Kenya's early years?
- When did mobile data revenue overtake voice as the largest connectivity sub-segment?

---

## Architecture

```
safaricom.co.ke (free, no auth)
        |
        |-- Results Booklets FY20-FY26 (structured template, Sections 1/2/4A-4C)
        |-- Press Release PDFs FY15-FY19 (narrative prose)
                    |
    Local: HP EliteBook (Docker Compose)
                    |
    Seed CSVs (one-time backfill FY15-FY26) --> Airflow 2.9.2
    safaricom_seed_dag   (manual trigger, one-time)
    safaricom_scraper_dag (every Monday 08:00 EAT)
                    |
    GCP: safaricom-intelligence (Terraform-provisioned)
                    |
    GCS: safaricom-intel-data-lake
        seed/ | pdfs/ | extracted/ | scraper_state/
                    |
    BigQuery
        raw --> staging (dbt views) --> mart (dbt tables)
                    |
    Looker Studio: 3-page dashboard
        1. M-PESA Intelligence
        2. Revenue Mix
        3. Kenya vs Ethiopia
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| IaC | Terraform | Provisions GCS bucket, BQ datasets, and all 4 raw tables |
| Storage | Google Cloud Storage | Raw zone - seed CSVs, downloaded PDFs, scraper JSONL |
| Warehouse | BigQuery | raw / staging / mart datasets, YEAR-partitioned on period_end_date |
| Orchestration | Apache Airflow 2.9.2 (Docker Compose, LocalExecutor) | seed_dag + scraper_dag |
| Extraction | Python - requests + pdfplumber | PDF download and table extraction |
| Transformation | dbt Cloud | Staging views, mart tables, YoY window functions |
| Visualization | Looker Studio | 3-page public dashboard |
| Secrets | .env with TF_VAR_ prefixes for Terraform, os.environ inside Python | No hardcoded values anywhere |

---

## Data Sources

All data sourced from **safaricom.co.ke** - free, no authentication, no API key required.

| Source | Coverage | Format | Feeds |
|---|---|---|---|
| Results Booklets | FY20-FY26 | Structured template - Sections 1 (KPIs), 2 (Income Statement), 4A-4C (segment detail) | All 4 raw tables |
| Press Release PDFs | FY15-FY19 | Narrative prose - headline figures embedded in text | company_overview only |

**Data quality decision:** All YoY% and growth metrics are computed in dbt via LAG window functions on period_end_date. The booklets' own printed percentages have minor internal inconsistencies (e.g. Group base-station % not recalculated after adding Ethiopia) and are never ingested.

---

## Project Structure

```
safaricom-intelligence/
|-- .env.example                    # env var template - copy to .env, never commit .env
|-- .gitignore
|-- README.md
|-- terraform/
|   |-- main.tf                     # GCS bucket + BQ datasets + 4 raw tables
|   |-- variables.tf
|   `-- outputs.tf
|-- ingestion/
|   |-- requirements.txt
|   |-- seed/
|   |   |-- seed_loader.py
|   |   `-- data/
|   |       |-- company_overview.csv    # FY15-FY26, 12 rows, Group level
|   |       |-- mpesa_metrics.csv       # FY15-FY26, 12 rows, Kenya segment
|   |       |-- revenue_segments.csv    # FY15-FY26, 12 rows, Kenya segment
|   |       `-- kenya_ethiopia.csv      # FY22-FY26 KE+ET, 10 rows
|   `-- scraper/
|       |-- safaricom_scraper.py    # standalone scraper utility
|       `-- pdf_parser.py          # pdfplumber extraction logic
|-- dags/
|   |-- safaricom_seed_dag.py      # one-time historical load
|   `-- safaricom_scraper_dag.py   # weekly auto-scraper
|-- dbt/
|   |-- dbt_project.yml
|   |-- models/
|   |   |-- staging/
|   |   |   |-- stg_company_overview.sql
|   |   |   |-- stg_mpesa_metrics.sql
|   |   |   |-- stg_revenue_segments.sql
|   |   |   |-- stg_kenya_ethiopia.sql
|   |   |   `-- schema.yml
|   |   `-- mart/
|   |       |-- mart_mpesa_growth_trends.sql
|   |       |-- mart_revenue_mix.sql
|   |       |-- mart_ke_et_trajectory.sql
|   |       `-- schema.yml
|   |-- macros/
|   |   `-- safe_divide.sql
|   `-- tests/
|       `-- assert_revenue_segments_connectivity_sum.sql
|-- docker/
|   |-- Dockerfile                 # extends kenya-health-airflow:latest, adds GCP + pdfplumber
|   `-- docker-compose.yml        # Airflow + Postgres 13 (ports 8085/5439)
`-- logs/                          # Airflow task logs (gitignored)
```

---

## Data Model

### Raw Layer (BigQuery dataset: raw)

All four tables share four leading columns: `period_label` (e.g. "FY26"), `period_type` ("FY" or "HY"), `fiscal_year` (INT64), `period_end_date` (DATE, YEAR partition key).

#### raw.company_overview - FY15-FY26, Group level

| Column | Type | Notes |
|---|---|---|
| total_revenue_kes_bn | NUMERIC | Group consolidated |
| service_revenue_kes_bn | NUMERIC | Group consolidated |
| ebit_kes_bn | NUMERIC | Blank FY15-FY16, not disclosed those years |
| net_income_kes_bn | NUMERIC | |
| active_customers_mn | NUMERIC | One-month active |
| capex_kes_bn | NUMERIC | PP&E + intangibles |

#### raw.mpesa_metrics - FY15-FY26, Safaricom Kenya

| Column | Type | Notes |
|---|---|---|
| mpesa_revenue_kes_bn | NUMERIC | |
| mpesa_txn_value_kes_bn | NUMERIC | KES billions (1 Trn = 1,000 Bn). Blank FY15-FY20 |
| mpesa_txn_volume_mn | NUMERIC | Millions of transactions. Blank FY15-FY20 |
| mpesa_customers_1m_mn | NUMERIC | One-month active M-PESA customers |
| merchants_mn | NUMERIC | Pochi + LNM combined. Pre-FY22 = LNM only |
| business_payments_kes_bn | NUMERIC | Blank FY15-FY21 |
| mpesa_global_kes_bn | NUMERIC | Visa Card + IMT revenue. Blank FY15-FY20 |
| merchant_overdraft_customers | NUMERIC | Blank FY15-FY23, product not tracked until FY24 |

#### raw.revenue_segments - FY15-FY26, Safaricom Kenya

| Column | Type | Notes |
|---|---|---|
| voice_kes_bn | NUMERIC | FY15-FY19: includes mobile incoming (not separately reported) |
| mobile_data_kes_bn | NUMERIC | |
| messaging_kes_bn | NUMERIC | |
| mobile_incoming_kes_bn | NUMERIC | Blank FY15-FY19, embedded in voice pre-FY20 reclassification |
| other_mobile_service_kes_bn | NUMERIC | |
| mpesa_kes_bn | NUMERIC | Ties to mpesa_metrics for same period |
| fixed_service_iot_kes_bn | NUMERIC | |
| connectivity_total_kes_bn | NUMERIC | voice + data + messaging + incoming + other |
| total_service_revenue_kes_bn | NUMERIC | Sanity-check sum: connectivity + mpesa + fixed |

#### raw.kenya_ethiopia - FY22-FY26, one row per geography per period

| Column | Type | Notes |
|---|---|---|
| geography | STRING | KE or ET |
| service_revenue_kes_bn | NUMERIC | |
| active_customers_1m_mn | NUMERIC | |
| active_customers_3m_mn | NUMERIC | |
| ebit_kes_bn | NUMERIC | Negative for ET until breakeven |
| capex_kes_bn | NUMERIC | ET: never disclosed in any STE appendix |

### Staging Layer (dbt views)

One view per raw table. Handles type casting, NULL coalescing, and period ordering. Models: `stg_company_overview`, `stg_mpesa_metrics`, `stg_revenue_segments`, `stg_kenya_ethiopia`.

### Mart Layer (dbt tables)

| Model | Computes |
|---|---|
| mart_mpesa_growth_trends | YoY revenue growth, txn value/volume growth, M-PESA % of service revenue - via LAG window on period_type |
| mart_revenue_mix | Long-format: one row per (period, segment), each segment as % of total_service_revenue - built for stacked-area charting |
| mart_ke_et_trajectory | KE and ET side by side per period, ET/KE customer ratio, period-over-period EBIT loss change |

---

## Infrastructure as Code (Terraform)

Terraform runs once to provision all GCP resources. It is not a running service.

```bash
cd terraform
export $(grep -v '^#' ../.env | grep -v '^$' | xargs)
terraform init
terraform apply
```

Terraform provisions:

- GCS bucket `safaricom-intel-data-lake` with four prefixes: `seed/`, `pdfs/`, `extracted/`, `scraper_state/`
- BigQuery dataset `raw` - one row per Safaricom reporting period
- BigQuery dataset `staging` - dbt views
- BigQuery dataset `mart` - dbt tables
- Four raw tables with explicit schemas, YEAR partitioning on `period_end_date`, and clustering

All table schemas are defined explicitly in `main.tf` - no autodetect used anywhere.

---

## Data Ingestion and Orchestration

### DAG Design

The pipeline uses Apache Airflow with LocalExecutor running inside a custom Docker image built on `kenya-health-airflow:latest`.

**safaricom_seed_dag (manual trigger, one-time)**

```
validate_seed_csvs --> upload_to_gcs --> load_to_bigquery --> trigger_dbt_run
```

- `validate_seed_csvs`: checks all 4 CSVs exist and headers exactly match the BQ DDL
- `upload_to_gcs`: copies CSVs to `gs://safaricom-intel-data-lake/seed/`
- `load_to_bigquery`: BQ Load Job per table, WRITE_TRUNCATE, explicit schema
- `trigger_dbt_run`: calls dbt Cloud API to rebuild staging + mart models

**safaricom_scraper_dag (every Monday 05:00 UTC = 08:00 EAT)**

```
check_ir_page --> download_booklet --> extract_tables --> load_to_bigquery --> trigger_dbt_run
```

- `check_ir_page`: ShortCircuitOperator - fetches IR results page, compares latest booklet URL against GCS marker. Short-circuits (skips all downstream) if no new booklet detected
- `download_booklet`: downloads new PDF to `gs://safaricom-intel-data-lake/pdfs/`
- `extract_tables`: pdfplumber extraction of Sections 1/2/4A-4C, writes JSONL to `extracted/`
- `load_to_bigquery`: BQ Load Job, WRITE_APPEND
- `trigger_dbt_run`: dbt Cloud API call + updates `scraper_state/last_processed_url.txt`

### PDF Extraction

`pdf_parser.py` uses `pdfplumber.extract_text(layout=True)` which preserves column spacing. Extraction was validated against real FY26 booklet text: 23/24 KPI rows and 9/9 income statement rows parsed correctly on the first pass using regex patterns against layout-aware text output.

---

## Transformations (dbt Cloud)

### Staging Models (materialized as views)

| Model | Key Transformations |
|---|---|
| stg_company_overview | Type casting, NULL coalescing for blank EBIT years, period ordering |
| stg_mpesa_metrics | NUMERIC casting, NULL handling for pre-FY20 txn columns |
| stg_revenue_segments | Connectivity sum validation, mobile_incoming NULL handling for FY15-FY19 |
| stg_kenya_ethiopia | Geography filter, EBIT sign validation (ET expected negative) |

### Mart Models (materialized as tables)

**mart_mpesa_growth_trends** - YoY growth via LAG:

```sql
mpesa_revenue_yoy_pct = (mpesa_revenue_kes_bn - LAG(mpesa_revenue_kes_bn)
    OVER (PARTITION BY period_type ORDER BY period_end_date))
    / LAG(mpesa_revenue_kes_bn) OVER (...)  * 100

mpesa_pct_of_service_revenue = mpesa_revenue_kes_bn / total_service_revenue_kes_bn * 100
```

**mart_revenue_mix** - Long-format segment share:

```sql
-- One row per (period, segment) for stacked-area charting
segment_pct_of_sr = segment_revenue / total_service_revenue_kes_bn * 100
```

**mart_ke_et_trajectory** - KE vs ET comparison:

```sql
et_ke_customer_ratio = et.active_customers_1m_mn / ke.active_customers_1m_mn
ebit_loss_change = et.ebit_kes_bn - LAG(et.ebit_kes_bn) OVER (ORDER BY period_end_date)
-- Positive = loss narrowing, negative = loss widening
```

### Tests

- `assert_revenue_segments_connectivity_sum`: flags any row where voice + data + messaging + incoming + other differs from connectivity_total by more than 0.05 Bn
- Schema tests: not_null on period_end_date and key metric columns, accepted_values on period_type and geography

---

## Dashboard

**Tool:** Looker Studio
**Dataset:** `safaricom-intelligence.mart.*`
**Live Dashboard:** [View Safaricom Intelligence Dashboard](https://lookerstudio.google.com)

### Page 1 - M-PESA Intelligence

M-PESA revenue trend FY20-FY26, M-PESA as % of total service revenue, transaction value and volume growth, merchant base growth (Pochi tills + Lipa na M-PESA), and Business Payments vs Global revenue breakdown.

### Page 2 - Revenue Mix

Stacked area chart showing voice, mobile data, messaging, M-PESA, and fixed service share of total service revenue from FY15 to FY26. The chart makes visible the structural shift from voice-dominated revenue (voice was 55.8% of connectivity in FY22) toward data and M-PESA. Also includes total-company revenue, EBIT, and net income trend using the longer Group-level history.

### Page 3 - Kenya vs Ethiopia

Dual-axis line chart (service revenue on left axis, active customers on right) for Kenya and Ethiopia side by side, FY22-FY26. EBIT trajectory comparison showing Ethiopia's loss-narrowing trend against Kenya's growing EBIT. ET/KE customer base ratio over time.

---

## Known Data Limitations

| Gap | Reason | Impact |
|---|---|---|
| ebit_kes_bn blank FY15-FY16 | Not separately disclosed in press commentaries | EBIT trend starts FY17 |
| mobile_incoming_kes_bn blank FY15-FY19 | Embedded in voice before FY20 reclassification - cannot be separated without internal Safaricom data | Connectivity sub-split starts FY20 |
| M-PESA txn value and volume blank FY15-FY20 | Not published in early booklets | Transaction volume trend starts FY21 |
| capex_kes_bn ET all years | Never disclosed in any STE appendix across all booklets | ET capex excluded from all models |
| FY23 ET service_revenue and ebit blank | STE was only 7 months operational in FY23, not a comparable full year | ET full-year trend starts FY24 |
| merchant_overdraft_customers blank FY15-FY23 | Product not consistently tracked and disclosed until FY24 | Merchant OD metric starts FY24 |

---

## Reproducibility - How to Run

### Prerequisites

| Requirement | Notes |
|---|---|
| Ubuntu / Linux | 22.04+ |
| Docker Engine + Compose plugin | V2 |
| Terraform 1.3.0+ | |
| gcloud CLI | |
| dbt Cloud account | Free tier sufficient |
| GCP account with billing enabled | |

### Step 1 - Clone and configure

```bash
git clone https://github.com/Derrick-Ryan-Giggs/safaricom-intelligence.git
cd safaricom-intelligence
cp .env.example .env
```

### Step 2 - GCP setup

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

### Step 3 - Provision infrastructure

```bash
cd terraform
export $(grep -v '^#' ../.env | grep -v '^$' | xargs)
terraform init
terraform apply
cd ..
```

### Step 4 - Build Docker image and start Airflow

```bash
mkdir -p logs
cd docker
docker build -t safaricom-intel-airflow:latest .
export $(grep -v '^#' ../.env | grep -v '^$' | xargs)
docker compose up airflow-init
# Wait for: "User admin created with role Admin"
docker compose up -d
```

Airflow UI at `http://localhost:8085` - login: `admin` / `admin`

### Step 5 - Seed historical data

Replace the four CSV files in `ingestion/seed/data/` with populated versions (FY15-FY26) sourced from Safaricom IR results booklets and press commentaries at `safaricom.co.ke/investor-relations`.

In the Airflow UI:

1. Unpause `safaricom_seed_dag`
2. Trigger manually (play button)
3. Monitor: validate_seed_csvs > upload_to_gcs > load_to_bigquery > trigger_dbt_run

### Step 6 - dbt Cloud setup

1. Create a project in dbt Cloud
2. Connect to BigQuery - upload `~/.gcp/safaricom-intelligence-sa.json` directly via the file picker
3. Set dataset to `staging`, location to `US`
4. Point to this repo, models path `dbt/`
5. Run `dbt run` then `dbt test`
6. Copy `DBT_ACCOUNT_ID`, `DBT_JOB_ID`, `DBT_API_TOKEN` into `.env`

### Step 7 - Activate weekly scraper

Unpause `safaricom_scraper_dag` in the Airflow UI. It runs every Monday at 08:00 EAT and auto-detects new Results Booklets from HY27 (November 2026) onward. No manual intervention needed after this point.

---

## Author

**Derrick Ryan Giggs**

- GitHub: [github.com/Derrick-Ryan-Giggs](https://github.com/Derrick-Ryan-Giggs)
- LinkedIn: [linkedin.com/in/ryan-giggs-a19330265](https://linkedin.com/in/ryan-giggs-a19330265)
- Medium: [medium.com/@derrickryangiggs](https://medium.com/@derrickryangiggs)
