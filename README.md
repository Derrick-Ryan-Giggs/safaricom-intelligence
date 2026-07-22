# Safaricom Intelligence

A cloud ELT pipeline that converts Safaricom PLC's public financial disclosure PDFs into a versioned, queryable BigQuery dataset — with dbt transformations and a Looker Studio dashboard built around three analytical lenses.

---

## Problem

Safaricom PLC — Kenya's largest telecom operator and the company behind M-PESA — publishes detailed segment-level KPIs twice a year via Results Booklets. These cover M-PESA sub-segments, connectivity revenue breakdown by type, and since FY2023 a Kenya vs Ethiopia geographic split.

This is one of the most granular publicly-disclosed telecom and fintech datasets in East Africa. But it exists only as a series of disconnected PDFs. Answering questions like "how has M-PESA's share of total revenue changed since FY15?" or "how does Ethiopia's customer growth compare to Kenya's early years?" requires manually opening a dozen-plus documents and transcribing numbers into a spreadsheet every time.

**Safaricom Intelligence** converts these public disclosures into a versioned, queryable BigQuery dataset spanning FY2015 through FY2026, refreshed automatically as new results are published, with a dbt transformation layer and Looker Studio dashboard.

---

## Analytical Objectives

### 1. M-PESA Deep-Dive (FY20-FY26)
Tracks M-PESA revenue, transaction value and volume, one-month-active customers, merchant network growth (Pochi tills + Lipa na M-PESA), Business Payments, M-PESA Global, and M-PESA share of total service revenue period over period.

### 2. Full Revenue Decomposition (FY15-FY26)
Covers Group KPIs (revenue, EBIT, net income, active customers, capex) over 12 years, enriched from FY20 onward with the full segment split: Voice, Mobile Data, Messaging, Mobile Incoming, Other Mobile Service, M-PESA, Fixed Service and IoT.

### 3. Kenya vs Ethiopia Comparative (FY22-FY26)
Places Kenya's mature-market KPIs side by side with STE's early-stage trajectory — service revenue, active customer base, and EBIT — surfacing how quickly Ethiopia's loss is narrowing and how its customer base ratio against Kenya evolves.

---

## Data Sources

All data sourced from **safaricom.co.ke** — free, no authentication, no API key required.

| Source | Coverage | Format | Feeds |
|---|---|---|---|
| Results Booklets | FY20-FY26 | Structured template, Sections 1/2/4A-4C | All 4 raw tables |
| Press Release PDFs | FY15-FY19 | Narrative prose | company_overview only |

**Data quality note:** all YoY% and growth metrics are computed in dbt via LAG window functions. The booklets' own printed percentages have minor internal inconsistencies and are never ingested.

---

## Architecture

```
safaricom.co.ke (free, no auth)
       |
       |-- Results Booklets FY20-FY26
       |-- Press Release PDFs FY15-FY19
                    |
     Local: HP EliteBook (Docker Compose)
                    |
     Seed CSVs (one-time FY15-FY26) --> Airflow 2.9.2
     safaricom_seed_dag   (manual, one-time)
     safaricom_scraper_dag (weekly, Monday 08:00 EAT)
                    |
     GCP: safaricom-intelligence (Terraform)
                    |
     GCS: safaricom-intel-data-lake
       seed/ | pdfs/ | extracted/ | scraper_state/
                    |
     BigQuery: raw --> staging (dbt) --> mart (dbt)
                    |
     Looker Studio: 3-page dashboard
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| IaC | Terraform |
| Storage | Google Cloud Storage |
| Warehouse | BigQuery |
| Orchestration | Apache Airflow 2.9.2 (Docker Compose, LocalExecutor) |
| Extraction | Python — requests + pdfplumber |
| Transformation | dbt Cloud |
| Visualization | Looker Studio |
| Secrets | .env with TF_VAR_ prefixes for Terraform, os.environ reads inside Python |

---

## Project Structure

```
safaricom-intelligence/
|-- .env.example
|-- .gitignore
|-- README.md
|-- terraform/
|   |-- main.tf          # GCS bucket + BQ datasets + 4 raw tables
|   |-- variables.tf
|   `-- outputs.tf
|-- ingestion/
|   |-- requirements.txt
|   |-- seed/
|   |   |-- seed_loader.py
|   |   `-- data/
|   |       |-- company_overview.csv    # FY15-FY26, 12 rows
|   |       |-- mpesa_metrics.csv       # FY15-FY26, 12 rows
|   |       |-- revenue_segments.csv    # FY15-FY26, 12 rows
|   |       `-- kenya_ethiopia.csv      # FY22-FY26 KE+ET, 10 rows
|   `-- scraper/
|       |-- safaricom_scraper.py
|       `-- pdf_parser.py
|-- dags/
|   |-- safaricom_seed_dag.py
|   `-- safaricom_scraper_dag.py
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
|   |-- Dockerfile
|   `-- docker-compose.yml
`-- logs/
```

---

## Data Model

### Raw Layer (BigQuery dataset: raw)

All four tables share four leading columns: `period_label` (e.g. "FY26"), `period_type` ("FY" or "HY"), `fiscal_year` (INT64), `period_end_date` (DATE, partition key).

#### raw.company_overview — FY15-FY26, Group level
| Column | Type | Notes |
|---|---|---|
| total_revenue_kes_bn | NUMERIC | Group consolidated |
| service_revenue_kes_bn | NUMERIC | Group consolidated |
| ebit_kes_bn | NUMERIC | Blank FY15-FY16, not disclosed those years |
| net_income_kes_bn | NUMERIC | |
| active_customers_mn | NUMERIC | One-month active |
| capex_kes_bn | NUMERIC | PP&E + intangibles |

#### raw.mpesa_metrics — FY15-FY26, Safaricom Kenya
| Column | Type | Notes |
|---|---|---|
| mpesa_revenue_kes_bn | NUMERIC | |
| mpesa_txn_value_kes_bn | NUMERIC | KES billions. Blank FY15-FY20 |
| mpesa_txn_volume_mn | NUMERIC | Millions. Blank FY15-FY20 |
| mpesa_customers_1m_mn | NUMERIC | One-month active M-PESA customers |
| merchants_mn | NUMERIC | Pochi + LNM combined. Pre-FY22 = LNM only |
| business_payments_kes_bn | NUMERIC | Blank FY15-FY21 |
| mpesa_global_kes_bn | NUMERIC | Visa Card + IMT revenue. Blank FY15-FY20 |
| merchant_overdraft_customers | NUMERIC | Blank FY15-FY23, product not tracked until FY24 |

#### raw.revenue_segments — FY15-FY26, Safaricom Kenya
| Column | Type | Notes |
|---|---|---|
| voice_kes_bn | NUMERIC | FY15-FY19: includes mobile incoming (not broken out) |
| mobile_data_kes_bn | NUMERIC | |
| messaging_kes_bn | NUMERIC | |
| mobile_incoming_kes_bn | NUMERIC | Blank FY15-FY19, embedded in voice pre-FY20 |
| other_mobile_service_kes_bn | NUMERIC | |
| mpesa_kes_bn | NUMERIC | Ties to mpesa_metrics for same period |
| fixed_service_iot_kes_bn | NUMERIC | |
| connectivity_total_kes_bn | NUMERIC | voice+data+messaging+incoming+other |
| total_service_revenue_kes_bn | NUMERIC | connectivity+mpesa+fixed (sanity-check sum) |

#### raw.kenya_ethiopia — FY22-FY26, one row per geography per period
| Column | Type | Notes |
|---|---|---|
| geography | STRING | KE or ET |
| service_revenue_kes_bn | NUMERIC | |
| active_customers_1m_mn | NUMERIC | |
| active_customers_3m_mn | NUMERIC | |
| ebit_kes_bn | NUMERIC | Negative for ET until breakeven |
| capex_kes_bn | NUMERIC | ET: never disclosed in any booklet |

### Staging (dbt views)
One view per raw table. Handles type casting, NULL coalescing, and period ordering.

### Mart (dbt tables)

| Model | Computes |
|---|---|
| mart_mpesa_growth_trends | YoY revenue growth, txn value/volume growth, M-PESA % of SR — via LAG on period_type |
| mart_revenue_mix | Long-format, one row per (period, segment), each segment as % of total_service_revenue |
| mart_ke_et_trajectory | KE vs ET side by side, ET/KE customer ratio, period-over-period EBIT loss change |

---

## Setup and Reproduction

### Prerequisites
- GCP account with billing enabled
- Terraform >= 1.3.0
- Docker with Compose plugin
- gcloud CLI
- dbt Cloud account (free tier sufficient)

### 1. GCP Setup

```bash
gcloud projects create safaricom-intelligence --name="Safaricom Intelligence"
gcloud config set project safaricom-intelligence
gcloud billing projects link safaricom-intelligence --billing-account=YOUR_ACCOUNT_ID
gcloud services enable bigquery.googleapis.com storage.googleapis.com iam.googleapis.com

gcloud iam service-accounts create safaricom-intel-sa \
  --display-name="Safaricom Intelligence SA"

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

### 2. Environment

```bash
cp .env.example .env
# Key path is already set. Add dbt tokens once dbt Cloud project is created.
```

### 3. Terraform

```bash
cd terraform
export $(grep -v '^#' ../.env | grep -v '^$' | xargs)
terraform init
terraform apply
```

Creates: GCS bucket `safaricom-intel-data-lake` with four prefixes (seed, pdfs, extracted, scraper_state), BigQuery datasets raw/staging/mart, all four raw tables with YEAR partitioning on `period_end_date` and clustering.

### 4. Airflow

```bash
mkdir -p logs
cd docker
docker build -t safaricom-intel-airflow:latest .
export $(grep -v '^#' ../.env | grep -v '^$' | xargs)
docker compose up airflow-init
# Wait for: "User admin created with role Admin"
docker compose up -d
```

UI at `http://localhost:8085` — login: `admin` / `admin`

### 5. Seed Historical Data

Populate the four CSVs in `ingestion/seed/data/` from the Safaricom IR page booklets and press commentaries (FY15-FY26), then in the Airflow UI:

1. Unpause `safaricom_seed_dag`
2. Trigger manually
3. Monitor four tasks: validate_seed_csvs → upload_to_gcs → load_to_bigquery → trigger_dbt_run

### 6. dbt Cloud

1. Create a new project in dbt Cloud
2. Connect to BigQuery — upload `~/.gcp/safaricom-intelligence-sa.json` directly
3. Set dataset to `staging`, location to `US`
4. Point to this repo, models path `dbt/`
5. Run `dbt run` then `dbt test`
6. Copy your `DBT_ACCOUNT_ID`, `DBT_JOB_ID`, `DBT_API_TOKEN` into `.env`

### 7. Looker Studio

Connect to `safaricom-intelligence.mart.*`:
- **Page 1 — M-PESA Intelligence**: revenue trend FY20-FY26, M-PESA % of SR, txn value/volume, merchant base growth
- **Page 2 — Revenue Mix**: stacked area chart voice/data/messaging/M-PESA/fixed share FY15-FY26
- **Page 3 — Kenya vs Ethiopia**: dual-axis revenue + customers, EBIT trajectory, ET/KE ratio

### 8. Activate Weekly Scraper

Unpause `safaricom_scraper_dag` in the Airflow UI. It runs every Monday at 08:00 EAT and auto-detects new booklets from HY27 (November 2026) onward. No manual intervention needed after this point.

---

## Airflow DAGs

### safaricom_seed_dag (manual trigger, one-time)
```
validate_seed_csvs → upload_to_gcs → load_to_bigquery → trigger_dbt_run
```

### safaricom_scraper_dag (every Monday 05:00 UTC)
```
check_ir_page → download_booklet → extract_tables → load_to_bigquery → trigger_dbt_run
```

`check_ir_page` is a `ShortCircuitOperator` — if no new booklet is detected it skips all downstream tasks. A GCS marker file (`scraper_state/last_processed_url.txt`) tracks the last processed booklet URL.

---

## Known Data Limitations

| Gap | Reason | Impact |
|---|---|---|
| ebit_kes_bn blank FY15-FY16 | Not disclosed in press commentaries | EBIT trend starts FY17 |
| mobile_incoming_kes_bn blank FY15-FY19 | Embedded in voice before FY20 reclassification | Connectivity sub-split starts FY20 |
| M-PESA txn value/volume blank FY15-FY20 | Not published in those booklets | Volume trend starts FY21 |
| capex_kes_bn ET all years | Never disclosed in any STE appendix | ET capex excluded from all models |
| FY23 ET service_revenue and EBIT blank | STE was only 7 months operational in FY23, not a comparable full year | ET full-year trend starts FY24 |
| merchant_overdraft_customers blank FY15-FY23 | Product not consistently tracked until FY24 | Merchant OD metric starts FY24 |

---

## Repository

**GitHub:** https://github.com/Derrick-Ryan-Giggs/safaricom-intelligence

**Author:** Derrick Ryan Giggs

**GCP Project:** safaricom-intelligence

**Current Status:** Infrastructure provisioned, seed data loaded FY15-FY26, dbt models written, Looker Studio dashboard pending