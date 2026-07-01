terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ─────────────────────────────────────────────
# GCS — raw data lake bucket
# ─────────────────────────────────────────────
resource "google_storage_bucket" "data_lake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  project                     = var.project_id
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 365 }
    action    { type = "Delete" }
  }
}

# Folder prefixes (GCS is flat but these make structure explicit)
resource "google_storage_bucket_object" "seed_prefix" {
  name    = "seed/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "keep"
}

resource "google_storage_bucket_object" "pdfs_prefix" {
  name    = "pdfs/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "keep"
}

resource "google_storage_bucket_object" "extracted_prefix" {
  name    = "extracted/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "keep"
}

resource "google_storage_bucket_object" "scraper_state_prefix" {
  name    = "scraper_state/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "keep"
}

# ─────────────────────────────────────────────
# BigQuery — datasets
# ─────────────────────────────────────────────
resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  project     = var.project_id
  location    = var.bq_location
  description = "Raw layer — one row per Safaricom reporting period, ingested directly from seed CSVs or scraper JSONL"

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  project     = var.project_id
  location    = var.bq_location
  description = "dbt staging models — type casting, renaming, light validation"

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "mart" {
  dataset_id  = "mart"
  project     = var.project_id
  location    = var.bq_location
  description = "dbt mart models — mpesa_growth_trends, revenue_mix, ke_et_trajectory"

  delete_contents_on_destroy = true
}

# ─────────────────────────────────────────────
# BigQuery — raw tables (explicit schema, no autodetect)
# ─────────────────────────────────────────────

# Shared period columns used across all 4 tables:
#   period_label   STRING   e.g. "FY26", "HY26"
#   period_type    STRING   "FY" or "HY"
#   fiscal_year    INT64    Safaricom FY number
#   period_end_date DATE    Real time-series key (partition column)

locals {
  period_columns = [
    {
      name        = "period_label"
      type        = "STRING"
      description = "Safaricom's own period label, e.g. FY26 or HY26"
    },
    {
      name        = "period_type"
      type        = "STRING"
      description = "FY or HY"
    },
    {
      name        = "fiscal_year"
      type        = "INT64"
      description = "Safaricom fiscal year number — HY26 carries fiscal_year=2026 even though period_end_date is Sep 2025"
    },
    {
      name        = "period_end_date"
      type        = "DATE"
      description = "Last day of the reporting period — primary time-series key and partition column"
    },
  ]
}

# ── raw.company_overview ─────────────────────
# FY14–FY26 (Group / consolidated level)
resource "google_bigquery_table" "company_overview" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "company_overview"
  project             = var.project_id
  deletion_protection = false
  description         = "Safaricom Group headline KPIs, FY14–FY26. Sourced from Results Booklets (FY20+) and Press Release PDFs (FY14–19)."

  time_partitioning {
    type  = "YEAR"
    field = "period_end_date"
  }

  clustering = ["period_type", "fiscal_year"]

  schema = jsonencode(concat(local.period_columns, [
    { name = "total_revenue_kes_bn",   type = "NUMERIC", description = "Total revenue, Group consolidated (KES billions)" },
    { name = "service_revenue_kes_bn", type = "NUMERIC", description = "Service revenue, Group consolidated (KES billions)" },
    { name = "ebit_kes_bn",            type = "NUMERIC", description = "Operating profit (EBIT), Group consolidated (KES billions)" },
    { name = "net_income_kes_bn",      type = "NUMERIC", description = "Profit for the period, Group consolidated (KES billions)" },
    { name = "active_customers_mn",    type = "NUMERIC", description = "One-month active customers, millions" },
    { name = "capex_kes_bn",           type = "NUMERIC", description = "Capital expenditure, Group consolidated (KES billions)" },
  ]))
}

# ── raw.mpesa_metrics ────────────────────────
# FY20–FY26 + HY20–HY26 (Safaricom Kenya M-PESA segment)
resource "google_bigquery_table" "mpesa_metrics" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "mpesa_metrics"
  project             = var.project_id
  deletion_protection = false
  description         = "M-PESA segment KPIs (Safaricom Kenya), FY20–FY26 + HY20–HY26. Sourced from Results Booklets Section 1b and 4A."

  time_partitioning {
    type  = "YEAR"
    field = "period_end_date"
  }

  clustering = ["period_type", "fiscal_year"]

  schema = jsonencode(concat(local.period_columns, [
    { name = "mpesa_revenue_kes_bn",       type = "NUMERIC", description = "M-PESA total revenue, Safaricom Kenya (KES billions)" },
    { name = "mpesa_txn_value_kes_bn",     type = "NUMERIC", description = "Total value of M-PESA transactions (KES billions)" },
    { name = "mpesa_txn_volume_mn",        type = "NUMERIC", description = "Total M-PESA transaction count (millions)" },
    { name = "mpesa_customers_1m_mn",      type = "NUMERIC", description = "One-month active M-PESA customers (millions)" },
    { name = "merchants_mn",               type = "NUMERIC", description = "Total merchant count — Pochi tills + Lipa na M-PESA (millions)" },
    { name = "business_payments_kes_bn",   type = "NUMERIC", description = "Section 4Ab — Business Payments revenue (KES billions)" },
    { name = "mpesa_global_kes_bn",        type = "NUMERIC", description = "Section 4Ad — M-PESA Global revenue (KES billions)" },
    { name = "merchant_overdraft_customers", type = "NUMERIC", description = "Section 4Ac — Merchant overdraft (Fuliza) customer count. Integer count stored as NUMERIC for BQ schema consistency." },
  ]))
}

# ── raw.revenue_segments ─────────────────────
# FY20–FY26 + HY20–HY26 (Safaricom Kenya segment breakdown)
resource "google_bigquery_table" "revenue_segments" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "revenue_segments"
  project             = var.project_id
  deletion_protection = false
  description         = "Safaricom Kenya revenue by segment, FY20–FY26 + HY20–HY26. Sourced from Results Booklets Sections 1b, 4B, and 4C."

  time_partitioning {
    type  = "YEAR"
    field = "period_end_date"
  }

  clustering = ["period_type", "fiscal_year"]

  schema = jsonencode(concat(local.period_columns, [
    { name = "voice_kes_bn",                  type = "NUMERIC", description = "Section 4B — Voice revenue (KES billions)" },
    { name = "mobile_data_kes_bn",            type = "NUMERIC", description = "Section 4B — Mobile Data revenue (KES billions)" },
    { name = "messaging_kes_bn",              type = "NUMERIC", description = "Section 4B — Messaging (SMS) revenue (KES billions)" },
    { name = "mobile_incoming_kes_bn",        type = "NUMERIC", description = "Section 4B — Mobile Incoming revenue (KES billions). Required to reconcile to connectivity_total." },
    { name = "other_mobile_service_kes_bn",   type = "NUMERIC", description = "Section 4B — Other Mobile Service revenue incl. content (KES billions)" },
    { name = "mpesa_kes_bn",                  type = "NUMERIC", description = "M-PESA revenue — ties to mpesa_metrics.mpesa_revenue_kes_bn for the same period (KES billions)" },
    { name = "fixed_service_iot_kes_bn",      type = "NUMERIC", description = "Section 4C — Fixed Service and IoT revenue (KES billions)" },
    { name = "connectivity_total_kes_bn",     type = "NUMERIC", description = "Section 1a/4B — total Connectivity revenue (KES billions)" },
    { name = "total_service_revenue_kes_bn",  type = "NUMERIC", description = "Sanity-check sum: connectivity_total + mpesa + fixed_service_iot (KES billions)" },
  ]))
}

# ── raw.kenya_ethiopia ───────────────────────
# FY23–FY26 + HY23–HY26 (one row per geography per period)
resource "google_bigquery_table" "kenya_ethiopia" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "kenya_ethiopia"
  project             = var.project_id
  deletion_protection = false
  description         = "Kenya vs Ethiopia KPIs side by side, FY23–FY26 + HY23–HY26. geography = KE or ET. Sourced from Results Booklets Sections 1b (KE) and 1c (ET)."

  time_partitioning {
    type  = "YEAR"
    field = "period_end_date"
  }

  clustering = ["period_type", "geography"]

  schema = jsonencode(concat(local.period_columns, [
    { name = "geography",               type = "STRING",  description = "KE = Safaricom Kenya, ET = Safaricom Telecommunications Ethiopia (STE). HY23 may have KE row only — STE launched commercially Oct 2022." },
    { name = "service_revenue_kes_bn",  type = "NUMERIC", description = "Service revenue for the geography (KES billions). Section 1b for KE, Section 1c for ET." },
    { name = "active_customers_1m_mn",  type = "NUMERIC", description = "One-month active customers (millions)" },
    { name = "active_customers_3m_mn",  type = "NUMERIC", description = "Three-month active customers (millions)" },
    { name = "ebit_kes_bn",             type = "NUMERIC", description = "Operating profit / loss for the geography (KES billions). ET will be negative until breakeven." },
    { name = "capex_kes_bn",            type = "NUMERIC", description = "Capital expenditure for the geography (KES billions)" },
  ]))
}