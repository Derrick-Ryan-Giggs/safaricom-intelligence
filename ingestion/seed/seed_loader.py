#!/usr/bin/env python3
"""
seed_loader.py
Reads the four seed CSVs from ingestion/seed/data/, uploads them to GCS,
then runs a BigQuery Load Job per table with explicit schema (WRITE_TRUNCATE).

Usage (run from project root):
    python3 ingestion/seed/seed_loader.py

All config via environment variables — see .env.example.
"""

import os, csv, json, logging
from pathlib import Path
from google.cloud import storage, bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config from env ────────────────────────────────────────────────────────────
PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
BUCKET_NAME  = os.environ["GCS_BUCKET_NAME"]
DATASET_RAW  = os.environ.get("BQ_DATASET_RAW", "raw")
CREDS_FILE   = os.environ["GCP_CREDENTIALS_FILE"]
DATA_DIR     = Path(__file__).parent / "data"

# ── BQ schemas — must match terraform/main.tf exactly ─────────────────────────
PERIOD_COLS = [
    bigquery.SchemaField("period_label",   "STRING"),
    bigquery.SchemaField("period_type",    "STRING"),
    bigquery.SchemaField("fiscal_year",    "INT64"),
    bigquery.SchemaField("period_end_date","DATE"),
]

SCHEMAS = {
    "company_overview": PERIOD_COLS + [
        bigquery.SchemaField("total_revenue_kes_bn",   "NUMERIC"),
        bigquery.SchemaField("service_revenue_kes_bn", "NUMERIC"),
        bigquery.SchemaField("ebit_kes_bn",            "NUMERIC"),
        bigquery.SchemaField("net_income_kes_bn",      "NUMERIC"),
        bigquery.SchemaField("active_customers_mn",    "NUMERIC"),
        bigquery.SchemaField("capex_kes_bn",           "NUMERIC"),
    ],
    "mpesa_metrics": PERIOD_COLS + [
        bigquery.SchemaField("mpesa_revenue_kes_bn",     "NUMERIC"),
        bigquery.SchemaField("mpesa_txn_value_kes_bn",   "NUMERIC"),
        bigquery.SchemaField("mpesa_txn_volume_mn",      "NUMERIC"),
        bigquery.SchemaField("mpesa_customers_1m_mn",    "NUMERIC"),
        bigquery.SchemaField("merchants_mn",             "NUMERIC"),
        bigquery.SchemaField("business_payments_kes_bn", "NUMERIC"),
        bigquery.SchemaField("mpesa_global_kes_bn",      "NUMERIC"),
        bigquery.SchemaField("merchant_overdraft_customers", "NUMERIC"),
    ],
    "revenue_segments": PERIOD_COLS + [
        bigquery.SchemaField("voice_kes_bn",                 "NUMERIC"),
        bigquery.SchemaField("mobile_data_kes_bn",           "NUMERIC"),
        bigquery.SchemaField("messaging_kes_bn",             "NUMERIC"),
        bigquery.SchemaField("mobile_incoming_kes_bn",       "NUMERIC"),
        bigquery.SchemaField("other_mobile_service_kes_bn",  "NUMERIC"),
        bigquery.SchemaField("mpesa_kes_bn",                 "NUMERIC"),
        bigquery.SchemaField("fixed_service_iot_kes_bn",     "NUMERIC"),
        bigquery.SchemaField("connectivity_total_kes_bn",    "NUMERIC"),
        bigquery.SchemaField("total_service_revenue_kes_bn", "NUMERIC"),
    ],
    "kenya_ethiopia": PERIOD_COLS + [
        bigquery.SchemaField("geography",              "STRING"),
        bigquery.SchemaField("service_revenue_kes_bn", "NUMERIC"),
        bigquery.SchemaField("active_customers_1m_mn", "NUMERIC"),
        bigquery.SchemaField("active_customers_3m_mn", "NUMERIC"),
        bigquery.SchemaField("ebit_kes_bn",            "NUMERIC"),
        bigquery.SchemaField("capex_kes_bn",           "NUMERIC"),
    ],
}


def get_clients():
    creds = service_account.Credentials.from_service_account_file(CREDS_FILE)
    gcs = storage.Client(project=PROJECT_ID, credentials=creds)
    bq  = bigquery.Client(project=PROJECT_ID, credentials=creds)
    return gcs, bq


def validate_csv(table_name: str, csv_path: Path) -> list[dict]:
    """Check CSV exists, headers match schema, return rows."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Seed CSV not found: {csv_path}")

    expected_cols = [f.name for f in SCHEMAS[table_name]]
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        actual_cols = reader.fieldnames or []
        if list(actual_cols) != expected_cols:
            raise ValueError(
                f"{table_name}: header mismatch\n"
                f"  expected: {expected_cols}\n"
                f"  got:      {list(actual_cols)}"
            )
        rows = list(reader)

    log.info(f"  {table_name}: {len(rows)} rows validated")
    return rows


def upload_to_gcs(gcs_client, table_name: str, csv_path: Path) -> str:
    """Upload CSV to GCS seed/ prefix, return GCS URI."""
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob_name = f"seed/{table_name}.csv"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(csv_path), content_type="text/csv")
    uri = f"gs://{BUCKET_NAME}/{blob_name}"
    log.info(f"  Uploaded → {uri}")
    return uri


def load_to_bigquery(bq_client, table_name: str, gcs_uri: str):
    """BQ Load Job from GCS → raw.{table_name}, WRITE_TRUNCATE, explicit schema."""
    table_ref = f"{PROJECT_ID}.{DATASET_RAW}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS[table_name],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        null_marker="",   # empty string in CSV → NULL in BQ
    )

    job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()  # wait for completion

    dest = bq_client.get_table(table_ref)
    log.info(f"  Loaded {dest.num_rows} rows → {table_ref}")


def main():
    log.info("=== Safaricom Intelligence — Seed Loader ===")
    log.info(f"Project : {PROJECT_ID}")
    log.info(f"Bucket  : {BUCKET_NAME}")
    log.info(f"Dataset : {DATASET_RAW}")
    log.info(f"Data dir: {DATA_DIR}")

    gcs_client, bq_client = get_clients()

    tables = ["company_overview", "mpesa_metrics", "revenue_segments", "kenya_ethiopia"]

    for table in tables:
        log.info(f"--- {table} ---")
        csv_path = DATA_DIR / f"{table}.csv"

        # 1. Validate
        validate_csv(table, csv_path)

        # 2. Upload to GCS
        gcs_uri = upload_to_gcs(gcs_client, table, csv_path)

        # 3. BQ Load Job
        load_to_bigquery(bq_client, table, gcs_uri)

    log.info("=== All tables loaded successfully ===")


if __name__ == "__main__":
    main()