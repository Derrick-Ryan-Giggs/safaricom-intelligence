"""
safaricom_seed_dag.py
One-time historical seed DAG — loads all 4 CSVs into BigQuery raw layer.
Trigger manually once all CSVs are fully populated.
Tasks: validate → upload_gcs → load_bigquery → trigger_dbt
"""

import os, subprocess
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "safaricom-intel", "retries": 1}

with DAG(
    dag_id="safaricom_seed_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # manual trigger only
    catchup=False,
    tags=["safaricom", "seed"],
) as dag:

    def validate_seed_csvs():
        import csv, os
        from pathlib import Path

        data_dir = Path("/opt/airflow/ingestion/seed/data")
        expected = {
            "company_overview":  ["period_label","period_type","fiscal_year","period_end_date",
                                   "total_revenue_kes_bn","service_revenue_kes_bn","ebit_kes_bn",
                                   "net_income_kes_bn","active_customers_mn","capex_kes_bn"],
            "mpesa_metrics":     ["period_label","period_type","fiscal_year","period_end_date",
                                   "mpesa_revenue_kes_bn","mpesa_txn_value_kes_bn","mpesa_txn_volume_mn",
                                   "mpesa_customers_1m_mn","merchants_mn","business_payments_kes_bn",
                                   "mpesa_global_kes_bn","fuliza_metric_tbd"],
            "revenue_segments":  ["period_label","period_type","fiscal_year","period_end_date",
                                   "voice_kes_bn","mobile_data_kes_bn","messaging_kes_bn",
                                   "mobile_incoming_kes_bn","other_mobile_service_kes_bn",
                                   "mpesa_kes_bn","fixed_service_iot_kes_bn",
                                   "connectivity_total_kes_bn","total_service_revenue_kes_bn"],
            "kenya_ethiopia":    ["period_label","period_type","fiscal_year","period_end_date",
                                   "geography","service_revenue_kes_bn","active_customers_1m_mn",
                                   "active_customers_3m_mn","ebit_kes_bn","capex_kes_bn"],
        }

        errors = []
        for table, cols in expected.items():
            path = data_dir / f"{table}.csv"
            if not path.exists():
                errors.append(f"{table}.csv not found"); continue
            with open(path) as f:
                actual = (csv.DictReader(f)).fieldnames or []
            if list(actual) != cols:
                errors.append(f"{table}: header mismatch — got {list(actual)}")
                continue
            with open(path) as f:
                rows = list(csv.DictReader(f))
            if len(rows) == 0:
                errors.append(f"{table}: has 0 data rows")
            print(f"  OK {table}: {len(rows)} rows")

        if errors:
            raise ValueError("Validation failed:\n" + "\n".join(errors))
        print("All CSVs validated.")


    def upload_to_gcs():
        import os
        from pathlib import Path
        from google.cloud import storage
        from google.oauth2 import service_account

        creds  = service_account.Credentials.from_service_account_file(os.environ["GCP_CREDENTIALS_FILE"])
        client = storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)
        bucket = client.bucket(os.environ["GCS_BUCKET_NAME"])

        data_dir = Path("/opt/airflow/ingestion/seed/data")
        for csv_file in data_dir.glob("*.csv"):
            if csv_file.name == ".keep": continue
            blob = bucket.blob(f"seed/{csv_file.name}")
            blob.upload_from_filename(str(csv_file), content_type="text/csv")
            print(f"  Uploaded gs://{os.environ['GCS_BUCKET_NAME']}/seed/{csv_file.name}")


    def load_to_bigquery():
        import os
        from google.cloud import bigquery
        from google.oauth2 import service_account

        creds  = service_account.Credentials.from_service_account_file(os.environ["GCP_CREDENTIALS_FILE"])
        client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)
        project = os.environ["GCP_PROJECT_ID"]
        dataset = os.environ.get("BQ_DATASET_RAW", "raw")
        bucket  = os.environ["GCS_BUCKET_NAME"]

        tables = ["company_overview", "mpesa_metrics", "revenue_segments", "kenya_ethiopia"]
        for table in tables:
            uri = f"gs://{bucket}/seed/{table}.csv"
            dest = f"{project}.{dataset}.{table}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                null_marker="",
            )
            # Use the table's existing schema (already created by Terraform)
            job = client.load_table_from_uri(uri, dest, job_config=job_config)
            job.result()
            result = client.get_table(dest)
            print(f"  Loaded {result.num_rows} rows → {dest}")


    def trigger_dbt():
        import os, requests
        account_id = os.environ["DBT_ACCOUNT_ID"]
        job_id     = os.environ["DBT_JOB_ID"]
        token      = os.environ["DBT_API_TOKEN"]

        url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
        headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}
        payload = {"cause": "Triggered by safaricom_seed_dag"}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        run_id = r.json()["data"]["id"]
        print(f"  dbt Cloud run triggered — run_id={run_id}")


    t1 = PythonOperator(task_id="validate_seed_csvs", python_callable=validate_seed_csvs)
    t2 = PythonOperator(task_id="upload_to_gcs",      python_callable=upload_to_gcs)
    t3 = PythonOperator(task_id="load_to_bigquery",   python_callable=load_to_bigquery)
    t4 = PythonOperator(task_id="trigger_dbt_run",    python_callable=trigger_dbt)

    t1 >> t2 >> t3 >> t4