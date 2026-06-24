"""
safaricom_scraper_dag.py
Weekly scraper DAG — checks Safaricom IR page for new Results Booklets,
downloads, extracts, loads to BigQuery, triggers dbt.
Schedule: every Monday 08:00 (UTC+3 = 05:00 UTC).
Tasks: check_ir_page → download_booklet → extract_tables → load_bigquery → trigger_dbt
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

default_args = {"owner": "safaricom-intel", "retries": 2}

with DAG(
    dag_id="safaricom_scraper_dag",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 5 * * 1",   # Monday 05:00 UTC (08:00 EAT)
    catchup=False,
    tags=["safaricom", "scraper"],
) as dag:

    def check_ir_page(**context):
        """
        Fetch Safaricom IR results page, find the latest booklet PDF link.
        Compare against last-processed marker stored in GCS.
        Returns False (short-circuit) if no new booklet found.
        """
        import os, re, requests
        from google.cloud import storage
        from google.oauth2 import service_account

        IR_URL = "https://www.safaricom.co.ke/investor-relations-landing/reports/financial-report/financial-results"

        creds  = service_account.Credentials.from_service_account_file(os.environ["GCP_CREDENTIALS_FILE"])
        client = storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)
        bucket = client.bucket(os.environ["GCS_BUCKET_NAME"])

        # Fetch IR page
        headers = {"User-Agent": "Mozilla/5.0 (compatible; SafaricomIntelBot/1.0)"}
        resp = requests.get(IR_URL, headers=headers, timeout=30)
        resp.raise_for_status()

        # Extract all PDF links matching booklet pattern
        pdf_links = re.findall(
            r'https://www\.safaricom\.co\.ke/images/[^"\s]+(?:Earnings|Results|Booklet)[^"\s]+\.pdf',
            resp.text,
            re.IGNORECASE,
        )

        if not pdf_links:
            print("No booklet PDF links found on IR page")
            return False

        latest_url = pdf_links[0]
        print(f"Latest booklet URL: {latest_url}")

        # Check last-processed marker
        marker_blob = bucket.blob("scraper_state/last_processed_url.txt")
        try:
            last_url = marker_blob.download_as_text().strip()
        except Exception:
            last_url = ""

        if latest_url == last_url:
            print(f"Already processed: {latest_url}")
            return False

        # Push URL to XCom for downstream tasks
        context["task_instance"].xcom_push(key="booklet_url", value=latest_url)
        print(f"New booklet found: {latest_url}")
        return True


    def download_booklet(**context):
        """Download the new booklet PDF to GCS pdfs/ prefix."""
        import os, re, requests
        from google.cloud import storage
        from google.oauth2 import service_account

        booklet_url = context["task_instance"].xcom_pull(
            task_ids="check_ir_page", key="booklet_url"
        )

        creds  = service_account.Credentials.from_service_account_file(os.environ["GCP_CREDENTIALS_FILE"])
        client = storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)
        bucket = client.bucket(os.environ["GCS_BUCKET_NAME"])

        headers = {"User-Agent": "Mozilla/5.0 (compatible; SafaricomIntelBot/1.0)"}
        resp = requests.get(booklet_url, headers=headers, timeout=60)
        resp.raise_for_status()

        filename = booklet_url.split("/")[-1]
        blob_name = f"pdfs/{filename}"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(resp.content, content_type="application/pdf")

        gcs_uri = f"gs://{os.environ['GCS_BUCKET_NAME']}/{blob_name}"
        context["task_instance"].xcom_push(key="gcs_pdf_uri", value=gcs_uri)
        context["task_instance"].xcom_push(key="filename", value=filename)
        print(f"Downloaded → {gcs_uri} ({len(resp.content):,} bytes)")


    def extract_tables(**context):
        """
        Run pdf_parser on the downloaded PDF.
        Write extracted data as JSONL to GCS extracted/ prefix.
        """
        import os, json, tempfile, requests
        from pathlib import Path
        from google.cloud import storage
        from google.oauth2 import service_account
        import sys
        sys.path.insert(0, "/opt/airflow/ingestion")
        from scraper.pdf_parser import parse_booklet

        ti       = context["task_instance"]
        gcs_uri  = ti.xcom_pull(task_ids="download_booklet", key="gcs_pdf_uri")
        filename = ti.xcom_pull(task_ids="download_booklet", key="filename")

        creds  = service_account.Credentials.from_service_account_file(os.environ["GCP_CREDENTIALS_FILE"])
        client = storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)
        bucket = client.bucket(os.environ["GCS_BUCKET_NAME"])

        # Download PDF from GCS to temp file
        blob_name = gcs_uri.replace(f"gs://{os.environ['GCS_BUCKET_NAME']}/", "")
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            bucket.blob(blob_name).download_to_filename(tmp.name)
            tmp_path = tmp.name

        # Parse
        extracted = parse_booklet(tmp_path)
        print(f"Extracted: {extracted}")

        # Write JSONL per table to GCS extracted/
        stem = filename.replace(".pdf", "")
        jsonl_paths = {}
        for table_name, rows in extracted.items():
            if not rows: continue
            jsonl_blob_name = f"extracted/{stem}_{table_name}.jsonl"
            jsonl_content = "\n".join(json.dumps(row) for row in rows)
            bucket.blob(jsonl_blob_name).upload_from_string(jsonl_content, content_type="application/json")
            jsonl_paths[table_name] = f"gs://{os.environ['GCS_BUCKET_NAME']}/{jsonl_blob_name}"
            print(f"  Written {len(rows)} rows → {jsonl_paths[table_name]}")

        ti.xcom_push(key="jsonl_paths", value=jsonl_paths)
        ti.xcom_push(key="booklet_url",
                     value=ti.xcom_pull(task_ids="check_ir_page", key="booklet_url"))


    def load_to_bigquery(**context):
        """Append extracted JSONL rows to raw.* tables (WRITE_APPEND)."""
        import os, json
        from google.cloud import bigquery, storage
        from google.oauth2 import service_account

        ti = context["task_instance"]
        jsonl_paths = ti.xcom_pull(task_ids="extract_tables", key="jsonl_paths")
        if not jsonl_paths:
            print("No JSONL paths — nothing to load"); return

        creds   = service_account.Credentials.from_service_account_file(os.environ["GCP_CREDENTIALS_FILE"])
        bq      = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)
        project = os.environ["GCP_PROJECT_ID"]
        dataset = os.environ.get("BQ_DATASET_RAW", "raw")

        for table_name, gcs_uri in jsonl_paths.items():
            dest = f"{project}.{dataset}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False,
            )
            job = bq.load_table_from_uri(gcs_uri, dest, job_config=job_config)
            job.result()
            print(f"  Appended → {dest}")


    def trigger_dbt(**context):
        """Trigger dbt Cloud job and update last-processed marker in GCS."""
        import os, requests
        from google.cloud import storage
        from google.oauth2 import service_account

        # Trigger dbt
        account_id = os.environ["DBT_ACCOUNT_ID"]
        job_id     = os.environ["DBT_JOB_ID"]
        token      = os.environ["DBT_API_TOKEN"]
        url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
        headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}
        r = requests.post(url, headers=headers,
                          json={"cause": "Triggered by safaricom_scraper_dag"}, timeout=30)
        r.raise_for_status()
        run_id = r.json()["data"]["id"]
        print(f"  dbt Cloud run triggered — run_id={run_id}")

        # Update last-processed marker
        ti  = context["task_instance"]
        url = ti.xcom_pull(task_ids="extract_tables", key="booklet_url")
        creds  = service_account.Credentials.from_service_account_file(os.environ["GCP_CREDENTIALS_FILE"])
        client = storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)
        bucket = client.bucket(os.environ["GCS_BUCKET_NAME"])
        bucket.blob("scraper_state/last_processed_url.txt").upload_from_string(url)
        print(f"  Marker updated → {url}")


    t1 = ShortCircuitOperator(task_id="check_ir_page",     python_callable=check_ir_page)
    t2 = PythonOperator(task_id="download_booklet",        python_callable=download_booklet)
    t3 = PythonOperator(task_id="extract_tables",          python_callable=extract_tables)
    t4 = PythonOperator(task_id="load_to_bigquery",        python_callable=load_to_bigquery)
    t5 = PythonOperator(task_id="trigger_dbt_run",         python_callable=trigger_dbt)

    t1 >> t2 >> t3 >> t4 >> t5