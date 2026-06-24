"""
safaricom_scraper.py
Standalone scraper utility — can be called directly or imported by the DAG.
Checks Safaricom IR page for new booklets, downloads and parses them.

Usage:
    python3 ingestion/scraper/safaricom_scraper.py
"""

import os, re, json, logging, tempfile
import requests
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

IR_URL    = "https://www.safaricom.co.ke/investor-relations-landing/reports/financial-report/financial-results"
HEADERS   = {"User-Agent": "Mozilla/5.0 (compatible; SafaricomIntelBot/1.0)"}
MARKER    = "scraper_state/last_processed_url.txt"


def get_gcs_client():
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GCP_CREDENTIALS_FILE"]
    )
    return storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=creds)


def get_latest_booklet_url() -> str | None:
    """Scrape the IR results page and return the latest booklet PDF URL."""
    log.info("Fetching IR page: %s", IR_URL)
    resp = requests.get(IR_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    pattern = (
        r"https://www\.safaricom\.co\.ke/images/[^\"\s]+(?:Earnings|Results|Booklet)[^\"\s]+\.pdf"
    )
    links = re.findall(pattern, resp.text, re.IGNORECASE)
    return links[0] if links else None


def get_last_processed_url(bucket) -> str:
    """Read the last-processed marker from GCS."""
    try:
        return bucket.blob(MARKER).download_as_text().strip()
    except Exception:
        return ""


def update_marker(bucket, url: str):
    """Write the latest processed URL to GCS marker."""
    bucket.blob(MARKER).upload_from_string(url)
    log.info(f"Marker updated → {url}")


def download_pdf(url: str) -> bytes:
    """Download PDF bytes from URL."""
    log.info(f"Downloading: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    log.info(f"Downloaded {len(resp.content):,} bytes")
    return resp.content


def run():
    """Main scraper run — check, download, parse, upload JSONL."""
    from ingestion.scraper.pdf_parser import parse_booklet

    bucket_name = os.environ["GCS_BUCKET_NAME"]
    gcs = get_gcs_client()
    bucket = gcs.bucket(bucket_name)

    # 1. Check for new booklet
    latest_url = get_latest_booklet_url()
    if not latest_url:
        log.info("No booklet URL found on IR page"); return

    last_url = get_last_processed_url(bucket)
    if latest_url == last_url:
        log.info(f"Already up to date: {latest_url}"); return

    log.info(f"New booklet: {latest_url}")

    # 2. Download PDF → GCS pdfs/
    pdf_bytes = download_pdf(latest_url)
    filename  = latest_url.split("/")[-1]
    blob_name = f"pdfs/{filename}"
    bucket.blob(blob_name).upload_from_string(pdf_bytes, content_type="application/pdf")
    log.info(f"Saved to gs://{bucket_name}/{blob_name}")

    # 3. Parse with pdfplumber
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    extracted = parse_booklet(tmp_path)

    # 4. Write JSONL per table to GCS extracted/
    stem = filename.replace(".pdf", "")
    for table_name, rows in extracted.items():
        if not rows: continue
        jsonl = "\n".join(json.dumps(r, default=str) for r in rows)
        jsonl_key = f"extracted/{stem}_{table_name}.jsonl"
        bucket.blob(jsonl_key).upload_from_string(jsonl, content_type="application/json")
        log.info(f"Extracted {len(rows)} rows → gs://{bucket_name}/{jsonl_key}")

    # 5. Update marker
    update_marker(bucket, latest_url)
    log.info("Scraper run complete.")


if __name__ == "__main__":
    run()