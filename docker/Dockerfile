FROM kenya-health-airflow:latest

USER airflow

RUN pip install --no-cache-dir \
    google-cloud-storage==2.16.0 \
    google-cloud-bigquery==3.20.1 \
    pdfplumber==0.11.0 \
    requests==2.31.0 \
    pandas==2.2.2