# README — Marketing Ingest/Transform/Load Airflow DAG

**Repository**: simple demo for the `marketing_ingest_transform_load` Airflow DAG
**DAG file**: `dags/marketing_pipeline.py` (TaskFlow API)
**Purpose**: demo a small production-minded pipeline: fetch CSV → validate → transform → load (SQLite / BigQuery) with clear places to adapt to S3/GCS and add monitoring.

---

## What’s included

* `dags/marketing_pipeline.py` — the DAG you asked for.
* `conf/marketing_pipeline_cfg.json` — example Airflow Variable JSON.
* `data/sample_campaign.csv` — example CSV for local testing.
* `requirements.txt` — packages you’ll want in the environment.
* `README.md` — this file.

---

## Repo structure (suggested)

```
/
├─ dags/
│  └─ marketing_pipeline.py
├─ data/
│  └─ sample_campaign.csv
├─ conf/
│  └─ marketing_pipeline_cfg.json
├─ tests/
│  └─ test_pipeline.py
├─ requirements.txt
└─ README.md
```

---

## Quickstart — run locally with Airflow (Docker Compose)

> These steps assume you have Docker & Docker Compose installed.

1. Clone repo and `cd` into it.

2. Install Python deps only if you want to run unit tests locally (optional):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Start Airflow (official provider images recommended). Using Docker Compose:

```bash
# if you have the official apache/airflow docker-compose (recommended)
docker compose up -d
# OR, if using older docker-compose:
# docker-compose up -d
```

4. Copy `dags/marketing_pipeline.py` into the local Airflow `dags/` folder (or place this repo's `dags/` under your Airflow home). Put `data/sample_campaign.csv` somewhere the DAG can read (or change Variable to point to `data/sample_campaign.csv` inside the container).

5. Open the Airflow UI (default `http://localhost:8080`), enable the DAG `marketing_ingest_transform_load`, and trigger a run or wait for the schedule.

---

## Configure DAG (Airflow Variable)

Create an Airflow Variable named `marketing_pipeline_cfg` (type: JSON) — example:

```json
{
  "source_type": "local",
  "source_path": "/opt/airflow/data/sample_campaign.csv",
  "target": "sqlite:///tmp/marketing_demo.db",
  "expected_columns": ["user_id","event_ts","campaign","cost","revenue"],
  "table_name": "marketing_events",
  "s3_bucket": "my-bucket",
  "s3_key": "raw/campaign.csv",
  "gcs_bucket": "my-gcs-bucket",
  "gcs_object": "raw/campaign.csv"
}
```

How to set it:

* In Airflow UI: Admin → Variables → Create → paste the JSON.
* Or CLI (inside the airflow container):

```bash
airflow variables set marketing_pipeline_cfg "$(cat conf/marketing_pipeline_cfg.json)"
```

---

## Connections (if using cloud providers)

* **S3 (AWS)**: create an Airflow Connection with `Conn Id` that `S3Hook` expects (default uses AWS env vars / default connection). Install `apache-airflow-providers-amazon`.
* **GCS / BigQuery**: install `apache-airflow-providers-google` and set up a GCP connection (service account JSON) as an Airflow Connection.

*Note:* The DAG contains graceful fallbacks and raises a clear error if the provider hook is not installed.

---

## Example sample CSV (data/sample\_campaign.csv)

```
user_id,event_ts,campaign,cost,revenue
1,2025-09-01T12:22:00Z,summer_launch,12.5,30
2,2025-09-01T13:01:00Z,summer_launch,10.0,15
3,2025-09-02T08:05:00Z,winter_teaser,7.5,0
```

---

## Local testing (without scheduling)

You can run specific tasks locally (useful for debugging) — run from inside the Airflow container or with Airflow CLI:

```bash
# test the fetch task for a given date (example, run once)
airflow tasks test marketing_ingest_transform_load fetch_csv 2025-09-01
airflow tasks test marketing_ingest_transform_load validate_and_parse 2025-09-01
airflow tasks test marketing_ingest_transform_load transform 2025-09-01
airflow tasks test marketing_ingest_transform_load load_target 2025-09-01
```

`airflow tasks test` executes the task in isolation (no scheduler or XComs persisted between tasks), but it’s good for quick unit-like checks.

---

## How to adapt to cloud targets (short guide)

### S3 source

* Set `"source_type": "s3"` and fill `s3_bucket` and `s3_key` in the Variable.
* Ensure `apache-airflow-providers-amazon` is in `requirements.txt`.
* Configure AWS credentials as Airflow Connection or environment variables in your container.

### GCS + BigQuery target

* Set `"source_type": "gcs"`, `gcs_bucket`, `gcs_object`.
* For BigQuery set `target` to: `"bigquery:project.dataset.table"` and replace the BigQuery placeholder in the DAG with a `BigQueryInsertJobOperator` or use `pandas-gbq`.
* Install `apache-airflow-providers-google` and configure a GCP service account in Airflow Connections.

---

## Observability & production hardening (notes)

* Add SLAs and `on_failure_callback` for alerts (Slack/email).
* Add unit tests for transform logic (place `transform()` logic into a small importable module so tests can call it directly).
* Add data quality tests (great expectations or custom checks). Consider integrating with dbt tests if loading into a warehouse.
* For BigQuery loads, prefer streaming via GCS → BigQuery load job or use `BigQueryInsertJobOperator` for reliability, schema evolution handling, and monitoring.

---

## CI suggestions (GitHub Actions example)

A simple workflow to run tests and lint on push:

```yaml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: 3.10
      - name: Install deps
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest -q
      - name: Lint
        run: flake8 .
```

Include unit tests that exercise `validate_and_parse` and `transform` using `pandas` fixtures (no Airflow runtime needed).

---

## Troubleshooting tips

* **Task fails with `ModuleNotFoundError` for provider hooks** — install the appropriate `apache-airflow-providers-*` package in your container and restart.
* **File not found for local CSV** — confirm the path inside the Airflow worker/container and update `source_path`.
* **BigQuery load placeholder** — the DAG throws `NotImplementedError` and instructs to replace with `BigQueryInsertJobOperator` — implement that for production loads.
* **XCom size** — DAG currently serializes DataFrames to parquet bytes for smaller XCom footprint. For large datasets, write intermediate artifact to GCS/S3 and pass path in XCom.

---

## Security & secrets

* Don’t hardcode credentials in Variables. Use Airflow Connections and Airflow’s secret backends (Vault, AWS Secrets Manager) for production secrets.


