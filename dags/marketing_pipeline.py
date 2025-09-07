# dags/marketing_pipeline.py
from datetime import timedelta
import logging
import os
import io

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# Optional imports for cloud hooks - gracefully handle missing providers
try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except Exception:
    S3Hook = None

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
except Exception:
    GCSHook = None
    BigQueryHook = None

import pandas as pd
from sqlalchemy import create_engine

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG config stored in Airflow Variables (editable in UI)
# Example Variable JSON (optional):
# {
#   "source_type": "local",  # local | s3 | gcs
#   "source_path": "/opt/airflow/data/campaign.csv",
#   "s3_bucket": "my-bucket",
#   "s3_key": "raw/campaign.csv",
#   "gcs_bucket": "my-gcs-bucket",
#   "gcs_object": "raw/campaign.csv",
#   "target": "sqlite:///tmp/marketing.db",  # or "bigquery:project.dataset.table"
#   "expected_columns": ["user_id","event_ts","campaign","cost","revenue"]
# }
CFG = Variable.get("marketing_pipeline_cfg", deserialize_json=True, default_var={
    "source_type": "local",
    "source_path": "/opt/airflow/data/campaign.csv",
    "target": "sqlite:///tmp/marketing.db",
    "expected_columns": ["user_id","event_ts","campaign","cost","revenue"],
})


with DAG(
    dag_id="marketing_ingest_transform_load",
    default_args=DEFAULT_ARGS,
    description="Ingest CSV -> validate -> transform -> load (BigQuery or SQLite)",
    schedule_interval="0 3 * * *",  # daily at 03:00
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["marketing", "etl", "example"],
) as dag:

    @task()
    def fetch_csv():
        """
        Fetch CSV from local path / S3 / GCS depending on CFG.
        Returns CSV content as bytes.
        """
        source_type = CFG.get("source_type", "local")
        logging.info("Fetching CSV from %s", source_type)

        if source_type == "s3":
            if not S3Hook:
                raise RuntimeError("S3Hook not available. Install 'apache-airflow-providers-amazon'.")
            bucket = CFG["s3_bucket"]
            key = CFG["s3_key"]
            hook = S3Hook()
            logging.info("Downloading s3://%s/%s", bucket, key)
            obj_bytes = hook.read_key(key, bucket_name=bucket)
            return obj_bytes.encode() if isinstance(obj_bytes, str) else obj_bytes

        if source_type == "gcs":
            if not GCSHook:
                raise RuntimeError("GCSHook not available. Install 'apache-airflow-providers-google'.")
            bucket = CFG["gcs_bucket"]
            obj = CFG["gcs_object"]
            hook = GCSHook()
            logging.info("Downloading gs://%s/%s", bucket, obj)
            file_bytes = hook.download(bucket_name=bucket, object_name=obj)
            return file_bytes

        # default: local path
        path = CFG["source_path"]
        logging.info("Reading local file %s", path)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Source file not found: {path}")
        with open(path, "rb") as f:
            return f.read()

    @task()
    def validate_and_parse(csv_bytes: bytes):
        """
        Parse CSV into a DataFrame and run basic validation.
        Returns serialized to_parquet bytes (smaller XCom footprint than raw df).
        """
        if not csv_bytes:
            raise AirflowSkipException("No CSV content found.")
        bio = io.BytesIO(csv_bytes)
        try:
            df = pd.read_csv(bio)
        except Exception as e:
            logging.exception("Failed to parse CSV")
            raise

        expected = CFG.get("expected_columns", [])
        missing = [c for c in expected if c not in df.columns]
        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        # Basic row-level validation examples
        if df.shape[0] == 0:
            raise ValueError("CSV contained no rows")
        if "user_id" in df.columns and df["user_id"].isnull().any():
            logging.warning("Found NULL user_id values; dropping them")
            df = df.dropna(subset=["user_id"])

        # cast types: example
        if "event_ts" in df.columns:
            df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
            null_ts = df["event_ts"].isnull().sum()
            if null_ts > 0:
                logging.warning("Found %d unparsable timestamps", null_ts)

        # Example validations that could be expanded
        # Save as parquet in-memory for efficient XCom transfer
        out = io.BytesIO()
        df.to_parquet(out, index=False)
        out.seek(0)
        return out.read()

    @task()
    def transform(parquet_bytes: bytes):
        """
        Perform transformation: enrich, dedupe, compute metrics.
        Returns transformed parquet bytes.
        """
        bio = io.BytesIO(parquet_bytes)
        df = pd.read_parquet(bio)

        # Example transforms
        # dedupe
        df = df.drop_duplicates()

        # sample enrichment: compute margin and daily bucket if columns exist
        if {"revenue", "cost"}.issubset(df.columns):
            df["margin"] = df["revenue"] - df["cost"]

        if "event_ts" in df.columns:
            df["event_date"] = df["event_ts"].dt.date

        # keep a small audit column and row count
        logging.info("Transformed rows: %d", len(df))
        out = io.BytesIO()
        df.to_parquet(out, index=False)
        out.seek(0)
        return out.read()

    @task()
    def load_target(parquet_bytes: bytes):
        """
        Load transformed rows into a target:
        - if CFG['target'] starts with 'bigquery:' -> use BigQuery (requires provider)
        - otherwise treat it as a SQLAlchemy connection string (e.g. sqlite:///tmp/db.sqlite)
        """
        target = CFG.get("target", "sqlite:///tmp/marketing.db")
        bio = io.BytesIO(parquet_bytes)
        df = pd.read_parquet(bio)
        logging.info("Loading %d rows into %s", len(df), target)

        if isinstance(target, str) and target.startswith("bigquery:"):
            if not BigQueryHook:
                raise RuntimeError("BigQueryHook not available. Install 'apache-airflow-providers-google'.")
            # target format: bigquery:project.dataset.table
            _, bq_ref = target.split(":", 1)
            project, dataset, table = bq_ref.split(".", 2)
            hook = BigQueryHook()
            # For large writes, use load_table_from_dataframe or write via GCS -> bq load job.
            # Here we attempt a direct load (note: depends on provider methods).
            logging.info("Attempting to insert rows to %s.%s.%s", project, dataset, table)
            # convert to list of dictionaries
            rows = df.to_dict(orient="records")
            # This uses insert_rows which may require target table to exist and schema matching.
            conn_id = hook.get_conn()
            cursor = conn_id.cursor()
            # Note: In practice you should use the BigQuery client libraries or BigQueryInsertJobOperator.
            # We'll raise to indicate the recommended approach if not implemented here.
            raise NotImplementedError("BigQuery load placeholder — replace with BigQueryInsertJobOperator or pandas-gbq")

        # Default: SQLAlchemy load (demo / local)
        engine = create_engine(target, connect_args={"check_same_thread": False} if target.startswith("sqlite") else {})
        table_name = CFG.get("table_name", "marketing_events")
        # Upsert strategy depends on DB; here we append and instruct dedupe elsewhere.
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info("Loaded rows into %s (SQLAlchemy target)", table_name)
        return {"rows_loaded": len(df), "table": table_name, "target": target}

    @task()
    def success_message(load_result: dict):
        logging.info("Pipeline succeeded. %s", load_result)
        return load_result

    # DAG graph
    raw = fetch_csv()
    parsed = validate_and_parse(raw)
    transformed = transform(parsed)
    loaded = load_target(transformed)
    okay = success_message(loaded)

    raw >> parsed >> transformed >> loaded >> okay
