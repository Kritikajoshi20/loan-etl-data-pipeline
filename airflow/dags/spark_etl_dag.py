from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import timedelta
from airflow.utils.dates import days_ago
import os
import shutil
import logging
import boto3

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="spark_etl_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    def run_pyspark_etl(**context):
        import sys
        # make sure /opt (host-mounted) is importable so "import etl" works
        if "/opt" not in sys.path:
            sys.path.insert(0, "/opt")

        ti = context.get("ti")

        # Allow override via Airflow Variable
        downloads_dir = Variable.get("DOWNLOADS_DIR", "/opt/airflow/downloads")

        # Ensure downloads dir exists
        try:
            os.makedirs(downloads_dir, exist_ok=True)
        except Exception as exc:
            logging.exception("Could not create downloads directory '%s': %s", downloads_dir, exc)
            raise

        # List input files — prefer CSV / CSV.GZ and ignore JSON metadata
        try:
            all_files = [f for f in os.listdir(downloads_dir) if not f.startswith(".")]
            csv_files = [f for f in all_files if f.lower().endswith(".csv") or f.lower().endswith(".csv.gz")]
            input_filenames = csv_files
        except Exception as exc:
            logging.exception("Failed to list downloads directory '%s': %s", downloads_dir, exc)
            raise

        if not input_filenames:
            logging.info("No CSV files found in %s — skipping ETL run.", downloads_dir)
            return "no_files"

        # use first CSV file found
        input_paths = [os.path.join(downloads_dir, f) for f in input_filenames]
        logging.info("Found %d CSV input file(s): %s", len(input_paths), input_filenames)
        input_file = input_paths[0]

        # Output location (clean)
        output_local = "/opt/etl/output/parquet_out"
        if os.path.exists(output_local):
            try:
                shutil.rmtree(output_local)
            except Exception:
                logging.exception("Failed to remove existing output directory %s", output_local)
                raise

        # Run ETL
        try:
            from etl.pyspark_etl import run_etl
            insights = run_etl(input_file, output_local, timestamp_col="timestamp")
        except Exception:
            logging.exception("ETL job failed for input %s", input_file)
            raise

        # Upload parquet to MinIO
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=boto3.session.Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        bucket = "loan-output"

        try:
            s3.create_bucket(Bucket=bucket)
            logging.info("Created bucket %s", bucket)
        except Exception as exc:
            logging.info("Could not create bucket %s (may already exist): %s", bucket, exc)

        uploaded = []
        for root, dirs, filenames in os.walk(output_local):
            for fname in filenames:
                local_path = os.path.join(root, fname)
                s3_key = os.path.relpath(local_path, output_local)
                try:
                    s3.upload_file(local_path, bucket, s3_key)
                    uploaded.append(s3_key)
                except Exception:
                    logging.exception("Failed to upload %s to s3://%s/%s", local_path, bucket, s3_key)
                    raise

        logging.info("Uploaded %d files to bucket %s: %s", len(uploaded), bucket, uploaded)

        # push insights to xcom
        if ti is not None:
            try:
                ti.xcom_push(key="insights", value=insights)
            except Exception:
                logging.exception("Failed to push insights to XCom.")

        return "etl_done"

    etl_task = PythonOperator(
        task_id="run_pyspark_etl",
        python_callable=run_pyspark_etl,
    )

    etl_task
