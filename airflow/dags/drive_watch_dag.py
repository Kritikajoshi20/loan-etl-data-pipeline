# airflow/dags/drive_watch_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import gzip
import shutil
import json
import pandas as pd
import logging
import boto3
import smtplib
from email.message import EmailMessage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# -------------------------
# Configuration (tweak via env or Airflow Variables)
# -------------------------
SERVICE_ACCOUNT_PATH = "/opt/airflow/credentials/gdrive-service-account.json"
DOWNLOAD_DIR = "/opt/airflow/downloads"
MINIO_INTERNAL_ENDPOINT = os.environ.get("MINIO_INTERNAL_URL", "http://minio:9000")
MINIO_PUBLIC_URL = os.environ.get("MINIO_PUBLIC_URL")  # optional - for presigned links external users can open
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "loan-output")
ATTACHMENT_CUTOFF_BYTES = int(os.environ.get("ATTACHMENT_CUTOFF_BYTES", 22 * 1024 * 1024))  # 22 MB

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="drive_watch_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # GoogleDriveSensor plugin (assumes you have a plugin named google_drive_sensor.py in plugins)
    from google_drive_sensor import GoogleDriveSensor

    wait_for_file = GoogleDriveSensor(
        task_id="wait_for_gdrive_file",
        folder_id="{{ var.value.GDRIVE_FOLDER_ID }}",
        poke_interval=60,
        timeout=60 * 60 * 6,
    )

    # -------------------------
    # Download from Google Drive and compress
    # -------------------------
    def download_and_compress(**context):
        ti = context["ti"]
        new_files = ti.xcom_pull(key="gdrive_new_files", task_ids="wait_for_gdrive_file")
        if not new_files:
            logging.info("No new files from Drive sensor.")
            return None

        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        metas = []

        # prepare Drive client
        SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_PATH, scopes=SCOPES
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        for f in new_files:
            file_id = f.get("id")
            name = f.get("name")
            try:
                meta = service.files().get(fileId=file_id, fields="id,name,owners,size,mimeType").execute()
            except Exception:
                logging.exception("Failed to fetch metadata for %s", file_id)
                meta = {"id": file_id, "name": name}

            # owners (not used for recipient here since we send to SMTP_USER)
            owners = meta.get("owners", [])

            local_path = os.path.join(DOWNLOAD_DIR, name)
            try:
                request = service.files().get_media(fileId=file_id)
                with open(local_path, "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                logging.info("Downloaded %s to %s", file_id, local_path)
            except Exception:
                logging.exception("Failed to download file %s", file_id)
                continue

            compressed_path = local_path + ".gz"
            try:
                with open(local_path, "rb") as sf, gzip.open(compressed_path, "wb") as gz:
                    shutil.copyfileobj(sf, gz)
            except Exception:
                logging.exception("Failed to compress %s", local_path)
                continue

            # attempt row count for CSVs
            rows = None
            try:
                mtype = meta.get("mimeType", "")
                if name.lower().endswith(".csv") or mtype.startswith("text/"):
                    df = pd.read_csv(local_path)
                    rows = len(df)
            except Exception:
                logging.info("Skipping CSV row count for %s (file may be large or not CSV).", local_path)

            m = {
                "file_id": file_id,
                "name": name,
                "mimeType": meta.get("mimeType"),
                "owners": [o.get("emailAddress") for o in owners if isinstance(o, dict) and (o.get("emailAddress") or o.get("email"))],
                "local_path": local_path,
                "compressed_path": compressed_path,
                "original_size": os.path.getsize(local_path) if os.path.exists(local_path) else None,
                "compressed_size": os.path.getsize(compressed_path) if os.path.exists(compressed_path) else None,
                "rows": rows,
            }
            metas.append(m)

        # save metadata file
        try:
            with open(os.path.join(DOWNLOAD_DIR, "latest_meta.json"), "w") as fh:
                json.dump(metas, fh)
        except Exception:
            logging.exception("Failed to write latest_meta.json")

        ti.xcom_push(key="file_meta", value=metas)
        return metas

    t_download = PythonOperator(
        task_id="download_and_compress",
        python_callable=download_and_compress,
    )

    # -------------------------
    # Upload compressed files to MinIO and generate presigned URLs
    # -------------------------
    def upload_to_minio_and_presign(**context):
        ti = context["ti"]
        metas = ti.xcom_pull(key="file_meta", task_ids="download_and_compress") or []
        if not metas:
            raise ValueError("No files to upload")

        s3_internal = boto3.client(
            "s3",
            endpoint_url=MINIO_INTERNAL_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=boto3.session.Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

        presign_endpoint = MINIO_PUBLIC_URL or MINIO_INTERNAL_ENDPOINT
        s3_presign = boto3.client(
            "s3",
            endpoint_url=presign_endpoint,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=boto3.session.Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

        # ensure bucket exists
        try:
            s3_internal.head_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            try:
                s3_internal.create_bucket(Bucket=MINIO_BUCKET)
            except Exception:
                logging.exception("Failed to create or verify bucket %s", MINIO_BUCKET)

        results = []
        for m in metas:
            comp = m.get("compressed_path")
            if not comp or not os.path.exists(comp):
                logging.warning("Missing compressed file for meta: %s", m)
                continue
            key = os.path.basename(comp)
            try:
                s3_internal.upload_file(comp, MINIO_BUCKET, key)
            except Exception:
                logging.exception("Failed upload %s to MinIO", comp)
                raise

            # presign
            try:
                url = s3_presign.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": MINIO_BUCKET, "Key": key},
                    ExpiresIn=24 * 3600,
                )
            except Exception:
                logging.exception("Failed to presign %s", key)
                url = None

            results.append({
                "file_meta": m,
                "minio_key": key,
                "minio_presigned_url": url,
            })

        ti.xcom_push(key="minio_uploads", value=results)
        return results

    t_upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio_and_presign,
    )

    # -------------------------
    # Send email to your Gmail (SMTP_USER)
    # -------------------------
    def send_email_notify(**context):
        ti = context["ti"]
        uploads = ti.xcom_pull(key="minio_uploads", task_ids="upload_to_minio") or []
        if not uploads:
            logging.info("No uploads to notify.")
            return

        smtp_user = os.environ.get("AIRFLOW__EMAIL__SMTP_USER")
        smtp_mail_from = os.environ.get("AIRFLOW__EMAIL__SMTP_MAIL_FROM", smtp_user)
        if not smtp_user:
            raise ValueError("AIRFLOW__EMAIL__SMTP_USER not configured in environment")

        recipients = [smtp_user]  # per your choice #1

        # Prepare email
        msg = EmailMessage()
        msg["From"] = smtp_mail_from or smtp_user
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = "Your Drive file was processed and uploaded"

        html_parts = ["<h3>Drive file processing complete</h3>"]
        for u in uploads:
            fm = u.get("file_meta", {})
            name = fm.get("name")
            orig = fm.get("original_size")
            comp_size = fm.get("compressed_size")
            rows = fm.get("rows")
            minio_url = u.get("minio_presigned_url")
            html_parts.append(f"<h4>{name}</h4>")
            html_parts.append("<ul>")
            html_parts.append(f"<li>Original size: {orig} bytes</li>")
            html_parts.append(f"<li>Compressed size: {comp_size} bytes</li>")
            if rows is not None:
                html_parts.append(f"<li>Rows (if CSV): {rows}</li>")
            if minio_url:
                html_parts.append(f"<li>Download link (expires in 24h): <a href='{minio_url}'>{minio_url}</a></li>")
            html_parts.append("</ul>")

        msg.set_content("\n\n".join([str(u.get("file_meta")) for u in uploads]))
        msg.add_alternative("".join(html_parts), subtype="html")

        # Attach compressed files if small enough
        for u in uploads:
            comp = u.get("file_meta", {}).get("compressed_path")
            if comp and os.path.exists(comp):
                size = os.path.getsize(comp)
                if size <= ATTACHMENT_CUTOFF_BYTES:
                    with open(comp, "rb") as fh:
                        msg.add_attachment(fh.read(), maintype="application", subtype="gzip", filename=os.path.basename(comp))
                else:
                    logging.info("Skipping attachment %s (size %d)", comp, size)

        # SMTP config from environment (must be present in your .env and compose)
        smtp_host = os.environ.get("AIRFLOW__EMAIL__SMTP_HOST")
        smtp_port = int(os.environ.get("AIRFLOW__EMAIL__SMTP_PORT", "587"))
        smtp_user = os.environ.get("AIRFLOW__EMAIL__SMTP_USER")
        smtp_pass = os.environ.get("AIRFLOW__EMAIL__SMTP_PASSWORD")
        starttls = os.environ.get("AIRFLOW__EMAIL__SMTP_STARTTLS", "True").lower() in ("1", "true", "yes")

        if not smtp_host:
            raise ValueError("SMTP host not configured in env")

        try:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
                smtp.ehlo()
                if starttls:
                    smtp.starttls()
                    smtp.ehlo()
                if smtp_user and smtp_pass:
                    smtp.login(smtp_user, smtp_pass)
                smtp.send_message(msg)
                logging.info("Notification sent to %s", recipients)
        except Exception:
            logging.exception("Failed to send notification email")
            raise

    t_notify = PythonOperator(
        task_id="send_email_notify",
        python_callable=send_email_notify,
    )

    # DAG ordering
    wait_for_file >> t_download >> t_upload >> t_notify
