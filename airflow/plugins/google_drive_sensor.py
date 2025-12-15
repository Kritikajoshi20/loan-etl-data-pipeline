# airflow/plugins/google_drive_sensor.py
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from google.oauth2 import service_account
from googleapiclient.discovery import build
import ast
from airflow.models import Variable

class GoogleDriveSensor(BaseSensorOperator):
    """
    Sensor that waits until a new file appears in a Google Drive folder.
    It uses a service account JSON located at /opt/airflow/credentials/gdrive-service-account.json
    and an Airflow Variable 'GDRIVE_FOLDER_ID' for the folder to watch.
    It stores seen file ids in Airflow Variable 'gdrive_seen' (a list as string).
    """

    template_fields = ("folder_id",)

    @apply_defaults
    def __init__(self, folder_id=None, service_account_path="/opt/airflow/credentials/gdrive-service-account.json", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_id = folder_id
        self.service_account_path = service_account_path

    def poke(self, context):
        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
        creds = service_account.Credentials.from_service_account_file(self.service_account_path, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)

        q = f"'{self.folder_id}' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id, name, mimeType, size)").execute()
        files = res.get('files', [])
        seen_raw = Variable.get("gdrive_seen", default_var="[]")
        try:
            seen = ast.literal_eval(seen_raw)
        except Exception:
            seen = []

        new_files = [f for f in files if f['id'] not in seen]
        if new_files:
            # push list of new files into xcom for downstream tasks
            ti = context['ti']
            ti.xcom_push(key='gdrive_new_files', value=new_files)
            # update seen var
            ids = seen + [f['id'] for f in new_files]
            Variable.set("gdrive_seen", str(ids))
            return True
        return False
