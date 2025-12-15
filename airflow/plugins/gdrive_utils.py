# airflow/plugins/gdrive_utils.py
from google.oauth2 import service_account
from googleapiclient.discovery import build
import io
from googleapiclient.http import MediaIoBaseDownload

def get_drive_service(sa_json_path):
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    creds = service_account.Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return service

def list_files_in_folder(service, folder_id):
    q = f"'{folder_id}' in parents and trashed=false"
    items = []
    page_token = None
    while True:
        res = service.files().list(q=q, fields="nextPageToken, files(id, name, mimeType, size)").execute()
        items.extend(res.get('files', []))
        page_token = res.get('nextPageToken')
        if not page_token:
            break
    return items

def download_file(service, file_id, file_path):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    return file_path

