from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator

import google.auth

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

default_args = {
    "owner": "Fernando Palacios",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 1),
    "email": ["fpalacios.fm.gcp@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "gdrive_to_gcs",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)


def transfer_file_gdrive_to_gcs(file_id: str, bucket_name: str, object_name: str):

    # Google Drive API must be enabled in GCP
    # Using google service account for gdrive api

    with open("gcloud_service_account.json", "w") as f:
        f.write(Variable.get("gdrive_service_account"))

    creds, _ = google.auth.load_credentials_from_file("gcloud_service_account.json")
    gcs_hook = GCSHook()

    try:
        # create gmail api client
        service = build("drive", "v3", credentials=creds)

        # prepare request
        request = service.files().get_media(fileId=file_id)

        with gcs_hook.provide_file_and_upload(
            bucket_name=bucket_name, object_name=object_name
        ) as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}.")

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None


task_transfer_file_from_gdrive_to_gcs = PythonOperator(
    task_id="transfer_file_from_gdrive_to_gcs",
    provide_context=True,
    python_callable=transfer_file_gdrive_to_gcs,
    op_kwargs={
        "file_id": "1rqmnKgl_HXOfM7p4G_RC5v4clvbmJvks",
        "bucket_name": "wizedeb_movie-analytics_fp_datalake",
        "object_name": "user_purchase.csv",
    },
    dag=dag,
)
