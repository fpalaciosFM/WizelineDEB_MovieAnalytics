from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
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
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "WDEB_CP_MovieAnalytics_ETL_PrepareInitialState",
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


task_load_gdrive_to_gcs_user_purchase = PythonOperator(
    dag=dag,
    id="load_gdrive_to_gcs_user_purchase",
    kwargs={
        "file_id": "1rqmnKgl_HXOfM7p4G_RC5v4clvbmJvks",
        "bucket_name": "wizeline-deb-fp-bucket-stage",
        "object_name": "user_purchase.csv",
    },
    python_callable=transfer_file_gdrive_to_gcs,
)


task_load_gdrive_to_gcs_movie_review = PythonOperator(
    dag=dag,
    id="load_gdrive_to_gcs_movie_review",
    kwargs={
        "file_id": "1eh_0vzQGWnUZm5OH8j5M1D88he0uHILi",
        "bucket_name": "wizeline-deb-fp-bucket-stage",
        "object_name": "movie_review.csv",
    },
    python_callable=transfer_file_gdrive_to_gcs,
)


task_load_gdrive_to_gcs_log_reviews = PythonOperator(
    dag=dag,
    id="load_gdrive_to_gcs_log_reviews",
    kwargs={
        "file_id": "1UVKS9V2PAQKvyBwkaMl2wEYa3zgL",
        "bucket_name": "wizeline-deb-fp-bucket-stage",
        "object_name": "log_reviews.csv",
    },
    python_callable=transfer_file_gdrive_to_gcs,
)

task_load_gdrive_to_gcs_user_purchase
task_load_gdrive_to_gcs_movie_review
task_load_gdrive_to_gcs_log_reviews