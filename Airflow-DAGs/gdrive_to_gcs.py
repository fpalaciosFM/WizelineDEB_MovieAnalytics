from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator

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
    gdrive_hook = GoogleDriveHook(gcp_conn_id="gdrive_email")
    gcs_hook = GCSHook()
    with gcs_hook.provide_file_and_upload(
        bucket_name=bucket_name, object_name=object_name
    ) as file:
        gdrive_hook.download_file(file_id=file_id, file_handle=file)
    return "success"


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
