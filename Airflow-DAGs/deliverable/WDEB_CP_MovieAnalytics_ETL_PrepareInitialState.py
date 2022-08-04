from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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


def get_gdrive_credentials_with_service_account_var(
    var_sa_id: str,
) -> google.auth.credentials.Credentials:
    # Google Drive API must be enabled in GCP
    # Using google service account for gdrive api
    with open(f"{var_sa_id}.json", "w") as file:
        file.write(Variable.get(var_sa_id))

    creds, _ = google.auth.load_credentials_from_file(f"{var_sa_id}.json")
    return creds


def transfer_csv_gdrive_to_postgres(file_id: str, table_name: str):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    creds = get_gdrive_credentials_with_service_account_var("gdrive_service_account")

    try:
        service = build("drive", "v3", credentials=creds)
        request = service.files().get_media(fileId=file_id)
        with open("temp.csv", "wb") as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}.")

        postgres_conn = pg_hook.get_conn()
        curr = postgres_conn.cursor("cursor")

        with open("temp.csv", "r") as file:
            next(file)
            curr.copy_expert(
                sql=f"COPY stg.{table_name} FROM STDIN DELIMITER ','  CSV HEADER",
                file=file,
            )
            postgres_conn.commit()
    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None


def transfer_file_gdrive_to_gcs(file_id: str, bucket_name: str, object_name: str):

    creds = get_gdrive_credentials_with_service_account_var("gdrive_service_account")
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


task_load_gdrive_to_gcs_movie_review = PythonOperator(
    dag=dag,
    task_id="load_gdrive_to_gcs_movie_review",
    op_kwargs={
        "file_id": "1eh_0vzQGWnUZm5OH8j5M1D88he0uHILi",
        "bucket_name": "wizeline-deb-movie-analytics-fpa",
        "object_name": "movie_review.csv",
    },
    python_callable=transfer_file_gdrive_to_gcs,
)


task_load_gdrive_to_gcs_log_reviews = PythonOperator(
    dag=dag,
    task_id="load_gdrive_to_gcs_log_reviews",
    op_kwargs={
        "file_id": "1UVKS9V2PAQKvyBwkaMl2wEYa3zgL-GlH",
        "bucket_name": "wizeline-deb-movie-analytics-fpa",
        "object_name": "log_reviews.csv",
    },
    python_callable=transfer_file_gdrive_to_gcs,
)

task_postgres_create_table_user_purchase = PostgresOperator(
    dag=dag,
    task_id="postgres_create_table_user_purchase",
    sql="""
        CREATE SCHEMA IF NOT EXISTS stg;
        CREATE TABLE IF NOT EXISTS stg.user_purchase(
            invoice_number varchar(10),
            stock_code varchar(20),
            detail varchar(1000),
            quantity int,
            invoice_date timestamp,
            unit_price numeric(8,3),
            customer_id int,
            country varchar(20)
        );
        TRUNCATE TABLE stg.user_purchase;
    """,
    postgres_conn_id="postgres_default",
    autocommit=True,
)

task_load_gdrive_csv_to_postgres_user_purchase = PythonOperator(
    dag=dag,
    task_id="load_gdrive_csv_to_postgres_user_purchase",
    op_kwargs={
        "file_id": "1rqmnKgl_HXOfM7p4G_RC5v4clvbmJvks",
        "table_name": "user_purchase",
    },
    python_callable=transfer_csv_gdrive_to_postgres,
)

trigger_run_movie_review_transformation_dag = TriggerDagRunOperator(
    dag=dag,
    task_id="run_movie_review_transformation_dag",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformMovieReview",
)

trigger_run_log_review_transformation_dag = TriggerDagRunOperator(
    dag=dag,
    task_id="run_log_review_transformation_dag",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformLogReview",
)

trigger_run_user_purchase_transformation_dag = TriggerDagRunOperator(
    dag=dag,
    task_id="run_user_purchase_transformation_dag",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformUserPurchase",
)

task_load_gdrive_to_gcs_movie_review >> trigger_run_movie_review_transformation_dag
task_load_gdrive_to_gcs_log_reviews >> trigger_run_log_review_transformation_dag
(
    task_postgres_create_table_user_purchase
    >> task_load_gdrive_csv_to_postgres_user_purchase
    >> trigger_run_user_purchase_transformation_dag
)
