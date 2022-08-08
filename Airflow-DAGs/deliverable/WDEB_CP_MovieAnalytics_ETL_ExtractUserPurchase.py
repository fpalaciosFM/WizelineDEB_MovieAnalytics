from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

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
    "WDEB_CP_MovieAnalytics_ETL_ExtractUserPurchase",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
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
