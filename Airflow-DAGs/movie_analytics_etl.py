from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import timedelta, datetime
import os
import io
import google.auth

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from airflow.models import Variable

# from pyspark.sql import SparkSession
# from pyspark.sql import DataFrame
import pandas as pd

default_args = {
    "owner": "Fernando Palacios",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 1),
    "email": ["fpalacios.fm.gcp@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "MovieAnalyticsETL_prototype_1",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)


def download_file_gdrive(file_id: str) -> bytes:
    """Downloads a file from google drive
    Args:
    file_id: ID of the file to download
    Returns : downloaded file with type 'bytes'.
    """

    with open("gcloud_service_account.json", "w") as f:
        f.write(Variable.get("gdrive_service_account"))

    # Google Drive API must be enabled in GCP
    # Using google service account for gdrive api
    creds, _ = google.auth.load_credentials_from_file("gcloud_service_account.json")

    try:
        # create gmail api client
        service = build("drive", "v3", credentials=creds)

        # prepare request
        request = service.files().get_media(fileId=file_id)

        # start file download
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}.")

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None

    return file.getvalue() if file != None else None


def download_gdrive_local(file_id: str, file_name: str) -> None:
    """Downloads a file from google drive and stores it in local machine
    Args:
        file_id: ID of the file to be downloaded
        file_name: the name that will be used to store the file
    Returns : downloaded file as 'bytes'.
    """
    with open(file_name, "wb") as file:
        bfile = download_file_gdrive(file_id)
        file.write(bfile)
    return f"The file '{file_name}' has been downloadeded"


def test_downloaded_csv_file(file_name: str) -> None:
    df = pd.read_csv(file_name)
    print(df.head(5))
    # spark = (
    #     SparkSession.builder()
    #     .master("local[1]")
    #     .appName("SparkMovieAnalytics")
    #     .getOrCreate()
    # )
    # df: DataFrame = spark.read.options(
    #     header="True", inferSchema="True", delimiter=","
    # ).csv(file_name)

    # print(f"type of df = '{type(df)}' with file '{file_name}'")
    # df.show(5)
    pass


def csv_to_postgres(file_name: str, destination_table_name: str):
    # Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    get_postgres_conn = pg_hook.get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.

    # Getting the current work directory (cwd)
    table_dir = os.getcwd()
    # r=root, d=directories, f=files
    for r, d, f in os.walk(table_dir):
        for file in f:
            if file.endswith(file_name):
                table_path = os.path.join(r, file)

    print(table_path)

    file = table_path
    with open(file, "r") as f:
        next(f)
        curr.copy_from(f, destination_table_name, sep=",")
        get_postgres_conn.commit()


task_download_user_purchase_csv = PythonOperator(
    task_id="download_user_purchase_csv",
    provide_context=True,
    python_callable=download_gdrive_local,
    op_kwargs={
        "file_id": "1rqmnKgl_HXOfM7p4G_RC5v4clvbmJvks",
        "file_name": "user_purchase.csv",
    },
    dag=dag,
)


task_download_movie_review_csv = PythonOperator(
    task_id="download_movie_review_csv",
    provide_context=True,
    python_callable=download_gdrive_local,
    op_kwargs={
        "file_id": "1eh_0vzQGWnUZm5OH8j5M1D88he0uHILi",
        "file_name": "movie_review.csv",
    },
    dag=dag,
)


task_download_log_reviews_csv = PythonOperator(
    task_id="download_log_reviews_csv",
    provide_context=True,
    python_callable=download_gdrive_local,
    op_kwargs={
        "file_id": "1UVKS9V2PAQKvyBwkaMl2wEYa3zgL-GlH",
        "file_name": "log_reviews.csv",
    },
    dag=dag,
)

task_test_user_purchase_csv = PythonOperator(
    task_id="test_user_purchase_csv",
    provide_context=True,
    python_callable=test_downloaded_csv_file,
    op_kwargs={
        "file_name": "user_purchase.csv",
    },
    dag=dag,
)


task_test_movie_review_csv = PythonOperator(
    task_id="test_movie_review_csv",
    provide_context=True,
    python_callable=test_downloaded_csv_file,
    op_kwargs={
        "file_name": "movie_review.csv",
    },
    dag=dag,
)


task_test_log_reviews_csv = PythonOperator(
    task_id="test_log_reviews_csv",
    provide_context=True,
    python_callable=test_downloaded_csv_file,
    op_kwargs={
        "file_name": "log_reviews.csv",
    },
    dag=dag,
)

create_table_user_purchase = PostgresOperator(
    task_id="create_table_user_purchase",
    sql="""
            CREATE TABLE user_purchase (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            );
        """,
    postgres_conn_id="conn_postgress",
    autocommit=True,
    dag=dag,
)

task_load_table_user_purchase = PythonOperator(
    task_id="task_load_table_user_purchase",
    provide_context=True,
    python_callable=csv_to_postgres,
    op_kwargs={
        "file_name": "user_purchase.csv",
        "destination_table_name": "user_purchase",
    },
    dag=dag,
)

task_download_user_purchase_csv >> task_test_user_purchase_csv
task_download_movie_review_csv >> task_test_movie_review_csv
task_download_log_reviews_csv >> task_test_log_reviews_csv
create_table_user_purchase
[
    create_table_user_purchase,
    task_test_user_purchase_csv,
] >> task_load_table_user_purchase
