from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import timedelta, datetime
import io
import google.auth

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# from pyspark.sql import SparkSession
# from pyspark.sql import DataFrame

default_args = {
    "owner": "Fernando Palacios",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 1),
    "email": ["fpalacios.fm.gcp@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "MovieAnalyticsETL",
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

    # Google Drive API must be enabled in GCP
    # Using google service account for gdrive api
    creds, _ = google.auth.load_credentials_from_file(
        "../.secrets/movies-analytics-355603-8705d961ef61.json"
    )

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
    task_id="task_test_user_purchase_csv",
    provide_context=True,
    python_callable=test_downloaded_csv_file,
    op_kwargs={
        "file_name": "user_purchase.csv",
    },
    dag=dag,
)


task_test_movie_review_csv = PythonOperator(
    task_id="download_movie_review_csv",
    provide_context=True,
    python_callable=test_downloaded_csv_file,
    op_kwargs={
        "file_name": "movie_review.csv",
    },
    dag=dag,
)


task_test_log_reviews_csv = PythonOperator(
    task_id="download_log_reviews_csv",
    provide_context=True,
    python_callable=test_downloaded_csv_file,
    op_kwargs={
        "file_name": "log_reviews.csv",
    },
    dag=dag,
)

task_download_user_purchase_csv >> task_test_user_purchase_csv
task_download_movie_review_csv >> task_test_movie_review_csv
task_download_log_reviews_csv >> task_test_log_reviews_csv
