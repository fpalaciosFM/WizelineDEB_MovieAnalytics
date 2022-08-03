from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)

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
    "WDEB_CP_MovieAnalytics_ETL_TransformUserPurchase",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

task_extract_postgres_to_csv_gcs_user_purchase = PostgresToGCSOperator(
    dag=dag,
    task_id="extract_postgres_to_csv_gcs_user_purchase",
    postgres_conn_id="postgres_default",
    sql="SELECT * FROM stg.user_purchase",
    bucket="wizeline-deb-movie-analytics-fpa",
    filename="user_purchase.csv",
)

task_extract_postgres_to_csv_gcs_user_purchase
