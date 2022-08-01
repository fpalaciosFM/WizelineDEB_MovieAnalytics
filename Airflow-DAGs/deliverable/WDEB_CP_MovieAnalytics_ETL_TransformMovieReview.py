from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocInstantiateWorkflowTemplateOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
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
    "WDEB_CP_MovieAnalytics_ETL_TransformMovieReview",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)

task_identify_positive_reviews = DataprocInstantiateWorkflowTemplateOperator(
    dag=dag,
    task_id="identify_positive_reviews",
    gcp_conn_id="google_cloud_default",
    template_id="wdeb-template-movie-review",
    region="us-west1",
)

task_bigquery_create_external_table = BigQueryCreateExternalTableOperator(
    dag=dag,
    task_id="bigquery_create_external_table",
    bucket="wizeline-deb-movie-analytics-fpa",
    source_objects="wizeline-deb-movie-analytics-fpa/parquet/movie_review.parquet/*.parquet",
    destination_project_dataset_table="stg.movie_review",
    source_format="parquet",
    gcp_conn_id="google_cloud_default",
    google_cloud_storage_conn_id="google_cloud_default",
)

task_identify_positive_reviews >> task_bigquery_create_external_table
