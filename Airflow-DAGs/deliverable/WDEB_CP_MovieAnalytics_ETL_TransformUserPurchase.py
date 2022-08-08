from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
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
    "WDEB_CP_MovieAnalytics_ETL_TransformUserPurchase",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

task_extract_postgres_to_json_gcs_user_purchase = PostgresToGCSOperator(
    dag=dag,
    task_id="extract_postgres_to_json_gcs_user_purchase",
    postgres_conn_id="postgres_default",
    sql="SELECT * FROM stg.user_purchase",
    bucket="wizeline-deb-movie-analytics-fpa",
    filename="user_purchase.json",
)

task_convert_json_to_parquet = DataprocInstantiateWorkflowTemplateOperator(
    dag=dag,
    task_id="convert_json_to_parquet",
    gcp_conn_id="google_cloud_default",
    template_id="wdeb-template-user-purchase",
    region="us-west1",
)

task_bigquery_create_external_table = BigQueryCreateExternalTableOperator(
    dag=dag,
    task_id="bigquery_create_external_table",
    bucket="wizeline-deb-movie-analytics-fpa",
    source_objects=["parquet/user_purchase.parquet/*.parquet"],
    destination_project_dataset_table="stg.user_purchase",
    source_format="parquet",
    google_cloud_storage_conn_id="google_cloud_default",
    location="us-west1",
)

(
    task_extract_postgres_to_json_gcs_user_purchase
    >> task_convert_json_to_parquet
    >> task_bigquery_create_external_table
)
