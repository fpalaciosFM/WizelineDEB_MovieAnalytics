from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocInstantiateWorkflowTemplateOperator,
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
    gcp_conn_id="google_cloud_default",
    template_id="wdeb-template-movie-review",
    region="us-west1",
)

task_identify_positive_reviews
