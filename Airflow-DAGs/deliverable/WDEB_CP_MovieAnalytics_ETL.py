from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    "WDEB_CP_MovieAnalytics_ETL",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    max_active_runs=1,
)

trigger_extract_log_review = TriggerDagRunOperator(
    dag=dag,
    task_id="extract_log_review",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_ExtractLogReview",
    wait_for_completion=True,
)

trigger_extract_movie_review = TriggerDagRunOperator(
    dag=dag,
    task_id="extract_movie_review",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_ExtractMovieReview",
    wait_for_completion=True,
)

trigger_extract_user_purchase = TriggerDagRunOperator(
    dag=dag,
    task_id="extract_user_purchase",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_ExtractUserPurchase",
    wait_for_completion=True,
)

trigger_transform_log_review = TriggerDagRunOperator(
    dag=dag,
    task_id="transform_log_review",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformLogReview",
    wait_for_completion=True,
)

trigger_transform_movie_review = TriggerDagRunOperator(
    dag=dag,
    task_id="transform_movie_review",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformMovieReview",
    wait_for_completion=True,
)

trigger_transform_user_purchase = TriggerDagRunOperator(
    dag=dag,
    task_id="transform_user_purchase",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformUserPurchase",
    wait_for_completion=True,
)

trigger_load_dim_tables = TriggerDagRunOperator(
    dag=dag,
    task_id="load_dim_tables",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_LoadDimTables",
    wait_for_completion=True,
)

trigger_load_fact_table = TriggerDagRunOperator(
    dag=dag,
    task_id="load_fact_table",
    trigger_dag_id="WDEB_CP_MovieAnalytics_ETL_LoadFactTable",
    wait_for_completion=True,
)

(
    trigger_extract_log_review
    >> trigger_transform_log_review
    >> trigger_load_dim_tables
    >> trigger_load_fact_table
)
(
    trigger_extract_movie_review
    >> trigger_transform_movie_review
    >> trigger_load_fact_table
)
(
    trigger_extract_user_purchase
    >> trigger_transform_user_purchase
    >> trigger_load_fact_table
)
