from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


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
    "load_csv_to_gcs",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)

POSTGRES_CONN_ID = "POSTGRES_CONN_TEST"


clear_table = PostgresOperator(
    task_id="clear_table",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""CREATE TABLE IF NOT EXISTS test(
        C1 int,
        C2 int
    )""",
    dag=dag
)
