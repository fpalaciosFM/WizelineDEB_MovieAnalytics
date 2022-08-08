from datetime import timedelta, datetime

from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
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
    "WDEB_CP_MovieAnalytics_ETL_LoadDimTables",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

task_dim_location_create = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="dim_location_create",
    gcp_conn_id="google_cloud_default",
    sql=""""SELECT RANK() OVER (ORDER BY location) AS id_dim_location, location, CASE WHEN location in ("Alaska", "Arizona", "California", "Colorado", "Hawaii", "Idaho", "Montana", "Nevada", "New Mexico", "Oregon", "Utah", "Washington", "Wyoming") THEN "West" -- source: https://en.wikipedia.org/wiki/Western_United_States ELSE "East" END region FROM `stg.log_review` GROUP BY location""",
    destination_dataset_table="dw.dim_location",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
)

# task_dim_browser_create = BigQueryExecuteQueryOperator(
#     dag=dag,
#     task_id="dim_browser_create",
#     gcp_conn_id="google_cloud_default",
#     sql=""""
#         DROP TABLE IF EXISTS dw.dim_browser;
#         CREATE TABLE
#         dw.dim_browser AS(
#         SELECT
#             RANK() OVER (ORDER BY browser) AS id_dim_browser,
#             browser
#         FROM
#             `stg.log_review`
#         GROUP BY
#             browser )
#     """,
# )

# task_dim_date_create = BigQueryExecuteQueryOperator(
#     dag=dag,
#     task_id="dim_date_create",
#     gcp_conn_id="google_cloud_default",
#     sql=""""
#         DROP TABLE IF EXISTS
#         dw.dim_date;
#         CREATE TABLE
#         dw.dim_date AS(
#         SELECT
#             RANK() OVER (ORDER BY log_date) AS id_dim_date,
#             log_date,
#             EXTRACT(DAY
#             FROM
#             log_date) day,
#             EXTRACT(MONTH
#             FROM
#             log_date) month,
#             EXTRACT(YEAR
#             FROM
#             log_date) year,
#             EXTRACT(QUARTER
#             FROM
#             log_date) season,
#         FROM
#             `stg.log_review`
#         GROUP BY
#             log_date )
#     """,
# )

# task_dim_devices_create = BigQueryExecuteQueryOperator(
#     dag=dag,
#     task_id="dim_devices_create",
#     gcp_conn_id="google_cloud_default",
#     sql=""""
#         DROP TABLE IF EXISTS dw.dim_devices;
#         CREATE TABLE
#         dw.dim_devices AS(
#         SELECT
#             RANK() OVER (ORDER BY device) AS id_dim_device,
#             device
#         FROM
#             `stg.log_review`
#         GROUP BY
#             device )
#     """,
# )

# task_dim_os_create = BigQueryExecuteQueryOperator(
#     dag=dag,
#     task_id="dim_os_create",
#     gcp_conn_id="google_cloud_default",
#     sql=""""
#         DROP TABLE IF EXISTS dw.dim_devices;
#         CREATE TABLE
#         dw.dim_devices AS(
#         SELECT
#             RANK() OVER (ORDER BY device) AS id_dim_device,
#             device
#         FROM
#             `stg.log_review`
#         GROUP BY
#             device )
#     """,
# )


task_dim_location_create
# task_dim_browser_create
# task_dim_date_create
# task_dim_devices_create
# task_dim_os_create
