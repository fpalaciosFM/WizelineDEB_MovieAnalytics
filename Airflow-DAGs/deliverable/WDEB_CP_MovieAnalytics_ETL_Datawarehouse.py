from datetime import timedelta, datetime

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor
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
    "WDEB_CP_MovieAnalytics_ETL_Datawarehouse",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

sensor_wait_for_transform_log_review = ExternalTaskSensor(
    dag=dag,
    task_id="wait_for_transform_log_review",
    external_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformLogReview",
    external_task_id="bigquery_create_external_table",
    execution_delta=timedelta(hours=-1),
)

sensor_wait_for_transform_movie_review = ExternalTaskSensor(
    dag=dag,
    task_id="wait_for_transform_movie_review",
    external_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformMovieReview",
    external_task_id="bigquery_create_external_table",
    execution_delta=timedelta(hours=-1),
)

sensor_wait_for_transform_user_purchase = ExternalTaskSensor(
    dag=dag,
    task_id="wait_for_transform_user_purchase",
    external_dag_id="WDEB_CP_MovieAnalytics_ETL_TransformUserPurchase",
    external_task_id="bigquery_create_external_table",
    execution_delta=timedelta(hours=-1),
)

task_dim_location_create = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="dim_location_create",
    sql=""""
        DROP TABLE IF EXISTS dw.dim_location;
        CREATE TABLE
        dw.dim_location AS(
        SELECT
            RANK() OVER (ORDER BY location) AS id_dim_location,
            location,
            CASE WHEN location in ("Alaska", "Arizona", "California", "Colorado", "Hawaii", "Idaho", "Montana", "Nevada", "New Mexico", "Oregon", "Utah", "Washington", "Wyoming") THEN "West" -- source: https://en.wikipedia.org/wiki/Western_United_States
            ELSE "East" END region
        FROM
            `stg.log_review`
        GROUP BY
            location )
    """,
)

task_dim_browser_create = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="dim_browser_create",
    sql=""""
        DROP TABLE IF EXISTS dw.dim_browser;
        CREATE TABLE
        dw.dim_browser AS(
        SELECT
            RANK() OVER (ORDER BY browser) AS id_dim_browser,
            browser
        FROM
            `stg.log_review`
        GROUP BY
            browser )
    """,
)

task_dim_date_create = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="dim_date_create",
    sql=""""
        DROP TABLE IF EXISTS
        dw.dim_date;
        CREATE TABLE
        dw.dim_date AS(
        SELECT
            RANK() OVER (ORDER BY log_date) AS id_dim_date,
            log_date,
            EXTRACT(DAY
            FROM
            log_date) day,
            EXTRACT(MONTH
            FROM
            log_date) month,
            EXTRACT(YEAR
            FROM
            log_date) year,
            EXTRACT(QUARTER
            FROM
            log_date) season,
        FROM
            `stg.log_review`
        GROUP BY
            log_date )
    """,
)

task_dim_devices_create = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="dim_devices_create",
    sql=""""
        DROP TABLE IF EXISTS dw.dim_devices;
        CREATE TABLE
        dw.dim_devices AS(
        SELECT
            RANK() OVER (ORDER BY device) AS id_dim_device,
            device
        FROM
            `stg.log_review`
        GROUP BY
            device )
    """,
)

task_dim_os_create = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="dim_os_create",
    sql=""""
        DROP TABLE IF EXISTS dw.dim_devices;
        CREATE TABLE
        dw.dim_devices AS(
        SELECT
            RANK() OVER (ORDER BY device) AS id_dim_device,
            device
        FROM
            `stg.log_review`
        GROUP BY
            device )
    """,
)

task_fact_movie_analytics = BigQueryExecuteQueryOperator(
    dag=dag,
    task_id="fact_movie_analytics",
    sql=""""
        DROP TABLE IF EXISTS
        dw.id_fact_movie_analytics;
        CREATE TABLE
        dw.fact_movie_analytics AS(
        WITH
            amount_collected_per_customer AS (
            SELECT
            customer_id,
            SUM(quantity * unit_price) AS amount_collected
            FROM
            `stg.user_purchase`
            GROUP BY
            customer_id )
        SELECT
            ROW_NUMBER() OVER () AS id_fact_movie_analytics,
            DIM_DATE.id_dim_date,
            DIM_LOC.id_dim_location,
            DIM_DEV.id_dim_device,
            DIM_OS.id_dim_os,
            DIM_BRO.id_dim_browser,
            SUM(AC.amount_collected) AS amount_collected,
            SUM(MR.positive_review) AS review_score,
            COUNT(MR.id_review) AS review_count,
            CURRENT_TIMESTAMP() AS insert_date
        FROM
            `stg.log_review` AS LR
        LEFT JOIN
            `dw.dim_location` AS DIM_LOC
        ON
            LR.location = DIM_LOC.location
        LEFT JOIN
            `dw.dim_devices` AS DIM_DEV
        ON
            LR.device = DIM_DEV.device
        LEFT JOIN
            `dw.dim_os` AS DIM_OS
        ON
            LR.os = DIM_OS.os
        LEFT JOIN
            `dw.dim_browser` AS DIM_BRO
        ON
            LR.browser = DIM_BRO.browser
        LEFT JOIN
            `stg.movie_review` AS MR
        ON
            LR.log_id = MR.id_review
        LEFT JOIN
            amount_collected_per_customer AS AC
        ON
            AC.customer_id = MR.cid
        LEFT JOIN
            `dw.dim_date` AS DIM_DATE
        ON
            LR.log_date = DIM_DATE.log_date
        GROUP BY
            DIM_LOC.id_dim_location,
            DIM_DEV.id_dim_device,
            DIM_OS.id_dim_os,
            DIM_BRO.id_dim_browser,
            DIM_DATE.id_dim_date )
    """,
)

sensor_wait_for_transform_log_review >> task_dim_location_create
sensor_wait_for_transform_log_review >> task_dim_browser_create
sensor_wait_for_transform_log_review >> task_dim_date_create
sensor_wait_for_transform_log_review >> task_dim_devices_create
sensor_wait_for_transform_log_review >> task_dim_os_create

sensor_wait_for_transform_movie_review >> task_fact_movie_analytics
sensor_wait_for_transform_user_purchase >> task_fact_movie_analytics

task_dim_location_create >> task_fact_movie_analytics
task_dim_browser_create >> task_fact_movie_analytics
task_dim_date_create >> task_fact_movie_analytics
task_dim_devices_create >> task_fact_movie_analytics
task_dim_os_create >> task_fact_movie_analytics
