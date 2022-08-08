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
    "WDEB_CP_MovieAnalytics_ETL_LoadFactTable",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
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

task_fact_movie_analytics
