import airflow
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from psycopg2.extras import execute_values

#default arguments

default_args = {
    'owner': 'Marco Mendoza',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 1),
    'email': ['marco.mendoza@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('insert_data_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='conn_postgress')
    get_postgres_conn = PostgresHook(postgres_conn_id='conn_postgress').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.

    # Getting the current work directory (cwd)
    table_dir = os.getcwd()
    # r=root, d=directories, f=files
    for r, d, f in os.walk(table_dir):
        for file in f:
            if file.endswith("table.csv"):
                table_path = os.path.join(r, file)

    print(table_path)

    file = table_path
    with open(file, 'r') as f:
        next(f)
        curr.copy_from(f, 'pokemon', sep=',')
        get_postgres_conn.commit()

task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS pokemon (
                            Number INTEGER,   
                            Name VARCHAR(255),
                            Type_1 VARCHAR(255),
                            Type_2 VARCHAR(255),
                            Total INTEGER,
                            HP INTEGER,
                            Attack INTEGER,
                            Defense INTEGER,
                            Sp_Atk INTEGER,
                            Sp_Def INTEGER,
                            Speed INTEGER,
                            Generation INTEGER,
                            Legendary VARCHAR(255));
                            """,
                            postgres_conn_id= 'conn_postgress',
                            autocommit=True,
                            dag= dag)

task2 = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)


task1 >> task2