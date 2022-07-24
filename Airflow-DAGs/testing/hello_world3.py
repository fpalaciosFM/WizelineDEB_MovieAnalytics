from asyncio.log import logger
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    hw = 'Hello world 3 from your first Airflow DAG!'
    logger.info(hw)
    return hw


dag = DAG('hello_world3', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(
    task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
