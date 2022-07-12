"""
S3 Sensor Connection Test
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
# from airflow.providers.http.sensors.http import HttpSensor, SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Marco Mendoza',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=0)
}

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval='@once')

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag)

sensor = S3KeySensor(
    task_id="check_s3",
    bucket_key="",
    wildcard_match=True,
    bucket_name='test-delet-me',
    aws_conn_id="conn_S3",
    timeout=18 * 60 * 60,
    poke_interval=120,
    dag=dag)

t1 >> sensor
