from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    hw = "Hello world from your first Airflow DAG!"

    with open("test_airflow.txt", "w") as f:
        f.write("hola mundo desde archivo")

    print("Hola Log")

    with open("test_airflow.txt", "r") as f:
        print(f.readlines())

    return hw


dag = DAG(
    "hello_world",
    description="Hello World DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

hello_operator = PythonOperator(
    task_id="hello_task", python_callable=print_hello, dag=dag
)

hello_operator
