from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    return "Hello from MinIO DAG!"

def print_world():
    return "World from MinIO DAG!"

with DAG(
    'sample_minio_dag',
    default_args=default_args,
    description='A sample DAG loaded from MinIO',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['sample', 'minio'],
) as dag:

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )

    world_task = PythonOperator(
        task_id='world_task',
        python_callable=print_world,
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Bash task from MinIO DAG!"',
    )

    hello_task >> world_task >> bash_task 