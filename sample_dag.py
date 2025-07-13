from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='example_kubernetes_pod_airflow_3_0_2',
    default_args=default_args,
    start_date=datetime(2025, 7, 10),
    schedule='@daily',
    catchup=False,
    tags=['example'],
) as dag:

    run_pod = KubernetesPodOperator(
        task_id='run_simple_pod',
        name='airflow-simple-pod',
        namespace='default',
        image='python:3.9-slim',
        image_pull_policy='IfNotPresent',  # 이미지 풀 정책 추가
        cmds=['bash', '-cx'],
        arguments=['echo "Hello from Airflow 3.0.2!"'],
        labels={'example': 'true'},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,  # 시작 타임아웃 추가
        service_account_name='airflow',  # 서비스 계정 명시
    )

    run_pod