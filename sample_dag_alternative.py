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
    dag_id='example_kubernetes_pod_busybox',
    default_args=default_args,
    start_date=datetime(2025, 7, 10),
    schedule='@daily',
    catchup=False,
    tags=['example'],
) as dag:

    run_pod = KubernetesPodOperator(
        task_id='run_simple_pod',
        name='airflow-simple-pod-busybox',
        namespace='default',
        image='busybox:1.35',  # 특정 버전 사용
        image_pull_policy='IfNotPresent',  # 로컬에 있으면 사용
        cmds=['echo'],
        arguments=['Hello from Airflow with busybox!'],
        labels={'example': 'true'},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        service_account_name='airflow',  # 서비스 계정 명시
        container_resources={
            'request_memory': '64Mi',
            'request_cpu': '50m',
            'limit_memory': '128Mi',
            'limit_cpu': '100m'
        }
    )

    run_pod 