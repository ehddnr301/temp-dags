from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from kubernetes.client import models as k8s


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='example_kubernetes_pod_airflow_3_0_2',
    default_args=default_args,
    start_date=datetime(2025, 7, 10),
    schedule='0 3 * * *',  # KST 03:00 (새벽 3시)
    catchup=False,
    tags=['example'],
    timezone='Asia/Seoul',
) as dag:

    # GH Archive 데이터 수집 태스크
    collect_gh_archive = KubernetesPodOperator(
        task_id='collect_gh_archive',
        name='gh-archive-collector',
        namespace='default',
        image='ehddnr/gh-archive-collector:latest',  # Docker 이미지 사용
        image_pull_policy='Always',
        cmds=['python'],
        arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab'],
        labels={'gh-archive': 'true'},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        service_account_name='airflow',
        termination_grace_period=30,
        reattach_on_restart=False,
    )

    collect_gh_archive