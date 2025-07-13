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
    schedule='@daily',
    catchup=False,
    tags=['example'],
) as dag:

    # GH Archive 데이터 수집 태스크
    collect_gh_archive = KubernetesPodOperator(
        task_id='collect_gh_archive',
        name='gh-archive-collector',
        namespace='default',
        image='python:3.11-slim',
        image_pull_policy='IfNotPresent',
        cmds=['bash', '-cx'],
        arguments=[
            'pip install pandas requests deltalake s3fs && '
            'python /scripts/gh_archive_daily_collector.py {{ ds }} CausalInferenceLab'
        ],
        labels={'gh-archive': 'true'},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        service_account_name='airflow',
        volumes=[
            k8s.V1Volume(
                name="scripts-volume",
                config_map=k8s.V1ConfigMapVolumeSource(name="gh-archive-scripts")
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="scripts-volume",
                mount_path="/scripts"
            )
        ],
    )

    collect_gh_archive