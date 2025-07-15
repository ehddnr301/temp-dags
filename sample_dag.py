import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='example_kubernetes_pod_airflow_3_0_2',
    default_args=default_args,
    # pendulum을 이용해 tz-aware datetime 지정
    start_date=pendulum.datetime(2025, 7, 10, tz='Asia/Seoul'),
    schedule='0 3 * * *',  # 매일 KST 03:00 실행
    catchup=False,
    tags=['example'],
) as dag:

    collect_gh_archive = KubernetesPodOperator(
        task_id='collect_gh_archive',
        name='gh-archive-collector',
        namespace='default',
        image='ehddnr/gh-archive-collector:latest',
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
