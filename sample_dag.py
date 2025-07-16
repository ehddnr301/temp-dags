import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'timezone': 'Asia/Seoul',
    'max_active_runs': 1,
}

with DAG(
    dag_id='gh_archive_separated_tasks',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule='0 3 * * *',  # 매일 KST 03:00 실행
    catchup=False,
    tags=['gh-archive'],
) as dag:

    download_task = KubernetesPodOperator(
        task_id='download_gh_archive',
        name='gh-archive-downloader',
        namespace='default',
        image='ehddnr/gh-archive-collector:latest',
        image_pull_policy='Always',
        cmds=['python'],
        arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab', 'download'],
        labels={'gh-archive': 'download'},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        service_account_name='airflow',
        termination_grace_period=30,
        reattach_on_restart=False,
    )

    process_task = KubernetesPodOperator(
        task_id='process_gh_archive',
        name='gh-archive-processor',
        namespace='default',
        image='ehddnr/gh-archive-collector:latest',
        image_pull_policy='Always',
        cmds=['python'],
        arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab', 'process'],
        labels={'gh-archive': 'process'},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        service_account_name='airflow',
        termination_grace_period=30,
        reattach_on_restart=False,
    )

    # 태스크 의존성 설정
    download_task >> process_task
