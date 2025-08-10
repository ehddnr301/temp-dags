import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup

# 조직 설정
TARGET_ORGANIZATIONS = ["CausalInferenceLab", "Pseudo-Lab", "apache"]
TARGET_LOGINS_STR = ",".join(TARGET_ORGANIZATIONS)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'timezone': 'Asia/Seoul',
    'max_active_runs': 3,
}

# 공통 KubernetesPodOperator 설정
COMMON_K8S_CONFIG = {
    'namespace': 'default',
    'image': 'ehddnr/gh-archive-collector:latest',
    'image_pull_policy': 'Always',
    'cmds': ['python'],
    'get_logs': True,
    'on_finish_action': 'delete_pod',
    'in_cluster': True,
    'startup_timeout_seconds': 600,
    'service_account_name': 'airflow',
    'termination_grace_period': 30,
    'reattach_on_restart': False,
    'affinity': {
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [
                    {
                        'matchExpressions': [
                            {
                                'key': 'node-role.kubernetes.io/worker',
                                'operator': 'Exists'
                            }
                        ]
                    }
                ]
            }
        }
    },
}

with DAG(
    dag_id='gh_archive_complete_pipeline',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule='0 3 * * *',  # 매일 KST 03:00 실행
    catchup=False,
    tags=['gh-archive'],
    description='gh-archive-complete-pipeline dag',
) as dag:

    # 1. 데이터 다운로드 (단일 태스크)
    download_task = KubernetesPodOperator(
        task_id='download_gh_archive',
        name='gh-archive-downloader',
        arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab', 'download'],
        labels={'gh-archive': 'download'},
        **COMMON_K8S_CONFIG
    )

    # 2. 데이터 처리 (압축 해제 및 Delta Lake 저장)
    process_task = KubernetesPodOperator(
        task_id='process_gh_archive',
        name='gh-archive-processor',
        arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab', 'process'],
        labels={'gh-archive': 'process'},
        **COMMON_K8S_CONFIG
    )

    # 3. 조직별 필터링 (단일 태스크)
    filter_task = KubernetesPodOperator(
        task_id='filter_gh_archive',
        name='gh-archive-filter',
        arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab', 'filter', TARGET_LOGINS_STR],
        labels={'gh-archive': 'filter'},
        **COMMON_K8S_CONFIG
    )

    # 4. 조직별 분리 (병렬 처리)
    with TaskGroup(group_id='split_by_organization') as split_group:
        split_tasks = []
        for org in TARGET_ORGANIZATIONS:
            split_task = KubernetesPodOperator(
                task_id=f'split_{org.lower().replace("-", "_")}',
                name=f'gh-archive-split-{org.lower()}',
                arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab', 'split', org],
                labels={'gh-archive': 'split', 'organization': org},
                **COMMON_K8S_CONFIG
            )
            split_tasks.append(split_task)

    # 5. 스키마 최적화 (병렬 처리)
    with TaskGroup(group_id='optimize_schemas') as optimize_group:
        optimize_tasks = []
        for org in TARGET_ORGANIZATIONS:
            optimize_task = KubernetesPodOperator(
                task_id=f'optimize_{org.lower().replace("-", "_")}',
                name=f'gh-archive-optimize-{org.lower()}',
                arguments=['/app/gh_archive_daily_collector.py', '{{ ds }}', 'CausalInferenceLab', 'optimize', org],
                labels={'gh-archive': 'optimize', 'organization': org},
                startup_timeout_seconds=900,  # 스키마 최적화는 더 많은 시간 필요
                **{k: v for k, v in COMMON_K8S_CONFIG.items() if k != 'startup_timeout_seconds'}
            )
            optimize_tasks.append(optimize_task)

    # 태스크 의존성 설정
    download_task >> process_task >> filter_task >> split_group >> optimize_group