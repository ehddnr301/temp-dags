"""
Optimized GitHub Archive DAG
Streamlined version with improved data flow and timezone support

Key Features:
1. Maintains current execution format
2. Download -> Process (with organization filtering) -> Delta Lake storage
3. Adds timezone columns (dt_kst, dt_utc, ts_kst, ts_utc)
4. Partitioning by dt_kst and organization
5. Prepared for future dbt integration
"""

import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup

# Configuration
TARGET_ORGANIZATIONS = ["CausalInferenceLab", "Pseudo-Lab", "apache"]
TARGET_LOGINS_STR = ",".join(TARGET_ORGANIZATIONS)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'timezone': 'Asia/Seoul',
    'max_active_runs': 1,  # Prevent overlapping runs
}

# Common Kubernetes Pod configuration
COMMON_K8S_CONFIG = {
    'namespace': 'default',
    'image': 'ehddnr/gh-archive-daily-collector:latest',  # Updated image with enhanced collector
    'image_pull_policy': 'Always',
    'cmds': ['python'],
    'get_logs': True,
    'on_finish_action': 'delete_pod',
    'in_cluster': True,
    'startup_timeout_seconds': 900,  # Increased timeout for processing
    'service_account_name': 'airflow',
    'termination_grace_period': 60,  # Graceful shutdown
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
    dag_id='gh_archive_optimized_pipeline',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule='0 10 * * *',  # Daily at 10 AM KST
    catchup=False,
    tags=['gh-archive', 'optimized', 'delta-lake'],
    description='Optimized GitHub Archive pipeline with timezone support and Delta Lake partitioning',
    doc_md="""
    # GitHub Archive Optimized Pipeline
    
    This DAG implements an optimized data pipeline for GitHub Archive data with:
    
    ## Features
    - **Streamlined Flow**: Download -> Process with filtering -> Delta Lake
    - **Timezone Support**: Automatic KST/UTC timezone conversion
    - **Smart Partitioning**: By dt_kst and organization
    - **Resource Optimization**: Reduced network bandwidth and storage
    - **dbt Ready**: Prepared for future dbt integration
    
    ## Data Flow
    1. **Download**: Fetch 24-hour data from GitHub Archive API
    2. **Process**: Filter by organizations and add timezone columns
    3. **Store**: Save to Delta Lake with optimal partitioning
    
    """
) as dag:

    # 1. Data Download Task
    download_task = KubernetesPodOperator(
        task_id='download_gh_archive_data',
        name='gh-archive-downloader',
        arguments=['/app/gh_archive_daily_collect.py', '{{ ds }}', 'download'],
        labels={
            'gh-archive': 'download',
            'pipeline': 'optimized',
            'date': '{{ ds }}'
        },
        doc_md="""
        Download GitHub Archive data for the execution date.
        
        - Downloads 24 hourly files from data.gharchive.org
        - Stores in MinIO raw bucket for processing
        - Uses wget with retry logic for reliability
        """,
        **COMMON_K8S_CONFIG
    )

    # 2. Process and Filter Task (replaces multiple steps from original)
    process_task = KubernetesPodOperator(
        task_id='process_and_filter_to_delta',
        name='gh-archive-processor',
        arguments=['/app/gh_archive_daily_collect.py', "{{ logical_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}", 'process'],
        labels={
            'gh-archive': 'process',
            'pipeline': 'optimized',
            'date': "{{ logical_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
        },
        startup_timeout_seconds=1200,  # Longer timeout for processing
        doc_md="""
        Process downloaded data with organization filtering and Delta Lake storage.
        
        This task replaces the original filter/split/optimize steps with:
        - Organization filtering during processing
        - Timezone column generation (dt_kst, dt_utc, ts_kst, ts_utc)
        - Direct Delta Lake storage with partitioning
        - Automatic cleanup of raw files
        """,
        **{k: v for k, v in COMMON_K8S_CONFIG.items() 
           if k not in ['startup_timeout_seconds', 'resources']}
    )

    # Task Dependencies
    download_task >> process_task
