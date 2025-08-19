"""
GitHub Followers/Following Collector DAG (Delta Lake)

Features:
- Fetches followers and following for one or more GitHub usernames
- Handles GitHub pagination with Authorization header (use GITHUB_TOKEN)
- Adds collection timestamp columns (UTC/KST) and daily partitions by dt_kst, username
- Stores results into MinIO/S3-backed Delta Lake table

Configuration:
- Trino: TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_PASSWORD, TRINO_CATALOG, TRINO_SCHEMA, TRINO_USERS_SQL
- Env: GITHUB_TOKEN (optional but recommended to avoid rate limit)
- MinIO/S3: uses envs consumed by utils.get_storage_options()
"""

import os
import pendulum

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


 


# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='gh_followers_following_to_delta',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule='0 6 * * *',  # daily 06:00 KST
    catchup=False,
    tags=['github', 'followers', 'delta-lake'],
    description='Collect GitHub followers/following for usernames and store in Delta Lake',
) as dag:

    # needed_env_keys = [
    #     'TRINO_HOST','TRINO_PORT','TRINO_USER','TRINO_PASSWORD','TRINO_CATALOG','TRINO_SCHEMA','TRINO_USERS_SQL',
    #     'AWS_ENDPOINT_URL','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_REGION','MINIO_SECURE',
    #     'DELTA_BUCKET','DELTA_TABLE_NAME','GITHUB_TOKEN'
    # ]
    # env_vars = {k: v for k, v in os.environ.items() if k in needed_env_keys and v is not None}

    collect_and_store = KubernetesPodOperator(
        task_id='collect_and_store_followers_following',
        image="ehddnr/gh-followers",
        name='gh-followers-following-collector',
        namespace='default',
        service_account_name='airflow',
        pod_template_file='/opt/airflow/pod_template.yaml',
        env_vars=[
            k8s.V1EnvVar(
                name="GITHUB_TOKEN",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="GITHUB_TOKEN",
                    )
                ),
            ),
        ],
        cmds=['python'],
        arguments=['/app/gh_follower_daily_collect.py'],
        get_logs=True,
        on_finish_action='delete_pod',
        in_cluster=True,
        image_pull_policy='Always',
        reattach_on_restart=False,
        startup_timeout_seconds=900,
        doc_md="""
        Fetch followers and following for configured GitHub usernames and write to Delta Lake.

        Configuration sources:
        - Trino: `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_PASSWORD`, `TRINO_CATALOG`, `TRINO_SCHEMA`, `TRINO_USERS_SQL`
        - Env: `GITHUB_TOKEN` (for higher rate limits)
        """,
        affinity={
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [
                        {
                            'matchExpressions': [
                                {
                                    'key': 'node-role.kubernetes.io/control-plane',
                                    'operator': 'Exists'
                                }
                            ]
                        }
                    ]
                }
            }
        }
    )


