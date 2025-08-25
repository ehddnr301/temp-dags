from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


schedule = CronDataIntervalTimetable("0 11 * * 1", timezone="Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='llm_weekly_reports',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 12, 30, tz='Asia/Seoul'),
    schedule=schedule,
    catchup=False,
    tags=['llm', 'weekly', 'reports'],
    description='Run LLM weekly reports for actor-week and repo-week every Monday 11:00 KST',
) as dag:

    actor_week = KubernetesPodOperator(
        task_id='generate_actor_week_report',
        image="ehddnr/gh-llm",
        name='llm-actor-week',
        namespace='default',
        service_account_name='airflow',
        pod_template_file='/opt/airflow/pod_template.yaml',
        env_vars=[
            k8s.V1EnvVar(
                name="OPENAI_API_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="OPENAI_API_KEY",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_ENDPOINT_URL",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="AWS_ENDPOINT_URL",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="AWS_ACCESS_KEY_ID",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="AWS_SECRET_ACCESS_KEY",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="MINIO_SECURE",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="MINIO_SECURE",
                    )
                ),
            ),
        ],
        cmds=['python'],
        arguments=['/app/main.py', '--week-start', "{{ logical_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}", '--report', 'actor-week', '--save-delta', '--delta-mode', 'append'],
        get_logs=True,
        on_finish_action='delete_pod',
        in_cluster=True,
        image_pull_policy='Always',
        reattach_on_restart=False,
        startup_timeout_seconds=1200,
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

    repo_week = KubernetesPodOperator(
        task_id='generate_repo_week_report',
        image="ehddnr/gh-llm",
        name='llm-repo-week',
        namespace='default',
        service_account_name='airflow',
        pod_template_file='/opt/airflow/pod_template.yaml',
        env_vars=[
            k8s.V1EnvVar(
                name="OPENAI_API_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="OPENAI_API_KEY",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_ENDPOINT_URL",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="AWS_ENDPOINT_URL",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="AWS_ACCESS_KEY_ID",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="AWS_SECRET_ACCESS_KEY",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="MINIO_SECURE",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="pseudo-secret",
                        key="MINIO_SECURE",
                    )
                ),
            ),
        ],
        cmds=['python'],
        arguments=['/app/main.py', '--report', 'repo-week', '--week-start', "{{ logical_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}", "--save-delta", "--delta-mode", "append"],
        get_logs=True,
        on_finish_action='delete_pod',
        in_cluster=True,
        image_pull_policy='Always',
        reattach_on_restart=False,
        startup_timeout_seconds=1200,
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

    [actor_week, repo_week]


