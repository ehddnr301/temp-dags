from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

PROJECT_DIR = "/opt/airflow/dags/temp-dags/dbt_gh_archive"
PROFILES_DIR = "/opt/airflow/dags/temp-dags/dbt_gh_archive/profiles"
PROFILES_YML = f"{PROFILES_DIR}/profiles.yml"

# Ensure dbt graph build (dbt ls) has required env var at parse time
os.environ.setdefault("LOAD_BASE_DATE_KST", "1970-01-01")

# Cosmos-based dbt orchestration (Kubernetes 실행 모드 및 미설치 예외처리 제거)
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.config import RenderConfig, ExecutionConfig

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

schedule = CronDataIntervalTimetable("0 11 * * *", timezone="Asia/Seoul")

with DAG(
    dag_id='dbt_gh_archive',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule=schedule,
    catchup=False,
    tags=['dbt', 'cosmos', 'gh-archive'],
    description='Run dbt_gh_archive project via Astronomer Cosmos',
) as dag:

    dbt = DbtTaskGroup(
        group_id="dbt_gh_archive",
        project_config=ProjectConfig(dbt_project_path=PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="dbt_gh_archive",
            target_name="k8s",
            profiles_yml_filepath=PROFILES_YML,
        ),
        render_config=RenderConfig(
            select=["tag:bi"],
        ),
        operator_args={
            'dbt_cmd_flags': [
                '--target', 'k8s',
            ],
            'env': {
                'DBT_TARGET_PATH': '/tmp/dbt_target',
                'DBT_LOG_PATH': '/tmp/dbt_logs',
                'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
                'LOAD_BASE_DATE_KST': '{{ ds }}',
            }
        }
    )

    dbt  # expose task group


