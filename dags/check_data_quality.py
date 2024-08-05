from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
from datetime import timedelta

from cosmos import ProjectConfig
from cosmos.operators import DbtTestOperator
from include.constants import DEFAULT_DBT_ROOT_PATH, PROFILE_CONFIG, EXECUTION_CONFIG

"""
default_args = {
    'owner': 'fred',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20, 12, 40),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
"""

with DAG(
        dag_id="check_data_quality",
        start_date=datetime(2024, 8, 5),
        schedule="@hourly",
        catchup=False,  # les executions manquées ne seront pas ratrappées
        max_active_runs=1,
        default_args={"owner": "fred", "retries": 3, 'retry_delay': timedelta(minutes=3)},
        tags=["quality"]
) as dag:
    check_freshness = BashOperator(
        task_id='check_sales_freshness',
        bash_command=f'dbt source freshness --profiles-dir {DEFAULT_DBT_ROOT_PATH} --project-dir {DEFAULT_DBT_ROOT_PATH}',
    )

    launch_tests = DbtTestOperator(
        task_id="launch_tests",
        project_dir=DEFAULT_DBT_ROOT_PATH,
        profile_config=PROFILE_CONFIG,
        outlets=[Dataset("TESTS://SALES_PROJECT")]
    )

    check_freshness >> launch_tests

