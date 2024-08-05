from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
from datetime import timedelta
from airflow.utils.task_group import TaskGroup
from pathlib import Path
from cosmos import DbtDag, ProjectConfig
from include.constants import DEFAULT_DBT_ROOT_PATH, PROFILE_CONFIG, EXECUTION_CONFIG

run_dbt_dag = DbtDag(
    dag_id="run_dbt_starburst",
    start_date=datetime(2024, 8, 2),
    catchup=False,
    schedule=[Dataset("TESTS://SALES_PROJECT")],
    default_args={"owner": "fred", "retries": 3, 'retry_delay': timedelta(minutes=3)},
    tags=["dbt_runs"],
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(DEFAULT_DBT_ROOT_PATH),
    profile_config=PROFILE_CONFIG,
    execution_config=EXECUTION_CONFIG

)


