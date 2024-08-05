from pathlib import Path
import os
from cosmos import ProfileConfig, ExecutionConfig

DEFAULT_ROOT_PATH = Path(__file__).parent.parent
DEFAULT_DBT_ROOT_PATH = DEFAULT_ROOT_PATH / "dbt/bi_modeling_starbust"
PROFILES_FILE_PATH = Path(DEFAULT_DBT_ROOT_PATH, "profiles.yml")
DBT_EXECUTABLE = Path(__file__).parent.parent / "dbt_venv/bin/dbt"

EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=str(DBT_EXECUTABLE)
)

PROFILE_CONFIG = ProfileConfig(
    profile_name="bi_modeling_starbust",
    target_name="dev",
    profiles_yml_filepath=PROFILES_FILE_PATH
)
