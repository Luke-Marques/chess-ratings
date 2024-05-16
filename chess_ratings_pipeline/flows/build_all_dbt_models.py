from pathlib import Path
from typing import List

from prefect import flow
from prefect.logging import get_logger
from prefect_dbt.cli.commands import DbtCoreOperation

from chess_ratings_pipeline.core.integrations.dbt.dbt_core import dbt_build


@flow
def trigger_dbt_flow(project_dir: Path, profiles_dir: Path, commands: List[str]):
    result = DbtCoreOperation(
        commands=list(commands),
        project_dir=str(project_dir),
        profiles_dir=str(profiles_dir),
    ).run()
    return result


@flow
def build_all_dbt_models():
    logger = get_logger()
    logger.info("Running dbt models via dbt core...")
    dbt_result: List[str] = dbt_build(
        project_dir=Path("dbt"), profiles_dir=Path("dbt"), commands=["pwd", "dbt run"]
    )
    logger.info(f"dbt result: {dbt_result}")
