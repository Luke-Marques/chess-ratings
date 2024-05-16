from pathlib import Path
from typing import List

from prefect import flow
from prefect.logging import get_logger

from chess_ratings_pipeline.core.integrations.dbt.dbt_core import dbt_build


@flow
def build_all_dbt_models():
    logger = get_logger()
    logger.info("Running dbt models via dbt core...")
    dbt_result: List[str] = dbt_build(
        project_dir=Path("dbt"),
        debug=True,
    )
    logger.info(f"dbt result: {dbt_result}")
