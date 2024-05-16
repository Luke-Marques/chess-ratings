from typing import List

from prefect import flow
from prefect.logging import get_logger

from chess_ratings_pipeline.core.integrations.dbt.dbt_core import trigger_dbt_flow


@flow
def build_all_dbt_models():
    logger = get_logger()
    logger.info("Running dbt models via dbt core...")
    dbt_result: List[str] = trigger_dbt_flow(commands=["pwd", "dbt debug", "dbt run"])
    logger.info(f"dbt result: {dbt_result}")
    return dbt_result
