import json
from datetime import datetime, timedelta
from typing import Tuple

from prefect import flow, get_run_logger, task
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion
from prefect_gcp.secret_manager import GcpSecret


@task(name="Fetch-dbt-GCP-Secret-Values")
def fetch_dbt_gcp_secret_values() -> Tuple[str, str]:
    # noinspection PyUnresolvedReferences
    # ToDo: make dynamic for dev/prod environment executions
    dbt_secrets_json = json.loads(GcpSecret.load("dbt-secrets").read_secret())

    return dbt_secrets_json["api_key"], dbt_secrets_json["account_id"]


@flow
def run_dbt_job(job_id: int):
    # Get logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `run_dbt_job` flow at {start_time} (local time).
    Inputs:
        job_id: {job_id}"""
    logger.info(start_message)

    logger.info("Fetching dbt Cloud credentials from Prefect Block")
    dbt_credentials = DbtCloudCredentials.load("chess-ratings-dev")

    logger.info("Triggering dbt job and waiting for it to complete")
    trigger_dbt_cloud_job_run_and_wait_for_completion(
        dbt_cloud_credentials=dbt_credentials,
        job_id=job_id,
    )

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `run_dbt_job` flow at {end_time} (local time).
        Time taken: {time_taken}"""
    logger.info(end_message)
