import os
import platform
import sys
from typing import Callable

from prefect import flow, get_run_logger, task


def table_text() -> Callable:
    return "{0:>22}:  {1:<50}".format


@task
def log_platform_info():
    logger = get_run_logger()
    platform.system()
    # Nifty formatter to display our output in orderly columns
    table_text = "{0:>22}:  {1:<50}".format
    # All Cloud Run job container hostnames should start with 'SandboxHost'
    logger.info(table_text("Host's network name", f"{platform.node()} 🚀"))
    logger.info(table_text("Python version", platform.python_version()))
    logger.info(table_text("Platform information (instance type)", platform.platform()))
    logger.info(table_text("OS/Arch", f"{sys.platform}/{platform.machine()}"))

    for k, v in os.environ.items():
        # we expect to see several CLOUD_RUN* environment variables
        if k.startswith("CLOUD_RUN"):
            logger.info(table_text(k, v))


@flow()
def health_check():
    log_platform_info()


if __name__ == "__main__":
    health_check()
