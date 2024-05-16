from pathlib import Path
from typing import Optional, List

from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation


@flow
def trigger_dbt_flow(
    commands: List[str],
    project_dir: Optional[Path | str] = "dbt",
    profiles_dir: Optional[Path | str] = "dbt",
) -> List[str]:
    dbt_result = DbtCoreOperation(
        commands=list(commands),
        project_dir=str(project_dir),
        profiles_dir=str(profiles_dir),
    ).run()
    return dbt_result
