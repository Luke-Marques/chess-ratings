from pathlib import Path
from typing import Optional, List

from prefect import flow
from prefect_dbt.cli import DbtCliProfile
from prefect_dbt.cli.commands import DbtCoreOperation


@flow
def trigger_dbt_flow(project_dir: Path, profiles_dir: Path, commands: List[str]):
    dbt_cli_profile = DbtCliProfile.load("chess-ratings-dev")
    with DbtCoreOperation(
        commands=list(commands),
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        dbt_cli_profile=dbt_cli_profile,
    ) as dbt_operation:
        dbt_process = dbt_operation.trigger()
        dbt_process.wait_for_completion()
        result: List[str] = dbt_process.fetch_result()
    return result


@flow
def dbt_run(
    project_dir: Path,
    profiles_dir: Path,
    models: Optional[Path] = None,
    debug: bool = True,
) -> List[str]:
    """
    Trigger a dbt debug and run flow.

    Args:
        project_dir (Path): The path to the dbt project directory.
        profiles_dir (Path): The path to the dbt profiles directory.
        models (Optional[Path]): The path to the models directory.

    Returns:
        List[str]: The dbt debug and run flow has been triggered.
    """
    commands = ["pwd", "dbt run"]
    if debug:
        commands = commands[:1] + ["dbt debug"] + commands[1:]
    if models:
        commands = [
            f"{c} --select {models}" for c in commands if c not in ["pwd", "dbt debug"]
        ]
    return trigger_dbt_flow(
        project_dir,
        profiles_dir,
        commands,
    )


@flow
def dbt_build(
    project_dir: Path,
    profiles_dir: Path,
    models: Optional[Path] = None,
    debug: bool = True,
) -> List[str]:
    """
    Trigger a dbt debug and build flow.

    Args:
        project_dir (Path): The path to the dbt project directory.
        profiles_dir (Path): The path to the dbt profiles directory.
        models (Optional[Path]): The path to the models directory.

    Returns:
        List[str]: The dbt debug and build flow has been triggered.
    """
    commands = ["pwd", "dbt build"]
    if debug:
        commands = commands[:1] + ["dbt debug"] + commands[1:]
    if models:
        commands = [
            f"{c} --select {models}" for c in commands if c not in ["pwd", "dbt debug"]
        ]
    return trigger_dbt_flow(
        project_dir,
        profiles_dir,
        commands,
    )


@flow
def dbt_test(
    project_dir: Path,
    profiles_dir: Path,
    models: Optional[Path] = None,
    debug: bool = True,
) -> List[str]:
    """
    Trigger a dbt debug and test flow.

    Args:
        project_dir (Path): The path to the dbt project directory.
        profiles_dir (Path): The path to the dbt profiles directory.
        models (Optional[Path]): The path to the models directory.

    Returns:
        List[str]: The dbt debug and test flow has been triggered.
    """
    commands = ["pwd", "dbt test"]
    if debug:
        commands = commands[:1] + ["dbt debug"] + commands[1:]
    if models:
        commands = [
            f"{c} --select {models}" for c in commands if c not in ["pwd", "dbt debug"]
        ]
    return trigger_dbt_flow(
        project_dir,
        profiles_dir,
        commands,
    )


@flow
def dbt_seed(
    project_dir: Path,
    profiles_dir: Path,
    models: Optional[Path] = None,
    debug: bool = True,
) -> List[str]:
    """
    Trigger a dbt debug and seed flow.

    Args:
        project_dir (Path): The path to the dbt project directory.
        profiles_dir (Path): The path to the dbt profiles directory.
        models (Optional[Path]): The path to the models directory.

    Returns:
        List[str]: The dbt debug and seed flow has been triggered.
    """
    commands = ["pwd", "dbt seed"]
    if debug:
        commands = commands[:1] + ["dbt debug"] + commands[1:]
    if models:
        commands = [
            f"{c} --select {models}" for c in commands if c not in ["pwd", "dbt debug"]
        ]
    return trigger_dbt_flow(
        project_dir,
        profiles_dir,
        commands,
    )


@flow
def dbt_clean(
    project_dir: Path,
    profiles_dir: Path,
) -> List[str]:
    """
    Trigger a dbt debug and clean flow.

    Args:
        project_dir (Path): The path to the dbt project directory.
        profiles_dir (Path): The path to the dbt profiles directory.

    Returns:
        List[str]: The dbt debug and clean flow has been triggered.
    """
    commands = ["pwd", "dbt clean"]
    return trigger_dbt_flow(
        project_dir,
        profiles_dir,
        commands,
    )
