from datetime import datetime, timedelta
from typing import Any, List


def generate_flow_start_log_message(
    flow_name: str,
    start_time: datetime,
    *flow_params: Any,
) -> str:
    start_time_string = f"Starting {flow_name} flow at {start_time}.\n"
    input_params_strings: List[str] = [
        f"\t{flow_param = }" for flow_param in flow_params
    ]
    start_message = (
        start_time_string + "Inputs:\n" + "\n".join(input_params_strings)
    )

    return start_message


def generate_flow_end_log_message(
    flow_name: str,
    start_time: datetime,
    end_time: datetime,
) -> str:
    duration: timedelta = end_time - start_time
    end_message = f"Finished {flow_name} flow at {end_time}.\nDuration: {duration}."

    return end_message