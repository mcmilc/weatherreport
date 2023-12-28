"""_summary_

Returns:
    _type_: _description_
"""
import datetime as dt
import numpy as np


def filter_temperature_by_time(
    timestamps, temperatures, baseline_timestamp: dt.datetime
):
    """_summary_

    Args:
        timestamps (_type_): _description_
        temperatures (_type_): _description_
        interval (_type_): _description_
        baseline_timestamp (dt.datetime): _description_

    Returns:
        _type_: _description_
    """
    timestamps = np.array(timestamps)
    temperatures = np.array(temperatures)
    index = len(timestamps) * [True]
    for i, _ in enumerate(index):
        if dt.datetime.fromisoformat(timestamps[i]) <= baseline_timestamp.replace(
            tzinfo=None
        ):
            index[i] = False
    timestamps = list(timestamps[index])
    temperatures = list(temperatures[index])
    return timestamps, temperatures
