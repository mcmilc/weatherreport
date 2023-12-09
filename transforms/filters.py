import datetime as dt
import numpy as np
from weatherreport.transforms.selectors import select_historical_temperature


def filter_temperature_by_time(data, interval, baseline_timestamp: dt.datetime):
    timestamps, temperatures = select_historical_temperature(data, interval)
    timestamps = np.array(timestamps)
    temperatures = np.array(temperatures)
    index = len(timestamps) * [True]
    for i in range(len(index)):
        if dt.datetime.fromisoformat(timestamps[i]) <= baseline_timestamp.replace(
            tzinfo=None
        ):
            index[i] = False
    timestamps = list(timestamps[index])
    temperatures = list(temperatures[index])
    return timestamps, temperatures
