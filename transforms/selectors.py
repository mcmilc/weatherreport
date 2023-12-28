def select_historical_temperature(data, interval="hourly"):
    timestamps = data[interval]["time"]
    temperature_data = [d for d in data[interval]["temperature_2m"] if d is not None]
    # should be part of a transform step
    L = len(temperature_data)
    timestamps = timestamps[:L]
    return timestamps, temperature_data


def select_current_temperature(data):
    timestamps = data["current"]["time"]
    temperature_data = data["current"]["temperature_2m"]
    return timestamps, temperature_data
