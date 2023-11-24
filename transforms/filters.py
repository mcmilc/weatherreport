def filter_historical_temperature(data, interval="hourly"):
    timestamps = data[interval]["time"]
    temperature_data = data[interval]["temperature_2m"]
    return timestamps, temperature_data


def filter_current_temperature(data):
    timestamps = data["current"]["time"]
    temperature_data = data["current"]["temperature_2m"]
    return timestamps, temperature_data
