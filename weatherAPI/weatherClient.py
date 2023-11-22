from southbayweather.weatherAPI.weatherAPI import WeatherAPI


class WeatherClient:
    def __init__(self, api: WeatherAPI, api_info: dict, city_info: dict):
        self._api = api
        self._api_info = api_info
        self._city_info = city_info

    def get_temperature_forecast(self, city: str, days: int, interval: str):
        """
        :param city
        :param days (int) number of forecast days
        :param interval (str) hourly or daily
        """
        url = self._api_info["forecast"]["main_url"]
        parameters = {
            "latitude": str(self._city_info[city]["latitude"]),
            "longitude": str(self._city_info[city]["longitude"]),
            interval: "temperature_2m",
            "forecast_days": str(days),
            "temperature_unit": self._api_info["temperature_unit"],
            "current": "temperature_2m",
            "timezone": self._city_info[city]["timezone"],
        }
        full_url = self._api.build_full_url(url, parameters)
        return self._api.request(full_url)

    def get_historical_temperature(
            self, start_date: str, end_date: str, city: str, interval: str):
        url = self._api_info["historical"]["main_url"]
        parameters = {
            "latitude": str(self._city_info[city]["latitude"]),
            "longitude": str(self._city_info[city]["longitude"]),
            "start_date": start_date,
            "end_date": end_date,
            "timezone": self._city_info[city]["timezone"],
            interval: "temperature_2m",
            "temperature_unit": self._api_info["temperature_unit"],
        }
        full_url = self._api.build_full_url(url, parameters)
        return self._api.request(full_url)
