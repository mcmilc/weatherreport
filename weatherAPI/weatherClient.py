from southbayweather.weatherAPI.weatherAPI import WeatherAPI


class WeatherClient:
    def __init__(self, api: WeatherAPI, api_info: dict, city_info: dict):
        self._api = api
        self._api_data = api_info
        self._city_info = city_info

    def get_temperature_forecast(self, city: str, days: int, interval: str):
        """
        :param city
        :param days (int) number of forecast days
        :param interval (str) hourly or daily
        """
        url = self._api_data["forecast"]["main_url"]
        parameters = {
            "latitude": str(self._city_info[city]["latitude"]),
            "longitude": str(self._city_info[city]["longitude"]),
            interval: "temperature_2m",
            "forecast_days": str(days),
        }
        full_url = self._api.build_full_url(url, parameters)
        print(full_url)
        return self._api.request(full_url)

    def get_historical_temperature(self):
        pass
