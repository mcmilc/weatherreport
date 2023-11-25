import json
from southbayweather.config.config import sbw_root

# HELPERS
from southbayweather.utilities.helpers import pjoin
from southbayweather.utilities.helpers import read_json
from southbayweather.utilities.helpers import build_date

# Weather API
from southbayweather.weatherAPI.weatherAPI import WeatherAPI


class WeatherClient:
    def __init__(self, api: WeatherAPI, api_info: dict, city_info: dict):
        self._api = api
        self._api_info = api_info
        self._city_info = city_info

    def _retrieve_data(self, url: str, parameters: dict) -> dict:
        """Generate url and request data

        Args:
            url (str)
            parameters (dict)

        Returns:
            dict: RESTAPI response converted to dict
        """
        full_url = self._api.build_full_url(url, parameters)
        return json.loads(self._api.request(full_url))

    def get_forecasted_temperature(self, city: str, days: int, interval: str) -> dict:
        """Returns forecasted temperature

        Args:
            city     (str):
            days     (int): number of future days of forecasted weather data
            interval (str): either hourly or daily

        Returns:
            dict: dictionary containing forecasted temperature with timestamps
        """
        url = self._api_info["forecasted"]["main_url"]
        parameters = {
            "latitude": str(self._city_info[city]["latitude"]),
            "longitude": str(self._city_info[city]["longitude"]),
            interval: "temperature_2m",
            "forecast_days": str(days),
            "temperature_unit": self._api_info["temperature_unit"],
            "timezone": self._city_info[city]["timezone"],
        }
        return self._retrieve_data(url, parameters)

    def get_recent_temperature(self, city: str, past_days: int, interval: str) -> dict:
        """Returns forecasted temperature

        Args:
            city      (str):
            past_days (int): number of past days to obtain weather data
            interval  (str): either hourly or daily

        Returns:
            dict: dictionary containing forecasted temperature with timestamps
        """
        url = self._api_info["forecasted"]["main_url"]
        parameters = {
            "latitude": str(self._city_info[city]["latitude"]),
            "longitude": str(self._city_info[city]["longitude"]),
            interval: "temperature_2m",
            "past_days": str(past_days),
            "forecast_days": "0",
            "temperature_unit": self._api_info["temperature_unit"],
            "timezone": self._city_info[city]["timezone"],
        }
        return self._retrieve_data(url, parameters)

    def get_historical_temperature(
        self, start_date: str, end_date: str, city: str, interval: str
    ) -> dict:
        """Returns historical temperature values

        Args:
            start_date (str):
            end_date   (str):
            city       (str):
            interval   (str): hourly or daily

        Returns:
            dict: dictionary containing historical temperature with timestamps
        """
        url = self._api_info["historical"]["main_url"]
        interval_param = (
            "temperature_2m" if interval == "hourly" else "temperature_2m_max"
        )
        parameters = {
            "latitude": str(self._city_info[city]["latitude"]),
            "longitude": str(self._city_info[city]["longitude"]),
            "start_date": start_date,
            "end_date": end_date,
            "timezone": self._city_info[city]["timezone"],
            interval: interval_param,
            "temperature_unit": self._api_info["temperature_unit"],
        }
        return self._retrieve_data(url, parameters)

    def get_current_temperature(self, city: str) -> dict:
        """Get current temperature for a city

        Args:
            city (str)

        Returns:
            dict: current temperature data
        """
        url = self._api_info["forecasted"]["main_url"]
        parameters = {
            "latitude": str(self._city_info[city]["latitude"]),
            "longitude": str(self._city_info[city]["longitude"]),
            "current": "temperature_2m",
            "temperature_unit": self._api_info["temperature_unit"],
            "timezone": self._city_info[city]["timezone"],
        }
        return self._retrieve_data(url, parameters)


def weatherClientFactory():
    """Simple weatherClient factory

    Returns:
        WeatherClient
    """
    api_info = read_json(filename=pjoin(sbw_root, "data", "api_info.json"))
    city_info = read_json(filename=pjoin(sbw_root, "data", "city_info.json"))
    return WeatherClient(api=WeatherAPI(), api_info=api_info, city_info=city_info)


if __name__ == "__main__":
    wc = weatherClientFactory()
    start_date = build_date(year=2023, month=11, day=20)
    end_date = build_date(year=2023, month=11, day=23)
    data = wc.get_historical_temperature(
        start_date=start_date, end_date=end_date, city="Hawthorne", interval="hourly"
    )
    print(data)
