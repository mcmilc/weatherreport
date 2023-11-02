import json
from southbayweather.utilities.helpers import build_date
from southbayweather.weatherAPI.weatherAPI import WeatherAPI


def main():
    main_url = "https://archive-api.open-meteo.com/v1/archive"
    start_date = build_date(year=2023, month=10, day=2)
    end_date = build_date(year=2023, month=10, day=21)
    timezone = "America%2FLos_Angeles"
    weather_parameter = "temperature_2m_mean"
    parameters = {
        "latitude": "33.9164",
        "longitude": "-118.35275",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": timezone,
        "daily": weather_parameter,
    }
    wAPI = WeatherAPI()
    wAPI.request(wAPI.build_full_url(main_url, parameters))
    out = weather_api_client.retrieve_response()
    result = json.loads(out)
    print(result)


if __name__ == "__main__":
    main()
