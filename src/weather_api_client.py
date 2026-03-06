import openmeteo_requests
import pandas as pd
import requests_cache

from retry_requests import retry


class WeatherApiClient:
    def __init__(self, config: dict):
        self.config = config

        cache_seconds = config["source"]["api"]["cache_seconds"]
        retries = config["source"]["api"]["retry"]
        self.url = config["source"]["api"]["base_url"]

        cache_session = requests_cache.CachedSession(".cache", expire_after=cache_seconds)
        retry_session = retry(cache_session, retries=retries, backoff_factor=0.2)
        self.client = openmeteo_requests.Client(session=retry_session)

    def get_weather(self):
        latitude = self.config["location"]["latitude"]
        longitude = self.config["location"]["longitude"]
        hourly_vars = self.config["weather_variables"]["hourly"]
        timezone_name = self.config["location"]["timezone"]

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": hourly_vars,
            "timezone": timezone_name,
        }

        responses = self.client.weather_api(self.url, params=params)
        response = responses[0]

        hourly = response.Hourly()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }

        for idx, var_name in enumerate(hourly_vars):
            hourly_data[var_name] = hourly.Variables(idx).ValuesAsNumpy()

        hourly_dataframe = pd.DataFrame(hourly_data)

        raw_dataframe = hourly_dataframe.copy()
        raw_dataframe["date"] = raw_dataframe["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        raw_payload = {
            "request_params": params,
            "metadata": {
                "latitude": response.Latitude(),
                "longitude": response.Longitude(),
                "elevation": response.Elevation(),
                "utc_offset_seconds": response.UtcOffsetSeconds(),
            },
            "hourly": {
                "columns": list(raw_dataframe.columns),
                "records": raw_dataframe.to_dict(orient="records")
            }
        }

        return raw_payload, hourly_dataframe