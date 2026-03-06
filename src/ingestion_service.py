from datetime import datetime, timezone

from weather_api_client import WeatherApiClient
from gcs_client import GCSClient


class IngestionService:
    def __init__(self, config: dict):
        self.config = config
        self.weather_client = WeatherApiClient(config)
        self.gcs_client = GCSClient(config)

    def run(self) -> None:
        location_name = self.config["location"]["name"]

        raw_payload, hourly_dataframe = self.weather_client.get_weather()

        now = datetime.now(timezone.utc)
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        ts = now.strftime("%Y%m%dT%H%M%SZ")

        base_path = (
            f'{self.config["storage"]["raw_path"]}/'
            f'location={location_name}/year={year}/month={month}/day={day}/hour={hour}'
        )

        if self.config["execution"]["save_raw_json"]:
            json_path = f"{base_path}/forecast_{ts}.json"
            self.gcs_client.upload_json(json_path, raw_payload)

        if self.config["execution"]["save_parquet"]:
            parquet_path = f"{base_path}/forecast_{ts}.parquet"
            self.gcs_client.upload_parquet(parquet_path, hourly_dataframe)

        print("Ingestão concluída com sucesso.")