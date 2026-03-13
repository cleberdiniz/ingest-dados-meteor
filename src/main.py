from __future__ import annotations

from datetime import datetime, timezone

from bq_client import BigQueryClient
from bronze_service import BronzeService
from config_loader import ConfigLoader
from gcs_client import GCSClient
from weather_api_client import WeatherApiClient


def build_forecast_blob_name(location: str, execution_dt: datetime) -> str:
    return (
        f"raw/open_meteo/forecast/"
        f"location={location}/"
        f"year={execution_dt.strftime('%Y')}/"
        f"month={execution_dt.strftime('%m')}/"
        f"day={execution_dt.strftime('%d')}/"
        f"hour={execution_dt.strftime('%H')}/"
        f"forecast_{execution_dt.strftime('%Y%m%dT%H%M%SZ')}.json"
    )


def normalize_payload(result):
    """
    Normaliza o retorno do WeatherApiClient para um payload serializável em JSON.
    Suporta dict, DataFrame e tuple.
    """
    # caso já venha pronto como dict/list
    if isinstance(result, (dict, list)):
        return result

    # caso venha DataFrame
    if hasattr(result, "to_dict"):
        return {"data": result.to_dict(orient="records")}

    # caso venha tuple
    if isinstance(result, tuple):
        first = result[0]

        # primeira posição é DataFrame
        if hasattr(first, "to_dict"):
            return {"data": first.to_dict(orient="records")}

        # primeira posição já é dict/list
        if isinstance(first, (dict, list)):
            return first

        # fallback: transforma a tupla inteira em texto
        return {"data": [str(item) for item in result]}

    # fallback genérico
    return {"data": [str(result)]}


def main() -> None:
    config = ConfigLoader("config/parameters.yaml").load()

    location = config["location"]["name"]
    execution_dt = datetime.now(timezone.utc)
    load_id = execution_dt.strftime("%Y%m%dT%H%M%SZ")

    weather_api_client = WeatherApiClient(config)
    gcs_client = GCSClient(config)
    bq_client = BigQueryClient(config)
    bronze_service = BronzeService(config, bq_client)

    bronze_service.ensure_infrastructure()

    result = weather_api_client.get_weather()
    payload = normalize_payload(result)

    blob_name = build_forecast_blob_name(location, execution_dt)

    gcs_client.upload_json(blob_name, payload)

    bronze_row = bronze_service.build_forecast_raw_row(
        payload=payload,
        source_path=blob_name,
        location=location,
        load_id=load_id,
        ingestion_timestamp=execution_dt,
    )

    bronze_service.save_forecast_raw_row(bronze_row)

    print("Ingestão concluída com sucesso.")


if __name__ == "__main__":
    main()