from __future__ import annotations

from datetime import datetime, timezone

from src.bq_client import BigQueryClient
from src.bronze_service import BronzeService
from src.config_loader import ConfigLoader
from src.gcs_client import GCSClient
from src.gold_service import GoldService
from src.silver_service import SilverService
from src.weather_api_client import WeatherApiClient


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
    Normaliza o retorno do WeatherApiClient para um payload JSON serializável.

    Casos suportados:
    - dict/list: retorna como está
    - tuple: retorna o primeiro elemento útil
    - dataframe-like: converte para records
    """
    if isinstance(result, (dict, list)):
        return result

    if isinstance(result, tuple):
        first = result[0]

        if isinstance(first, (dict, list)):
            return first

        if hasattr(first, "to_dict"):
            return {"data": first.to_dict(orient="records")}

        return {"data": [str(item) for item in result]}

    if hasattr(result, "to_dict"):
        return {"data": result.to_dict(orient="records")}

    return {"data": [str(result)]}


def main() -> None:
    print("[INFO] Carregando configuração...")
    config = ConfigLoader("config/parameters.yaml").load()

    location = config["location"]["name"]
    execution_dt = datetime.now(timezone.utc)
    load_id = execution_dt.strftime("%Y%m%dT%H%M%SZ")

    print("[INFO] Inicializando clients e services...")
    weather_api_client = WeatherApiClient(config)
    gcs_client = GCSClient(config)
    bq_client = BigQueryClient(config)

    bronze_service = BronzeService(config, bq_client)
    silver_service = SilverService(config, bq_client)
    gold_service = GoldService(config, bq_client)

    print("[INFO] Garantindo infraestrutura Bronze...")
    bronze_service.ensure_infrastructure()

    print("[INFO] Coletando dados da API meteorológica...")
    result = weather_api_client.get_weather()
    payload = normalize_payload(result)

    blob_name = build_forecast_blob_name(location, execution_dt)

    print(f"[INFO] Salvando payload RAW no GCS: {blob_name}")
    gcs_client.upload_json(blob_name, payload)

    print("[INFO] Montando linha Bronze...")
    bronze_row = bronze_service.build_forecast_raw_row(
        payload=payload,
        source_path=blob_name,
        location=location,
        load_id=load_id,
        ingestion_timestamp=execution_dt,
    )

    print("[INFO] Gravando Bronze no BigQuery...")
    bronze_service.save_forecast_raw_row(bronze_row)

    print("[INFO] Executando transformação Silver...")
    silver_service.run()

    print("[INFO] Executando transformação Gold...")
    gold_service.run()

    print("[INFO] Pipeline concluído com sucesso.")


if __name__ == "__main__":
    main()