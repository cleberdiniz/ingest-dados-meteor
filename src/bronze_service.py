from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from schema_registry import SchemaRegistry


class BronzeService:
    def __init__(self, config: dict, bq_client):
        self.config = config
        self.bq_client = bq_client

        self.bucket_name = config["gcp"]["bucket_name"]
        self.dataset_bronze = config["bigquery"]["dataset_bronze"]
        self.table_forecast_raw = config["bigquery"]["table_forecast_raw"]

    def ensure_infrastructure(self) -> None:
        self.bq_client.ensure_dataset(
            dataset_id=self.dataset_bronze,
            location="southamerica-east1",
        )

        self.bq_client.ensure_table(
            dataset_id=self.dataset_bronze,
            table_id=self.table_forecast_raw,
            schema=SchemaRegistry.open_meteo_forecast_raw_schema(),
            partition_field="ingestion_date",
            cluster_fields=["location"],
        )

    def build_forecast_raw_row(
        self,
        payload: dict[str, Any],
        source_path: str,
        location: str,
        load_id: str,
        ingestion_timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        ingestion_timestamp = ingestion_timestamp or datetime.now(timezone.utc)
        source_file_name = PurePosixPath(source_path).name

        payload_str = json.dumps(payload, ensure_ascii=False, sort_keys=True)

        row_hash = hashlib.md5(
            f"{location}|{source_path}|{payload_str}".encode("utf-8")
        ).hexdigest()

        return {
            "location": location,
            "latitude": self._safe_float(payload.get("latitude")),
            "longitude": self._safe_float(payload.get("longitude")),
            "generationtime_ms": self._safe_float(payload.get("generationtime_ms")),
            "utc_offset_seconds": self._safe_int(payload.get("utc_offset_seconds")),
            "timezone": payload.get("timezone"),
            "timezone_abbreviation": payload.get("timezone_abbreviation"),
            "elevation": self._safe_float(payload.get("elevation")),
            "source_file_name": source_file_name,
            "source_path": source_path,
            "bucket_name": self.bucket_name,
            "load_id": load_id,
            "row_hash": row_hash,
            "ingestion_timestamp": ingestion_timestamp.isoformat(),
            "ingestion_date": ingestion_timestamp.date().isoformat(),
            "payload_json": payload_str,
        }

    def save_forecast_raw_row(self, row: dict[str, Any]) -> None:
        self.bq_client.insert_rows(
            dataset_id=self.dataset_bronze,
            table_id=self.table_forecast_raw,
            rows=[row],
        )

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None