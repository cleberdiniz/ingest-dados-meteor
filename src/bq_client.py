from __future__ import annotations

from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryClient:
    def __init__(self, config: dict):
        self.config = config
        self.project_id = config["gcp"]["project_id"]

        credentials_cfg = config["gcp"]["credentials"]
        credentials_method = credentials_cfg["method"]

        if credentials_method == "service_account_file":
            json_path = credentials_cfg["service_account_json_path"]

            credentials = service_account.Credentials.from_service_account_file(
                json_path
            )
        else:
            raise ValueError(f"Método de credencial não suportado: {credentials_method}")

        self.client = bigquery.Client(
            project=self.project_id,
            credentials=credentials,
        )

    def ensure_dataset(
        self,
        dataset_id: str,
        location: str = "southamerica-east1",
    ) -> None:
        dataset_ref = f"{self.project_id}.{dataset_id}"

        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            self.client.create_dataset(dataset)

    def ensure_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: list[bigquery.SchemaField],
        partition_field: str | None = None,
        cluster_fields: list[str] | None = None,
    ) -> None:
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        try:
            self.client.get_table(table_ref)
            return
        except NotFound:
            pass

        table = bigquery.Table(table_ref, schema=schema)

        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )

        if cluster_fields:
            table.clustering_fields = cluster_fields

        self.client.create_table(table)

    def insert_rows(
        self,
        dataset_id: str,
        table_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        print(f"[DEBUG] project_id config: {self.project_id}")
        print(f"[DEBUG] table_ref insert: {table_ref}")

        try:
            table = self.client.get_table(table_ref)
            print(f"[DEBUG] table found before insert: {table.full_table_id}")
        except Exception as e:
            print(f"[DEBUG] get_table failed before insert: {repr(e)}")
            raise

        errors = self.client.insert_rows_json(table_ref, rows)

        if errors:
            raise RuntimeError(
                f"Falha ao inserir linhas em {table_ref}. Errors: {errors}"
            )