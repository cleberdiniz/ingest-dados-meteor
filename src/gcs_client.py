import json
from io import BytesIO
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account


class GCSClient:
    def __init__(self, config: dict):
        self.config = config
        self.project_id = config["gcp"]["project_id"]
        self.bucket_name = config["gcp"]["bucket_name"]

        credentials_cfg = config["gcp"]["credentials"]
        credentials_method = credentials_cfg["method"]

        if credentials_method == "service_account_file":
            json_path = credentials_cfg["service_account_json_path"]

            credentials = service_account.Credentials.from_service_account_file(
                json_path
            )

        else:
            raise ValueError(f"Método de credencial não suportado: {credentials_method}")

        self.client = storage.Client(
            project=self.project_id,
            credentials=credentials
        )
        self.bucket = self.client.bucket(self.bucket_name)

    def upload_json(self, destination_blob_name: str, payload: dict) -> None:
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(
            data=json.dumps(payload, ensure_ascii=False, indent=2),
            content_type="application/json"
        )

    def upload_parquet(self, destination_blob_name: str, dataframe) -> None:
        buffer = BytesIO()
        dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)

        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_file(
            buffer,
            content_type="application/octet-stream"
        )