class SilverService:

    def __init__(self, config: dict, bq_client):
        self.config = config
        self.bq_client = bq_client
        self.project_id = config["gcp"]["project_id"]

        self.dataset_silver = "silver_meteor"
        self.dataset_bronze = config["bigquery"]["dataset_bronze"]
        self.table_bronze = config["bigquery"]["table_forecast_raw"]

    def run(self):

        print("[INFO] Criando dataset Silver...")

        create_dataset_query = f"""
        CREATE SCHEMA IF NOT EXISTS `{self.project_id}.{self.dataset_silver}`
        """
        self.bq_client.client.query(create_dataset_query).result()

        print("[INFO] Criando tabela Silver...")

        create_table_query = f"""
        CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_silver}.forecast_hourly` AS

        WITH base AS (
          SELECT
            location,
            ingestion_timestamp,
            payload_json
          FROM `{self.project_id}.{self.dataset_bronze}.{self.table_bronze}`
        )

        SELECT
          location,
          ingestion_timestamp,
          TIMESTAMP(JSON_VALUE(record, '$.date')) AS time,
          CAST(JSON_VALUE(record, '$.temperature_2m') AS FLOAT64) AS temperature,
          CAST(JSON_VALUE(record, '$.relative_humidity_2m') AS FLOAT64) AS humidity,
          CAST(JSON_VALUE(record, '$.precipitation') AS FLOAT64) AS precipitation,
          CAST(JSON_VALUE(record, '$.wind_speed_10m') AS FLOAT64) AS wind_speed

        FROM base,
        UNNEST(JSON_EXTRACT_ARRAY(payload_json, '$.hourly.records')) AS record
        """

        job = self.bq_client.client.query(create_table_query)
        job.result()

        print(f"[INFO] Silver atualizado com sucesso. Job ID: {job.job_id}")