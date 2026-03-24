class GoldService:
    def __init__(self, config: dict, bq_client):
        self.config = config
        self.bq_client = bq_client
        self.project_id = config["gcp"]["project_id"]

        self.dataset_gold = "gold_meteor"
        self.dataset_silver = "silver_meteor"
        self.table_silver = "forecast_hourly"

    def run(self) -> None:
        print("[INFO] Criando dataset Gold...")

        create_dataset_query = f"""
        CREATE SCHEMA IF NOT EXISTS `{self.project_id}.{self.dataset_gold}`
        """
        self.bq_client.client.query(create_dataset_query).result()

        print("[INFO] Criando tabela Gold...")

        create_table_query = f"""
        CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_gold}.daily_summary` AS
        SELECT
          location,
          DATE(time) AS date,
          AVG(temperature) AS avg_temperature,
          MAX(temperature) AS max_temperature,
          MIN(temperature) AS min_temperature,
          AVG(humidity) AS avg_humidity,
          SUM(precipitation) AS total_rain
        FROM `{self.project_id}.{self.dataset_silver}.{self.table_silver}`
        GROUP BY location, date
        ORDER BY date
        """

        job = self.bq_client.client.query(create_table_query)
        job.result()

        print(f"[INFO] Gold atualizado com sucesso. Job ID: {job.job_id}")