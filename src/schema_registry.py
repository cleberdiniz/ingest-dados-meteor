from google.cloud import bigquery


class SchemaRegistry:
    @staticmethod
    def open_meteo_forecast_raw_schema() -> list[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("generationtime_ms", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("utc_offset_seconds", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("timezone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timezone_abbreviation", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("elevation", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("source_file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_path", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("bucket_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("load_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("row_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ingestion_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("payload_json", "STRING", mode="REQUIRED"),
        ]