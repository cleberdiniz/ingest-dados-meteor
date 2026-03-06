from config_loader import ConfigLoader
from ingestion_service import IngestionService


def main():
    config = ConfigLoader().load()
    service = IngestionService(config)
    service.run()


if __name__ == "__main__":
    main()