import yaml
from pathlib import Path


class ConfigLoader:
    def __init__(self, config_path: str = "config/parameters.yaml"):
        self.config_path = Path(config_path)

    def load(self) -> dict:
        with open(self.config_path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file)