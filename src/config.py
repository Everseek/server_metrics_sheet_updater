import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    def __init__(self):
        """
        Carga configuración desde config.yaml y variables de entorno.
        """
        yaml_path = Path("config.yaml")
        if not yaml_path.exists():
            raise FileNotFoundError("Falta el archivo config.yaml en la raíz")

        with open(yaml_path, "r", encoding="utf-8") as f:
            self._yaml = yaml.safe_load(f)

        self.creds_firebase = os.getenv("FIREBASE_CREDENTIALS_PATH")
        self.creds_sheets = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
        self.collection_name = os.getenv("FIRESTORE_COLLECTION_NAME")
        self.sheet_id = os.getenv("GOOGLE_SHEET_ID")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    @property
    def servers_config(self): return self._yaml["sheets"]["servers"]

    @property
    def cameras_config(self): return self._yaml["sheets"]["cameras"]

    @property
    def timezone(self): return self._yaml["settings"].get("timezone", "America/Santiago")

config = Config()