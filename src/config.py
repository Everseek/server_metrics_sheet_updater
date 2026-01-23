from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict, Mapping
import yaml
from dotenv import load_dotenv

load_dotenv()

class Config:
    """
    Carga config.yaml y variables de entorno.

    :raises FileNotFoundError: Si no existe config.yaml.
    :raises ValueError: Si falta alguna variable de entorno requerida.
    """

    # region obtencion de configuración
    def __init__(self) -> None:
        # Carga config.yaml
        yaml_path: Path = Path("config.yaml")
        if not yaml_path.exists():
            raise FileNotFoundError("Falta el archivo config.yaml en la raíz")
        with yaml_path.open("r", encoding="utf-8") as handle:
            self._yaml: Dict[str, Any] = yaml.safe_load(handle)

        # Carga las credenciales
        self.creds_firebase: str = self._require_env("FIREBASE_CREDENTIALS_PATH")
        self.creds_sheets: str = self._require_env(
            "GOOGLE_SHEETS_CREDENTIALS_PATH"
        )

        # Nombre de la colección en Firestore
        self.collection_name: str = self._require_env("FIRESTORE_COLLECTION_NAME")

        # ID de la hoja de cálculo de Google Sheets
        self.sheet_id: str = self._require_env("GOOGLE_SHEET_ID")

        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")


    def _require_env(self, key: str) -> str:
        """
        Obtiene una variable de entorno obligatoria,
        si no existe hace un raise de ValueError.

        :param key: Nombre de la variable.
        :type key: str
        :return: Valor.
        :rtype: str
        :raises ValueError: Si falta la variable o está vacía.
        """
        value: str = os.getenv(key, "").strip()
        if not value:
            raise ValueError(
                f"Falta variable de entorno requerida: {key}"
            )
        return value


    # region Propierties
    @property
    def servers_config(self) -> Mapping[str, Any]:
        """
        Config YAML de servers.

        :return: mapping.
        :rtype: Mapping[str, Any]
        """
        return self._yaml["sheets"]["servers"]


    @property
    def cameras_config(self) -> Mapping[str, Any]:
        """
        Config YAML de cameras.

        :return: mapping.
        :rtype: Mapping[str, Any]
        """
        return self._yaml["sheets"]["cameras"]


    @property
    def timezone(self) -> str:
        """
        Timezone configurada.

        :return: timezone.
        :rtype: str
        """
        settings: Mapping[str, Any] = self._yaml.get("settings", {})
        return str(settings.get("timezone", "America/Santiago"))


# Instancia de configuración global
config: Config = Config()