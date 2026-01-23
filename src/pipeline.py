from __future__ import annotations
import logging
from datetime import datetime
from typing import Any, List, Tuple
import pytz
from src.config import config
from src.services.firestore import FirestoreService
from src.services.sheets import SheetsService
from src.services.transformer import DataTransformer

# region logging, mover despues al config.py xd
logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log: logging.Logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Ejecuta pipeline:
    - Firestore -> docs
    - Transformer -> DataFrames
    - Sheets snapshot + history
    - Dashboard

    :return: None
    :rtype: None
    """
    log.info(">>> Iniciando")

    # Configuración de zona horaria, de ahí mover al config.py
    tz_chile = pytz.timezone(config.timezone)
    now_utc = datetime.now(pytz.utc)
    now_chile = now_utc.astimezone(tz_chile)
    str_chile = now_chile.strftime("%Y-%m-%d %H:%M:%S")
    str_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        # conexion con Firestore
        log.info("Conectando a Firestore...")
        firestore = FirestoreService()

        # obtencion de los documentos
        raw_docs: List[Tuple[str, Any]] = list(firestore.get_documents())
        if not raw_docs:
            log.warning("No hay datos disponibles. Finalizando.")
            return

        # transformacion de los datos
        log.info("Transformando datos...")
        transformer = DataTransformer()
        datasets = transformer.process_data(raw_docs)

        # Conexion con Google Sheets
        log.info("Conectando a Google Sheets...")
        sheets = SheetsService()

        # region Actualizacion del sheet

        # Actualiza la hoja de servidores
        log.info("Procesando Servidores...")
        sheets.update_snapshot(
            tab_config=config.servers_config,
            df=datasets["servers"],
            time_chile=str_chile,
        )

        # agrega el historial de servidores
        sheets.append_history(
            tab_config=config.servers_config,
            df=datasets["servers"],
        )

        # Actualiza la hoja de cámaras
        log.info("Procesando Cámaras...")
        sheets.update_snapshot(
            tab_config=config.cameras_config,
            df=datasets["cameras"],
            time_chile=str_chile,
        )

        # agrega el historial de cámaras
        sheets.append_history(
            tab_config=config.cameras_config,
            df=datasets["cameras"],
        )

        log.info("Actualizando Dashboard ...")
        sheets.setup_dashboard()

        log.info("<<< Finalizado con éxito.")
    except Exception as exc:
        log.error("Error en el pipeline: %s", exc, exc_info=True)
