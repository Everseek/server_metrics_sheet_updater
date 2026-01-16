import logging
from datetime import datetime
import pytz
from src.config import config
from src.services.firestore import FirestoreService
from src.services.transformer import DataTransformer
from src.services.sheets import SheetsService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info(">>> Iniciando Pipeline ETL")

    try:
        # TimeStamps
        # Usa zona horaria definida en config
        tz_chile = pytz.timezone(config.timezone)
        now_utc = datetime.now(pytz.utc)
        now_chile = now_utc.astimezone(tz_chile)

        # Formatos string pal header
        str_chile = now_chile.strftime("%Y-%m-%d %H:%M:%S")
        str_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

        # Extracción de los datazos
        logger.info("Conectando a Firestore...")
        firestore = FirestoreService()
        raw_docs = list(firestore.get_documents())
        
        if not raw_docs:
            logger.warning("No hay datos. Finalizando.")
            return

        # Transformación de los datitos
        logger.info("Transformando datos...")
        transformer = DataTransformer()
        datasets = transformer.process_data(raw_docs, str_chile)
        #datasets = transformer.process_data(raw_docs)

        # Carga a google shit
        logger.info("Conectando a Google Sheets...")
        sheets = SheetsService()

        # region SERVIDORES
        logger.info("Procesando Servidores...")
        # Actualiza hoja principal
        sheets.update_snapshot(config.servers_config, datasets["servers"], str_chile, str_utc)
        # Guarda en Historial
        sheets.append_history(config.servers_config, datasets["servers"], str_chile)

        # region CÁMARAS
        logger.info("Procesando Cámaras...")
        # Actualizaa hoja principal
        sheets.update_snapshot(config.cameras_config, datasets["cameras"], str_chile, str_utc)
        # Guarda en Historial
        sheets.append_history(config.cameras_config, datasets["cameras"], str_chile)

        logger.info("<<< Pipeline finalizado con éxito.")

    except Exception as e:
        logger.error(f"Error fatal en el pipeline: {e}", exc_info=True)