"""
Monitoreo de metricas para Fedora 31.
Recolecta las metricas necesesarias y las envía a Firebase.

PSI: Al parecer no se usará, Fernando ya había desarrollado un script
que recolecta las metricas.
"""

import os
import time
import socket
import logging
import logging.handlers
from datetime import datetime
from typing import Dict, Any, Optional

import psutil
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv

# region Path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
ENV_PATH = os.path.join(BASE_DIR, '.env')

# region logging
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# rotacion de logs
log_handler = logging.handlers.TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, 'monitor.log'),
    when='midnight',
    interval=1,
    backupCount=30,
    encoding='utf-8'
)
log_handler.suffix = "%Y-%m-%d"
log_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Manda los logs a la consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# region ENV
load_dotenv(dotenv_path=ENV_PATH)
CRED_PATH = os.getenv('FIREBASE_CRED_PATH')
DB_URL = os.getenv('FIREBASE_DB_URL')
INTERVAL = int(os.getenv('INTERVAL_SECONDS', 10))
DEVICE_ID = os.getenv('HOSTNAME_OVERRIDE')
if not DEVICE_ID:
    raise Exception(
        "No se encuentra HOSTNAME_OVERRIDE en el archivo .env"
    )


#region FUNCS
def initialize_firebase() -> None:
    """
    Inicializa Firebase.
    Lee las credenciales desde el env y tira error si falla.
    """
    if not CRED_PATH or not DB_URL:
        logger.critical("Missing credentials or DB URL in .env file.")
        exit(1)

    abs_cred_path = os.path.join(BASE_DIR, CRED_PATH)

    try:
        cred = credentials.Certificate(abs_cred_path)
        firebase_admin.initialize_app(cred, {
            'databaseURL': DB_URL
        })
        logger.info(f"Firebase inicializado en {DEVICE_ID}")
    except Exception as err:
        logger.critical(f"Fallo la inicializacion de Firebase: {err}")
        exit(1)


def get_system_metrics() -> Dict[str, Any]:
    """
    Recolecta las metricas con PSUtils.

    Metricas:
        - CPU Usage (%)
        - Load Average (1, 5, 15 min)
        - RAM Usage
        - Disk Usage (Root)
        - Disk I/O Counters

    :return: Diccionario con las metricas.
    """
    try:
        cpu_usage = psutil.cpu_percent(interval=1)
        load_avg = psutil.getloadavg()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()

        data = {
            "timestamp": datetime.now().isoformat(),
            "cpu": {
                "usage_percent": cpu_usage,
                "load_avg_1min": load_avg[0],
                "load_avg_5min": load_avg[1],
                "load_avg_15min": load_avg[2]
            },
            "memory": {
                "total_gb": round(memory.total / (1024**3), 2),
                "used_percent": memory.percent,
                "available_gb": round(memory.available / (1024**3), 2)
            },
            "disk": {
                "total_gb": round(disk.total / (1024**3), 2),
                "used_percent": disk.percent,
                "read_count": disk_io.read_count if disk_io else 0,
                "write_count": disk_io.write_count if disk_io else 0
            }
        }
        return data
    except Exception as err:
        logger.error(f"Fallo la recoleccion de metricas: {err}")
        return {}


def upload_metrics_to_firebase(data: Dict[str, Any]) -> None:
    """
    Sube las metricas a firebase, una al historial del server y
    otro al estado actual.

    /servers/{DEVICE_ID}/metrics
    /servers/{DEVICE_ID}/current_state

    :param data: Diciconario con las metricas.
    :return: None
    """
    if not data:
        return

    try:
        # Agrega al historial
        history_ref = db.reference(f'servers/{DEVICE_ID}/metrics')
        history_ref.push(data)

        # Actualiza el estado actual
        current_ref = db.reference(f'servers/{DEVICE_ID}/current_state')
        current_ref.set(data)
    except Exception as err:
        logger.error(f"Fallo la subida a Firebase: {err}")


def main() -> None:
    initialize_firebase()
    logger.info("Iniciando monitoreo...")

    try:
        while True:
            metrics = get_system_metrics()
            upload_metrics_to_firebase(metrics)
            sleep_time = max(0, INTERVAL - 1)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        logger.info("Monitoreo finalizado por el usuario.")
    except Exception as err:
        logger.critical(f"Error en el monitoreo: {err}")
        raise

if __name__ == "__main__":
    main()