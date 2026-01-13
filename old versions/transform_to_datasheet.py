"""
Módulo para exportar datos de monitoreo de servidores desde Firestore a Excel.

Este script conecta a Firestore, lee documentos de una colección y exporta
los datos a Excel, aplanando estructuras anidadas y normalizando timestamps.

Requisitos:
- python-dotenv
- firebase-admin
- pandas
- openpyxl

Variables de entorno (.env):
- FIREBASE_CREDENTIALS_PATH=/ruta/al/serviceAccountKey.json
- FIRESTORE_COLLECTION_NAME=raptor_server_monitoring
- OUTPUT_FILE=reporte_servidores_raptor.xlsx
- LOG_LEVEL=INFO
- LIMIT=            (vacío = sin límite)
- LOG_EVERY=250
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import firebase_admin
import pandas as pd
from dotenv import load_dotenv
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import Client
from google.cloud.firestore_v1.base_document import DocumentSnapshot


@dataclass(frozen=True)
class Settings:
    """
    Configuración del exportador cargada desde variables de entorno.

    :param credentials_path: Ruta al JSON de credenciales.
    :type credentials_path: Path
    :param collection_name: Nombre de colección en Firestore.
    :type collection_name: str
    :param output_file: Ruta del archivo Excel a generar.
    :type output_file: Path
    :param log_level: Nivel de logging.
    :type log_level: int
    :param limit: Límite de documentos a leer.
    :type limit: Optional[int]
    :param log_every: Frecuencia de logs de progreso.
    :type log_every: int
    """
    credentials_path: Path
    collection_name: str
    output_file: Path
    log_level: int
    limit: Optional[int]
    log_every: int


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Configura logging para mostrar salida en consola.

    :param level: Nivel de logging (ej. logging.INFO).
    :type level: int
    :return: Logger configurado para el módulo.
    :rtype: logging.Logger
    """
    logger = logging.getLogger("firestore_export")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger


def _require_env(name: str) -> str:
    """
    Obtiene una variable de entorno requerida y valida que no esté vacía.

    :param name: Nombre de la variable.
    :type name: str
    :return: Valor no vacío.
    :rtype: str
    :raises ValueError: Si la variable no existe o está vacía.
    """
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Falta variable de entorno requerida: {name}")
    return value.strip()


def _optional_int(name: str) -> Optional[int]:
    """
    Obtiene una variable de entorno opcional y la convierte a int.

    :param name: Nombre de la variable.
    :type name: str
    :return: int si existe y es válida; si no, None.
    :rtype: Optional[int]
    :raises ValueError: Si existe pero no es un entero válido.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None

    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(
            f"Variable {name} debe ser int. Valor recibido: {raw!r}"
        ) from exc


def _log_level_from_env(value: str) -> int:
    """
    Convierte un texto de nivel de logging a constante logging.

    :param value: Nivel en texto (DEBUG/INFO/WARNING/ERROR/CRITICAL).
    :type value: str
    :return: Nivel numérico de logging.
    :rtype: int
    :raises ValueError: Si el nivel no es válido.
    """
    normalized = value.strip().upper()
    mapping: Dict[str, int] = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if normalized not in mapping:
        raise ValueError(
            "LOG_LEVEL inválido. Usa: DEBUG, INFO, WARNING, ERROR, CRITICAL"
        )

    return mapping[normalized]


def _resolve_path(value: str, base_dir: Path) -> Path:
    """
    Resuelve un path. Si es relativo, lo hace relativo a base_dir.

    :param value: Path como texto (puede ser relativo o absoluto).
    :type value: str
    :param base_dir: Directorio base para resolver relativos.
    :type base_dir: Path
    :return: Path resuelto.
    :rtype: Path
    """
    raw = Path(value)
    if raw.is_absolute():
        return raw
    return (base_dir / raw).resolve()



def load_settings(env_path: Optional[Path] = None) -> Settings:
    """
    Carga variables desde .env y retorna Settings validadas.

    Si ``env_path`` es ``None``, python-dotenv buscará un .env en el cwd.

    Importante:
    - Las rutas (credenciales y output) se resuelven de forma estable.
    - Si ``env_path`` se entrega, los paths relativos se resuelven relativo
      a la carpeta donde está ese .env.
    - Si no se entrega, se resuelven relativo a la carpeta del script.

    :param env_path: Ruta al archivo .env.
    :type env_path: Optional[Path]
    :return: Configuración validada.
    :rtype: Settings
    :raises ValueError: Si faltan variables requeridas o son inválidas.
    """
    dotenv_path: Optional[Path] = env_path.resolve() if env_path else None

    load_dotenv(dotenv_path=str(dotenv_path) if dotenv_path else None)

    if dotenv_path is not None:
        base_dir = dotenv_path.parent
    else:
        base_dir = Path(__file__).resolve().parent

    credentials_path = _resolve_path(
        _require_env("FIREBASE_CREDENTIALS_PATH"),
        base_dir=base_dir,
    )
    collection_name = _require_env("FIRESTORE_COLLECTION_NAME")
    output_file = _resolve_path(
        _require_env("OUTPUT_FILE"),
        base_dir=base_dir,
    )

    log_level_raw = os.getenv("LOG_LEVEL", "INFO")
    log_level = _log_level_from_env(log_level_raw)

    limit = _optional_int("LIMIT")

    log_every_raw = os.getenv("LOG_EVERY", "250").strip()
    try:
        log_every = int(log_every_raw)
    except ValueError as exc:
        raise ValueError(
            f"LOG_EVERY debe ser int. Valor recibido: {log_every_raw!r}"
        ) from exc

    return Settings(
        credentials_path=credentials_path,
        collection_name=collection_name,
        output_file=output_file,
        log_level=log_level,
        limit=limit,
        log_every=log_every,
    )



def initialize_firestore(cred_path: Path) -> Client:
    """
    Inicializa Firebase Admin (si no está inicializado) y retorna cliente
    Firestore.

    Esta función NO realiza escrituras: solo prepara el cliente para lecturas.

    :param cred_path: Ruta al JSON de credenciales de servicio.
    :type cred_path: Path
    :return: Cliente de Firestore.
    :rtype: Client
    :raises FileNotFoundError: Si el archivo de credenciales no existe.
    """
    if not cred_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {cred_path}")

    if not firebase_admin._apps:
        cred = credentials.Certificate(str(cred_path))
        firebase_admin.initialize_app(cred)

    return firestore.client()


def _flatten_mapping(
    data: Mapping[str, Any],
    parent_key: str = "",
    sep: str = ".",
) -> Dict[str, Any]:
    """
    Aplana un mapping recursivamente usando claves compuestas.

    Ejemplo:
    {"cpu": {"usage": 10}} -> {"cpu.usage": 10}

    Listas/tuplas se serializan a JSON string para que Excel las acepte.

    :param data: Mapping a aplanar.
    :type data: Mapping[str, Any]
    :param parent_key: Prefijo de clave.
    :type parent_key: str
    :param sep: Separador entre niveles de clave.
    :type sep: str
    :return: Diccionario aplanado.
    :rtype: Dict[str, Any]
    """
    flat: Dict[str, Any] = {}

    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, Mapping):
            flat.update(_flatten_mapping(value, new_key, sep))
            continue

        if isinstance(value, (list, tuple)):
            flat[new_key] = json.dumps(value, ensure_ascii=False)
            continue

        flat[new_key] = value

    return flat


def _iter_documents(
    db_client: Client,
    collection_name: str,
    limit: Optional[int] = None,
) -> Iterable[DocumentSnapshot]:
    """
    Itera documentos de una colección (solo lectura).

    :param db_client: Cliente Firestore.
    :type db_client: Client
    :param collection_name: Nombre de colección.
    :type collection_name: str
    :param limit: Máximo de documentos a traer (None = sin límite).
    :type limit: Optional[int]
    :return: Iterador de DocumentSnapshot.
    :rtype: Iterable[DocumentSnapshot]
    """
    col_ref = db_client.collection(collection_name)

    if limit is None:
        return col_ref.stream()

    return col_ref.limit(limit).stream()


def fetch_server_data(
    db_client: Client,
    collection_name: str,
    logger: logging.Logger,
    limit: Optional[int] = None,
    log_every: int = 250,
) -> List[Dict[str, Any]]:
    """
    Recupera documentos de Firestore y los transforma en filas planas.

    - Server Name = doc.id
    - Timestamp Query = campo raíz 'timestamp_query'
    - server_stats = aplanado recursivo (incluye anidados)

    :param db_client: Cliente Firestore.
    :type db_client: Client
    :param collection_name: Nombre de la colección.
    :type collection_name: str
    :param logger: Logger para salida en consola.
    :type logger: logging.Logger
    :param limit: Límite de documentos a recuperar.
    :type limit: Optional[int]
    :param log_every: Cada cuántos docs loguear progreso.
    :type log_every: int
    :return: Lista de filas listas para DataFrame.
    :rtype: List[Dict[str, Any]]
    """
    docs = _iter_documents(db_client, collection_name, limit)

    rows: List[Dict[str, Any]] = []
    count = 0

    for doc in docs:
        count += 1
        doc_dict = doc.to_dict()

        if not doc_dict:
            continue

        row: Dict[str, Any] = {
            "Server Name": doc.id,
            "Timestamp Query": doc_dict.get("timestamp_query"),
        }

        server_stats_raw = doc_dict.get("server_stats", {})
        if isinstance(server_stats_raw, Mapping):
            row.update(_flatten_mapping(server_stats_raw))
        else:
            row["server_stats"] = server_stats_raw

        rows.append(row)

        if log_every > 0 and count % log_every == 0:
            logger.info("Leídos %s documentos...", count)

    logger.info("Total documentos leídos: %s", count)
    logger.info("Total filas válidas: %s", len(rows))
    return rows


def _normalize_datetime_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
) -> pd.DataFrame:
    """
    Normaliza columnas de tiempo para compatibilidad con Excel.

    - Convierte a datetime (si puede) con coerción en errores
    - Interpreta/normaliza como UTC cuando aplica
    - Quita tz para Excel

    :param df: DataFrame de entrada.
    :type df: pd.DataFrame
    :param columns: Columnas candidatas a datetime.
    :type columns: Sequence[str]
    :return: DataFrame con columnas normalizadas.
    :rtype: pd.DataFrame
    """
    for col in columns:
        if col not in df.columns:
            continue

        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        df[col] = parsed.dt.tz_convert(None)

    return df


def export_to_excel(
    data: Sequence[Mapping[str, Any]],
    filename: Path,
    logger: logging.Logger,
) -> None:
    """
    Exporta datos a Excel.

    :param data: Filas a exportar.
    :type data: Sequence[Mapping[str, Any]]
    :param filename: Ruta del archivo de salida.
    :type filename: Path
    :param logger: Logger para salida en consola.
    :type logger: logging.Logger
    :return: None
    :rtype: None
    """
    if not data:
        logger.warning("No se encontraron datos para exportar.")
        return

    df = pd.DataFrame(list(data))

    df = _normalize_datetime_columns(
        df,
        columns=("Timestamp Query", "timestamp_boot"),
    )

    fixed_cols = ["Server Name", "Timestamp Query"]
    other_cols = [c for c in df.columns if c not in fixed_cols]
    ordered_cols = [c for c in fixed_cols if c in df.columns] + other_cols
    df = df[ordered_cols]

    if filename.suffix.lower() not in {".xlsx", ".xls"}:
        logger.warning(
            "Extensión no parece Excel (%s). Se recomienda .xlsx",
            filename.suffix,
        )

    filename.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_excel(filename, index=False, engine="openpyxl")
        logger.info("Archivo guardado: %s", filename)
        logger.info("Registros exportados: %s", len(df))
        logger.info("Columnas exportadas: %s", len(df.columns))
    except Exception:
        logger.exception("Error al guardar el Excel.")


def main() -> None:
    """
    Punto de entrada del script.
    """
    try:
        settings = load_settings()
        logger = configure_logging(settings.log_level)

        db_client = initialize_firestore(settings.credentials_path)
        logger.info("Conectado a Firestore. Descarga (solo lectura) iniciada...")

        server_data = fetch_server_data(
            db_client=db_client,
            collection_name=settings.collection_name,
            logger=logger,
            limit=settings.limit,
            log_every=settings.log_every,
        )

        export_to_excel(
            data=server_data,
            filename=settings.output_file,
            logger=logger,
        )

    except (FileNotFoundError, ValueError) as exc:
        logger = configure_logging(logging.ERROR)
        logger.error("%s", exc)
    except Exception:
        logger = configure_logging(logging.ERROR)
        logger.exception("Ocurrió un error inesperado.")


if __name__ == "__main__":
    main()
