from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Tuple

from src.config import load_settings
from src.firestore_client import FirestoreClient
from src.logging_utils import configure_logging
from src.sheets_client import GoogleSheetsClient
from src.transformer import FirestoreToFramesTransformer


def _collect_documents(
    client: FirestoreClient,
    collection_name: str,
    limit: int | None,
    logger: logging.Logger,
    log_every: int,
) -> List[Tuple[str, Mapping[str, Any]]]:
    """
    Descarga documentos Firestore a memoria (doc_id, doc_dict).

    :param client: Cliente Firestore conectado.
    :type client: FirestoreClient
    :param collection_name: Colección.
    :type collection_name: str
    :param limit: Límite docs.
    :type limit: int | None
    :param logger: Logger.
    :type logger: logging.Logger
    :param log_every: Cada cuántos loguear.
    :type log_every: int
    :return: Lista de documentos.
    :rtype: List[Tuple[str, Mapping[str, Any]]]
    """
    documents: List[Tuple[str, Mapping[str, Any]]] = []
    count = 0

    for doc in client.iter_documents(collection_name, limit):
        count += 1
        doc_dict = doc.to_dict()
        if not doc_dict:
            continue

        documents.append((doc.id, doc_dict))

        if log_every > 0 and count % log_every == 0:
            logger.info("Leídos %s documentos...", count)

    logger.info("Total documentos leídos: %s", count)
    logger.info("Docs válidos: %s", len(documents))
    return documents


def run() -> None:
    """
    Orquestador, logica principal.

    :return: None
    :rtype: None
    """
    try:
        settings = load_settings()
        logger = configure_logging(settings.log_level)

        firestore_client = (
            FirestoreClient(settings.firebase_credentials_path)
        )
        firestore_client.connect()

        logger.info(
            "Conectado a Firestore, descargando documentos..."
        )

        documents = _collect_documents(
            client=firestore_client,
            collection_name=settings.firestore_collection_name,
            limit=settings.limit,
            logger=logger,
            log_every=settings.log_every,
        )

        transformer = FirestoreToFramesTransformer(settings.chile_tz)
        frames = transformer.transform(documents)

        sheets_client = GoogleSheetsClient(
            credentials_path=settings.sheets_credentials_path,
            sheet_id=settings.google_sheet_id,
        )

        if frames.servers.empty:
            logger.warning(
                "df_servers vacío: no se actualiza pestaña servers."
            )
        else:
            logger.info(
                "Actualizando pestaña %s (servers)...",
                settings.servers_sheet_name,
            )
            sheets_client.replace_dataframe(
                worksheet_name=settings.servers_sheet_name,
                df=frames.servers,
            )
            logger.info(
                "OK servers: rows=%s cols=%s",
                len(frames.servers),
                len(frames.servers.columns),
            )

        if frames.cameras.empty:
            logger.warning(
                "df_cameras vacío: no se actualiza pestaña cameras."
            )
        else:
            logger.info(
                "Actualizando pestaña %s (cameras)...",
                settings.cameras_sheet_name,
            )
            sheets_client.replace_dataframe(
                worksheet_name=settings.cameras_sheet_name,
                df=frames.cameras,
            )
            logger.info(
                "OK cameras: rows=%s cols=%s",
                len(frames.cameras),
                len(frames.cameras.columns),
            )

        logger.info("Sync completada OK.")
    except (FileNotFoundError, ValueError) as exc:
        logger = configure_logging(logging.ERROR)
        logger.error("%s", exc)
    except Exception:
        logger = configure_logging(logging.ERROR)
        logger.exception("Error inesperado.")
