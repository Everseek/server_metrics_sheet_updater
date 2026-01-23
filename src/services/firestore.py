from __future__ import annotations
from typing import Any, Dict, Iterator, Optional, Tuple
import firebase_admin
from firebase_admin import credentials, firestore
from src.config import config


class FirestoreService:
    """
    Cliente Firestore.

    :return: None
    :rtype: None
    """

    def __init__(self) -> None:
        # Si no hay apps inicializadas, inicializa una nueva, asi se evita
        # duplicacion
        if not firebase_admin._apps:
            cred = credentials.Certificate(config.creds_firebase)
            firebase_admin.initialize_app(cred)
        self.client = firestore.client()

    def get_documents(
        self,
        limit: Optional[int] = None,
    ) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """
        Obtiene documentos de Firestore

        :param limit: Límite opcional.
        :type limit: Optional[int]
        :yield: (doc_id, doc_dict)
        :rtype: Iterator[Tuple[str, Dict[str, Any]]]
        """
        # Referencia a la colección
        ref = self.client.collection(config.collection_name)

        # Obtiene los documentos
        stream = ref.limit(limit).stream() if limit else ref.stream()

        # Itera los documentos
        for doc in stream:
            yield doc.id, doc.to_dict()
