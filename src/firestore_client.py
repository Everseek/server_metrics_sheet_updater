from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import Client
from google.cloud.firestore_v1.base_document import DocumentSnapshot


class FirestoreClient:
    """
    Cliente Firestore solo-lectura.

    :param credentials_path: Path al JSON service account.
    :type credentials_path: Path
    """

    def __init__(self, credentials_path: Path) -> None:
        self._credentials_path = credentials_path
        self._client: Optional[Client] = None

    def connect(self) -> Client:
        """
        Inicializa Firebase Admin y retorna cliente Firestore.

        :return: Cliente Firestore.
        :rtype: Client
        :raises FileNotFoundError: Si no existe el JSON.
        """
        if not self._credentials_path.exists():
            raise FileNotFoundError(
                f"No existe: {self._credentials_path}"
            )

        if not firebase_admin._apps:
            cred = credentials.Certificate(
                str(self._credentials_path)
            )
            firebase_admin.initialize_app(cred)

        self._client = firestore.client()
        return self._client

    def iter_documents(
        self,
        collection_name: str,
        limit: Optional[int],
    ) -> Iterable[DocumentSnapshot]:
        """
        Itera documentos de una colección (solo lectura).

        :param collection_name: Colección.
        :type collection_name: str
        :param limit: Límite docs (None = sin límite).
        :type limit: Optional[int]
        :return: Iterador de documentos.
        :rtype: Iterable[DocumentSnapshot]
        :raises RuntimeError: Si no se llamó connect().
        """
        if self._client is None:
            raise RuntimeError("FirestoreClient no está conectado.")

        col_ref = self._client.collection(collection_name)
        if limit is None:
            return col_ref.stream()

        return col_ref.limit(limit).stream()
