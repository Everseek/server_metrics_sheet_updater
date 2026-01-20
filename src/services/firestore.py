import firebase_admin
from firebase_admin import credentials, firestore
from src.config import config

class FirestoreService:
    def __init__(self):
        if not firebase_admin._apps:
            cred = credentials.Certificate(config.creds_firebase)
            firebase_admin.initialize_app(cred)
        self.client = firestore.client()

    def get_documents(self, limit=None):
        """
        Generador que entrega (id, data_dict).

        :param limit: Número máximo de documentos a obtener.
        :yield: Tuplas de (id del documento, diccionario de datos).
        """
        ref = self.client.collection(config.collection_name)
        stream = ref.limit(limit).stream() if limit else ref.stream()
        
        for doc in stream:
            yield doc.id, doc.to_dict()