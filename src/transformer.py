from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Tuple

import pandas as pd


@dataclass(frozen=True)
class ExportFrames:
    """
    Resultado de transformación a dataframes.

    :param servers: DataFrame de servidores (1 fila por servidor).
    :type servers: pd.DataFrame
    :param cameras: DataFrame de cámaras (N filas por servidor).
    :type cameras: pd.DataFrame
    """
    servers: pd.DataFrame
    cameras: pd.DataFrame


class FirestoreToFramesTransformer:
    """
    Transforma documentos Firestore a DataFrames (servers y cameras).

    Idea clave:
    - servers: campos raíz (excepto cameras_status) + server_stats expandido.
    - cameras: 1 fila por cámara (Server Name + camera_name + campos cámara).
    - timestamps: todo a hora Chile y naive.
    """

    def __init__(self, chile_tz: str) -> None:
        self._chile_tz = chile_tz

    def _to_chile_dt_naive(self, value: Any) -> Any:
        """
        Convierte timestamps a hora Chile (naive).

        :param value: Valor original.
        :type value: Any
        :return: datetime naive o valor original.
        :rtype: Any
        """
        if isinstance(value, (int, float)):
            parsed = pd.to_datetime(
                value,
                errors="coerce",
                unit="s",
                utc=True
            )
            if pd.isna(parsed):
                return value
            
            local = parsed.tz_convert(self._chile_tz)
            
            return local.tz_localize(None)
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(parsed):
            return value

        local = parsed.tz_convert(self._chile_tz)
        return local.tz_localize(None)

    def _normalize_timestamp_fields(
        self,
        row: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normaliza keys que contienen "timestamp".

        - epoch seconds -> agrega "<key>_dt"
        - datetime/string -> reemplaza en la misma key
        - todo a hora Chile naive

        :param row: Registro.
        :type row: Dict[str, Any]
        :return: Registro normalizado.
        :rtype: Dict[str, Any]
        """
        out: Dict[str, Any] = dict(row)

        for key, value in list(row.items()):
            if "timestamp" not in key.lower():
                continue

            if isinstance(value, (int, float)):
                out[f"{key}_dt"] = self._to_chile_dt_naive(value)
                continue

            out[key] = self._to_chile_dt_naive(value)

        return out

    def _extract_servers_row(
        self,
        server_name: str,
        doc_dict: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """
        1 fila server (hoja servers).

        :param server_name: doc.id.
        :type server_name: str
        :param doc_dict: Documento como dict.
        :type doc_dict: Mapping[str, Any]
        :return: Fila server.
        :rtype: Dict[str, Any]
        """
        row: Dict[str, Any] = {"Server Name": server_name}

        for key, value in doc_dict.items():
            if key == "cameras_status":
                continue

            if key == "server_stats" and isinstance(value, Mapping):
                for sk, sv in value.items():
                    row[f"server_stats_{sk}"] = sv
                continue

            row[key] = value

        if "timestamp_query" in row:
            row["Timestamp Query"] = row.pop("timestamp_query")

        return self._normalize_timestamp_fields(row)

    def _extract_cameras_rows(
        self,
        server_name: str,
        cameras_status: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        N filas cameras (hoja cameras).

        :param server_name: doc.id.
        :type server_name: str
        :param cameras_status: Mapa cámaras.
        :type cameras_status: Mapping[str, Any]
        :return: Filas cámaras.
        :rtype: List[Dict[str, Any]]
        """
        rows: List[Dict[str, Any]] = []

        for camera_name, camera_data in cameras_status.items():
            if not isinstance(camera_data, Mapping):
                continue

            row: Dict[str, Any] = {
                "Server Name": server_name,
                "camera_name": camera_name,
            }

            for key, value in camera_data.items():
                row[key] = value

            rows.append(self._normalize_timestamp_fields(row))

        return rows

    def transform(
        self,
        documents: List[Tuple[str, Mapping[str, Any]]],
    ) -> ExportFrames:
        """
        Convierte docs a ExportFrames.

        :param documents: Lista (doc_id, doc_dict).
        :type documents: List[Tuple[str, Mapping[str, Any]]]
        :return: ExportFrames.
        :rtype: ExportFrames
        """
        servers_rows: List[Dict[str, Any]] = []
        cameras_rows: List[Dict[str, Any]] = []

        for doc_id, doc_dict in documents:
            servers_rows.append(self._extract_servers_row(doc_id, doc_dict))

            cameras_status = doc_dict.get("cameras_status")
            if isinstance(cameras_status, Mapping):
                cameras_rows.extend(self._extract_cameras_rows(
                    doc_id,
                    cameras_status,
                ))

        return ExportFrames(
            servers=pd.DataFrame(servers_rows),
            cameras=pd.DataFrame(cameras_rows),
        )
