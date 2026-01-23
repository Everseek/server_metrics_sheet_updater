from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

import pandas as pd

from src.config import config


class DataTransformer:
    """
    Transforma docs crudos Firestore a DataFrames.

    Produce:
    - servers: snapshot de servidores.
    - cameras: snapshot de cámaras.
    """

    def __init__(self) -> None:
        self.tz: str = config.timezone

    def process_data(
        self,
        raw_docs: List[Tuple[str, Mapping[str, Any]]],
    ) -> Dict[str, pd.DataFrame]:
        """
        Coordina la transformación.

        :param raw_docs: Docs Firestore.
        :type raw_docs: List[Tuple[str, Mapping[str, Any]]]
        :return: dict con df servers/cameras.
        :rtype: Dict[str, pd.DataFrame]
        """
        servers_rows: List[Dict[str, Any]] = []
        cameras_rows: List[Dict[str, Any]] = []

        for doc_id, data in raw_docs:
            server_row = self._flatten_server(doc_id=doc_id, data=dict(data))
            servers_rows.append(server_row)

            server_query_time = server_row.get("timestamp_query_dt")

            cameras_status = data.get("cameras_status")
            if isinstance(cameras_status, dict):
                for cam_name, cam_data in cameras_status.items():
                    if not isinstance(cam_data, dict):
                        continue

                    cam_row = self._flatten_camera(
                        server_id=doc_id,
                        cam_name=str(cam_name),
                        data=dict(cam_data),
                    )

                    if server_query_time is not None:
                        cam_row["timestamp_query_dt"] = server_query_time

                    cameras_rows.append(cam_row)

        df_servers = pd.DataFrame(servers_rows)
        df_cameras = pd.DataFrame(cameras_rows)

        df_servers = self._rename_and_filter(
            df=df_servers,
            columns_config=config.servers_config["columns"],
        )
        df_cameras = self._rename_and_filter(
            df=df_cameras,
            columns_config=config.cameras_config["columns"],
        )

        return {"servers": df_servers, "cameras": df_cameras}

    def _flatten_server(
        self,
        doc_id: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aplana servidor.

        :param doc_id: id servidor.
        :type doc_id: str
        :param data: dict Firestore.
        :type data: Dict[str, Any]
        :return: fila plana.
        :rtype: Dict[str, Any]
        """
        row: Dict[str, Any] = {"Server Name": doc_id}

        for key, value in data.items():
            if key == "cameras_status":
                continue

            if key == "server_stats" and isinstance(value, dict):
                for sk, sv in value.items():
                    row[f"server_stats_{sk}"] = sv
                continue

            row[key] = value

        return self._fix_timestamps(row=row)

    def _flatten_camera(
        self,
        server_id: str,
        cam_name: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Aplana cámara.

        :param server_id: servidor padre.
        :type server_id: str
        :param cam_name: nombre cámara.
        :type cam_name: str
        :param data: dict cámara.
        :type data: Dict[str, Any]
        :return: fila plana.
        :rtype: Dict[str, Any]
        """
        row: Dict[str, Any] = {"Server Name": server_id, "camera_name": cam_name}
        row.update(data)
        return self._fix_timestamps(row=row)

    def _fix_timestamps(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normaliza timestamps a datetime naive en timezone config.

        :param row: fila original.
        :type row: Dict[str, Any]
        :return: fila con *_dt.
        :rtype: Dict[str, Any]
        """
        new_row: Dict[str, Any] = dict(row)

        for key, value in row.items():
            key_lower = key.lower()
            if "timestamp" not in key_lower and "utc" not in key_lower:
                continue

            try:
                dt = pd.to_datetime(
                    value,
                    unit="s" if isinstance(value, (int, float)) else None,
                    utc=True,
                    errors="coerce",
                )
                if pd.isna(dt):
                    continue

                out_key = key if "_dt" in key else f"{key}_dt"
                converted = dt.tz_convert(self.tz).tz_localize(None)
                new_row[out_key] = converted
            except Exception:
                continue

        return new_row

    def _rename_and_filter(
        self,
        df: pd.DataFrame,
        columns_config: Mapping[str, Any],
    ) -> pd.DataFrame:
        """
        Renombra y filtra columnas según YAML.

        :param df: DataFrame original.
        :type df: pd.DataFrame
        :param columns_config: config YAML.
        :type columns_config: Mapping[str, Any]
        :return: DataFrame final.
        :rtype: pd.DataFrame
        """
        if df.empty:
            return df

        rename_map: Dict[str, str] = {
            str(src): str(spec["name"])
            for src, spec in columns_config.items()
        }
        out = df.rename(columns=rename_map)

        final_cols: List[str] = [
            str(spec["name"])
            for spec in columns_config.values()
            if str(spec["name"]) in out.columns
        ]

        return out[final_cols]
