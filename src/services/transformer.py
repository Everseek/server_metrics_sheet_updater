import pandas as pd
from typing import Dict, List
from src.config import config

class DataTransformer:
    def __init__(self):
        self.tz = config.timezone

    def process_data(
        self,
        raw_docs: List[tuple],
        timestamp_str: str # <--- NUEVO PARAMETRO
    ) -> Dict[str, pd.DataFrame]:
        """
        Coordina la transformación de Servidores y Cámaras.
        """
        servers_rows = []
        cameras_rows = []

        for doc_id, data in raw_docs:
            # Procesa Servidor
            server_row = self._flatten_server(doc_id, data)
            # Inyectamos la fecha de consulta para que sea una columna
            server_row["query_timestamp"] = timestamp_str 
            servers_rows.append(server_row)

            # Procesa Cámaras
            if "cameras_status" in data and isinstance(data["cameras_status"], dict):
                for cam_name, cam_data in data["cameras_status"].items():
                    if isinstance(cam_data, dict):
                        cam_row = self._flatten_camera(
                            doc_id,
                            cam_name,
                            cam_data
                        )
                        # Inyectamos la fecha de consulta a la cámara también
                        cam_row["query_timestamp"] = timestamp_str
                        cameras_rows.append(cam_row)

        # Crea los DataFrames
        df_servers = pd.DataFrame(servers_rows)
        df_cameras = pd.DataFrame(cameras_rows)

        # Aplica Renombrado y ORDENAMIENTO según el YAML
        df_servers = self._rename_and_filter(
            df_servers,
            config.servers_config["columns"]
        )
        df_cameras = self._rename_and_filter(
            df_cameras,
            config.cameras_config["columns"]
        )

        return {"servers": df_servers, "cameras": df_cameras}

    def _flatten_server(
        self,
        doc_id: str,
        data: dict
    ) -> dict:
        """Aplana la estructura anidada de Firestore."""
        row = {"Server Name": doc_id}
        for k, v in data.items():
            if k == "cameras_status": continue
            # Aplana server_stats
            if k == "server_stats" and isinstance(v, dict):
                for sk, sv in v.items():
                    row[f"server_stats_{sk}"] = sv
            else:
                row[k] = v
        return self._fix_timestamps(row)

    def _flatten_camera(
        self,
        server_id: str,
        cam_name: str,
        data: dict
    ) -> dict:
        row = {
            "Server Name": server_id,
            "camera_name": cam_name
        }
        row.update(data)
        return self._fix_timestamps(row)

    def _fix_timestamps(
        self,
        row: dict
    ) -> dict:
        """
        Busca campos timestamp, los convierte a datetime y ajusta la
        zona horaria.
        """
        new_row = row.copy()
        for k, v in row.items():
            if "timestamp" in k.lower() or "utc" in k.lower():
                try:
                    dt = pd.to_datetime(
                        v,
                        unit='s' if isinstance(v, (int, float)) else None,
                        utc=True
                    )
                    if pd.notna(dt):
                        # Guardar con sufijo _dt para diferenciar del raw
                        new_key = f"{k}_dt" if "_dt" not in k else k
                        new_row[new_key] = dt.tz_convert(self.tz).tz_localize(None)
                except:
                    pass
        return new_row

    def _rename_and_filter(
        self,
        df: pd.DataFrame,
        columns_config: dict
    ) -> pd.DataFrame:
        """Renombra columnas usando el YAML y descarta las que no estén configuradas."""
        if df.empty: return df
        
        # Mapeo { Nombre_Viejo: Nombre_Nuevo }
        rename_map = {k: v["name"] for k, v in columns_config.items()}
        
        # Renombrar las columnas que existan
        df = df.rename(columns=rename_map)
        
        # El filtro hace dos cosas:
        # 1. Elimina columnas que no estén en el config (como 'epoch')
        # 2. ORDENA las columnas tal cual aparecen en el YAML
        final_cols = [v["name"] for k, v in columns_config.items() if v["name"] in df.columns]
        
        return df[final_cols]