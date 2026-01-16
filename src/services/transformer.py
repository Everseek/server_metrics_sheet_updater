import pandas as pd
from typing import Dict, List
from src.config import config

class DataTransformer:
    def __init__(self):
        self.tz = config.timezone

    def process_data(
        self,
        raw_docs: List[tuple]
    ) -> Dict[str, pd.DataFrame]:
        """
        Coordina la transformación de Servidores y Cámaras.
        """
        servers_rows = []
        cameras_rows = []

        for doc_id, data in raw_docs:
            # 1. Procesar Servidor
            # _flatten_server capturará 'timestamp_query' automáticamente
            # y _fix_timestamps lo convertirá a 'timestamp_query_dt'
            server_row = self._flatten_server(doc_id, data)
            servers_rows.append(server_row)

            # Capturamos el timestamp del servidor para pasarlo a las cámaras
            # Si no existe, usamos None
            server_query_time = server_row.get("timestamp_query_dt")

            # 2. Procesar Cámaras
            if "cameras_status" in data and isinstance(data["cameras_status"], dict):
                for cam_name, cam_data in data["cameras_status"].items():
                    if isinstance(cam_data, dict):
                        cam_row = self._flatten_camera(
                            doc_id,
                            cam_name,
                            cam_data
                        )
                        # HERENCIA: Asignamos el timestamp del servidor a la cámara
                        if server_query_time is not None:
                            cam_row["timestamp_query_dt"] = server_query_time
                        
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
        
        # Aquí se procesará 'timestamp_query' -> 'timestamp_query_dt'
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
        zona horaria. Detecta strings y números.
        """
        new_row = row.copy()
        for k, v in row.items():
            # Detectamos cualquier key que tenga "timestamp" o "utc"
            if "timestamp" in k.lower() or "utc" in k.lower():
                try:
                    # Convertimos a datetime. 'coerce' maneja errores suavemente.
                    dt = pd.to_datetime(
                        v,
                        unit='s' if isinstance(v, (int, float)) else None,
                        utc=True,
                        errors='coerce' 
                    )
                    
                    if pd.notna(dt):
                        # Guardar con sufijo _dt para diferenciar del raw
                        new_key = f"{k}_dt" if "_dt" not in k else k
                        
                        # Convertir a zona horaria local y quitar info de zona (naive)
                        # para que Excel/Sheets no se confundan
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
        
        rename_map = {k: v["name"] for k, v in columns_config.items()}
        df = df.rename(columns=rename_map)
        
        # Filtra y ordena según el YAML
        final_cols = [v["name"] for k, v in columns_config.items() if v["name"] in df.columns]
        
        return df[final_cols]