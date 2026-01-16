"""
Servicio encargado de la interacción con la API de Google Sheets.

Este módulo maneja la escritura de datos, aplicación de formatos,
estilos visuales y reglas de formato condicional (umbrales).
"""

from typing import Dict, List, Any, Optional
import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1, a1_range_to_grid_range
from src.config import config


class SheetsService:
    """
    Clase para gestionar la conexión y manipulación de hojas de cálculo
    de Google Sheets.
    """

    # Definición de formatos de número para la API de Sheets
    FORMATS: Dict[str, Dict[str, Any]] = {
        # Para valores con decimales (ej: 45.2)
        "NUMBER": {
            "numberFormat": {"type": "NUMBER", "pattern": "0.0"}
        },
        # NUEVO: Para valores enteros sin decimales (ej: 5, 1768585498)
        "INTEGER": {
            "numberFormat": {"type": "NUMBER", "pattern": "0"}
        },
        "PERCENT": {
            "numberFormat": {"type": "PERCENT", "pattern": "0.0%"}
        },
        "DATE_TIME": {
            "numberFormat": {
                "type": "DATE_TIME",
                "pattern": "yyyy-mm-dd hh:mm:ss"
            }
        },
        "TEXT": {
            "numberFormat": {"type": "TEXT"}
        },
        # Formato de Duración para el contador (ej: 0:05:30)
        "DURATION": {
            "numberFormat": {"type": "TIME", "pattern": "[h]:mm:ss"}
        }
    }

    # Definición de estilos visuales (colores, fuentes, bordes)
    STYLES: Dict[str, Dict[str, Any]] = {
        # Azul de Google: Fondo Azul, Letra Blanca, Centrado (Para Títulos A1:A3)
        "HEADER_BLUE": {
            "backgroundColor": {"red": 0.258, "green": 0.52, "blue": 0.956},
            "textFormat": {
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                "bold": True,
                "fontSize": 10
            },
            "horizontalAlignment": "CENTER"
        },
        # Amarillo: Fondo Amarillo, Letra Negra, Centrado Vert/Horiz
        "COLUMN_YELLOW": {
            "backgroundColor": {"red": 0.98, "green": 0.73, "blue": 0.01},
            "textFormat": {
                "foregroundColor": {"red": 0, "green": 0, "blue": 0},
                "bold": True
            },
            "verticalAlignment": "MIDDLE",
            "horizontalAlignment": "CENTER"
        },
        # BASE: Bordes negros sólidos, texto negro
        "TABLE_BASE": {
            "borders": {
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            },
            "textFormat": {
                "foregroundColor": {"red": 0, "green": 0, "blue": 0}
            }
        },
        # METADATA_VALUE: Fondo BLANCO explícito (para corregir el azul residual)
        "METADATA_VALUE": {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},  # Blanco
            "borders": {
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            },
            "textFormat": {
                "foregroundColor": {"red": 0, "green": 0, "blue": 0}
            },
            "horizontalAlignment": "CENTER"
        },
        # ALERTA: Fondo Rojo claro, Letra Rojo oscuro (para thresholds)
        "ALERT_RED": {
            "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8},
            "textFormat": {
                "foregroundColor": {"red": 0.8, "green": 0.0, "blue": 0.0},
                "bold": True
            }
        }
    }

    # NOTA: "!=" se maneja manualmente como CUSTOM_FORMULA
    CONDITION_MAP: Dict[str, str] = {
        ">": "NUMBER_GREATER",
        ">=": "NUMBER_GREATER_THAN_EQ",
        "<": "NUMBER_LESS",
        "<=": "NUMBER_LESS_THAN_EQ",
        "==": "TEXT_EQ",       # Funciona para comparaciones exactas
        "between": "NUMBER_BETWEEN",        # Rango inclusivo
        "not_between": "NUMBER_NOT_BETWEEN"  # Fuera de rango
    }

    def __init__(self) -> None:
        """
        Inicializa el cliente de gspread utilizando las credenciales
        definidas en la configuración.
        """
        gc = gspread.service_account(filename=config.creds_sheets)
        self.sh = gc.open_by_key(config.sheet_id)

    def update_snapshot(
        self,
        tab_config: Dict[str, Any],
        df: pd.DataFrame,
        time_chile: str,
        time_utc: str
    ) -> None:
        """
        Actualiza la hoja principal (snapshot) con los datos actuales.

        Realiza las siguientes acciones:
        Limpieza profunda (Datos, Merges y Reglas Condicionales).
        Sanitiza los datos (quita espacios).
        Escribe cabeceras y datos.
        Aplica formatos.
        """
        if df.empty:
            return

        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        tab_name: str = tab_config["tab_name"]
        ws: gspread.Worksheet = self._get_or_create_worksheet(tab_name)

        self._clean_sheet_metadata(ws)

        ws.update("A1", [["Título"], ["Última actualización (Chile)"], ["Tiempo transcurrido:"]])
        ws.update("B1", [[tab_config.get("title", "Reporte")], [str(time_chile)]])
        ws.update_acell("B3", '=NOW()-B2')

        ws.format("A1:A3", self.STYLES["HEADER_BLUE"])
        ws.format("B1:B3", self.STYLES["METADATA_VALUE"])
        ws.format("B2", self.FORMATS["DATE_TIME"])
        ws.format("B3", self.FORMATS["DURATION"])

        start_row: int = 6
        set_with_dataframe(
            ws,
            df,
            row=start_row,
            resize=True
        )

        self._apply_data_formats(ws, df, tab_config["columns"], start_row)
        self._apply_visual_styles(ws, df, tab_config, start_row)
        self._apply_conditional_rules(ws, df, tab_config["columns"], start_row)

        if "merge_column" in tab_config:
            self._merge_cells(
                ws,
                df,
                tab_config["columns"][tab_config["merge_column"]]["name"],
                start_row
            )

    def append_history(
        self,
        tab_config: Dict[str, Any],
        df: pd.DataFrame,
        time_chile: str
    ) -> None:
        """Agrega los datos actuales al final de la hoja de historial."""
        if df.empty:
            return

        hist_tab_name: Optional[str] = tab_config.get("history_tab")
        if not hist_tab_name:
            return

        df_history = df.copy()
        #df_history.insert(0, "Fecha Extracción", time_chile)

        ws: gspread.Worksheet = self._get_or_create_worksheet(hist_tab_name)

        check_header = ws.get_values("A1")
        needs_header = not check_header or not check_header[0]

        if needs_header:
            set_with_dataframe(
                ws,
                df_history,
                row=1,
                include_column_header=True,
                resize=True
            )
            ws.format("1:1", self.STYLES["HEADER_BLUE"])
        else:
            payload = df_history.astype(str).values.tolist()
            ws.append_rows(payload)

    def _get_or_create_worksheet(self, name: str) -> gspread.Worksheet:
        try:
            return self.sh.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(
                title=name,
                rows=100,
                cols=20
            )

    def _clean_sheet_metadata(self, ws: gspread.Worksheet) -> None:
        """
        Realiza una limpieza total de la hoja:
        1. Borra contenido y formato de celdas.
        2. Deshace todas las celdas fusionadas (Unmerge).
        3. ELIMINA todas las reglas de formato condicional existentes.
        """
        ws.clear()

        try:
            ws.unmerge_cells(f"A1:Z{ws.row_count}")
        except Exception:
            pass
        try:
            meta = self.sh.fetch_sheet_metadata({'includeGridData': False})
            sheet_meta = next(
                (s for s in meta['sheets'] if s['properties']['sheetId'] == ws.id),
                None
            )

            if sheet_meta and 'conditionalFormats' in sheet_meta:
                rules_count = len(sheet_meta['conditionalFormats'])
                requests = []
                for _ in range(rules_count):
                    requests.append({
                        "deleteConditionalFormatRule": {
                            "sheetId": ws.id,
                            "index": 0
                        }
                    })
                
                if requests:
                    self.sh.batch_update({'requests': requests})

        except Exception as e:
            print(f"Advertencia: No se pudieron limpiar reglas antiguas: {e}")

    def _apply_data_formats(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        columns_config: Dict[str, Any],
        start_row_idx: int
    ) -> None:
        batch: List[Dict[str, Any]] = []
        final_name_map = {v["name"]: v for k, v in columns_config.items()}

        for i, col_name in enumerate(df.columns):
            if col_name in final_name_map:
                fmt_type = final_name_map[col_name].get("format", "TEXT")
                rule = self.FORMATS.get(fmt_type)

                if rule:
                    col_letter = rowcol_to_a1(1, i + 1).replace("1", "")
                    data_start = start_row_idx + 1
                    batch.append({
                        "range": (
                            f"{col_letter}{data_start}:"
                            f"{col_letter}{data_start + len(df)}"
                        ),
                        "format": rule
                    })
        if batch:
            ws.batch_format(batch)

    def _apply_visual_styles(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        tab_config: Dict[str, Any],
        start_row_idx: int
    ) -> None:
        batch_styles: List[Dict[str, Any]] = []
        last_col_idx = len(df.columns)
        last_row_idx = start_row_idx + len(df)
        last_col_letter = rowcol_to_a1(1, last_col_idx).replace("1", "")

        # Estilos base
        full_table_range = f"A{start_row_idx}:{last_col_letter}{last_row_idx}"
        batch_styles.append({
            "range": full_table_range,
            "format": self.STYLES["TABLE_BASE"]
        })

        header_range = f"A{start_row_idx}:{last_col_letter}{start_row_idx}"
        batch_styles.append({
            "range": header_range,
            "format": self.STYLES["HEADER_BLUE"]
        })

        if "merge_column" in tab_config:
            target_config = tab_config["columns"][tab_config["merge_column"]]
            target_col_name = target_config["name"]

            if target_col_name in df.columns:
                col_idx = df.columns.get_loc(target_col_name) + 1
                col_let = rowcol_to_a1(1, col_idx).replace("1", "")
                side_range = (
                    f"{col_let}{start_row_idx + 1}:"
                    f"{col_let}{last_row_idx}"
                )
                batch_styles.append({
                    "range": side_range,
                    "format": self.STYLES["COLUMN_YELLOW"]
                })

        if batch_styles:
            ws.batch_format(batch_styles)

    def _apply_conditional_rules(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        columns_config: Dict[str, Any],
        start_row_idx: int
    ) -> None:
        rules: List[Dict[str, Any]] = []
        display_to_config = {v["name"]: v for k, v in columns_config.items()}

        row_start = start_row_idx + 1
        row_end = start_row_idx + len(df)

        for i, col_name in enumerate(df.columns):
            conf = display_to_config.get(col_name)

            if conf and "threshold" in conf:
                threshold: Dict[str, Any] = conf["threshold"]
                operator: str = threshold.get("operator", "")
                
                col_letter = rowcol_to_a1(1, i + 1).replace("1", "")
                range_a1 = f"{col_letter}{row_start}:{col_letter}{row_end}"
                grid_range = a1_range_to_grid_range(range_a1, ws.id)

                condition_type: Optional[str] = None
                condition_values: List[Dict[str, str]] = []

                if operator == "!=":
                    val = threshold.get("value")
                    fmt = conf.get("format", "TEXT")
                    
                    val_str = f'"{val}"' if fmt == "TEXT" else str(val)
                    formula = f'=TRIM({col_letter}{row_start})<>{val_str}'
                    
                    condition_type = "CUSTOM_FORMULA"
                    condition_values = [{"userEnteredValue": formula}]
                else:
                    gs_type = self.CONDITION_MAP.get(operator)
                    if gs_type:
                        condition_type = gs_type
                        if operator in ["between", "not_between"]:
                            val_min = threshold.get("min", 0)
                            val_max = threshold.get("max", 0)
                            condition_values = [
                                {"userEnteredValue": str(val_min)},
                                {"userEnteredValue": str(val_max)}
                            ]
                        else:
                            val_single = threshold.get("value")
                            condition_values = [
                                {"userEnteredValue": str(val_single)}
                            ]

                if condition_type:
                    rule = {
                        "ranges": [grid_range],
                        "booleanRule": {
                            "condition": {
                                "type": condition_type,
                                "values": condition_values
                            },
                            "format": self.STYLES["ALERT_RED"]
                        }
                    }
                    rules.append({
                        "addConditionalFormatRule": {
                            "rule": rule,
                            "index": 0
                        }
                    })

        if rules:
            self.sh.batch_update({"requests": rules})

    def _merge_cells(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        col_name: str,
        start_row_idx: int
    ) -> None:
        if col_name not in df.columns:
            return

        col_idx = df.columns.get_loc(col_name) + 1
        values = df[col_name].tolist()
        data_offset = start_row_idx + 1

        start = 0
        for i in range(1, len(values)):
            if values[i] != values[start]:
                if i - start > 1:
                    start_cell = rowcol_to_a1(start + data_offset, col_idx)
                    end_cell = rowcol_to_a1(i + data_offset - 1, col_idx)
                    ws.merge_cells(f"{start_cell}:{end_cell}")
                start = i

        if len(values) - start > 1:
            start_cell = rowcol_to_a1(start + data_offset, col_idx)
            end_cell = rowcol_to_a1(len(values) + data_offset - 1, col_idx)
            ws.merge_cells(f"{start_cell}:{end_cell}")