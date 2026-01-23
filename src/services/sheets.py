from __future__ import annotations
import time
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    cast
)

import gspread
import pandas as pd
from gspread.exceptions import APIError
from gspread.utils import a1_range_to_grid_range, rowcol_to_a1
from src.config import config
from src.services.sheets_dashboard import DashboardBuilder
from src.services.sheets_styles import CONDITION_MAP, FORMATS, STYLES


@dataclass(frozen=True, slots=True)
class _SheetArea:
    """
    Define un área de trabajo grande para limpieza y reset de formato.

    Se usa para asegurar que no queden bordes/colores “fantasma” hacia la
    derecha o hacia abajo.

    :param max_rows: Cantidad máxima de filas a considerar en limpieza.
    :type max_rows: int
    :param max_cols: Cantidad máxima de columnas a considerar en limpieza.
    :type max_cols: int
    """
    max_rows: int
    max_cols: int


class SheetsService:
    """
    Servicio principal para escritura en Google Sheets.
    """
    # Area para limpieza y reset de formato
    _DEFAULT_AREA: _SheetArea = _SheetArea(max_rows=1200, max_cols=40)

    def __init__(self) -> None:
        """
        Crea cliente gspread y carga spreadsheet.

        :return: None
        :rtype: None
        """
        # Obtiene el cliente de Google Sheets.
        gc: gspread.Client = gspread.service_account(
            filename=config.creds_sheets
        )

        # Abre el sheet con el ID
        self.sh: gspread.Spreadsheet = gc.open_by_key(config.sheet_id)

        # Builder del dashboard usando el mismo spreadsheet abierto.
        self.dashboard: DashboardBuilder = DashboardBuilder(self.sh)


    def update_snapshot(
        self,
        tab_config: Mapping[str, Any],
        df: pd.DataFrame,
        time_chile: str,
    ) -> None:
        """
        Escribe la hoja con formato consistente.

        Diseño:
        - A1:B1 título (merge).
        - A2:A3 labels metadata.
        - B2:B3 valores metadata.
        - Fila 6: header del DataFrame.
        - Desde fila 7: datos.

        :param tab_config: Configuración YAML de la pestaña.
        :type tab_config: Mapping[str, Any]
        :param df: DataFrame ya transformado a nombres legibles.
        :type df: pd.DataFrame
        :param time_chile: Timestamp Chile en string.
        :type time_chile: str
        :return: None
        :rtype: None
        """
        if df.empty:
            # Si no hay datos se va a la v
            return

        # Normaliza strings, NaN y datetimes para una escritura estable.
        normalized_df: pd.DataFrame = self._normalize_df_for_sheet(df)

        # Obtiene el nombre de la hoja desde config
        tab_name: str = tab_config["tab_name"]

        # Crea u obtiene el sheet.
        ws: gspread.Worksheet = self._get_or_create_worksheet(
            tab_name,
            rows=self._DEFAULT_AREA.max_rows,
            cols=self._DEFAULT_AREA.max_cols,
        )

        # La tabla parte en fila 6 (header), por diseño.
        start_row: int = 6

        # Vacia la hoja para evitar que se mezcle con cosas viejas
        self._reset_worksheet(ws, area=self._DEFAULT_AREA)

        # Escribe las tabla, titulo y todo en un batchUpdate
        self._write_snapshot_values(
            ws=ws,
            tab_config=tab_config,
            df=normalized_df,
            start_row=start_row,
            time_chile=time_chile,
        )

        # Estructura y aplica estilos en otro batchUpdate
        self._apply_snapshot_structure_and_styles(
            ws=ws,
            tab_config=tab_config,
            df=normalized_df,
            start_row=start_row,
        )


    def append_history(
        self,
        tab_config: Mapping[str, Any],
        df: pd.DataFrame,
    ) -> None:
        """
        Agrega registros al historial.

        - Si el historial está vacío (sin header), se crea con header.
        - Si existe, se append con USER_ENTERED para que Sheets parsee fechas.

        :param tab_config: Config YAML de la pestaña.
        :type tab_config: Mapping[str, Any]
        :param df: DataFrame
        :type df: pd.DataFrame
        :return: None
        :rtype: None
        """
        # a la v si está vacio
        if df.empty:
            return

        # Obtiene el nombre de la hoja desde config
        hist_tab_name: Optional[str] = cast(
            Optional[str],
            tab_config.get("history_tab"),
        )
        if not hist_tab_name:
            return

        # Normaliza el DataFrame igual que snapshot
        df_history: pd.DataFrame = self._normalize_df_for_sheet(df)

        # Obtiene el sheet
        ws: gspread.Worksheet = self._get_or_create_worksheet(
            hist_tab_name,
            rows=self._DEFAULT_AREA.max_rows,
            cols=self._DEFAULT_AREA.max_cols,
        )

        # Chequeamos si hay header en la fila 1
        header_row: List[List[str]] = ws.get_values("A1:Z1")
        has_header: bool = bool(header_row and header_row[0])

        if not has_header:
            # Si no hay header lo crea
            
            # Vacia la hoja
            self._reset_worksheet(ws, area=self._DEFAULT_AREA)

            # Escribe el header y todos los valores
            values: List[List[Any]] = self._df_to_values_with_header(df_history)
            self._values_batch_update(
                ws=ws,
                updates=[{"range": "A1", "values": values}],
            )

            # Aplica estilo de header y formatos por columna
            self._apply_history_styles_and_formats(
                ws=ws,
                tab_config=tab_config,
                df=df_history,
                start_row=1,
            )
            return

        # Si ya existe header, solo append_rows
        payload_rows: List[List[Any]] = (
            self._df_to_values_rows_only(df_history)
        )

        # Append con backoff
        self._execute_with_backoff(
            func=lambda: ws.append_rows(
                payload_rows,
                value_input_option="USER_ENTERED",
            )
        )


    def setup_dashboard(self) -> None:
        """
        Construye o reconstruye el dashboard.

        :return: None
        :rtype: None
        """
        self.dashboard.build()


    def _get_or_create_worksheet(
        self,
        name: str,
        rows: int,
        cols: int,
    ) -> gspread.Worksheet:
        """
        Obtiene worksheet o la crea con tamaño base.

        :param name: Nombre de la pestaña.
        :type name: str
        :param rows: Filas iniciales.
        :type rows: int
        :param cols: Columnas iniciales.
        :type cols: int
        :return: Worksheet.
        :rtype: gspread.Worksheet
        """
        # Obtiene o crea pestaña
        try:
            # intenta obtener la pestaña
            ws: gspread.Worksheet = self.sh.worksheet(name)
        except gspread.WorksheetNotFound:
            # Crea pestaña si no existe
            ws = self.sh.add_worksheet(
                title=name,
                rows=rows,
                cols=cols
            )

        # Asegura tamaño mínimo para evitar cosas fuera de rango
        if ws.row_count < rows:
            self._execute_with_backoff(
                func=lambda: ws.resize(rows=rows)
            )
        if ws.col_count < cols:
            self._execute_with_backoff(
                func=lambda: ws.resize(cols=cols)
            )
        return ws

    def _reset_worksheet(
        self,
        ws: gspread.Worksheet,
        area: _SheetArea,
    ) -> None:
        """
        Limpia valores, merges, formatos, condicionales y validaciones.

        :param ws: Worksheet objetivo.
        :type ws: gspread.Worksheet
        :param area: Área amplia para reset.
        :type area: _SheetArea
        :return: None
        :rtype: None
        """
        # Limpia valores sin tocar formatos.
        self._execute_with_backoff(func=ws.clear)

        # Lee metadata para saber cuántas reglas borrar.
        meta: Dict[str, Any] = self.sh.fetch_sheet_metadata(
            {"includeGridData": False}
        )
        sheet_meta: Optional[Dict[str, Any]] = next(
            (
                s
                for s in meta.get("sheets", [])
                if s.get("properties", {}).get("sheetId") == ws.id
            ),
            None,
        )

        # Cuenta reglas condicionales existentes.
        cond_formats: List[Any] = []
        if sheet_meta is not None:
            cond_formats = sheet_meta.get("conditionalFormats", []) or []

        # Arma requests
        requests: List[Dict[str, Any]] = []

        # Borra las reglas condicionales.
        for _ in range(len(cond_formats)):
            requests.append(
                {
                    "deleteConditionalFormatRule": {
                        "sheetId": ws.id,
                        "index": 0,
                    }
                }
            )

        # Des-merge global (por si hay merges viejos).
        requests.append(
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": area.max_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": area.max_cols,
                    }
                }
            }
        )

        # Limpieza de formatos, por si quedaron
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": area.max_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": area.max_cols,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Limpieza de validaciones, por si quedaron reglas viejas
        requests.append(
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": area.max_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": area.max_cols,
                    },
                    "rule": None,
                }
            }
        )

        # Gridlines ocultas para que se vea “limpio”.
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "gridProperties": {
                            "hideGridlines": True,
                            "frozenRowCount": 0,
                        },
                    },
                    "fields": (
                        "gridProperties.hideGridlines,"
                        "gridProperties.frozenRowCount"
                    ),
                }
            }
        )

        # Aplica requests
        self._batch_update_requests(requests=requests)


    def _write_snapshot_values(
        self,
        ws: gspread.Worksheet,
        tab_config: Mapping[str, Any],
        df: pd.DataFrame,
        start_row: int,
        time_chile: str,
    ) -> None:
        """
        Escribe todos los valores del snapshot en una sola llamada.

        :param ws: Worksheet objetivo.
        :type ws: gspread.Worksheet
        :param tab_config: Config YAML.
        :type tab_config: Mapping[str, Any]
        :param df: DataFrame normalizado.
        :type df: pd.DataFrame
        :param start_row: Fila donde va header de tabla.
        :type start_row: int
        :param time_chile: Timestamp Chile string.
        :type time_chile: str
        :return: None
        :rtype: None
        """
        title: str = tab_config.get("title", "Reporte")

        # Arma valores para título, metadata y tabla completa.
        table_values: List[List[Any]] = self._df_to_values_with_header(df)

        # Metadata, B3 usa fórmula para “tiempo transcurrido”.
        updates: List[Dict[str, Any]] = [
            {"range": "A1", "values": [[title]]},
            {
                "range": "A2",
                "values": [
                    ["Última actualización (Chile)"],
                    ["Tiempo transcurrido:"],
                ],
            },
            {
                "range": "B2",
                "values": [
                    [time_chile],
                    ["=NOW()-B2"],
                ],
            },
            {"range": f"A{start_row}", "values": table_values},
        ]

        # Aplica todos los valores en un solo batchUpdate
        self._values_batch_update(
            ws=ws,
            updates=updates
        )


    def _apply_snapshot_structure_and_styles(
        self,
        ws: gspread.Worksheet,
        tab_config: Mapping[str, Any],
        df: pd.DataFrame,
        start_row: int,
    ) -> None:
        """
        Aplica merges, estilos, formatos numéricos y condicionales.
        Se hace en 1 batchUpdate para evitar cuota.

        :param ws: Worksheet objetivo.
        :type ws: gspread.Worksheet
        :param tab_config: Config YAML.
        :type tab_config: Mapping[str, Any]
        :param df: DataFrame normalizado.
        :type df: pd.DataFrame
        :param start_row: Fila donde está header de tabla.
        :type start_row: int
        :return: None
        :rtype: None
        """
        # Calcula ancho real de la tabla
        col_count: int = int(df.shape[1])
        row_count: int = int(df.shape[0])

        # Última columna en A1 notation (ej: "H").
        last_col_letter: str = rowcol_to_a1(1, col_count).replace("1", "")

        # Header en start_row, datos desde start_row+1.
        data_first_row: int = start_row + 1
        data_last_row: int = start_row + row_count

        # Arma requests
        requests: List[Dict[str, Any]] = []

        # Merge del título A1:B1
        requests.append(
            {
                "mergeCells": {
                    "range": a1_range_to_grid_range("A1:B1", ws.id),
                    "mergeType": "MERGE_ALL",
                }
            }
        )

        # Estilo del título
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range("A1:B1", ws.id),
                    "cell": {"userEnteredFormat": STYLES["HEADER_MAIN"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Estilo metadata labels A2:A3
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range("A2:A3", ws.id),
                    "cell": {"userEnteredFormat": STYLES["METADATA_LABEL"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Estilo metadata values B2:B3
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range("B2:B3", ws.id),
                    "cell": {"userEnteredFormat": STYLES["METADATA_VALUE"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Formato DATE_TIME para B2
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range("B2", ws.id),
                    "cell": {"userEnteredFormat": FORMATS["DATE_TIME"]},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

        # Formato DURATION para B3
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range("B3", ws.id),
                    "cell": {"userEnteredFormat": FORMATS["DURATION"]},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

        # Estilo base de tabla solo hasta última columna real
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range(
                        f"A{start_row}:{last_col_letter}{data_last_row}",
                        ws.id,
                    ),
                    "cell": {"userEnteredFormat": STYLES["TABLE_BASE"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Header de tabla, fila start_row
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range(
                        f"A{start_row}:{last_col_letter}{start_row}",
                        ws.id,
                    ),
                    "cell": {"userEnteredFormat": STYLES["HEADER_BLUE"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Formatos por columna
        requests.extend(
            self._build_column_format_requests(
                ws=ws,
                tab_config=tab_config,
                df=df,
                data_first_row=data_first_row,
                data_last_row=data_last_row,
            )
        )

        # Condicionales con guardia de “Servidor” para no pintar vacíos
        requests.extend(
            self._build_conditional_format_requests(
                ws=ws,
                df=df,
                tab_config=tab_config,
                data_first_row=data_first_row,
            )
        )

        # Aplica requests
        self._batch_update_requests(requests=requests)

    def _apply_history_styles_and_formats(
        self,
        ws: gspread.Worksheet,
        tab_config: Mapping[str, Any],
        df: pd.DataFrame,
        start_row: int,
    ) -> None:
        """
        Aplica estilo y formatos al historial cuando se crea.

        :param ws: Worksheet historial.
        :type ws: gspread.Worksheet
        :param tab_config: Config YAML.
        :type tab_config: Mapping[str, Any]
        :param df: DataFrame escrito en historial.
        :type df: pd.DataFrame
        :param start_row: Fila inicial (1 para historial).
        :type start_row: int
        :return: None
        :rtype: None
        """
        col_count: int = int(df.shape[1])
        last_col_letter: str = rowcol_to_a1(1, col_count).replace("1", "")

        # En historial, header es fila 1 y datos desde fila 2.
        data_first_row: int = start_row + 1

        requests: List[Dict[str, Any]] = []

        # Estilo header
        requests.append(
            {
                "repeatCell": {
                    "range": a1_range_to_grid_range(
                        f"A1:{last_col_letter}1",
                        ws.id,
                    ),
                    "cell": {"userEnteredFormat": STYLES["HEADER_BLUE"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Formatos por columna aplicados a un rango amplio.
        requests.extend(
            self._build_column_format_requests(
                ws=ws,
                tab_config=tab_config,
                df=df,
                data_first_row=data_first_row,
                data_last_row=self._DEFAULT_AREA.max_rows,
            )
        )

        # Aplica requests
        self._batch_update_requests(requests=requests)


    def _build_column_format_requests(
        self,
        ws: gspread.Worksheet,
        tab_config: Mapping[str, Any],
        df: pd.DataFrame,
        data_first_row: int,
        data_last_row: int,
    ) -> List[Dict[str, Any]]:
        """
        Construye requests para numberFormat por columna.

        :param ws: Worksheet objetivo.
        :type ws: gspread.Worksheet
        :param tab_config: Config YAML.
        :type tab_config: Mapping[str, Any]
        :param df: DataFrame con columnas ya renombradas.
        :type df: pd.DataFrame
        :param data_first_row: Primera fila de datos (sin header).
        :type data_first_row: int
        :param data_last_row: Última fila de datos.
        :type data_last_row: int
        :return: Lista de requests repeatCell.
        :rtype: List[Dict[str, Any]]
        """
        # Mapeo por “nombre humano” hacia config.
        cols_conf: Mapping[str, Any] = cast(
            Mapping[str, Any],
            tab_config["columns"],
        )

        # Mapeo por “nombre humano”  hacia config.
        display_map: Dict[str, Mapping[str, Any]] = {
            cast(str, v["name"]): cast(Mapping[str, Any], v)
            for v in cols_conf.values()
        }

        requests: List[Dict[str, Any]] = []

        for idx, col_name in enumerate(df.columns, start=1):
            conf: Optional[Mapping[str, Any]] = display_map.get(col_name)
            if conf is None:
                continue

            fmt_name: str = cast(str, conf.get("format", "TEXT"))
            fmt: Optional[Mapping[str, Any]] = FORMATS.get(fmt_name)
            if fmt is None:
                continue

            col_letter: str = rowcol_to_a1(1, idx).replace("1", "")

            # Rango solo de datos para snapshot
            a1: str = f"{col_letter}{data_first_row}:{col_letter}{data_last_row}"

            requests.append(
                {
                    "repeatCell": {
                        "range": a1_range_to_grid_range(a1, ws.id),
                        "cell": {"userEnteredFormat": cast(Dict[str, Any], fmt)},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            )

        return requests

    def _build_conditional_format_requests(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        tab_config: Mapping[str, Any],
        data_first_row: int,
    ) -> List[Dict[str, Any]]:
        """
        Construye reglas condicionales con guardia anti-filas-vacías.

        - Siempre usamos CUSTOM_FORMULA.
        - AND($A<>"", ...) para que si no hay Servidor no pinte nada.
        - Aplica rango amplio hasta max_rows para que no dependa del largo.

        :param ws: Worksheet objetivo.
        :type ws: gspread.Worksheet
        :param df: DataFrame con columnas humanas.
        :type df: pd.DataFrame
        :param tab_config: Config YAML.
        :type tab_config: Mapping[str, Any]
        :param data_first_row: Primera fila con datos.
        :type data_first_row: int
        :return: Lista de requests addConditionalFormatRule.
        :rtype: List[Dict[str, Any]]
        """
        cols_conf: Mapping[str, Any] =  tab_config["columns"]

        display_map: Dict[str, Mapping[str, Any]] = {v["name"]: v for v in cols_conf.values()}

        # Columna "Servidor" debe existir para guardia.
        server_col_idx: Optional[int] = None
        for idx, col_name in enumerate(df.columns, start=1):
            if col_name.strip().lower() == "servidor":
                server_col_idx = idx
                break

        # Si no hay "Servidor", no aplica reglas, as evita falsos positivos
        if server_col_idx is None:
            return []

        # Aplica reglas
        server_letter: str = rowcol_to_a1(1, server_col_idx).replace("1", "")
        
        requests: List[Dict[str, Any]] = []

        for idx, col_name in enumerate(df.columns, start=1):
            conf: Optional[Mapping[str, Any]] = display_map.get(col_name)
            if conf is None:
                continue

            threshold: Optional[Mapping[str, Any]] = cast(
                Optional[Mapping[str, Any]],
                conf.get("threshold"),
            )
            if threshold is None:
                continue

            op: str = cast(str, threshold.get("operator", "")).strip()
            if not op:
                continue

            col_letter: str = rowcol_to_a1(1, idx).replace("1", "")

            # Rango amplio para que nunca "sobre" formato viejo.
            a1_range: str = (
                f"{col_letter}{data_first_row}:{col_letter}"
                f"{self._DEFAULT_AREA.max_rows}"
            )
            grid_range: Dict[str, Any] = a1_range_to_grid_range(
                a1_range,
                ws.id,
            )

            # Celda "ancla" de la fórmula (Sheets la adapta por fila).
            anchor_cell: str = f"{col_letter}{data_first_row}"
            anchor_server: str = f"${server_letter}{data_first_row}"

            # Construye fórmula
            formula: Optional[str] = self._build_threshold_formula(
                operator=op,
                conf=conf,
                threshold=threshold,
                anchor_cell=anchor_cell,
                anchor_server=anchor_server,
            )
            if formula is None:
                continue

            # Construye regla
            rule: Dict[str, Any] = {
                "ranges": [grid_range],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [{"userEnteredValue": formula}],
                    },
                    "format": STYLES["ALERT_RED"],
                },
            }

            # Inserta index 0 para que "gane" sobre otras reglas.
            requests.append(
                {"addConditionalFormatRule": {"rule": rule, "index": 0}}
            )

        return requests

    def _build_threshold_formula(
        self,
        operator: str,
        conf: Mapping[str, Any],
        threshold: Mapping[str, Any],
        anchor_cell: str,
        anchor_server: str,
    ) -> Optional[str]:
        """
        Construye fórmula condicional robusta.

        Guardas:
        - anchor_server<>"" evita pintar filas sin servidor.
        - Para comparaciones numéricas: anchor_cell<>"" evita pintar vacíos.

        :param operator: Operador (>, <, !=, etc).
        :type operator: str
        :param conf: Config de columna.
        :type conf: Mapping[str, Any]
        :param threshold: Config threshold.
        :type threshold: Mapping[str, Any]
        :param anchor_cell: Celda objetivo (ej: "G7").
        :type anchor_cell: str
        :param anchor_server: Celda servidor con columna absoluta (ej: "$A7").
        :type anchor_server: str
        :return: Fórmula o None si no se soporta.
        :rtype: Optional[str]
        """
        fmt: str = cast(str, conf.get("format", "TEXT"))

        if operator == "!=":
            value: Any = threshold.get("value")
            if fmt == "TEXT":
                # Para texto, comillas.
                value_str: str = f'"{value}"'
                # TRIM evita que espacios te rompan el estado.
                return (
                    f"=AND({anchor_server}<>\"\", "
                    f"TRIM({anchor_cell})<>{value_str})"
                )

            # Para no-texto, comparamos directo.
            return (
                f"=AND({anchor_server}<>\"\", {anchor_cell}<>{value})"
            )

        # Operadores numéricos estándar.
        gs_type: Optional[str] = CONDITION_MAP.get(operator)
        if gs_type is None:
            return None

        # Para numéricos, evitamos pintar cuando la celda está vacía.
        if operator in (">", ">=", "<", "<="):
            value_num: Any = threshold.get("value")
            return (
                f"=AND({anchor_server}<>\"\", {anchor_cell}<>\"\", "
                f"{anchor_cell}{operator}{value_num})"
            )

        # between/not_between (si algún día lo usas en YAML).
        if operator in ("between", "not_between"):
            min_val: Any = threshold.get("min")
            max_val: Any = threshold.get("max")
            if operator == "between":
                return (
                    f"=AND({anchor_server}<>\"\", {anchor_cell}<>\"\", "
                    f"{anchor_cell}>={min_val}, {anchor_cell}<={max_val})"
                )
            return (
                f"=AND({anchor_server}<>\"\", {anchor_cell}<>\"\", "
                f"OR({anchor_cell}<{min_val}, {anchor_cell}>{max_val}))"
            )

        return None

    def _normalize_df_for_sheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza DataFrame para escritura estable en Sheets sin romper tipos.

        Problema que corrige:
        - Antes: columnas dtype=object se forzaban a str con astype(str),
        lo que convertía números a texto. Charts no reconocen series numéricas.
        - Ahora: solo se hace strip a strings, se intenta coerción numérica
        cuando la columna es "mayoritariamente numérica", y se limpian NaN.

        :param df: DataFrame original.
        :type df: pd.DataFrame
        :return: DataFrame normalizado.
        :rtype: pd.DataFrame
        """
        out: pd.DataFrame = df.copy()

        # Strip solo en valores string para evitar espacios invisibles en estados,
        # nombres, etc., sin convertir números a texto.
        for col in out.columns:
            out[col] = out[col].map(
                lambda v: v.strip() if isinstance(v, str) else v
            )

        # Datetimes a string parseable por Sheets (con USER_ENTERED).
        for col in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[col]):
                out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Intento de coerción numérica:
        # Si una columna es "casi toda numérica" (ignorando vacíos), la convertimos
        # a float. Esto hace que en el historial los valores queden como números
        # reales y los charts puedan tomar series.
        for col in out.columns:
            if out[col].dtype != "object":
                continue

            series_obj = out[col]

            # Normalizamos separador decimal en strings ("," -> ".") solo para
            # probar conversión; no tocamos valores no-string.
            candidate = series_obj.map(
                lambda v: v.replace(",", ".") if isinstance(v, str) else v
            )

            # Contamos no-vacíos de forma segura (sin comparaciones raras).
            non_empty_mask = candidate.map(
                lambda v: v is not None and v != ""
            )
            non_empty_count = int(non_empty_mask.sum())

            # Si está vacía, no hacemos nada.
            if non_empty_count == 0:
                continue

            numeric = pd.to_numeric(candidate, errors="coerce")
            success_count = int(numeric.notna().sum())

            # Umbral: si al menos 90% de los no-vacíos convierten a número,
            # asumimos que la columna es numérica y la convertimos.
            if (success_count / non_empty_count) >= 0.9:
                out[col] = numeric

        # NaN -> "" para que Sheets no reciba "nan".
        out = out.where(pd.notnull(out), "")

        return out

    def _df_to_values_with_header(self, df: pd.DataFrame) -> List[List[Any]]:
        """
        Convierte DF a matriz con header incluida.

        :param df: DataFrame normalizado.
        :type df: pd.DataFrame
        :return: Matriz [header] + rows.
        :rtype: List[List[Any]]
        """
        header: List[Any] = list(df.columns)
        rows: List[List[Any]] = df.values.tolist()
        return [header, *rows]

    def _df_to_values_rows_only(self, df: pd.DataFrame) -> List[List[Any]]:
        """
        Convierte DF a matriz solo filas (para append).

        :param df: DataFrame normalizado.
        :type df: pd.DataFrame
        :return: Filas como listas.
        :rtype: List[List[Any]]
        """
        return df.values.tolist()

    def _values_batch_update(
        self,
        ws: gspread.Worksheet,
        updates: Sequence[Mapping[str, Any]],
    ) -> None:
        """
        Wrapper de batch_update para valores con USER_ENTERED.

        Esto es crítico para:
        - fórmulas (que se calculen)
        - fechas (que Sheets las parse)

        :param ws: Worksheet objetivo.
        :type ws: gspread.Worksheet
        :param updates: Lista de ranges y values.
        :type updates: Sequence[Mapping[str, Any]]
        :return: None
        :rtype: None
        """
        self._execute_with_backoff(
            func=lambda: ws.batch_update(
                list(updates),
                value_input_option="USER_ENTERED",
            )
        )

    def _batch_update_requests(self, requests: Sequence[Dict[str, Any]]) -> None:
        """
        Envía un spreadsheets.batchUpdate con requests.

        :param requests: Requests de Google Sheets API.
        :type requests: Sequence[Dict[str, Any]]
        :return: None
        :rtype: None
        """
        if not requests:
            return

        self._execute_with_backoff(
            func=lambda: self.sh.batch_update({"requests": list(requests)})
        )

    def _execute_with_backoff(self, func: Any) -> Any:
        """
        Ejecuta una llamada a la API con retry/backoff.

        Esto reemplaza los time.sleep “a mano”:
        - Si Google responde 429/503/500, reintentamos.
        - Backoff exponencial con jitter simple.

        :param func: Callable sin argumentos.
        :type func: Any
        :return: Resultado de func.
        :rtype: Any
        """
        max_attempts: int = 6
        base_delay: float = 0.8

        for attempt in range(1, max_attempts + 1):
            try:
                return func()
            except APIError as exc:
                status: Optional[int] = None
                try:
                    status = int(getattr(exc.response, "status_code", 0))
                except Exception:
                    status = None

                # Solo reintentamos errores típicos de cuota/temporalidad.
                retryable: bool = status in (429, 500, 503)

                if not retryable or attempt == max_attempts:
                    raise

                # Backoff exponencial.
                delay: float = base_delay * (2 ** (attempt - 1))

                # Jitter simple (sin random import, suficiente para repartir).
                delay += (attempt * 0.17)

                time.sleep(delay)

        # Nunca debería llegar acá.
        return None
