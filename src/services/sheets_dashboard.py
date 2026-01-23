from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, cast

import gspread
from gspread.utils import a1_range_to_grid_range, a1_to_rowcol

from src.config import config
from src.services.sheets_styles import STYLES


class DashboardBuilder:
    """
    Construye el Dashboard con batch updates.

    revisar ya que jepete lo toco todo
    :return: None
    :rtype: None
    """

    def __init__(self, spreadsheet: gspread.Spreadsheet) -> None:
        """
        :param spreadsheet: Spreadsheet ya abierto por gspread.
        :type spreadsheet: gspread.Spreadsheet
        """
        self.sh: gspread.Spreadsheet = spreadsheet

    def build(self) -> None:
        """
        Orquesta la construcción completa del Dashboard.

        :return: None
        :rtype: None
        """
        ws: gspread.Worksheet = self._prepare_worksheet("Dashboard")

        value_batch: List[Dict[str, Any]] = []
        request_batch: List[Dict[str, Any]] = []

        srv_metrics: List[str] = self._get_numeric_metrics(config.servers_config)
        cam_metrics: List[str] = self._get_numeric_metrics(config.cameras_config)

        self._batch_layout(value_batch=value_batch, req_batch=request_batch)
        self._batch_selectors(
            ws=ws,
            value_batch=value_batch,
            request_batch=request_batch,
            srv_metrics=srv_metrics,
            cam_metrics=cam_metrics,
        )
        self._batch_formulas(
            value_batch=value_batch,
            request_batch=request_batch,
            srv_conf=config.servers_config,
            cam_conf=config.cameras_config,
        )
        self._batch_charts(request_batch=request_batch, sheet_id=ws.id)

        # Valores y fórmulas en USER_ENTERED para que Sheets las calcule.
        if value_batch:
            ws.batch_update(value_batch, value_input_option="USER_ENTERED")

        # Estructura (formatos, validaciones, charts) en un solo batch.
        if request_batch:
            self.sh.batch_update({"requests": request_batch})

    def _prepare_worksheet(self, name: str) -> gspread.Worksheet:
        """
        Obtiene o crea el worksheet y lo deja “limpio” y consistente.

        :param name: Nombre del dashboard.
        :type name: str
        :return: Worksheet listo.
        :rtype: gspread.Worksheet
        """
        try:
            ws: gspread.Worksheet = self.sh.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = self.sh.add_worksheet(title=name, rows=100, cols=35)

        # Limpiamos valores del dashboard (rápido).
        ws.clear()

        # Limpiamos charts incrustados si existían.
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

        requests: List[Dict[str, Any]] = []

        if sheet_meta and sheet_meta.get("charts"):
            requests.extend(
                [
                    {
                        "deleteEmbeddedObject": {
                            "objectId": c["chartId"],
                        }
                    }
                    for c in sheet_meta["charts"]
                ]
            )

        # Limpiamos validaciones antiguas.
        requests.append(
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1000,
                        "startColumnIndex": 0,
                        "endColumnIndex": 50,
                    },
                    "rule": None,
                }
            }
        )

        # Propiedades base del sheet.
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "index": 0,
                        "gridProperties": {"hideGridlines": True},
                    },
                    "fields": "index,gridProperties.hideGridlines",
                }
            }
        )

        if requests:
            self.sh.batch_update({"requests": requests})

        return ws

    def _batch_layout(
        self,
        value_batch: List[Dict[str, Any]],
        req_batch: List[Dict[str, Any]],
    ) -> None:
        """
        Layout base del dashboard.

        :param value_batch: Batch de valores.
        :type value_batch: List[Dict[str, Any]]
        :param req_batch: Batch de requests.
        :type req_batch: List[Dict[str, Any]]
        :return: None
        :rtype: None
        """
        # Texto base.
        value_batch.extend(
            [
                {"range": "A1", "values": [["DASHBOARD OPERATIVO - RAPTOR"]]},
                {"range": "B3", "values": [["Servidor:"]]},
                {"range": "E3", "values": [["Métrica:"]]},
                {"range": "B28", "values": [["Cámara:"]]},
                {"range": "E28", "values": [["Métrica Cámara:"]]},
                # Headers ocultos para datos del chart.
                {"range": "AA4", "values": [["Fecha", "Valor"]]},
                {"range": "AD4", "values": [["Fecha", "Valor"]]},
            ]
        )

        # Merge título.
        req_batch.append(
            {
                "mergeCells": {
                    "range": {
                        "sheetId": 0,  # placeholder, se corrige abajo
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 26,
                    },
                    "mergeType": "MERGE_ALL",
                }
            }
        )

        # Estilos.
        req_batch.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": 0,  # placeholder, se corrige abajo
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 26,
                    },
                    "cell": {"userEnteredFormat": STYLES["HEADER_DASHBOARD"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        req_batch.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": 0,  # placeholder
                        "startRowIndex": 1,
                        "endRowIndex": 100,
                        "startColumnIndex": 0,
                        "endColumnIndex": 26,
                    },
                    "cell": {"userEnteredFormat": STYLES["DASHBOARD_BG"]},
                    "fields": "userEnteredFormat",
                }
            }
        )

        # Labels bold.
        for cell in ("B3", "E3", "B28", "E28"):
            req_batch.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": 0,
                            **a1_range_to_grid_range(cell, 0),
                        },
                        "cell": {"userEnteredFormat": STYLES["LABEL_BOLD"]},
                        "fields": "userEnteredFormat",
                    }
                }
            )

        # Reescribimos el sheetId placeholder cuando sepamos el real.
        # Se corrige en _batch_selectors, donde ya tenemos ws.id.

    def _batch_selectors(
        self,
        ws: gspread.Worksheet,
        value_batch: List[Dict[str, Any]],
        request_batch: List[Dict[str, Any]],
        srv_metrics: Sequence[str],
        cam_metrics: Sequence[str],
    ) -> None:
        """
        Agrega dropdowns y defaults.

        :param ws: Worksheet dashboard.
        :type ws: gspread.Worksheet
        :param value_batch: Batch de valores.
        :type value_batch: List[Dict[str, Any]]
        :param request_batch: Batch de requests.
        :type request_batch: List[Dict[str, Any]]
        :param srv_metrics: Métricas numéricas.
        :type srv_metrics: Sequence[str]
        :param cam_metrics: Métricas numéricas cámara.
        :type cam_metrics: Sequence[str]
        :return: None
        :rtype: None
        """
        h_srv: str = config.servers_config["history_tab"]
        h_cam: str = config.cameras_config["history_tab"]

        def_srv: Optional[str] = self._get_valid_default(tab_name=h_srv, col=1)
        def_cam: Optional[str] = self._get_valid_default(tab_name=h_cam, col=2)

        if def_srv:
            value_batch.append({"range": "C3", "values": [[def_srv]]})
        if srv_metrics:
            value_batch.append({"range": "F3", "values": [[srv_metrics[0]]]})
        if def_cam:
            value_batch.append({"range": "C28", "values": [[def_cam]]})
        if cam_metrics:
            value_batch.append({"range": "F28", "values": [[cam_metrics[0]]]})

        # Fix: corregimos los sheetId placeholders en requests ya agregados.
        for req in request_batch:
            if "mergeCells" in req:
                req["mergeCells"]["range"]["sheetId"] = ws.id
            if "repeatCell" in req and "range" in req["repeatCell"]:
                req["repeatCell"]["range"]["sheetId"] = ws.id

        # Validaciones.
        request_batch.extend(
            [
                {
                    "setDataValidation": {
                        "range": a1_range_to_grid_range("C3", ws.id),
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_RANGE",
                                "values": [
                                    {
                                        "userEnteredValue": (
                                            f"='{h_srv}'!A2:A"
                                        )
                                    }
                                ],
                            },
                            "showCustomUi": True,
                        },
                    }
                },
                {
                    "setDataValidation": {
                        "range": a1_range_to_grid_range("F3", ws.id),
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [
                                    {"userEnteredValue": m}
                                    for m in srv_metrics
                                ],
                            },
                            "showCustomUi": True,
                        },
                    }
                },
                {
                    "setDataValidation": {
                        "range": a1_range_to_grid_range("C28", ws.id),
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_RANGE",
                                "values": [
                                    {
                                        "userEnteredValue": (
                                            f"='{h_cam}'!B2:B"
                                        )
                                    }
                                ],
                            },
                            "showCustomUi": True,
                        },
                    }
                },
                {
                    "setDataValidation": {
                        "range": a1_range_to_grid_range("F28", ws.id),
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [
                                    {"userEnteredValue": m}
                                    for m in cam_metrics
                                ],
                            },
                            "showCustomUi": True,
                        },
                    }
                },
            ]
        )
    def _batch_formulas(
        self,
        value_batch: List[Dict[str, Any]],
        request_batch: List[Dict[str, Any]],
        srv_conf: Mapping[str, Any],
        cam_conf: Mapping[str, Any],
    ) -> None:
        """
        Agrega fórmulas que alimentan los charts.

        Problema que corrige:
        - XMATCH fallaba si el header tenía espacios o caracteres invisibles,
        devolviendo error y dejando AA/AB vacíos.
        - Ahora se hace TRIM tanto del selector como del header row.

        :param value_batch: Batch de valores.
        :type value_batch: List[Dict[str, Any]]
        :param request_batch: Batch de requests.
        :type request_batch: List[Dict[str, Any]]
        :param srv_conf: Config servidores.
        :type srv_conf: Mapping[str, Any]
        :param cam_conf: Config cámaras.
        :type cam_conf: Mapping[str, Any]
        :return: None
        :rtype: None
        """
        h_srv: str = cast(str, srv_conf["history_tab"])
        h_cam: str = cast(str, cam_conf["history_tab"])

        # Header row “limpio” para hacer matching robusto.
        hdr_srv: str = f"ARRAYFORMULA(TRIM('{h_srv}'!1:1))"
        hdr_cam: str = f"ARRAYFORMULA(TRIM('{h_cam}'!1:1))"

        # Server: devuelve 2 columnas (Fecha, Valor) desde historial.
        f_srv: str = (
            f'=IFERROR('
            f'SORT('
            f'FILTER('
            f'HSTACK('
            f'CHOOSECOLS(\'{h_srv}\'!A:Z,'
            f'XMATCH("Fecha consulta",{hdr_srv},0)),'
            f'CHOOSECOLS(\'{h_srv}\'!A:Z,'
            f'XMATCH(TRIM(F3),{hdr_srv},0))'
            f'),'
            f'TRIM(\'{h_srv}\'!A:A)=TRIM(C3)'
            f'),'
            f'1,TRUE'
            f'),'
            f'""'
            f')'
        )

        # Camera: devuelve 2 columnas (Fecha, Valor) desde historial.
        f_cam: str = (
            f'=IFERROR('
            f'SORT('
            f'FILTER('
            f'HSTACK('
            f'CHOOSECOLS(\'{h_cam}\'!A:Z,'
            f'XMATCH("Fecha - hora consulta",{hdr_cam},0)),'
            f'CHOOSECOLS(\'{h_cam}\'!A:Z,'
            f'XMATCH(TRIM(F28),{hdr_cam},0))'
            f'),'
            f'TRIM(\'{h_cam}\'!B:B)=TRIM(C28)'
            f'),'
            f'1,TRUE'
            f'),'
            f'""'
            f')'
        )

        # Fórmulas “spill” (2 columnas) desde AA5 y AD5.
        value_batch.append({"range": "AA5", "values": [[f_srv]]})
        value_batch.append({"range": "AD5", "values": [[f_cam]]})

    # def _batch_formulas(
    #     self,
    #     value_batch: List[Dict[str, Any]],
    #     request_batch: List[Dict[str, Any]],
    #     srv_conf: Mapping[str, Any],
    #     cam_conf: Mapping[str, Any],
    # ) -> None:
    #     """
    #     Agrega fórmulas que alimentan los charts.

    #     Fix:
    #     - se escribe en USER_ENTERED (en build()).
    #     - los charts ahora toman AA4:AB..., AD4:AE... (con header).

    #     :param value_batch: Batch de valores.
    #     :type value_batch: List[Dict[str, Any]]
    #     :param request_batch: Batch de requests.
    #     :type request_batch: List[Dict[str, Any]]
    #     :param srv_conf: Config servidores.
    #     :type srv_conf: Mapping[str, Any]
    #     :param cam_conf: Config cámaras.
    #     :type cam_conf: Mapping[str, Any]
    #     :return: None
    #     :rtype: None
    #     """
    #     h_srv: str = cast(str, srv_conf["history_tab"])
    #     h_cam: str = cast(str, cam_conf["history_tab"])

    #     f_srv: str = (
    #         f'=IFERROR(SORT(FILTER(HSTACK('
    #         f'CHOOSECOLS(\'{h_srv}\'!A:Z, '
    #         f'XMATCH("Fecha consulta", \'{h_srv}\'!1:1)), '
    #         f'CHOOSECOLS(\'{h_srv}\'!A:Z, '
    #         f'XMATCH(F3, \'{h_srv}\'!1:1))), '
    #         f'\'{h_srv}\'!A:A = C3), 1, TRUE), "")'
    #     )

    #     f_cam: str = (
    #         f'=IFERROR(SORT(FILTER(HSTACK('
    #         f'CHOOSECOLS(\'{h_cam}\'!A:Z, '
    #         f'XMATCH("Fecha - hora consulta", \'{h_cam}\'!1:1)), '
    #         f'CHOOSECOLS(\'{h_cam}\'!A:Z, '
    #         f'XMATCH(F28, \'{h_cam}\'!1:1))), '
    #         f'\'{h_cam}\'!B:B = C28), 1, TRUE), "")'
    #     )

    #     # Fórmulas “spill” desde AA5 y AD5.
    #     value_batch.append({"range": "AA5", "values": [[f_srv]]})
    #     value_batch.append({"range": "AD5", "values": [[f_cam]]})

    #     # Formatos de fecha (solo númeroFormat para no pisar otros estilos).
    #     date_fmt: Dict[str, Any] = {
    #         "numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm"}
    #     }

    #     request_batch.extend(
    #         [
    #             {
    #                 "repeatCell": {
    #                     "range": a1_range_to_grid_range("AA5:AA1000", 0),
    #                     "cell": {"userEnteredFormat": date_fmt},
    #                     "fields": "userEnteredFormat.numberFormat",
    #                 }
    #             },
    #             {
    #                 "repeatCell": {
    #                     "range": a1_range_to_grid_range("AD5:AD1000", 0),
    #                     "cell": {"userEnteredFormat": date_fmt},
    #                     "fields": "userEnteredFormat.numberFormat",
    #                 }
    #             },
    #         ]
    #     )

    #     # Corregimos sheetId=0 placeholder para estos repeatCell.
    #     # El id real se fija en _batch_charts y en _batch_selectors.
    #     # Lo ajustamos allí.

    def _batch_charts(self, request_batch: List[Dict[str, Any]], sheet_id: int) -> None:
        """
        Crea charts para servidor y cámara.

        Fix:
        - Rango incluye header row 4 (AA4:AB..., AD4:AE...).
        - headerCount=1 ahora calza con el rango.

        :param request_batch: Batch requests.
        :type request_batch: List[Dict[str, Any]]
        :param sheet_id: SheetId real del dashboard.
        :type sheet_id: int
        :return: None
        :rtype: None
        """
        # Arreglamos placeholders en repeatCell de fechas.
        for req in request_batch:
            if "repeatCell" in req:
                rng: Dict[str, Any] = req["repeatCell"]["range"]
                if rng.get("sheetId") == 0:
                    rng["sheetId"] = sheet_id
            if "mergeCells" in req:
                rng2: Dict[str, Any] = req["mergeCells"]["range"]
                if rng2.get("sheetId") == 0:
                    rng2["sheetId"] = sheet_id

        # Anchor de charts.
        r_srv, c_srv = a1_to_rowcol("B6")
        r_cam, c_cam = a1_to_rowcol("B31")

        # Server chart: domain AA, series AB. Incluye header row 4.
        request_batch.append(
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": "Evolución Temporal (Servidor)",
                            "basicChart": {
                                "chartType": "LINE",
                                "legendPosition": "BOTTOM_LEGEND",
                                "axis": [
                                    {"position": "BOTTOM_AXIS", "title": "Fecha"},
                                    {"position": "LEFT_AXIS"},
                                ],
                                "domains": [
                                    {
                                        "domain": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": sheet_id,
                                                        "startRowIndex": 3,
                                                        "endRowIndex": 1000,
                                                        "startColumnIndex": 26,
                                                        "endColumnIndex": 27,
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "series": [
                                    {
                                        "series": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": sheet_id,
                                                        "startRowIndex": 3,
                                                        "endRowIndex": 1000,
                                                        "startColumnIndex": 27,
                                                        "endColumnIndex": 28,
                                                    }
                                                ]
                                            }
                                        },
                                        "targetAxis": "LEFT_AXIS",
                                    }
                                ],
                                "headerCount": 1,
                                "interpolateNulls": True,
                            },
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": sheet_id,
                                    "rowIndex": r_srv - 1,
                                    "columnIndex": c_srv - 1,
                                }
                            }
                        },
                    }
                }
            }
        )

        # Camera chart: domain AD, series AE. Incluye header row 4.
        request_batch.append(
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": "Evolución Temporal (Cámara)",
                            "basicChart": {
                                "chartType": "LINE",
                                "legendPosition": "BOTTOM_LEGEND",
                                "axis": [
                                    {"position": "BOTTOM_AXIS", "title": "Fecha"},
                                    {"position": "LEFT_AXIS"},
                                ],
                                "domains": [
                                    {
                                        "domain": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": sheet_id,
                                                        "startRowIndex": 3,
                                                        "endRowIndex": 1000,
                                                        "startColumnIndex": 29,
                                                        "endColumnIndex": 30,
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "series": [
                                    {
                                        "series": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": sheet_id,
                                                        "startRowIndex": 3,
                                                        "endRowIndex": 1000,
                                                        "startColumnIndex": 30,
                                                        "endColumnIndex": 31,
                                                    }
                                                ]
                                            }
                                        },
                                        "targetAxis": "LEFT_AXIS",
                                    }
                                ],
                                "headerCount": 1,
                                "interpolateNulls": True,
                            },
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": sheet_id,
                                    "rowIndex": r_cam - 1,
                                    "columnIndex": c_cam - 1,
                                }
                            }
                        },
                    }
                }
            }
        )

    def _get_numeric_metrics(self, tab_config: Mapping[str, Any]) -> List[str]:
        """
        Devuelve solo métricas numéricas para dropdown.

        :param tab_config: Config YAML de tab.
        :type tab_config: Mapping[str, Any]
        :return: Lista de nombres humanos.
        :rtype: List[str]
        """
        cols: Mapping[str, Any] = cast(Mapping[str, Any], tab_config["columns"])
        out: List[str] = []

        for v in cols.values():
            fmt: str = cast(str, v.get("format", "TEXT"))
            if fmt in ("NUMBER", "INTEGER", "PERCENT"):
                out.append(cast(str, v["name"]))

        return out

    def _get_valid_default(self, tab_name: str, col: int) -> Optional[str]:
        """
        Obtiene un default “razonable” del historial.

        :param tab_name: Nombre del historial.
        :type tab_name: str
        :param col: Índice de columna.
        :type col: int
        :return: Valor o None.
        :rtype: Optional[str]
        """
        try:
            ws: gspread.Worksheet = self.sh.worksheet(tab_name)
            vals: List[str] = ws.col_values(col)[1:]
            if vals:
                return vals[0]
            return None
        except Exception:
            return None
