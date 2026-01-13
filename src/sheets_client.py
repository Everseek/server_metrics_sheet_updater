from __future__ import annotations

from pathlib import Path

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe


class GoogleSheetsClient:
    """
    Cliente para actualizar pestañas de Google Sheets.

    :param credentials_path: JSON service account.
    :type credentials_path: Path
    :param sheet_id: ID del spreadsheet.
    :type sheet_id: str
    """

    def __init__(
        self,
        credentials_path: Path,
        sheet_id: str
    ) -> None:
        self._credentials_path = credentials_path
        self._sheet_id = sheet_id


    def _open_sheet(self) -> gspread.Spreadsheet:
        """
        Abre el spreadsheet.

        :return: Spreadsheet.
        :rtype: gspread.Spreadsheet
        :raises FileNotFoundError: Si no existe JSON.
        """
        if not self._credentials_path.exists():
            raise FileNotFoundError(
                f"No existe: {self._credentials_path}"
            )

        gc = gspread.service_account(
            filename=str(self._credentials_path)
        )
        return gc.open_by_key(self._sheet_id)


    @staticmethod
    def _open_or_create_worksheet(
        sheet: gspread.Spreadsheet,
        title: str,
        rows: int,
        cols: int,
    ) -> gspread.Worksheet:
        """
        Abre o crea pestaña.

        :param sheet: Spreadsheet.
        :type sheet: gspread.Spreadsheet
        :param title: Nombre pestaña.
        :type title: str
        :param rows: Filas.
        :type rows: int
        :param cols: Columnas.
        :type cols: int
        :return: Worksheet.
        :rtype: gspread.Worksheet
        """
        try:
            return sheet.worksheet(title)
        except gspread.WorksheetNotFound:
            return sheet.add_worksheet(
                title=title,
                rows=rows,
                cols=cols
            )


    def replace_dataframe(
        self,
        worksheet_name: str,
        df: pd.DataFrame,
    ) -> None:
        """
        Reemplaza contenido completo de una pestaña por un DataFrame.

        :param worksheet_name: Nombre pestaña.
        :type worksheet_name: str
        :param df: DataFrame.
        :type df: pd.DataFrame
        :return: None
        :rtype: None
        """
        sheet = self._open_sheet()

        ws = self._open_or_create_worksheet(
            sheet=sheet,
            title=worksheet_name,
            rows=max(len(df) + 10, 100),
            cols=max(len(df.columns) + 5, 20),
        )
        ws.clear()
        set_with_dataframe(
            worksheet=ws,
            dataframe=df,
            include_index=False,
            include_column_header=True,
            resize=True,
        )
