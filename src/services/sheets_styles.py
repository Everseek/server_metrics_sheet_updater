"""
Todas las opciones de estilo de los sheets de Google Sheets,
para mantener todo ordenadito.
"""

from __future__ import annotations
from typing import Any, Dict

RAPTOR_BLUE: Dict[str, float] = {"red": 0.258, "green": 0.52, "blue": 0.956}
WHITE: Dict[str, float] = {"red": 1.0, "green": 1.0, "blue": 1.0}
BLACK: Dict[str, float] = {"red": 0.0, "green": 0.0, "blue": 0.0}

FORMATS: Dict[str, Dict[str, Any]] = {
    "NUMBER": {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}},
    "INTEGER": {"numberFormat": {"type": "NUMBER", "pattern": "0"}},
    "PERCENT": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}},
    "DATE_TIME": {
        "numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}
    },
    "TEXT": {"numberFormat": {"type": "TEXT"}},
    "DURATION": {"numberFormat": {"type": "TIME", "pattern": "[h]:mm:ss"}},
}

STYLES: Dict[str, Dict[str, Any]] = {
    "HEADER_DASHBOARD": {
        "backgroundColor": RAPTOR_BLUE,
        "textFormat": {"foregroundColor": WHITE, "bold": True, "fontSize": 14},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    },
    "HEADER_MAIN": {
        "backgroundColor": RAPTOR_BLUE,
        "textFormat": {"foregroundColor": WHITE, "bold": True, "fontSize": 11},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    },
    "DASHBOARD_BG": {"backgroundColor": WHITE},
    "LABEL_BOLD": {
        "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": BLACK},
        "horizontalAlignment": "RIGHT",
        "verticalAlignment": "MIDDLE",
    },
    "HEADER_BLUE": {
        "backgroundColor": RAPTOR_BLUE,
        "textFormat": {"foregroundColor": WHITE, "bold": True, "fontSize": 10},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
    },
    "METADATA_LABEL": {
        "backgroundColor": RAPTOR_BLUE,
        "textFormat": {
            "foregroundColor": WHITE,
            "bold": True,
            "fontSize": 10,
        },
        "horizontalAlignment": "RIGHT",
        "verticalAlignment": "MIDDLE",
        "borders": {
            "bottom": {"style": "SOLID", "color": WHITE},
            "top": {"style": "SOLID", "color": WHITE},
            "left": {"style": "SOLID", "color": WHITE},
            "right": {"style": "SOLID", "color": WHITE},
        },
    },
    "METADATA_VALUE": {
        "backgroundColor": WHITE,
        "textFormat": {"foregroundColor": BLACK, "bold": True},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
        "borders": {
            "bottom": {"style": "SOLID", "color": BLACK},
            "top": {"style": "SOLID", "color": BLACK},
            "left": {"style": "SOLID", "color": BLACK},
            "right": {"style": "SOLID", "color": BLACK},
        },
    },
    "TABLE_BASE": {
        "borders": {
            "top": {"style": "SOLID"},
            "bottom": {"style": "SOLID"},
            "left": {"style": "SOLID"},
            "right": {"style": "SOLID"},
        },
        "textFormat": {"foregroundColor": BLACK},
        "verticalAlignment": "MIDDLE",
    },
    "ALERT_RED": {
        "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8},
        "textFormat": {
            "foregroundColor": {"red": 0.8, "green": 0.0, "blue": 0.0},
            "bold": True,
        },
    },
}

# Mapeo de condiciones para formato condicional, quizás era
# demaciado crear un archivo solo para esto así que lo deje aquí
CONDITION_MAP: Dict[str, str] = {
    ">": "NUMBER_GREATER",
    ">=": "NUMBER_GREATER_THAN_EQ",
    "<": "NUMBER_LESS",
    "<=": "NUMBER_LESS_THAN_EQ",
    "==": "TEXT_EQ",
    "between": "NUMBER_BETWEEN",
    "not_between": "NUMBER_NOT_BETWEEN",
}
