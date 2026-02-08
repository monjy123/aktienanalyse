# -*- coding: utf-8 -*-
"""
Shared Utilities für Aktienanalyse.

Enthält:
- fmp_api: Zentralisierte FMP API Request Funktionen
- calculations: EV, TTM, CAGR Berechnungen
- formatting: Deutsche Zahlen-/Datumsformatierung
- logging_config: Shared Logging Setup
"""

from .fmp_api import api_request, get_fmp_api_key, get_session
from .calculations import (
    calculate_ev,
    calculate_cagr,
    calculate_ttm,
    calculate_average_filtered,
    safe_round
)
from .formatting import format_de, parse_german_number, parse_german_date

__all__ = [
    # FMP API
    'api_request',
    'get_fmp_api_key',
    'get_session',
    # Calculations
    'calculate_ev',
    'calculate_cagr',
    'calculate_ttm',
    'calculate_average_filtered',
    'safe_round',
    # Formatting
    'format_de',
    'parse_german_number',
    'parse_german_date',
]
