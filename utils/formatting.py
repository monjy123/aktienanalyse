# -*- coding: utf-8 -*-
"""
Deutsche Zahlen- und Datumsformatierung.

Enthält:
- format_de: Zahl ins deutsche Format (1.234,56)
- parse_german_number: Deutsche Zahl parsen (1.234,56 -> 1234.56)
- parse_german_date: Deutsches Datum parsen (12.02.2026 -> 2026-02-12)
"""

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation


def format_de(value, decimals=2):
    """
    Formatiert Zahl ins deutsche Format (1.234,56).

    Args:
        value: Zahl zum Formatieren
        decimals: Anzahl Nachkommastellen (default: 2)

    Returns:
        Formatierter String oder '-' bei None/Fehler
    """
    if value is None:
        return '-'
    try:
        # Zahl formatieren mit gewünschten Nachkommastellen
        formatted = f"{value:,.{decimals}f}"
        # Englisch -> Deutsch: Erst Komma zu Temp, dann Punkt zu Komma, dann Temp zu Punkt
        formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        return formatted
    except (ValueError, TypeError):
        return '-'


def format_de_percent(value, decimals=1):
    """
    Formatiert Prozent im deutschen Format.

    Args:
        value: Prozentwert (bereits in %, nicht als Dezimal)
        decimals: Anzahl Nachkommastellen (default: 1)

    Returns:
        Formatierter String mit % oder '-'
    """
    if value is None:
        return '-'
    try:
        formatted = f"{value:,.{decimals}f}"
        formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        return f"{formatted}%"
    except (ValueError, TypeError):
        return '-'


def format_de_billions(value, decimals=1):
    """
    Formatiert Milliarden-Werte im deutschen Format.

    Args:
        value: Wert in Einheiten (wird durch 1 Mrd geteilt)
        decimals: Anzahl Nachkommastellen (default: 1)

    Returns:
        Formatierter String (z.B. "1,2" für 1,2 Mrd)
    """
    if value is None:
        return '-'
    try:
        billions = value / 1_000_000_000
        formatted = f"{billions:,.{decimals}f}"
        formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        return formatted
    except (ValueError, TypeError):
        return '-'


def format_de_date(value):
    """
    Formatiert Datum im deutschen Format (TT.MM.JJJJ).

    Args:
        value: datetime, date oder String im Format YYYY-MM-DD

    Returns:
        Formatierter String (z.B. "12.02.2026") oder '-'
    """
    if value is None:
        return '-'
    try:
        # Falls es ein String ist (YYYY-MM-DD)
        if isinstance(value, str):
            value = datetime.strptime(value, '%Y-%m-%d').date()
        # Falls es ein datetime oder date Objekt ist
        if hasattr(value, 'strftime'):
            return value.strftime('%d.%m.%Y')
        return '-'
    except (ValueError, TypeError):
        return '-'


def parse_german_number(text):
    """
    Parst deutsche Zahlenformate.

    Beispiele:
    - "1.234,56" -> 1234.56
    - "1.234,56 EUR" -> 1234.56
    - "-" -> None

    Args:
        text: Zu parsender Text

    Returns:
        Decimal-Wert oder None bei Fehler
    """
    if not text or str(text).strip() in ['-', '—', '']:
        return None
    try:
        # Entferne Währung, Prozent und Whitespace
        cleaned = re.sub(r'[A-Za-z%\s]', '', str(text))
        # Deutsche -> Englische Notation
        cleaned = cleaned.replace('.', '').replace(',', '.')
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def parse_german_date(text):
    """
    Parst deutsches Datum.

    Beispiele:
    - "12.02.2026" -> "2026-02-12"
    - "12.02.2026 (e)*" -> "2026-02-12"

    Args:
        text: Zu parsender Text

    Returns:
        Datum als String im ISO-Format (YYYY-MM-DD) oder None
    """
    if not text:
        return None
    # Entferne (e)* Markierung (Schätzwert-Markierung auf finanzen.net)
    text = re.sub(r'\s*\(e\)\*?\s*', '', str(text)).strip()
    try:
        dt = datetime.strptime(text, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_currency(text):
    """
    Extrahiert Währung aus Text.

    Args:
        text: Text mit Währungscode

    Returns:
        Währungscode (EUR, USD, etc.) oder None
    """
    if not text:
        return None
    match = re.search(r'(EUR|USD|JPY|GBP|CHF|DKK|SEK|NOK)', str(text))
    return match.group(1) if match else None
