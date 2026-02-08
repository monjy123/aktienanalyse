# -*- coding: utf-8 -*-
"""
Zentrale Berechnungsfunktionen für Kennzahlen.

Enthält:
- EV (Enterprise Value)
- CAGR (Compound Annual Growth Rate)
- TTM (Trailing Twelve Months)
- Durchschnittsberechnungen mit Outlier-Filterung
- Safe Rounding Utility
"""

import math


def safe_round(value, decimals=2):
    """
    Rundet Werte sicher mit None-Handling.

    Args:
        value: Zu rundender Wert (kann None sein)
        decimals: Anzahl Nachkommastellen (default: 2)

    Returns:
        Gerundeter Wert oder None
    """
    if value is None:
        return None
    try:
        return round(value, decimals)
    except (TypeError, ValueError):
        return None


def calculate_ev(market_cap, net_debt=None, minority_interest=None):
    """
    Berechnet Enterprise Value.

    EV = Market Cap + Net Debt + Minority Interest

    Args:
        market_cap: Marktkapitalisierung (erforderlich)
        net_debt: Nettoverschuldung (optional)
        minority_interest: Minderheitsanteile (optional)

    Returns:
        Enterprise Value oder None falls market_cap fehlt
    """
    if market_cap is None:
        return None

    ev = market_cap
    if net_debt is not None:
        ev += net_debt
    if minority_interest is not None:
        ev += minority_interest

    return ev


def calculate_cagr(end_value, start_value, years):
    """
    Berechnet die Compound Annual Growth Rate (CAGR).

    CAGR = ((end / start) ^ (1 / years) - 1) * 100

    Args:
        end_value: Endwert
        start_value: Startwert
        years: Anzahl Jahre

    Returns:
        CAGR in Prozent oder None falls Berechnung nicht möglich
    """
    if start_value is None or end_value is None or years is None:
        return None
    if years <= 0:
        return None
    if start_value <= 0 or end_value <= 0:
        return None

    try:
        cagr = ((end_value / start_value) ** (1 / years) - 1) * 100
        return cagr
    except (ValueError, ZeroDivisionError):
        return None


def calculate_ttm(periods, is_semiannual=False):
    """
    Berechnet Trailing Twelve Months Werte aus Perioden-Daten.

    Args:
        periods: Liste von Dictionaries mit 'net_income' und 'operating_income'
                 Sollte bereits nach Datum absteigend sortiert sein
        is_semiannual: True für Halbjahresberichterstatter (2 Perioden),
                       False für Quartalsberichterstatter (4 Perioden)

    Returns:
        Dict mit {'ttm_net_income': ..., 'ttm_ebit': ..., 'period_count': ...}
        oder None-Werte falls nicht genug Daten
    """
    required_periods = 2 if is_semiannual else 4

    if not periods or len(periods) < required_periods:
        return {
            'ttm_net_income': None,
            'ttm_ebit': None,
            'period_count': 0
        }

    # Nimm nur die benötigte Anzahl Perioden
    latest_periods = periods[:required_periods]

    ttm_net_income = sum(p.get('net_income') or 0 for p in latest_periods)
    ttm_ebit = sum(p.get('operating_income') or 0 for p in latest_periods)

    return {
        'ttm_net_income': ttm_net_income,
        'ttm_ebit': ttm_ebit,
        'period_count': required_periods
    }


def calculate_average_filtered(values, max_reasonable=None):
    """
    Berechnet Durchschnitt mit Outlier-Filterung.

    Schritte:
    1. Ignoriert None-Werte, NaN, Infinity
    2. Ignoriert negative Werte
    3. Ignoriert Werte > max_reasonable (falls angegeben)
    4. Ignoriert Werte > 2 Standardabweichungen vom Mittelwert

    Args:
        values: Liste von Werten
        max_reasonable: Absoluter Maximalwert (z.B. 200 für PE, 100 für EV/EBIT)

    Returns:
        Tuple (Durchschnittswert, Anzahl verwendeter Werte) oder (None, 0)
    """
    # Schritt 1: Nur positive, valide Werte behalten
    valid = []
    for v in values:
        if v is None:
            continue
        try:
            if math.isnan(v) or math.isinf(v):
                continue
        except TypeError:
            continue
        if v <= 0:
            continue
        valid.append(v)

    # Schritt 2: Absoluten Maximalwert anwenden (falls angegeben)
    if max_reasonable is not None:
        valid = [v for v in valid if v <= max_reasonable]

    if not valid:
        return None, 0

    if len(valid) == 1:
        return valid[0], 1

    # Schritt 3: Mittelwert und Standardabweichung berechnen
    mean = sum(valid) / len(valid)
    variance = sum((x - mean) ** 2 for x in valid) / len(valid)
    std_dev = variance ** 0.5

    # Schritt 4: Werte außerhalb 2σ entfernen
    if std_dev > 0:
        upper_bound = mean + 2 * std_dev
        filtered = [v for v in valid if v <= upper_bound]
    else:
        filtered = valid

    if not filtered:
        return None, 0

    return sum(filtered) / len(filtered), len(filtered)


def clean_value(value, max_abs=10000):
    """
    Filtert ungültige Werte (None, NaN, Infinity, extreme Werte).

    Args:
        value: Zu prüfender Wert
        max_abs: Maximaler Absolutwert (default: 10000)

    Returns:
        Bereinigter Wert oder None
    """
    if value is None:
        return None

    # String-Werte abfangen
    if isinstance(value, str):
        try:
            value = float(value)
        except (ValueError, TypeError):
            return None

    try:
        if math.isnan(value) or math.isinf(value):
            return None
    except TypeError:
        return None

    # Filtere extreme Werte
    if abs(value) > max_abs:
        return None

    return value


def to_percent(value):
    """
    Konvertiert Dezimalwert zu Prozent (0.25 -> 25.0).

    Args:
        value: Dezimalwert (z.B. 0.25 für 25%)

    Returns:
        Prozentwert oder None
    """
    cleaned = clean_value(value)
    return cleaned * 100 if cleaned is not None else None


def calc_diff_percent(current, reference):
    """
    Berechnet prozentuale Abweichung.

    ((current / reference) - 1) * 100

    Args:
        current: Aktueller Wert
        reference: Referenzwert (z.B. historischer Durchschnitt)

    Returns:
        Prozentuale Abweichung oder None
    """
    if current is None or reference is None:
        return None
    if reference == 0:
        return None
    try:
        return ((current / reference) - 1) * 100
    except (TypeError, ZeroDivisionError):
        return None
