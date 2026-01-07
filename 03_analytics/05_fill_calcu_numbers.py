#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Befüllt analytics.calcu_numbers mit berechneten Kennzahlen aus fmp_filtered_numbers.

Berechnete Kennzahlen:
- FY PE/EV-EBIT (Ganzjahresdaten)
- TTM PE/EV-EBIT (Trailing Twelve Months aus 4 Quartalen)
- EV (Enterprise Value) = Market Cap + Net Debt + Minority Interest
- EBIT (Operating Income)
- PE/EV-EBIT Durchschnitte (5/10/15/20 Jahre)
- CAGR für Revenue, EBIT, Net Income (3/5/10 Jahre)
- Eigenkapitalquote (Equity Ratio)
- Net Debt / EBITDA
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
from collections import defaultdict
from mysql.connector import Error
from db import get_connection


def calculate_cagr(end_value, start_value, years):
    """
    Berechnet die Compound Annual Growth Rate (CAGR).
    Returns None wenn Berechnung nicht möglich.
    """
    if start_value is None or end_value is None or years <= 0:
        return None
    if start_value <= 0 or end_value <= 0:
        return None
    try:
        cagr = ((end_value / start_value) ** (1 / years) - 1) * 100
        return cagr
    except (ValueError, ZeroDivisionError):
        return None


def calculate_average_filtered(values, max_reasonable_value=None):
    """
    Berechnet Durchschnitt mit Outlier-Filterung.
    - Ignoriert None-Werte
    - Ignoriert negative Werte
    - Ignoriert Werte > max_reasonable_value (falls angegeben)
    - Ignoriert Werte > 2 Standardabweichungen vom Mittelwert

    Args:
        values: Liste von Werten
        max_reasonable_value: Absoluter Maximalwert (z.B. 200 für PE, 100 für EV/EBIT)

    Returns:
        Tuple (Durchschnittswert, Anzahl verwendeter Werte) oder (None, 0) falls nicht genug Daten
    """
    # Schritt 1: Nur positive, valide Werte behalten
    valid = [v for v in values if v is not None and not np.isnan(v) and not np.isinf(v) and v > 0]

    # Schritt 2: Absoluten Maximalwert anwenden (falls angegeben)
    if max_reasonable_value is not None:
        valid = [v for v in valid if v <= max_reasonable_value]

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


def get_value_n_years_ago(data_by_year, current_year, n_years, field_idx):
    """Holt Wert von vor n Jahren aus dem Dictionary."""
    target_year = current_year - n_years
    if target_year in data_by_year:
        return data_by_year[target_year][field_idx]
    return None


def get_values_last_n_years(data_by_year, current_year, n_years, field_idx):
    """Holt alle Werte der letzten n Jahre (inklusive aktuelles Jahr)."""
    values = []
    for year in range(current_year - n_years + 1, current_year + 1):
        if year in data_by_year and data_by_year[year][field_idx] is not None:
            values.append(data_by_year[year][field_idx])
    return values


def calculate_ttm_values(rows, current_row):
    """
    Berechnet TTM (Trailing Twelve Months) Werte aus den letzten 4 Quartalen.

    Args:
        rows: Alle Zeilen des Tickers (sortiert nach Datum)
        current_row: Die aktuelle Zeile für die TTM berechnet werden soll

    Returns:
        Dict mit ttm_net_income, ttm_ebit oder None-Werten
    """
    current_date = current_row['date']

    # Finde die letzten 4 Quartale (nicht FY) vor/inkl. current_date
    quarterly_rows = [
        r for r in rows
        if r['period'] != 'FY' and r['date'] <= current_date
    ]
    # Sortiere absteigend nach Datum und nimm die letzten 4
    quarterly_rows = sorted(quarterly_rows, key=lambda x: x['date'], reverse=True)[:4]

    if len(quarterly_rows) < 4:
        return {'ttm_net_income': None, 'ttm_ebit': None}

    # Summiere Net Income und EBIT der letzten 4 Quartale
    ttm_net_income = None
    ttm_ebit = None

    net_incomes = [r['net_income'] for r in quarterly_rows if r['net_income'] is not None]
    ebits = [r['operating_income'] for r in quarterly_rows if r['operating_income'] is not None]

    if len(net_incomes) == 4:
        ttm_net_income = sum(net_incomes)

    if len(ebits) == 4:
        ttm_ebit = sum(ebits)

    return {'ttm_net_income': ttm_net_income, 'ttm_ebit': ttm_ebit}


def process_ticker(rows):
    """
    Verarbeitet alle Zeilen eines Tickers und berechnet die Kennzahlen.
    rows: Liste von Tupeln mit den Daten aus fmp_filtered_numbers

    Returns: Liste von Dictionaries mit berechneten Werten
    """
    # Sortiere nach Datum
    rows = sorted(rows, key=lambda x: x['date'])

    # Gruppiere FY-Daten nach Jahr für historische Berechnungen
    fy_data_by_year = {}
    for row in rows:
        if row['period'] == 'FY':
            year = row['date'].year
            fy_data_by_year[year] = row

    results = []

    for row in rows:
        result = {
            'ticker': row['ticker'],
            'isin': row['isin'],
            'stock_index': row['stock_index'],
            'company_name': row['company_name'],
            'date': row['date'],
            'period': row['period'],
        }

        # === Basis-Kennzahlen (FY) ===

        # FY PE = Price / EPS (oder Market Cap / Net Income)
        fy_pe = None
        if row['price'] and row['eps'] and row['eps'] > 0:
            fy_pe = row['price'] / row['eps']
        elif row['market_cap'] and row['net_income'] and row['net_income'] > 0:
            fy_pe = row['market_cap'] / row['net_income']
        result['fy_pe'] = fy_pe

        # EBIT = Operating Income
        ebit = row['operating_income']
        result['ebit'] = ebit

        # EV = Market Cap + Net Debt + Minority Interest
        ev = None
        if row['market_cap'] is not None:
            ev = row['market_cap']
            if row['net_debt'] is not None:
                ev += row['net_debt']
            if row['minority_interest'] is not None:
                ev += row['minority_interest']
        result['ev'] = ev

        # FY EV/EBIT
        fy_ev_ebit = None
        if ev is not None and ebit is not None and ebit > 0:
            fy_ev_ebit = ev / ebit
        result['fy_ev_ebit'] = fy_ev_ebit

        # === TTM-Kennzahlen (Trailing Twelve Months) ===
        ttm = calculate_ttm_values(rows, row)
        result['ttm_net_income'] = ttm['ttm_net_income']
        result['ttm_ebit'] = ttm['ttm_ebit']

        # TTM PE = Market Cap / TTM Net Income
        ttm_pe = None
        if row['market_cap'] and ttm['ttm_net_income'] and ttm['ttm_net_income'] > 0:
            ttm_pe = row['market_cap'] / ttm['ttm_net_income']
        result['ttm_pe'] = ttm_pe

        # TTM EV/EBIT = EV / TTM EBIT
        ttm_ev_ebit = None
        if ev is not None and ttm['ttm_ebit'] and ttm['ttm_ebit'] > 0:
            ttm_ev_ebit = ev / ttm['ttm_ebit']
        result['ttm_ev_ebit'] = ttm_ev_ebit

        # === Historische Durchschnitte (nur für FY sinnvoll berechenbar) ===
        current_year = row['date'].year

        if row['period'] == 'FY':
            # PE Durchschnitte (max_reasonable_value=200, da PE > 200 für Durchschnitte unrealistisch)
            # Nur berechnen wenn mindestens 50% der angeforderten Jahre verfügbar sind
            for n_years, col_name in [(5, 'pe_avg_5y'), (10, 'pe_avg_10y'),
                                       (15, 'pe_avg_15y'), (20, 'pe_avg_20y')]:
                pe_values = []
                for year in range(current_year - n_years + 1, current_year + 1):
                    if year in fy_data_by_year:
                        fy_row = fy_data_by_year[year]
                        if fy_row['price'] and fy_row['eps'] and fy_row['eps'] > 0:
                            pe_values.append(fy_row['price'] / fy_row['eps'])
                        elif fy_row['market_cap'] and fy_row['net_income'] and fy_row['net_income'] > 0:
                            pe_values.append(fy_row['market_cap'] / fy_row['net_income'])

                # Nur berechnen wenn mindestens 50% der Jahre verfügbar sind
                min_required_years = max(3, int(n_years * 0.5))  # Mindestens 3 Jahre, oder 50% der angeforderten Jahre
                if len(pe_values) >= min_required_years:
                    avg_value, count = calculate_average_filtered(pe_values, max_reasonable_value=200)
                    result[col_name] = avg_value
                    result[col_name + '_count'] = count
                else:
                    result[col_name] = None
                    result[col_name + '_count'] = None

            # EV/EBIT Durchschnitte (max_reasonable_value=100, da EV/EBIT > 100 unrealistisch)
            # Nur berechnen wenn mindestens 50% der angeforderten Jahre verfügbar sind
            for n_years, col_name in [(5, 'ev_ebit_avg_5y'), (10, 'ev_ebit_avg_10y'),
                                       (15, 'ev_ebit_avg_15y'), (20, 'ev_ebit_avg_20y')]:
                ev_ebit_values = []
                for year in range(current_year - n_years + 1, current_year + 1):
                    if year in fy_data_by_year:
                        fy_row = fy_data_by_year[year]
                        # EV berechnen
                        fy_ev = None
                        if fy_row['market_cap'] is not None:
                            fy_ev = fy_row['market_cap']
                            if fy_row['net_debt'] is not None:
                                fy_ev += fy_row['net_debt']
                            if fy_row['minority_interest'] is not None:
                                fy_ev += fy_row['minority_interest']
                        fy_ebit = fy_row['operating_income']
                        if fy_ev is not None and fy_ebit is not None and fy_ebit > 0:
                            ev_ebit_values.append(fy_ev / fy_ebit)

                # Nur berechnen wenn mindestens 50% der Jahre verfügbar sind
                min_required_years = max(3, int(n_years * 0.5))  # Mindestens 3 Jahre, oder 50% der angeforderten Jahre
                if len(ev_ebit_values) >= min_required_years:
                    avg_value, count = calculate_average_filtered(ev_ebit_values, max_reasonable_value=100)
                    result[col_name] = avg_value
                    result[col_name + '_count'] = count
                else:
                    result[col_name] = None
                    result[col_name + '_count'] = None

            # === CAGR Berechnungen ===

            # Revenue CAGR
            for n_years, col_name in [(3, 'revenue_cagr_3y'), (5, 'revenue_cagr_5y'),
                                       (10, 'revenue_cagr_10y')]:
                start_year = current_year - n_years
                if start_year in fy_data_by_year:
                    start_val = fy_data_by_year[start_year]['revenue']
                    end_val = row['revenue']
                    result[col_name] = calculate_cagr(end_val, start_val, n_years)
                else:
                    result[col_name] = None

            # EBIT CAGR
            for n_years, col_name in [(3, 'ebit_cagr_3y'), (5, 'ebit_cagr_5y'),
                                       (10, 'ebit_cagr_10y')]:
                start_year = current_year - n_years
                if start_year in fy_data_by_year:
                    start_val = fy_data_by_year[start_year]['operating_income']
                    end_val = row['operating_income']
                    result[col_name] = calculate_cagr(end_val, start_val, n_years)
                else:
                    result[col_name] = None

            # Net Income CAGR
            for n_years, col_name in [(3, 'net_income_cagr_3y'), (5, 'net_income_cagr_5y'),
                                       (10, 'net_income_cagr_10y')]:
                start_year = current_year - n_years
                if start_year in fy_data_by_year:
                    start_val = fy_data_by_year[start_year]['net_income']
                    end_val = row['net_income']
                    result[col_name] = calculate_cagr(end_val, start_val, n_years)
                else:
                    result[col_name] = None
        else:
            # Für Quartale: Durchschnitte und CAGR auf None setzen
            result['pe_avg_5y'] = None
            result['pe_avg_10y'] = None
            result['pe_avg_15y'] = None
            result['pe_avg_20y'] = None
            result['pe_avg_5y_count'] = None
            result['pe_avg_10y_count'] = None
            result['pe_avg_15y_count'] = None
            result['pe_avg_20y_count'] = None
            result['ev_ebit_avg_5y'] = None
            result['ev_ebit_avg_10y'] = None
            result['ev_ebit_avg_15y'] = None
            result['ev_ebit_avg_20y'] = None
            result['ev_ebit_avg_5y_count'] = None
            result['ev_ebit_avg_10y_count'] = None
            result['ev_ebit_avg_15y_count'] = None
            result['ev_ebit_avg_20y_count'] = None
            result['revenue_cagr_3y'] = None
            result['revenue_cagr_5y'] = None
            result['revenue_cagr_10y'] = None
            result['ebit_cagr_3y'] = None
            result['ebit_cagr_5y'] = None
            result['ebit_cagr_10y'] = None
            result['net_income_cagr_3y'] = None
            result['net_income_cagr_5y'] = None
            result['net_income_cagr_10y'] = None

        # === Bilanz-Kennzahlen (für alle Perioden) ===

        # Eigenkapitalquote = Total Equity / Total Assets * 100
        equity_ratio = None
        if row['total_equity'] is not None and row['total_assets'] is not None and row['total_assets'] > 0:
            equity_ratio = (row['total_equity'] / row['total_assets']) * 100
        result['equity_ratio'] = equity_ratio

        # Net Debt / EBITDA
        net_debt_ebitda = None
        if row['net_debt'] is not None and row['ebitda'] is not None and row['ebitda'] > 0:
            net_debt_ebitda = row['net_debt'] / row['ebitda']
        result['net_debt_ebitda'] = net_debt_ebitda

        # === Margen (für alle Perioden) ===

        # Gewinnmarge = Net Income / Revenue * 100
        profit_margin = None
        if row['net_income'] is not None and row['revenue'] is not None and row['revenue'] > 0:
            profit_margin = (row['net_income'] / row['revenue']) * 100
        result['profit_margin'] = profit_margin

        # Operative Marge = Operating Income / Revenue * 100
        operating_margin = None
        if row['operating_income'] is not None and row['revenue'] is not None and row['revenue'] > 0:
            operating_margin = (row['operating_income'] / row['revenue']) * 100
        result['operating_margin'] = operating_margin

        # === Margen-Durchschnitte (nur für FY sinnvoll berechenbar) ===
        if row['period'] == 'FY':
            # Gewinnmarge Durchschnitte (rollierend)
            for n_years, col_name in [(3, 'profit_margin_avg_3y'), (5, 'profit_margin_avg_5y'),
                                       (10, 'profit_margin_avg_10y')]:
                margin_values = []
                for year in range(current_year - n_years + 1, current_year + 1):
                    if year in fy_data_by_year:
                        fy_row = fy_data_by_year[year]
                        if fy_row['net_income'] is not None and fy_row['revenue'] is not None and fy_row['revenue'] > 0:
                            margin_values.append((fy_row['net_income'] / fy_row['revenue']) * 100)
                if margin_values:
                    avg_value, _ = calculate_average_filtered(margin_values, max_reasonable_value=100)
                    result[col_name] = avg_value
                else:
                    result[col_name] = None

            # Gewinnmarge 5-Jahres-Durchschnitt 2015-2019 (fixer Zeitraum)
            profit_margin_2015_2019 = []
            for year in range(2015, 2020):
                if year in fy_data_by_year:
                    fy_row = fy_data_by_year[year]
                    if fy_row['net_income'] is not None and fy_row['revenue'] is not None and fy_row['revenue'] > 0:
                        profit_margin_2015_2019.append((fy_row['net_income'] / fy_row['revenue']) * 100)
            if profit_margin_2015_2019:
                avg_value, _ = calculate_average_filtered(profit_margin_2015_2019, max_reasonable_value=100)
                result['profit_margin_avg_5y_2019'] = avg_value
            else:
                result['profit_margin_avg_5y_2019'] = None

            # Operative Marge Durchschnitte (rollierend)
            for n_years, col_name in [(3, 'operating_margin_avg_3y'), (5, 'operating_margin_avg_5y'),
                                       (10, 'operating_margin_avg_10y')]:
                margin_values = []
                for year in range(current_year - n_years + 1, current_year + 1):
                    if year in fy_data_by_year:
                        fy_row = fy_data_by_year[year]
                        if fy_row['operating_income'] is not None and fy_row['revenue'] is not None and fy_row['revenue'] > 0:
                            margin_values.append((fy_row['operating_income'] / fy_row['revenue']) * 100)
                if margin_values:
                    avg_value, _ = calculate_average_filtered(margin_values, max_reasonable_value=100)
                    result[col_name] = avg_value
                else:
                    result[col_name] = None

            # Operative Marge 5-Jahres-Durchschnitt 2015-2019 (fixer Zeitraum)
            operating_margin_2015_2019 = []
            for year in range(2015, 2020):
                if year in fy_data_by_year:
                    fy_row = fy_data_by_year[year]
                    if fy_row['operating_income'] is not None and fy_row['revenue'] is not None and fy_row['revenue'] > 0:
                        operating_margin_2015_2019.append((fy_row['operating_income'] / fy_row['revenue']) * 100)
            if operating_margin_2015_2019:
                avg_value, _ = calculate_average_filtered(operating_margin_2015_2019, max_reasonable_value=100)
                result['operating_margin_avg_5y_2019'] = avg_value
            else:
                result['operating_margin_avg_5y_2019'] = None
        else:
            # Für Quartale: Margen-Durchschnitte auf None setzen
            result['profit_margin_avg_3y'] = None
            result['profit_margin_avg_5y'] = None
            result['profit_margin_avg_10y'] = None
            result['profit_margin_avg_5y_2019'] = None
            result['operating_margin_avg_3y'] = None
            result['operating_margin_avg_5y'] = None
            result['operating_margin_avg_10y'] = None
            result['operating_margin_avg_5y_2019'] = None

        results.append(result)

    return results


def main():
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor(dictionary=True)

        # Alle Daten aus fmp_filtered_numbers laden
        print("Lade Daten aus fmp_filtered_numbers...")
        cur.execute("""
            SELECT
                ticker, isin, stock_index, company_name, date, period,
                price, market_cap, net_debt, minority_interest,
                operating_income, ebitda, revenue, net_income, eps,
                total_assets, total_equity
            FROM analytics.fmp_filtered_numbers
            ORDER BY isin, date
        """)

        all_rows = cur.fetchall()
        print(f"  {len(all_rows):,} Datensätze geladen")

        # Gruppiere nach ISIN (ein Unternehmen kann mehrere Ticker haben)
        data_by_isin = defaultdict(list)
        for row in all_rows:
            data_by_isin[row['isin']].append(row)

        print(f"  {len(data_by_isin):,} Unternehmen gefunden")

        # Verarbeite jedes Unternehmen
        print("Berechne Kennzahlen...")
        all_results = []
        processed = 0

        for isin, rows in data_by_isin.items():
            results = process_ticker(rows)
            all_results.extend(results)
            processed += 1
            if processed % 100 == 0:
                print(f"  {processed}/{len(data_by_isin)} Unternehmen verarbeitet...")

        print(f"  {len(all_results):,} Ergebnisse berechnet")

        # Tabelle leeren und neu befüllen
        print("Lösche bestehende Daten...")
        cur.execute("TRUNCATE TABLE analytics.calcu_numbers")

        # Batch-Insert
        print("Füge berechnete Daten ein...")
        insert_sql = """
            INSERT INTO analytics.calcu_numbers (
                ticker, isin, stock_index, company_name, date, period,
                fy_pe, fy_ev_ebit, ebit, ev,
                ttm_net_income, ttm_ebit, ttm_pe, ttm_ev_ebit,
                pe_avg_5y, pe_avg_10y, pe_avg_15y, pe_avg_20y,
                pe_avg_5y_count, pe_avg_10y_count, pe_avg_15y_count, pe_avg_20y_count,
                ev_ebit_avg_5y, ev_ebit_avg_10y, ev_ebit_avg_15y, ev_ebit_avg_20y,
                ev_ebit_avg_5y_count, ev_ebit_avg_10y_count, ev_ebit_avg_15y_count, ev_ebit_avg_20y_count,
                revenue_cagr_3y, revenue_cagr_5y, revenue_cagr_10y,
                ebit_cagr_3y, ebit_cagr_5y, ebit_cagr_10y,
                net_income_cagr_3y, net_income_cagr_5y, net_income_cagr_10y,
                equity_ratio, net_debt_ebitda,
                profit_margin, operating_margin,
                profit_margin_avg_3y, profit_margin_avg_5y, profit_margin_avg_10y, profit_margin_avg_5y_2019,
                operating_margin_avg_3y, operating_margin_avg_5y, operating_margin_avg_10y, operating_margin_avg_5y_2019
            ) VALUES (
                %(ticker)s, %(isin)s, %(stock_index)s, %(company_name)s, %(date)s, %(period)s,
                %(fy_pe)s, %(fy_ev_ebit)s, %(ebit)s, %(ev)s,
                %(ttm_net_income)s, %(ttm_ebit)s, %(ttm_pe)s, %(ttm_ev_ebit)s,
                %(pe_avg_5y)s, %(pe_avg_10y)s, %(pe_avg_15y)s, %(pe_avg_20y)s,
                %(pe_avg_5y_count)s, %(pe_avg_10y_count)s, %(pe_avg_15y_count)s, %(pe_avg_20y_count)s,
                %(ev_ebit_avg_5y)s, %(ev_ebit_avg_10y)s, %(ev_ebit_avg_15y)s, %(ev_ebit_avg_20y)s,
                %(ev_ebit_avg_5y_count)s, %(ev_ebit_avg_10y_count)s, %(ev_ebit_avg_15y_count)s, %(ev_ebit_avg_20y_count)s,
                %(revenue_cagr_3y)s, %(revenue_cagr_5y)s, %(revenue_cagr_10y)s,
                %(ebit_cagr_3y)s, %(ebit_cagr_5y)s, %(ebit_cagr_10y)s,
                %(net_income_cagr_3y)s, %(net_income_cagr_5y)s, %(net_income_cagr_10y)s,
                %(equity_ratio)s, %(net_debt_ebitda)s,
                %(profit_margin)s, %(operating_margin)s,
                %(profit_margin_avg_3y)s, %(profit_margin_avg_5y)s, %(profit_margin_avg_10y)s, %(profit_margin_avg_5y_2019)s,
                %(operating_margin_avg_3y)s, %(operating_margin_avg_5y)s, %(operating_margin_avg_10y)s, %(operating_margin_avg_5y_2019)s
            )
        """

        batch_size = 5000
        for i in range(0, len(all_results), batch_size):
            batch = all_results[i:i+batch_size]
            cur.executemany(insert_sql, batch)
            print(f"  {min(i+batch_size, len(all_results)):,}/{len(all_results):,} eingefügt...")

        conn.commit()
        print(f"\nErfolgreich {len(all_results):,} Datensätze in calcu_numbers eingefügt!")

        # Statistik ausgeben
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN fy_pe IS NOT NULL THEN 1 ELSE 0 END) as fy_pe_count,
                SUM(CASE WHEN ttm_pe IS NOT NULL THEN 1 ELSE 0 END) as ttm_pe_count,
                SUM(CASE WHEN fy_ev_ebit IS NOT NULL THEN 1 ELSE 0 END) as fy_ev_ebit_count,
                SUM(CASE WHEN ttm_ev_ebit IS NOT NULL THEN 1 ELSE 0 END) as ttm_ev_ebit_count,
                SUM(CASE WHEN ev IS NOT NULL THEN 1 ELSE 0 END) as ev_count,
                SUM(CASE WHEN pe_avg_10y IS NOT NULL THEN 1 ELSE 0 END) as pe_avg_10y_count,
                SUM(CASE WHEN revenue_cagr_5y IS NOT NULL THEN 1 ELSE 0 END) as revenue_cagr_5y_count,
                SUM(CASE WHEN equity_ratio IS NOT NULL THEN 1 ELSE 0 END) as equity_ratio_count,
                SUM(CASE WHEN profit_margin IS NOT NULL THEN 1 ELSE 0 END) as profit_margin_count,
                SUM(CASE WHEN operating_margin IS NOT NULL THEN 1 ELSE 0 END) as operating_margin_count,
                SUM(CASE WHEN profit_margin_avg_5y IS NOT NULL THEN 1 ELSE 0 END) as profit_margin_avg_5y_count,
                SUM(CASE WHEN operating_margin_avg_5y IS NOT NULL THEN 1 ELSE 0 END) as operating_margin_avg_5y_count,
                SUM(CASE WHEN profit_margin_avg_5y_2019 IS NOT NULL THEN 1 ELSE 0 END) as profit_margin_avg_5y_2019_count,
                SUM(CASE WHEN operating_margin_avg_5y_2019 IS NOT NULL THEN 1 ELSE 0 END) as operating_margin_avg_5y_2019_count
            FROM analytics.calcu_numbers
        """)
        stats = cur.fetchone()
        print("\n=== Statistik ===")
        print(f"Gesamt: {stats['total']:,}")
        print(f"FY PE berechnet: {stats['fy_pe_count']:,}")
        print(f"TTM PE berechnet: {stats['ttm_pe_count']:,}")
        print(f"FY EV/EBIT berechnet: {stats['fy_ev_ebit_count']:,}")
        print(f"TTM EV/EBIT berechnet: {stats['ttm_ev_ebit_count']:,}")
        print(f"EV berechnet: {stats['ev_count']:,}")
        print(f"PE 10J-Durchschnitt: {stats['pe_avg_10y_count']:,}")
        print(f"Revenue CAGR 5J: {stats['revenue_cagr_5y_count']:,}")
        print(f"Eigenkapitalquote: {stats['equity_ratio_count']:,}")
        print(f"Gewinnmarge: {stats['profit_margin_count']:,}")
        print(f"Operative Marge: {stats['operating_margin_count']:,}")
        print(f"Gewinnmarge 5J-Durchschnitt: {stats['profit_margin_avg_5y_count']:,}")
        print(f"Operative Marge 5J-Durchschnitt: {stats['operating_margin_avg_5y_count']:,}")
        print(f"Gewinnmarge 5J-Durchschnitt (2015-2019): {stats['profit_margin_avg_5y_2019_count']:,}")
        print(f"Operative Marge 5J-Durchschnitt (2015-2019): {stats['operating_margin_avg_5y_2019_count']:,}")

    except Error as e:
        print(f"Datenbankfehler: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
