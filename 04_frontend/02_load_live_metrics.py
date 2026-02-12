#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Befüllt analytics.live_metrics mit aktuellen Kennzahlen.

Quellen:
- calcu_numbers: Aktuellste Zeile pro ISIN (Bewertungen, CAGRs, Bilanz, TTM-Fundamentals)
- calcu_numbers (2019): Historische 10-Jahres-Durchschnitte für Vor-Corona-Vergleich
- raw_data.yf_prices: Aktuellster Schlusskurs
- fmp_filtered_numbers: SharesOutstanding, Net Debt, Minority Interest für EV-Berechnung
- yfinance API: TTM PE, Forward PE, Payout Ratio, Profit/Operating Margins
- earnings_calendar (finanzen.net): Nächste Earnings-Termine (primär), yfinance als Fallback

Update-Frequenz: Täglich (Kurse ändern sich)

WICHTIG: TTM PE und TTM EV/EBIT werden hier mit dem AKTUELLEN Kurs neu berechnet!
Zusätzlich werden täglich yfinance-Metriken (yf_ttm_pe, yf_forward_pe, etc.) geladen.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from tqdm import tqdm
from mysql.connector import Error
from db import get_connection
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

# Threading Konfiguration
MAX_WORKERS = 3  # Reduziert um Rate Limiting zu vermeiden
REQUEST_DELAY = 0.5  # Sekunden zwischen Requests


def get_yf_metrics(ticker_yf):
    """Hole täglich aktualisierte Kennzahlen von yfinance API mit Retry-Logik."""
    import math
    import time

    if not ticker_yf:
        return None

    # Retry-Logik bei Rate Limiting
    max_retries = 3
    retry_delay = 2  # Sekunden

    for attempt in range(max_retries):
        try:
            # Kleines Delay um Rate Limiting zu vermeiden
            if attempt > 0:
                time.sleep(retry_delay * attempt)  # Exponential backoff

            stock = yf.Ticker(ticker_yf)
            info = stock.info

            # Prüfe ob gültige Daten zurückkamen
            if not info:
                return None

            def clean_value(value):
                """Filtere ungültige Werte (None, NaN, Infinity, extreme Werte)."""
                if value is None:
                    return None

                # String-Werte abfangen (manchmal gibt yfinance Strings zurück)
                if isinstance(value, str):
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        return None

                # Prüfe auf NaN oder Infinity
                if math.isnan(value) or math.isinf(value):
                    return None

                # Filtere extreme Werte (z.B. PE > 10000 oder < -1000)
                if abs(value) > 10000:
                    return None

                return value

            # Hilfsfunktion: Dezimalwert zu Prozent umwandeln (0.25 -> 25.0)
            def to_percent(val):
                cleaned = clean_value(val)
                return cleaned * 100 if cleaned is not None else None

            # Nächstes Earnings Date aus get_earnings_dates() extrahieren
            # Diese Methode liefert eine Liste aller bekannten Earnings-Dates
            # Wir wählen das nächste zukünftige Datum aus
            earnings_date = None
            try:
                from datetime import datetime
                ed = stock.get_earnings_dates(limit=8)  # Hole die nächsten 8
                if ed is not None and not ed.empty:
                    today = datetime.now().astimezone()
                    for dt in ed.index:
                        if dt > today:
                            earnings_date = dt.strftime('%Y-%m-%d')
                            break  # Erstes zukünftiges Datum gefunden
            except Exception:
                pass  # Earnings dates nicht verfügbar

            result = {
                "ttm_pe": clean_value(info.get("trailingPE")),
                "forward_pe": clean_value(info.get("forwardPE")),
                "payout_ratio": to_percent(info.get("payoutRatio")),
                "profit_margin": to_percent(info.get("profitMargins")),
                "operating_margin": to_percent(info.get("operatingMargins")),
                "next_earnings_date": earnings_date,
            }

            # Nur zurückgeben wenn mindestens ein Wert vorhanden ist
            if any(v is not None for v in result.values()):
                return result
            else:
                return None

        except Exception as e:
            error_msg = str(e)

            # Bei Rate Limiting: Retry
            if "Too Many Requests" in error_msg or "Rate limited" in error_msg:
                if attempt < max_retries - 1:
                    continue  # Nächster Versuch
                else:
                    return None  # Nach max_retries aufgeben

            # Bei anderen Fehlern: nicht retrien
            return None

    return None


def main():
    print("=" * 60)
    print("LIVE METRICS LADEN (Kennzahlen)")
    print("=" * 60)

    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor(dictionary=True)

        # Schritt 1: Aktuellste FY-Zeile pro ISIN aus calcu_numbers holen
        # (inkl. TTM-Fundamentaldaten für Neuberechnung mit aktuellem Kurs)
        print("\nLade aktuellste FY-Kennzahlen aus calcu_numbers...")
        cur.execute("""
            SELECT
                c.isin,
                MAX(c.ticker) as ticker,
                MAX(c.company_name) as company_name,
                MAX(c.stock_index) as stock_index,
                MAX(c.date) as date,
                AVG(c.fy_pe) as fy_pe,
                AVG(c.fy_ev_ebit) as fy_ev_ebit,
                AVG(c.ttm_net_income) as ttm_net_income,
                AVG(c.ttm_ebit) as ttm_ebit,
                AVG(c.pe_avg_5y) as pe_avg_5y,
                AVG(c.pe_avg_10y) as pe_avg_10y,
                AVG(c.pe_avg_15y) as pe_avg_15y,
                AVG(c.pe_avg_20y) as pe_avg_20y,
                MAX(c.pe_avg_5y_count) as pe_avg_5y_count,
                MAX(c.pe_avg_10y_count) as pe_avg_10y_count,
                MAX(c.pe_avg_15y_count) as pe_avg_15y_count,
                MAX(c.pe_avg_20y_count) as pe_avg_20y_count,
                AVG(c.ev_ebit_avg_5y) as ev_ebit_avg_5y,
                AVG(c.ev_ebit_avg_10y) as ev_ebit_avg_10y,
                AVG(c.ev_ebit_avg_15y) as ev_ebit_avg_15y,
                AVG(c.ev_ebit_avg_20y) as ev_ebit_avg_20y,
                MAX(c.ev_ebit_avg_5y_count) as ev_ebit_avg_5y_count,
                MAX(c.ev_ebit_avg_10y_count) as ev_ebit_avg_10y_count,
                MAX(c.ev_ebit_avg_15y_count) as ev_ebit_avg_15y_count,
                MAX(c.ev_ebit_avg_20y_count) as ev_ebit_avg_20y_count,
                AVG(c.revenue_cagr_3y) as revenue_cagr_3y,
                AVG(c.revenue_cagr_5y) as revenue_cagr_5y,
                AVG(c.revenue_cagr_10y) as revenue_cagr_10y,
                AVG(c.ebit_cagr_3y) as ebit_cagr_3y,
                AVG(c.ebit_cagr_5y) as ebit_cagr_5y,
                AVG(c.ebit_cagr_10y) as ebit_cagr_10y,
                AVG(c.net_income_cagr_3y) as net_income_cagr_3y,
                AVG(c.net_income_cagr_5y) as net_income_cagr_5y,
                AVG(c.net_income_cagr_10y) as net_income_cagr_10y,
                AVG(c.equity_ratio) as equity_ratio,
                AVG(c.net_debt_ebitda) as net_debt_ebitda,
                AVG(c.profit_margin) as profit_margin,
                AVG(c.operating_margin) as operating_margin,
                AVG(c.profit_margin_avg_3y) as profit_margin_avg_3y,
                AVG(c.profit_margin_avg_5y) as profit_margin_avg_5y,
                AVG(c.profit_margin_avg_10y) as profit_margin_avg_10y,
                AVG(c.operating_margin_avg_3y) as operating_margin_avg_3y,
                AVG(c.operating_margin_avg_5y) as operating_margin_avg_5y,
                AVG(c.operating_margin_avg_10y) as operating_margin_avg_10y
            FROM analytics.calcu_numbers c
            INNER JOIN (
                SELECT isin, MAX(date) as max_date
                FROM analytics.calcu_numbers
                WHERE period = 'FY'
                GROUP BY isin
            ) latest ON c.isin = latest.isin AND c.date = latest.max_date
            WHERE c.period = 'FY'
            GROUP BY c.isin
        """)
        latest_data = cur.fetchall()
        print(f"  -> {len(latest_data)} Ticker mit aktuellen Kennzahlen")

        # Schritt 2: Historische 2019-Durchschnitte holen (pe_avg_10y_2019, ev_ebit_avg_10y_2019, margin_avg_5y_2019)
        print("\nLade 2019er Durchschnitte (Vor-Corona)...")
        cur.execute("""
            SELECT
                isin,
                AVG(pe_avg_10y) as pe_avg_10y_2019,
                AVG(ev_ebit_avg_10y) as ev_ebit_avg_10y_2019,
                AVG(profit_margin_avg_5y_2019) as profit_margin_avg_5y_2019,
                AVG(operating_margin_avg_5y_2019) as operating_margin_avg_5y_2019
            FROM analytics.calcu_numbers
            WHERE period = 'FY'
              AND YEAR(date) = 2019
            GROUP BY isin
        """)
        data_2019 = {row["isin"]: row for row in cur.fetchall()}
        print(f"  -> {len(data_2019)} Ticker mit 2019er Daten")

        # Schritt 3: AKTUELLSTE Kurse aus raw_data.yf_prices (NICHT aus fmp!)
        print("\nLade aktuelle Kurse aus raw_data.yf_prices...")
        cur.execute("""
            SELECT
                p.isin,
                p.close as price,
                p.date as price_date
            FROM raw_data.yf_prices p
            INNER JOIN (
                SELECT isin, MAX(date) as max_date
                FROM raw_data.yf_prices
                GROUP BY isin
            ) latest ON p.isin = latest.isin AND p.date = latest.max_date
        """)
        price_data = {row["isin"]: row for row in cur.fetchall()}
        print(f"  -> {len(price_data)} Ticker mit aktuellen Kursen")

        # Schritt 4: Shares Outstanding, Net Debt, Minority Interest aus fmp_filtered_numbers
        # (für Market Cap und EV Berechnung)
        print("\nLade Shares Outstanding und Bilanz-Daten aus fmp_filtered_numbers...")
        cur.execute("""
            SELECT
                f.isin,
                MAX(f.weighted_average_shs_out) as shares_outstanding,
                AVG(f.net_debt) as net_debt,
                AVG(f.minority_interest) as minority_interest
            FROM analytics.fmp_filtered_numbers f
            INNER JOIN (
                SELECT isin, MAX(date) as max_date
                FROM analytics.fmp_filtered_numbers
                WHERE period = 'FY'
                GROUP BY isin
            ) latest ON f.isin = latest.isin AND f.date = latest.max_date
            WHERE f.period = 'FY'
            GROUP BY f.isin
        """)
        shares_data = {row["isin"]: row for row in cur.fetchall()}
        print(f"  -> {len(shares_data)} Ticker mit Shares Outstanding")

        # Schritt 4b: TTM-Werte berechnen
        # - Quartalsberichterstatter: Summe der letzten 4 Quartale (innerhalb 15 Monate)
        # - Halbjahresberichterstatter: Summe der letzten 2 Halbjahre (innerhalb 15 Monate)
        print("\nBerechne TTM-Werte (Quartals- und Halbjahresberichterstatter)...")

        # Alle nicht-FY Perioden der letzten 15 Monate pro ISIN
        cur.execute("""
            SELECT
                isin,
                date,
                period,
                AVG(net_income) as net_income,
                AVG(operating_income) as operating_income
            FROM analytics.fmp_filtered_numbers
            WHERE period != 'FY'
              AND date >= DATE_SUB(CURDATE(), INTERVAL 15 MONTH)
              AND net_income IS NOT NULL
            GROUP BY isin, date, period
            ORDER BY isin, date DESC
        """)

        from collections import defaultdict
        periods_by_isin = defaultdict(list)
        for row in cur.fetchall():
            periods_by_isin[row['isin']].append(row)

        ttm_data = {}
        quarterly_count = 0
        semiannual_count = 0

        for isin, periods in periods_by_isin.items():
            # Sortiere nach Datum absteigend (neueste zuerst)
            periods = sorted(periods, key=lambda x: x['date'], reverse=True)

            # Prüfe ob Quartals- oder Halbjahresberichterstatter
            period_types = set(p['period'] for p in periods)

            # Halbjahresberichterstatter: nur Q2/Q4 oder H1/H2
            is_semiannual = period_types.issubset({'Q2', 'Q4', 'H1', 'H2'})

            if is_semiannual and len(periods) >= 2:
                # Nimm die letzten 2 Halbjahre
                latest_periods = periods[:2]
                ni_vals = [p['net_income'] for p in latest_periods]
                ttm_ni = sum(ni_vals) if all(v is not None for v in ni_vals) else None
                ebit_vals = [p['operating_income'] for p in latest_periods]
                ttm_ebit = sum(ebit_vals) if all(v is not None for v in ebit_vals) else None
                ttm_data[isin] = {
                    'ttm_net_income': ttm_ni,
                    'ttm_ebit': ttm_ebit,
                    'period_count': 2,
                    'type': 'semiannual'
                }
                semiannual_count += 1
            elif len(periods) >= 4:
                # Quartalsberichterstatter: nimm die letzten 4 Quartale
                latest_periods = periods[:4]
                ni_vals = [p['net_income'] for p in latest_periods]
                ttm_ni = sum(ni_vals) if all(v is not None for v in ni_vals) else None
                ebit_vals = [p['operating_income'] for p in latest_periods]
                ttm_ebit = sum(ebit_vals) if all(v is not None for v in ebit_vals) else None
                ttm_data[isin] = {
                    'ttm_net_income': ttm_ni,
                    'ttm_ebit': ttm_ebit,
                    'period_count': 4,
                    'type': 'quarterly'
                }
                quarterly_count += 1

        print(f"  -> {len(ttm_data)} Ticker mit TTM-Daten")
        print(f"     - {quarterly_count} Quartalsberichterstatter (4 Quartale)")
        print(f"     - {semiannual_count} Halbjahresberichterstatter (2 Halbjahre)")

        # Schritt 4c: Nächste Earnings-Termine aus finanzen.net (earnings_calendar)
        print("\nLade nächste Earnings-Termine aus earnings_calendar (finanzen.net)...")
        cur.execute("""
            SELECT
                isin,
                MIN(release_date) as next_earnings_date
            FROM analytics.earnings_calendar
            WHERE release_date >= CURDATE()
              AND event_type = 'earnings'
            GROUP BY isin
        """)
        earnings_dates = {row["isin"]: row["next_earnings_date"] for row in cur.fetchall()}
        print(f"  -> {len(earnings_dates)} Ticker mit nächstem Earnings-Termin")

        # Schritt 4d: yfinance Metriken laden (parallel)
        print(f"\nLade yfinance Metriken (max {MAX_WORKERS} parallel)...")

        # Ticker-Mapping erstellen (ISIN -> yf_ticker)
        ticker_mapping = {}
        for row in latest_data:
            if row["ticker"]:
                ticker_mapping[row["isin"]] = row["ticker"]

        yf_metrics = {}
        failed_count = 0
        rate_limited_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(get_yf_metrics, ticker_yf): isin
                for isin, ticker_yf in ticker_mapping.items()
            }

            with tqdm(total=len(futures), desc="yfinance API") as pbar:
                for future in as_completed(futures):
                    isin = futures[future]
                    try:
                        result = future.result()
                        if result:
                            yf_metrics[isin] = result
                        else:
                            failed_count += 1
                    except Exception as e:
                        failed_count += 1
                        if "Rate limited" in str(e):
                            rate_limited_count += 1
                    pbar.update(1)

        print(f"  -> {len(yf_metrics)} Ticker mit yfinance Metriken")
        print(f"  -> {failed_count} Ticker fehlgeschlagen")
        if rate_limited_count > 0:
            print(f"  ⚠️  {rate_limited_count} davon durch Rate Limiting")

        # Schritt 4e: Beta aus eigenen Kursdaten berechnen
        print("\nBerechne Beta aus Kursdaten (wöchentlich, 3 Jahre)...")

        # Index-Zuordnung pro ISIN aus tickerlist
        isin_to_index = {}
        for row in latest_data:
            if row["stock_index"]:
                isin_to_index[row["isin"]] = row["stock_index"]

        # Wöchentliche Aktien-Kurse (letzter Handelstag pro Woche, letzte 3 Jahre)
        cur.execute("""
            SELECT isin, date, close
            FROM raw_data.yf_prices
            WHERE date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
              AND close IS NOT NULL
            ORDER BY isin, date
        """)
        from collections import defaultdict
        stock_prices_raw = defaultdict(list)
        for r in cur.fetchall():
            stock_prices_raw[r["isin"]].append((r["date"], float(r["close"])))

        # Wöchentliche Index-Kurse (letzte 3 Jahre)
        cur.execute("""
            SELECT stock_index, date, close
            FROM raw_data.index_prices
            WHERE date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
              AND close IS NOT NULL
            ORDER BY stock_index, date
        """)
        index_prices_raw = defaultdict(list)
        for r in cur.fetchall():
            index_prices_raw[r["stock_index"]].append((r["date"], float(r["close"])))

        def to_weekly(prices):
            """Resample auf wöchentlich (letzter Kurs pro Kalenderwoche)."""
            weekly = {}
            for dt, close in prices:
                week_key = dt.isocalendar()[:2]  # (year, week)
                weekly[week_key] = close  # Letzter Wert gewinnt (chronologisch sortiert)
            # Sortiert nach Woche zurückgeben
            return sorted(weekly.items())

        def calc_returns(weekly_data):
            """Berechnet wöchentliche Returns aus sortierten (week_key, close) Paaren."""
            returns = {}
            prev = None
            for week_key, close in weekly_data:
                if prev is not None:
                    returns[week_key] = (close / prev) - 1
                prev = close
            return returns

        # Index-Returns vorberechnen
        index_weekly_returns = {}
        for idx_name, prices in index_prices_raw.items():
            weekly = to_weekly(prices)
            index_weekly_returns[idx_name] = calc_returns(weekly)

        beta_data = {}
        beta_count = 0
        beta_skipped = 0

        for isin, stock_index in isin_to_index.items():
            if stock_index not in index_weekly_returns:
                beta_skipped += 1
                continue
            if isin not in stock_prices_raw:
                beta_skipped += 1
                continue

            stock_weekly = to_weekly(stock_prices_raw[isin])
            stock_returns = calc_returns(stock_weekly)
            idx_returns = index_weekly_returns[stock_index]

            # Gemeinsame Wochen finden
            common_weeks = sorted(set(stock_returns.keys()) & set(idx_returns.keys()))

            if len(common_weeks) < 52:
                beta_skipped += 1
                continue

            stock_arr = np.array([stock_returns[w] for w in common_weeks])
            index_arr = np.array([idx_returns[w] for w in common_weeks])

            var_index = np.var(index_arr, ddof=1)
            if var_index == 0:
                beta_skipped += 1
                continue

            cov = np.cov(stock_arr, index_arr, ddof=1)[0, 1]
            beta = cov / var_index

            # Plausibilitätscheck
            if beta < -2 or beta > 5 or np.isnan(beta) or np.isinf(beta):
                beta_skipped += 1
                continue

            beta_data[isin] = round(beta, 4)
            beta_count += 1

        print(f"  -> {beta_count} Ticker mit berechnetem Beta")
        print(f"  -> {beta_skipped} Ticker übersprungen (zu wenig Daten oder kein Index)")

        # Speicher freigeben
        del stock_prices_raw, index_prices_raw, index_weekly_returns

        # Schritt 5: Daten kombinieren und in live_metrics speichern
        print("\nBerechne TTM-Kennzahlen mit aktuellen Kursen und speichere...")

        # Hilfsfunktion: Prozentuale Abweichung berechnen
        def calc_diff_percent(current, average):
            """Berechnet prozentuale Abweichung: ((aktuell / durchschnitt) - 1) * 100"""
            if current is None or average is None or average == 0:
                return None
            return ((current / average) - 1) * 100

        insert_sql = """
        INSERT INTO analytics.live_metrics (
            isin, ticker, company_name, stock_index,
            price, price_date, market_cap,
            fy_pe, fy_ev_ebit, ttm_pe, ttm_ev_ebit,
            pe_avg_5y, pe_avg_10y, pe_avg_15y, pe_avg_20y, pe_avg_10y_2019,
            pe_avg_5y_count, pe_avg_10y_count, pe_avg_15y_count, pe_avg_20y_count,
            ev_ebit_avg_5y, ev_ebit_avg_10y, ev_ebit_avg_15y, ev_ebit_avg_20y, ev_ebit_avg_10y_2019,
            ev_ebit_avg_5y_count, ev_ebit_avg_10y_count, ev_ebit_avg_15y_count, ev_ebit_avg_20y_count,
            revenue_cagr_3y, revenue_cagr_5y, revenue_cagr_10y,
            ebit_cagr_3y, ebit_cagr_5y, ebit_cagr_10y,
            net_income_cagr_3y, net_income_cagr_5y, net_income_cagr_10y,
            equity_ratio, net_debt_ebitda,
            profit_margin, operating_margin,
            profit_margin_avg_3y, profit_margin_avg_5y, profit_margin_avg_10y, profit_margin_avg_5y_2019,
            operating_margin_avg_3y, operating_margin_avg_5y, operating_margin_avg_10y, operating_margin_avg_5y_2019,
            yf_ttm_pe, yf_forward_pe, yf_payout_ratio, yf_profit_margin, yf_operating_margin,
            beta,
            next_earnings_date,
            yf_ttm_pe_vs_avg_5y, yf_ttm_pe_vs_avg_10y, yf_ttm_pe_vs_avg_15y, yf_ttm_pe_vs_avg_20y, yf_ttm_pe_vs_avg_10y_2019,
            yf_fwd_pe_vs_avg_5y, yf_fwd_pe_vs_avg_10y, yf_fwd_pe_vs_avg_15y, yf_fwd_pe_vs_avg_20y, yf_fwd_pe_vs_avg_10y_2019,
            ev_ebit_vs_avg_5y, ev_ebit_vs_avg_10y, ev_ebit_vs_avg_15y, ev_ebit_vs_avg_20y, ev_ebit_vs_avg_10y_2019
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s,
            %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            ticker = VALUES(ticker),
            company_name = VALUES(company_name),
            stock_index = VALUES(stock_index),
            price = VALUES(price),
            price_date = VALUES(price_date),
            market_cap = VALUES(market_cap),
            fy_pe = VALUES(fy_pe),
            fy_ev_ebit = VALUES(fy_ev_ebit),
            ttm_pe = VALUES(ttm_pe),
            ttm_ev_ebit = VALUES(ttm_ev_ebit),
            pe_avg_5y = VALUES(pe_avg_5y),
            pe_avg_10y = VALUES(pe_avg_10y),
            pe_avg_15y = VALUES(pe_avg_15y),
            pe_avg_20y = VALUES(pe_avg_20y),
            pe_avg_10y_2019 = COALESCE(VALUES(pe_avg_10y_2019), pe_avg_10y_2019),
            pe_avg_5y_count = VALUES(pe_avg_5y_count),
            pe_avg_10y_count = VALUES(pe_avg_10y_count),
            pe_avg_15y_count = VALUES(pe_avg_15y_count),
            pe_avg_20y_count = VALUES(pe_avg_20y_count),
            ev_ebit_avg_5y = VALUES(ev_ebit_avg_5y),
            ev_ebit_avg_10y = VALUES(ev_ebit_avg_10y),
            ev_ebit_avg_15y = VALUES(ev_ebit_avg_15y),
            ev_ebit_avg_20y = VALUES(ev_ebit_avg_20y),
            ev_ebit_avg_10y_2019 = COALESCE(VALUES(ev_ebit_avg_10y_2019), ev_ebit_avg_10y_2019),
            ev_ebit_avg_5y_count = VALUES(ev_ebit_avg_5y_count),
            ev_ebit_avg_10y_count = VALUES(ev_ebit_avg_10y_count),
            ev_ebit_avg_15y_count = VALUES(ev_ebit_avg_15y_count),
            ev_ebit_avg_20y_count = VALUES(ev_ebit_avg_20y_count),
            revenue_cagr_3y = VALUES(revenue_cagr_3y),
            revenue_cagr_5y = VALUES(revenue_cagr_5y),
            revenue_cagr_10y = VALUES(revenue_cagr_10y),
            ebit_cagr_3y = VALUES(ebit_cagr_3y),
            ebit_cagr_5y = VALUES(ebit_cagr_5y),
            ebit_cagr_10y = VALUES(ebit_cagr_10y),
            net_income_cagr_3y = VALUES(net_income_cagr_3y),
            net_income_cagr_5y = VALUES(net_income_cagr_5y),
            net_income_cagr_10y = VALUES(net_income_cagr_10y),
            equity_ratio = VALUES(equity_ratio),
            net_debt_ebitda = VALUES(net_debt_ebitda),
            profit_margin = VALUES(profit_margin),
            operating_margin = VALUES(operating_margin),
            profit_margin_avg_3y = VALUES(profit_margin_avg_3y),
            profit_margin_avg_5y = VALUES(profit_margin_avg_5y),
            profit_margin_avg_10y = VALUES(profit_margin_avg_10y),
            profit_margin_avg_5y_2019 = COALESCE(VALUES(profit_margin_avg_5y_2019), profit_margin_avg_5y_2019),
            operating_margin_avg_3y = VALUES(operating_margin_avg_3y),
            operating_margin_avg_5y = VALUES(operating_margin_avg_5y),
            operating_margin_avg_10y = VALUES(operating_margin_avg_10y),
            operating_margin_avg_5y_2019 = COALESCE(VALUES(operating_margin_avg_5y_2019), operating_margin_avg_5y_2019),
            yf_ttm_pe = VALUES(yf_ttm_pe),
            yf_forward_pe = VALUES(yf_forward_pe),
            yf_payout_ratio = VALUES(yf_payout_ratio),
            yf_profit_margin = VALUES(yf_profit_margin),
            yf_operating_margin = VALUES(yf_operating_margin),
            beta = VALUES(beta),
            next_earnings_date = VALUES(next_earnings_date),
            yf_ttm_pe_vs_avg_5y = VALUES(yf_ttm_pe_vs_avg_5y),
            yf_ttm_pe_vs_avg_10y = VALUES(yf_ttm_pe_vs_avg_10y),
            yf_ttm_pe_vs_avg_15y = VALUES(yf_ttm_pe_vs_avg_15y),
            yf_ttm_pe_vs_avg_20y = VALUES(yf_ttm_pe_vs_avg_20y),
            yf_ttm_pe_vs_avg_10y_2019 = VALUES(yf_ttm_pe_vs_avg_10y_2019),
            yf_fwd_pe_vs_avg_5y = VALUES(yf_fwd_pe_vs_avg_5y),
            yf_fwd_pe_vs_avg_10y = VALUES(yf_fwd_pe_vs_avg_10y),
            yf_fwd_pe_vs_avg_15y = VALUES(yf_fwd_pe_vs_avg_15y),
            yf_fwd_pe_vs_avg_20y = VALUES(yf_fwd_pe_vs_avg_20y),
            yf_fwd_pe_vs_avg_10y_2019 = VALUES(yf_fwd_pe_vs_avg_10y_2019),
            ev_ebit_vs_avg_5y = VALUES(ev_ebit_vs_avg_5y),
            ev_ebit_vs_avg_10y = VALUES(ev_ebit_vs_avg_10y),
            ev_ebit_vs_avg_15y = VALUES(ev_ebit_vs_avg_15y),
            ev_ebit_vs_avg_20y = VALUES(ev_ebit_vs_avg_20y),
            ev_ebit_vs_avg_10y_2019 = VALUES(ev_ebit_vs_avg_10y_2019),
            updated_at = NOW()
        """

        success_count = 0
        cur_insert = conn.cursor()

        for row in tqdm(latest_data, desc="Speichern"):
            isin = row["isin"]

            # 2019er Daten hinzufügen
            hist_2019 = data_2019.get(isin, {})
            pe_avg_10y_2019 = hist_2019.get("pe_avg_10y_2019")
            ev_ebit_avg_10y_2019 = hist_2019.get("ev_ebit_avg_10y_2019")
            profit_margin_avg_5y_2019 = hist_2019.get("profit_margin_avg_5y_2019")
            operating_margin_avg_5y_2019 = hist_2019.get("operating_margin_avg_5y_2019")

            # Aktueller Kurs aus yf_prices
            prices = price_data.get(isin, {})
            price = prices.get("price")
            price_date = prices.get("price_date")

            # Shares Outstanding und Bilanz-Daten aus fmp
            shares = shares_data.get(isin, {})
            shares_outstanding = shares.get("shares_outstanding")
            net_debt = shares.get("net_debt")
            minority_interest = shares.get("minority_interest")

            # Market Cap berechnen: Kurs * Shares Outstanding
            market_cap = None
            if price is not None and shares_outstanding is not None and shares_outstanding > 0:
                market_cap = price * shares_outstanding

            # TTM-Werte aus den letzten 4 Quartalen (NICHT aus calcu_numbers FY!)
            ttm = ttm_data.get(isin, {})
            ttm_net_income = ttm.get("ttm_net_income")
            ttm_ebit = ttm.get("ttm_ebit")

            # TTM PE berechnen: Market Cap / TTM Net Income
            ttm_pe = None
            if market_cap is not None and ttm_net_income is not None and ttm_net_income > 0:
                ttm_pe = market_cap / ttm_net_income

            # EV berechnen: Market Cap + Net Debt + Minority Interest
            ev = None
            if market_cap is not None:
                ev = market_cap
                if net_debt is not None:
                    ev += net_debt
                if minority_interest is not None:
                    ev += minority_interest

            # TTM EV/EBIT berechnen: EV / TTM EBIT
            ttm_ev_ebit = None
            if ev is not None and ttm_ebit is not None and ttm_ebit > 0:
                ttm_ev_ebit = ev / ttm_ebit

            # yfinance Metriken holen
            yf_data = yf_metrics.get(isin, {})
            yf_ttm_pe = yf_data.get("ttm_pe")
            yf_forward_pe = yf_data.get("forward_pe")
            yf_payout_ratio = yf_data.get("payout_ratio")
            yf_profit_margin = yf_data.get("profit_margin")
            yf_operating_margin = yf_data.get("operating_margin")
            beta = beta_data.get(isin)

            # Nächstes Earnings-Datum: Primär aus finanzen.net, Fallback yfinance
            next_earnings_date = earnings_dates.get(isin)
            if next_earnings_date is None:
                next_earnings_date = yf_data.get("next_earnings_date")

            # Prozentuale Abweichungen berechnen
            # YF TTM PE vs. historische Durchschnitte
            yf_ttm_pe_vs_avg_5y = calc_diff_percent(yf_ttm_pe, row["pe_avg_5y"])
            yf_ttm_pe_vs_avg_10y = calc_diff_percent(yf_ttm_pe, row["pe_avg_10y"])
            yf_ttm_pe_vs_avg_15y = calc_diff_percent(yf_ttm_pe, row["pe_avg_15y"])
            yf_ttm_pe_vs_avg_20y = calc_diff_percent(yf_ttm_pe, row["pe_avg_20y"])
            yf_ttm_pe_vs_avg_10y_2019 = calc_diff_percent(yf_ttm_pe, pe_avg_10y_2019)

            # YF Forward PE vs. historische Durchschnitte
            yf_fwd_pe_vs_avg_5y = calc_diff_percent(yf_forward_pe, row["pe_avg_5y"])
            yf_fwd_pe_vs_avg_10y = calc_diff_percent(yf_forward_pe, row["pe_avg_10y"])
            yf_fwd_pe_vs_avg_15y = calc_diff_percent(yf_forward_pe, row["pe_avg_15y"])
            yf_fwd_pe_vs_avg_20y = calc_diff_percent(yf_forward_pe, row["pe_avg_20y"])
            yf_fwd_pe_vs_avg_10y_2019 = calc_diff_percent(yf_forward_pe, pe_avg_10y_2019)

            # TTM EV/EBIT vs. historische Durchschnitte
            ev_ebit_vs_avg_5y = calc_diff_percent(ttm_ev_ebit, row["ev_ebit_avg_5y"])
            ev_ebit_vs_avg_10y = calc_diff_percent(ttm_ev_ebit, row["ev_ebit_avg_10y"])
            ev_ebit_vs_avg_15y = calc_diff_percent(ttm_ev_ebit, row["ev_ebit_avg_15y"])
            ev_ebit_vs_avg_20y = calc_diff_percent(ttm_ev_ebit, row["ev_ebit_avg_20y"])
            ev_ebit_vs_avg_10y_2019 = calc_diff_percent(ttm_ev_ebit, ev_ebit_avg_10y_2019)

            try:
                cur_insert.execute(insert_sql, (
                    isin,
                    row["ticker"],
                    row["company_name"],
                    row["stock_index"],
                    price,
                    price_date,
                    market_cap,
                    row["fy_pe"],
                    row["fy_ev_ebit"],
                    ttm_pe,
                    ttm_ev_ebit,
                    row["pe_avg_5y"],
                    row["pe_avg_10y"],
                    row["pe_avg_15y"],
                    row["pe_avg_20y"],
                    pe_avg_10y_2019,
                    row["pe_avg_5y_count"],
                    row["pe_avg_10y_count"],
                    row["pe_avg_15y_count"],
                    row["pe_avg_20y_count"],
                    row["ev_ebit_avg_5y"],
                    row["ev_ebit_avg_10y"],
                    row["ev_ebit_avg_15y"],
                    row["ev_ebit_avg_20y"],
                    ev_ebit_avg_10y_2019,
                    row["ev_ebit_avg_5y_count"],
                    row["ev_ebit_avg_10y_count"],
                    row["ev_ebit_avg_15y_count"],
                    row["ev_ebit_avg_20y_count"],
                    row["revenue_cagr_3y"],
                    row["revenue_cagr_5y"],
                    row["revenue_cagr_10y"],
                    row["ebit_cagr_3y"],
                    row["ebit_cagr_5y"],
                    row["ebit_cagr_10y"],
                    row["net_income_cagr_3y"],
                    row["net_income_cagr_5y"],
                    row["net_income_cagr_10y"],
                    row["equity_ratio"],
                    row["net_debt_ebitda"],
                    row["profit_margin"],
                    row["operating_margin"],
                    row["profit_margin_avg_3y"],
                    row["profit_margin_avg_5y"],
                    row["profit_margin_avg_10y"],
                    profit_margin_avg_5y_2019,
                    row["operating_margin_avg_3y"],
                    row["operating_margin_avg_5y"],
                    row["operating_margin_avg_10y"],
                    operating_margin_avg_5y_2019,
                    yf_ttm_pe,
                    yf_forward_pe,
                    yf_payout_ratio,
                    yf_profit_margin,
                    yf_operating_margin,
                    beta,
                    next_earnings_date,
                    # YF TTM PE vs. Durchschnitte
                    yf_ttm_pe_vs_avg_5y,
                    yf_ttm_pe_vs_avg_10y,
                    yf_ttm_pe_vs_avg_15y,
                    yf_ttm_pe_vs_avg_20y,
                    yf_ttm_pe_vs_avg_10y_2019,
                    # YF Forward PE vs. Durchschnitte
                    yf_fwd_pe_vs_avg_5y,
                    yf_fwd_pe_vs_avg_10y,
                    yf_fwd_pe_vs_avg_15y,
                    yf_fwd_pe_vs_avg_20y,
                    yf_fwd_pe_vs_avg_10y_2019,
                    # EV/EBIT vs. Durchschnitte
                    ev_ebit_vs_avg_5y,
                    ev_ebit_vs_avg_10y,
                    ev_ebit_vs_avg_15y,
                    ev_ebit_vs_avg_20y,
                    ev_ebit_vs_avg_10y_2019,
                ))
                success_count += 1
            except Error as e:
                print(f"  Fehler bei {isin}: {e}")

        conn.commit()
        cur_insert.close()

        # Statistik - konsolidierte Query für alle Zähler
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN ttm_pe IS NOT NULL THEN 1 ELSE 0 END) as with_ttm_pe,
                SUM(CASE WHEN fy_pe IS NOT NULL THEN 1 ELSE 0 END) as with_fy_pe,
                SUM(CASE WHEN pe_avg_10y_2019 IS NOT NULL THEN 1 ELSE 0 END) as with_2019,
                SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) as with_price,
                SUM(CASE WHEN market_cap IS NOT NULL THEN 1 ELSE 0 END) as with_market_cap,
                SUM(CASE WHEN yf_ttm_pe IS NOT NULL THEN 1 ELSE 0 END) as with_yf_ttm_pe,
                SUM(CASE WHEN yf_forward_pe IS NOT NULL THEN 1 ELSE 0 END) as with_yf_forward_pe,
                SUM(CASE WHEN yf_profit_margin IS NOT NULL THEN 1 ELSE 0 END) as with_yf_profit_margin,
                SUM(CASE WHEN beta IS NOT NULL THEN 1 ELSE 0 END) as with_beta,
                SUM(CASE WHEN next_earnings_date IS NOT NULL THEN 1 ELSE 0 END) as with_next_earnings,
                MAX(price_date) as latest_price_date
            FROM analytics.live_metrics
        """)
        stats = cur.fetchone()
        total = stats["total"]
        with_ttm_pe = stats["with_ttm_pe"]
        with_fy_pe = stats["with_fy_pe"]
        with_2019 = stats["with_2019"]
        with_price = stats["with_price"]
        with_market_cap = stats["with_market_cap"]
        with_yf_ttm_pe = stats["with_yf_ttm_pe"]
        with_yf_forward_pe = stats["with_yf_forward_pe"]
        with_yf_profit_margin = stats["with_yf_profit_margin"]
        with_beta = stats["with_beta"]
        with_next_earnings = stats["with_next_earnings"]
        latest_price_date = stats["latest_price_date"]

        cur.execute("""
            SELECT stock_index, COUNT(*) as cnt
            FROM analytics.live_metrics
            GROUP BY stock_index
            ORDER BY cnt DESC
        """)
        index_stats = cur.fetchall()

        print(f"\nGesamt Eintraege:        {total:,}")
        print(f"Mit aktuellem Kurs:      {with_price:,} ({with_price*100/total:.1f}%)" if total > 0 else "")
        print(f"Mit Market Cap:          {with_market_cap:,} ({with_market_cap*100/total:.1f}%)" if total > 0 else "")
        print(f"Mit TTM PE (berechnet):  {with_ttm_pe:,} ({with_ttm_pe*100/total:.1f}%)" if total > 0 else "")
        print(f"Mit FY PE:               {with_fy_pe:,} ({with_fy_pe*100/total:.1f}%)" if total > 0 else "")
        print(f"Mit 2019er Daten:        {with_2019:,} ({with_2019*100/total:.1f}%)" if total > 0 else "")
        print(f"\nYFinance API Metriken:")
        print(f"Mit yf TTM PE:           {with_yf_ttm_pe:,} ({with_yf_ttm_pe*100/total:.1f}%)" if total > 0 else "")
        print(f"Mit yf Forward PE:       {with_yf_forward_pe:,} ({with_yf_forward_pe*100/total:.1f}%)" if total > 0 else "")
        print(f"Mit yf Profit Margin:    {with_yf_profit_margin:,} ({with_yf_profit_margin*100/total:.1f}%)" if total > 0 else "")
        print(f"Mit Beta:                {with_beta:,} ({with_beta*100/total:.1f}%)" if total > 0 else "")
        print(f"\nfinanzen.net Daten:")
        print(f"Mit nächstem Earnings:   {with_next_earnings:,} ({with_next_earnings*100/total:.1f}%)" if total > 0 else "")
        print(f"  (Quelle: earnings_calendar, Fallback: yfinance)")
        print(f"\nAktuellstes Kursdatum:   {latest_price_date}")

        print("\nNach Index:")
        for row in index_stats:
            print(f"   {row['stock_index']:20} {row['cnt']:>6,}")

    except Error as e:
        print(f"\nDatenbankfehler: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    print("\nFertig.")


if __name__ == "__main__":
    main()
