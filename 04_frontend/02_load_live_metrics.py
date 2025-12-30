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

            result = {
                "ttm_pe": clean_value(info.get("trailingPE")),
                "forward_pe": clean_value(info.get("forwardPE")),
                "payout_ratio": to_percent(info.get("payoutRatio")),
                "profit_margin": to_percent(info.get("profitMargins")),
                "operating_margin": to_percent(info.get("operatingMargins")),
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
                c.ticker,
                c.company_name,
                c.stock_index,
                c.date,
                c.fy_pe,
                c.fy_ev_ebit,
                c.ttm_net_income,
                c.ttm_ebit,
                c.pe_avg_5y,
                c.pe_avg_10y,
                c.pe_avg_15y,
                c.pe_avg_20y,
                c.ev_ebit_avg_5y,
                c.ev_ebit_avg_10y,
                c.ev_ebit_avg_15y,
                c.ev_ebit_avg_20y,
                c.revenue_cagr_3y,
                c.revenue_cagr_5y,
                c.revenue_cagr_10y,
                c.ebit_cagr_3y,
                c.ebit_cagr_5y,
                c.ebit_cagr_10y,
                c.net_income_cagr_3y,
                c.net_income_cagr_5y,
                c.net_income_cagr_10y,
                c.equity_ratio,
                c.net_debt_ebitda,
                c.profit_margin,
                c.operating_margin,
                c.profit_margin_avg_3y,
                c.profit_margin_avg_5y,
                c.profit_margin_avg_10y,
                c.operating_margin_avg_3y,
                c.operating_margin_avg_5y,
                c.operating_margin_avg_10y
            FROM analytics.calcu_numbers c
            INNER JOIN (
                SELECT isin, MAX(date) as max_date
                FROM analytics.calcu_numbers
                WHERE period = 'FY'
                GROUP BY isin
            ) latest ON c.isin = latest.isin AND c.date = latest.max_date
            WHERE c.period = 'FY'
        """)
        latest_data = cur.fetchall()
        print(f"  -> {len(latest_data)} Ticker mit aktuellen Kennzahlen")

        # Schritt 2: Historische 2019-Durchschnitte holen (pe_avg_10y_2019, ev_ebit_avg_10y_2019, margin_avg_5y_2019)
        print("\nLade 2019er Durchschnitte (Vor-Corona)...")
        cur.execute("""
            SELECT
                isin,
                pe_avg_10y as pe_avg_10y_2019,
                ev_ebit_avg_10y as ev_ebit_avg_10y_2019,
                profit_margin_avg_5y_2019,
                operating_margin_avg_5y_2019
            FROM analytics.calcu_numbers
            WHERE period = 'FY'
              AND YEAR(date) = 2019
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
                f.weighted_average_shs_out as shares_outstanding,
                f.net_debt,
                f.minority_interest
            FROM analytics.fmp_filtered_numbers f
            INNER JOIN (
                SELECT isin, MAX(date) as max_date
                FROM analytics.fmp_filtered_numbers
                WHERE period = 'FY'
                GROUP BY isin
            ) latest ON f.isin = latest.isin AND f.date = latest.max_date
            WHERE f.period = 'FY'
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
                net_income,
                operating_income
            FROM analytics.fmp_filtered_numbers
            WHERE period != 'FY'
              AND date >= DATE_SUB(CURDATE(), INTERVAL 15 MONTH)
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
                ttm_ni = sum(p['net_income'] or 0 for p in latest_periods)
                ttm_ebit = sum(p['operating_income'] or 0 for p in latest_periods)
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
                ttm_ni = sum(p['net_income'] or 0 for p in latest_periods)
                ttm_ebit = sum(p['operating_income'] or 0 for p in latest_periods)
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

        # Schritt 4c: yfinance Metriken laden (parallel)
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
            ev_ebit_avg_5y, ev_ebit_avg_10y, ev_ebit_avg_15y, ev_ebit_avg_20y, ev_ebit_avg_10y_2019,
            revenue_cagr_3y, revenue_cagr_5y, revenue_cagr_10y,
            ebit_cagr_3y, ebit_cagr_5y, ebit_cagr_10y,
            net_income_cagr_3y, net_income_cagr_5y, net_income_cagr_10y,
            equity_ratio, net_debt_ebitda,
            profit_margin, operating_margin,
            profit_margin_avg_3y, profit_margin_avg_5y, profit_margin_avg_10y, profit_margin_avg_5y_2019,
            operating_margin_avg_3y, operating_margin_avg_5y, operating_margin_avg_10y, operating_margin_avg_5y_2019,
            yf_ttm_pe, yf_forward_pe, yf_payout_ratio, yf_profit_margin, yf_operating_margin,
            yf_ttm_pe_vs_avg_5y, yf_ttm_pe_vs_avg_10y, yf_ttm_pe_vs_avg_15y, yf_ttm_pe_vs_avg_20y, yf_ttm_pe_vs_avg_10y_2019,
            yf_fwd_pe_vs_avg_5y, yf_fwd_pe_vs_avg_10y, yf_fwd_pe_vs_avg_15y, yf_fwd_pe_vs_avg_20y, yf_fwd_pe_vs_avg_10y_2019,
            ev_ebit_vs_avg_5y, ev_ebit_vs_avg_10y, ev_ebit_vs_avg_15y, ev_ebit_vs_avg_20y, ev_ebit_vs_avg_10y_2019
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
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
            ev_ebit_avg_5y = VALUES(ev_ebit_avg_5y),
            ev_ebit_avg_10y = VALUES(ev_ebit_avg_10y),
            ev_ebit_avg_15y = VALUES(ev_ebit_avg_15y),
            ev_ebit_avg_20y = VALUES(ev_ebit_avg_20y),
            ev_ebit_avg_10y_2019 = COALESCE(VALUES(ev_ebit_avg_10y_2019), ev_ebit_avg_10y_2019),
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
                    row["ev_ebit_avg_5y"],
                    row["ev_ebit_avg_10y"],
                    row["ev_ebit_avg_15y"],
                    row["ev_ebit_avg_20y"],
                    ev_ebit_avg_10y_2019,
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

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics")
        total = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE ttm_pe IS NOT NULL")
        with_ttm_pe = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE fy_pe IS NOT NULL")
        with_fy_pe = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE pe_avg_10y_2019 IS NOT NULL")
        with_2019 = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE price IS NOT NULL")
        with_price = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE market_cap IS NOT NULL")
        with_market_cap = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE yf_ttm_pe IS NOT NULL")
        with_yf_ttm_pe = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE yf_forward_pe IS NOT NULL")
        with_yf_forward_pe = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM analytics.live_metrics WHERE yf_profit_margin IS NOT NULL")
        with_yf_profit_margin = cur.fetchone()["cnt"]

        cur.execute("SELECT MAX(price_date) as max_date FROM analytics.live_metrics")
        latest_price_date = cur.fetchone()["max_date"]

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
