#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analysiere Yara International Daten
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from db import get_connection

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # Stammdaten
    cur.execute("""
        SELECT ticker, company_name, currency, isin
        FROM analytics.company_info
        WHERE ticker = 'yar.ol'
    """)
    print('=== Stammdaten ===')
    company_data = cur.fetchone()
    if company_data:
        for key, value in company_data.items():
            print(f'{key}: {value}')
        isin = company_data['isin']
    else:
        print("Keine Daten für yar.ol gefunden!")
        sys.exit(1)

    # Live Metrics PE-Daten
    cur.execute("""
        SELECT ttm_pe, fy_pe,
               pe_avg_5y, pe_avg_10y, pe_avg_15y, pe_avg_20y,
               pe_avg_5y_count, pe_avg_10y_count, pe_avg_15y_count, pe_avg_20y_count,
               ttm_ev_ebit, fy_ev_ebit,
               ev_ebit_avg_5y, ev_ebit_avg_10y, ev_ebit_avg_15y, ev_ebit_avg_20y,
               ev_ebit_avg_5y_count, ev_ebit_avg_10y_count, ev_ebit_avg_15y_count, ev_ebit_avg_20y_count,
               market_cap
        FROM analytics.live_metrics
        WHERE isin = %s
    """, (isin,))
    print('\n=== Live Metrics ===')
    metrics = cur.fetchone()
    if metrics:
        print('\n--- KGV Werte ---')
        print(f'TTM KGV: {metrics["ttm_pe"]}')
        print(f'FY KGV: {metrics["fy_pe"]}')
        print(f'KGV Ø 5J: {metrics["pe_avg_5y"]} (n={metrics["pe_avg_5y_count"]})')
        print(f'KGV Ø 10J: {metrics["pe_avg_10y"]} (n={metrics["pe_avg_10y_count"]})')
        print(f'KGV Ø 15J: {metrics["pe_avg_15y"]} (n={metrics["pe_avg_15y_count"]})')
        print(f'KGV Ø 20J: {metrics["pe_avg_20y"]} (n={metrics["pe_avg_20y_count"]})')

        print('\n--- EV/EBIT Werte ---')
        print(f'TTM EV/EBIT: {metrics["ttm_ev_ebit"]}')
        print(f'FY EV/EBIT: {metrics["fy_ev_ebit"]}')
        print(f'EV/EBIT Ø 5J: {metrics["ev_ebit_avg_5y"]} (n={metrics["ev_ebit_avg_5y_count"]})')
        print(f'EV/EBIT Ø 10J: {metrics["ev_ebit_avg_10y"]} (n={metrics["ev_ebit_avg_10y_count"]})')
        print(f'EV/EBIT Ø 15J: {metrics["ev_ebit_avg_15y"]} (n={metrics["ev_ebit_avg_15y_count"]})')
        print(f'EV/EBIT Ø 20J: {metrics["ev_ebit_avg_20y"]} (n={metrics["ev_ebit_avg_20y_count"]})')

        print(f'\n--- Sonstiges ---')
        print(f'Market Cap: {metrics["market_cap"]:,.0f}')
    else:
        print("Keine Metrics gefunden!")

    # Historische KGV-Daten aus fmp_filtered_numbers für bessere Detailanalyse
    cur.execute("""
        SELECT YEAR(date) as year, date, period, price, eps, market_cap, net_income,
               operating_income, revenue
        FROM analytics.fmp_filtered_numbers
        WHERE isin = %s AND period = 'FY'
        ORDER BY date DESC
        LIMIT 20
    """, (isin,))
    print('\n=== Historische Daten (fmp_filtered_numbers) ===')
    for row in cur.fetchall():
        pe = None
        if row['price'] and row['eps'] and row['eps'] > 0:
            pe = row['price'] / row['eps']
        elif row['market_cap'] and row['net_income'] and row['net_income'] > 0:
            pe = row['market_cap'] / row['net_income']

        ev_ebit = None
        if row['market_cap'] and row['operating_income'] and row['operating_income'] > 0:
            ev_ebit = row['market_cap'] / row['operating_income']

        print(f"\n{row['year']} ({row['date']}):")
        print(f"  Market Cap: {row['market_cap']:,.0f} NOK")
        print(f"  Price: {row['price']}, EPS: {row['eps']}")
        print(f"  Net Income: {row['net_income']:,.0f} NOK")
        print(f"  Operating Income (EBIT): {row['operating_income']:,.0f} NOK")
        print(f"  Revenue: {row['revenue']:,.0f} NOK")
        if pe:
            print(f"  KGV: {pe:.2f}")
        if ev_ebit:
            print(f"  EV/EBIT (simple): {ev_ebit:.2f}")

    cur.close()
    conn.close()

except Exception as e:
    print(f"Fehler: {e}")
    import traceback
    traceback.print_exc()
