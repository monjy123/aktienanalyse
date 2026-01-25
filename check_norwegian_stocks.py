#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prüfe alle norwegischen Aktien auf ähnliche Datenfehler wie bei Yara
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

conn = get_connection()
cur = conn.cursor(dictionary=True)

# Alle norwegischen Aktien finden
cur.execute("""
    SELECT DISTINCT ticker, company_name, isin
    FROM raw_data.fmp_financial_statements
    WHERE ticker LIKE '%.OL'
    ORDER BY ticker
""")
norwegian_stocks = cur.fetchall()

print(f"=== Gefunden: {len(norwegian_stocks)} norwegische Aktien (.OL) ===\n")

problematic_stocks = []

for stock in norwegian_stocks:
    ticker = stock['ticker']
    company_name = stock['company_name']
    isin = stock['isin']

    # Hole die letzten 5 Jahre FY Daten
    cur.execute("""
        SELECT date, YEAR(date) as year, period,
               revenue, net_income, operating_income, eps,
               weighted_average_shs_out
        FROM raw_data.fmp_financial_statements
        WHERE ticker = %s AND period = 'FY'
        ORDER BY date DESC
        LIMIT 5
    """, (ticker,))

    years_data = cur.fetchall()

    if len(years_data) < 2:
        continue

    # Prüfe auf extreme Sprünge oder verdächtig niedrige absolute Werte
    issues = []

    for i in range(len(years_data) - 1):
        current = years_data[i]
        previous = years_data[i + 1]

        current_year = current['year']
        prev_year = previous['year']

        # Net Income Prüfung
        if current['net_income'] and previous['net_income']:
            if abs(previous['net_income']) > 0:
                ni_ratio = abs(current['net_income']) / abs(previous['net_income'])

                # Verdächtig: Sprung um Faktor 50+ oder 1/50
                if ni_ratio > 50 or ni_ratio < 0.02:
                    issues.append(f"Net Income Sprung {prev_year}→{current_year}: {ni_ratio:.1f}x")

        # Operating Income Prüfung
        if current['operating_income'] and previous['operating_income']:
            if abs(previous['operating_income']) > 0:
                oi_ratio = abs(current['operating_income']) / abs(previous['operating_income'])

                # Verdächtig: Sprung um Faktor 50+ oder 1/50
                if oi_ratio > 50 or oi_ratio < 0.02:
                    issues.append(f"Operating Income Sprung {prev_year}→{current_year}: {oi_ratio:.1f}x")

        # EPS Prüfung (ähnliche Logik)
        if current['eps'] and previous['eps']:
            if abs(previous['eps']) > 0.01:  # Nur wenn EPS nicht nahe 0
                eps_ratio = abs(current['eps']) / abs(previous['eps'])

                if eps_ratio > 50 or eps_ratio < 0.02:
                    issues.append(f"EPS Sprung {prev_year}→{current_year}: {eps_ratio:.1f}x")

    # Prüfe auch auf verdächtig niedrige EPS Werte bei hoher Revenue
    latest = years_data[0]
    if latest['revenue'] and latest['eps'] and latest['revenue'] > 1_000_000_000:  # Revenue > 1 Mrd
        if abs(latest['eps']) < 1.0:  # EPS < 1 bei > 1 Mrd Revenue ist verdächtig
            # Berechne was EPS sein sollte
            if latest['weighted_average_shs_out'] and latest['net_income']:
                expected_eps = latest['net_income'] / latest['weighted_average_shs_out']
                if abs(expected_eps - latest['eps']) / max(abs(expected_eps), abs(latest['eps'])) > 10:  # >10x Unterschied
                    issues.append(f"EPS {latest['year']}: {latest['eps']:.2f} (erwartet ~{expected_eps:.2f})")

    if issues:
        problematic_stocks.append({
            'ticker': ticker,
            'company_name': company_name,
            'isin': isin,
            'issues': issues,
            'latest_data': latest
        })

print(f"\n=== Problematische Aktien: {len(problematic_stocks)} ===\n")

for stock in problematic_stocks:
    print(f"\n{stock['ticker']} - {stock['company_name']}")
    print(f"ISIN: {stock['isin']}")
    print(f"Neueste Daten ({stock['latest_data']['year']}):")
    print(f"  Revenue: {stock['latest_data']['revenue']:,.0f} NOK" if stock['latest_data']['revenue'] else "  Revenue: None")
    print(f"  Net Income: {stock['latest_data']['net_income']:,.0f} NOK" if stock['latest_data']['net_income'] else "  Net Income: None")
    print(f"  Operating Income: {stock['latest_data']['operating_income']:,.0f} NOK" if stock['latest_data']['operating_income'] else "  Operating Income: None")
    print(f"  EPS: {stock['latest_data']['eps']}")
    print(f"\nProbleme:")
    for issue in stock['issues']:
        print(f"  - {issue}")

# Detaillierte Ausgabe für die ersten 3 problematischen Aktien
print(f"\n\n=== Detaillierte Daten für erste 3 problematische Aktien ===\n")

for stock in problematic_stocks[:3]:
    ticker = stock['ticker']
    print(f"\n{'='*60}")
    print(f"{ticker} - {stock['company_name']}")
    print(f"{'='*60}")

    cur.execute("""
        SELECT YEAR(date) as year, date, period,
               revenue, net_income, operating_income, eps
        FROM raw_data.fmp_financial_statements
        WHERE ticker = %s AND period = 'FY'
        ORDER BY date DESC
        LIMIT 5
    """, (ticker,))

    for row in cur.fetchall():
        print(f"\n{row['year']} ({row['date']}):")
        print(f"  Revenue: {row['revenue']:,.0f}" if row['revenue'] else "  Revenue: None")
        print(f"  Net Income: {row['net_income']:,.0f}" if row['net_income'] else "  Net Income: None")
        print(f"  Operating Income: {row['operating_income']:,.0f}" if row['operating_income'] else "  Operating Income: None")
        print(f"  EPS: {row['eps']}")

cur.close()
conn.close()

print(f"\n\n=== Zusammenfassung ===")
print(f"Geprüfte norwegische Aktien: {len(norwegian_stocks)}")
print(f"Problematische Aktien gefunden: {len(problematic_stocks)}")
if problematic_stocks:
    print(f"\nBetroffene Ticker:")
    for stock in problematic_stocks:
        print(f"  - {stock['ticker']}: {stock['company_name']}")
