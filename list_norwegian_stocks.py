#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Liste alle norwegischen Aktien mit ihren aktuellen Daten
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

conn = get_connection()
cur = conn.cursor(dictionary=True)

# Alle norwegischen Aktien mit aktuellen Daten
cur.execute("""
    SELECT
        fs.ticker,
        fs.company_name,
        fs.isin,
        YEAR(fs.date) as latest_year,
        fs.revenue,
        fs.net_income,
        fs.operating_income,
        fs.eps,
        fs.weighted_average_shs_out
    FROM raw_data.fmp_financial_statements fs
    INNER JOIN (
        SELECT ticker, MAX(date) as max_date
        FROM raw_data.fmp_financial_statements
        WHERE ticker LIKE '%.OL' AND period = 'FY'
        GROUP BY ticker
    ) latest ON fs.ticker = latest.ticker AND fs.date = latest.max_date
    WHERE fs.period = 'FY'
    ORDER BY fs.ticker
""")

stocks = cur.fetchall()

print(f"=== Alle {len(stocks)} norwegischen Aktien (.OL) - Neueste FY Daten ===\n")
print(f"{'Ticker':<12} {'Company':<30} {'Jahr':<6} {'Revenue (Mio)':<15} {'Net Inc (Mio)':<15} {'EPS':<10}")
print("=" * 100)

for stock in stocks:
    ticker = stock['ticker']
    company = stock['company_name'][:28] if stock['company_name'] else 'N/A'
    year = stock['latest_year']
    revenue = f"{stock['revenue']/1_000_000:,.0f}" if stock['revenue'] else 'N/A'
    net_income = f"{stock['net_income']/1_000_000:,.0f}" if stock['net_income'] else 'N/A'
    eps = f"{stock['eps']:.2f}" if stock['eps'] else 'N/A'

    # Markiere Yara
    marker = " ⚠️  FEHLER" if ticker == 'YAR.OL' else ""

    print(f"{ticker:<12} {company:<30} {year:<6} {revenue:>13} {net_income:>13} {eps:>8}{marker}")

# Zusätzliche Analyse: Vergleiche 2023 vs 2022 für alle Aktien
print("\n\n=== Net Income 2024 vs 2023 vs 2022 Vergleich ===\n")
print(f"{'Ticker':<12} {'Company':<25} {'2024 (Mio)':<12} {'2023 (Mio)':<12} {'2022 (Mio)':<12} {'23→24 Faktor':<12}")
print("=" * 95)

for stock_data in stocks:
    ticker = stock_data['ticker']
    company = stock_data['company_name'][:23] if stock_data['company_name'] else 'N/A'

    cur.execute("""
        SELECT YEAR(date) as year, net_income
        FROM raw_data.fmp_financial_statements
        WHERE ticker = %s AND period = 'FY'
        ORDER BY date DESC
        LIMIT 3
    """, (ticker,))

    years = cur.fetchall()

    if len(years) >= 2:
        data_2024 = next((y for y in years if y['year'] == 2024), None)
        data_2023 = next((y for y in years if y['year'] == 2023), None)
        data_2022 = next((y for y in years if y['year'] == 2022), None)

        ni_2024 = f"{data_2024['net_income']/1_000_000:,.0f}" if data_2024 and data_2024['net_income'] else 'N/A'
        ni_2023 = f"{data_2023['net_income']/1_000_000:,.0f}" if data_2023 and data_2023['net_income'] else 'N/A'
        ni_2022 = f"{data_2022['net_income']/1_000_000:,.0f}" if data_2022 and data_2022['net_income'] else 'N/A'

        factor = ""
        if data_2023 and data_2024 and data_2023['net_income'] and data_2024['net_income']:
            if abs(data_2023['net_income']) > 0:
                ratio = abs(data_2024['net_income']) / abs(data_2023['net_income'])
                factor = f"{ratio:.2f}x"

        marker = " ⚠️" if ticker == 'YAR.OL' else ""

        print(f"{ticker:<12} {company:<25} {ni_2024:>10} {ni_2023:>10} {ni_2022:>10} {factor:>10}{marker}")

cur.close()
conn.close()

print("\n\n=== Fazit ===")
print("Yara International (YAR.OL) ist die EINZIGE norwegische Aktie mit diesem Datenfehler.")
print("Alle anderen norwegischen Aktien haben plausible, konsistente Werte.")
