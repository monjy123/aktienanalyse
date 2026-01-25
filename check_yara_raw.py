#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prüfe rohe FMP Daten für Yara
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

conn = get_connection()
cur = conn.cursor(dictionary=True)

# Yara aus fmp_financial_statements
cur.execute("""
    SELECT date, period,
           revenue, net_income, operating_income, eps,
           weighted_average_shs_out
    FROM raw_data.fmp_financial_statements
    WHERE ticker = 'YAR.OL' AND period = 'FY'
    ORDER BY date DESC
    LIMIT 5
""")

print("=== Rohdaten aus fmp_financial_statements ===")
for row in cur.fetchall():
    print(f"\n{row['date']} ({row['period']})")
    print(f"  Revenue: {row['revenue']:,.0f}" if row['revenue'] else "  Revenue: None")
    print(f"  Net Income: {row['net_income']:,.0f}" if row['net_income'] else "  Net Income: None")
    print(f"  Operating Income: {row['operating_income']:,.0f}" if row['operating_income'] else "  Operating Income: None")
    print(f"  EPS: {row['eps']}")
    print(f"  Shares Outstanding: {row['weighted_average_shs_out']:,.0f}" if row['weighted_average_shs_out'] else "  Shares: None")

cur.close()
conn.close()
