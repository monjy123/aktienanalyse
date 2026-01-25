#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnose-Script für DSM Firmenich (DSFIR.AS)
Zeigt welche Daten in yf_prices und fmp_filtered_numbers vorhanden sind
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

def diagnose_dsfir():
    try:
        # Verbindung zu raw_data
        conn_raw = get_connection(db_name="raw_data")
        cur_raw = conn_raw.cursor(dictionary=True)

        # Verbindung zu analytics
        conn_analytics = get_connection(db_name="analytics")
        cur_analytics = conn_analytics.cursor(dictionary=True)

        # ISIN für DSFIR.AS finden
        cur_analytics.execute("""
            SELECT DISTINCT isin, ticker, company_name
            FROM fmp_filtered_numbers
            WHERE ticker = 'DSFIR.AS'
            LIMIT 1
        """)

        ticker_info = cur_analytics.fetchone()
        if not ticker_info:
            print("❌ DSFIR.AS nicht in fmp_filtered_numbers gefunden")
            return

        isin = ticker_info['isin']
        company_name = ticker_info['company_name']
        print(f"✅ Gefunden: {company_name}")
        print(f"   ISIN: {isin}")
        print(f"   Ticker: DSFIR.AS\n")

        # 1. Prüfe yf_prices
        print("=" * 70)
        print("1. HISTORISCHE PREISDATEN (raw_data.yf_prices)")
        print("=" * 70)

        cur_raw.execute("""
            SELECT
                YEAR(date) AS jahr,
                COUNT(*) AS anzahl_tage,
                MIN(date) AS erste_datum,
                MAX(date) AS letzte_datum,
                MIN(close) AS min_preis,
                MAX(close) AS max_preis,
                AVG(close) AS avg_preis
            FROM yf_prices
            WHERE isin = %s
            GROUP BY YEAR(date)
            ORDER BY jahr DESC
        """, (isin,))

        price_data = cur_raw.fetchall()
        if price_data:
            print(f"Preisdaten für {len(price_data)} Jahre gefunden:\n")
            print(f"{'Jahr':<8} {'Tage':<8} {'Von':<12} {'Bis':<12} {'Avg Preis':<12}")
            print("-" * 70)
            for row in price_data:
                print(f"{row['jahr']:<8} {row['anzahl_tage']:<8} "
                      f"{str(row['erste_datum']):<12} {str(row['letzte_datum']):<12} "
                      f"{row['avg_preis']:.2f}")
        else:
            print("❌ Keine Preisdaten in yf_prices gefunden!\n")

        # 2. Prüfe fmp_filtered_numbers
        print("\n" + "=" * 70)
        print("2. FUNDAMENTALDATEN MIT PREISEN (analytics.fmp_filtered_numbers)")
        print("=" * 70)

        cur_analytics.execute("""
            SELECT
                date,
                period,
                price,
                avg_price,
                market_cap,
                eps,
                net_income,
                revenue,
                weighted_average_shs_out,
                CASE
                    WHEN price IS NOT NULL AND eps IS NOT NULL AND eps > 0
                    THEN price / eps
                    ELSE NULL
                END AS kgv_calculated
            FROM fmp_filtered_numbers
            WHERE isin = %s AND period = 'FY'
            ORDER BY date DESC
        """, (isin,))

        filtered_data = cur_analytics.fetchall()
        if filtered_data:
            print(f"\nFundamentaldaten für {len(filtered_data)} Jahre gefunden:\n")
            print(f"{'Datum':<12} {'Periode':<8} {'Price':<10} {'EPS':<10} {'KGV':<10} {'Market Cap':<15} {'Revenue':<15}")
            print("-" * 95)
            for row in filtered_data:
                price_str = f"{row['price']:.2f}" if row['price'] else "NULL"
                eps_str = f"{row['eps']:.2f}" if row['eps'] else "NULL"
                kgv_str = f"{row['kgv_calculated']:.2f}" if row['kgv_calculated'] else "NULL"
                mcap_str = f"{row['market_cap']/1e9:.2f}B" if row['market_cap'] else "NULL"
                revenue_str = f"{row['revenue']/1e9:.2f}B" if row['revenue'] else "NULL"

                print(f"{str(row['date']):<12} {row['period']:<8} {price_str:<10} {eps_str:<10} "
                      f"{kgv_str:<10} {mcap_str:<15} {revenue_str:<15}")
        else:
            print("❌ Keine Daten in fmp_filtered_numbers gefunden!\n")

        # 3. Zusammenfassung
        print("\n" + "=" * 70)
        print("3. ZUSAMMENFASSUNG")
        print("=" * 70)

        years_with_price = sum(1 for row in filtered_data if row['price'] is not None)
        years_with_eps = sum(1 for row in filtered_data if row['eps'] is not None and row['eps'] > 0)
        years_with_kgv = sum(1 for row in filtered_data if row['price'] is not None and row['eps'] is not None and row['eps'] > 0)

        print(f"Fundamentaldaten (FY): {len(filtered_data)} Jahre")
        print(f"  - Mit Preis:         {years_with_price} Jahre")
        print(f"  - Mit EPS > 0:       {years_with_eps} Jahre")
        print(f"  - KGV berechenbar:   {years_with_kgv} Jahre")
        print(f"\nPreisdaten in yf_prices: {len(price_data)} Jahre")

        if years_with_kgv < 5:
            print(f"\n⚠️  PROBLEM: Nur {years_with_kgv} Jahre haben Price UND EPS!")
            print("   → Alle KGV-Durchschnitte (5J/10J/15J/20J) verwenden die gleichen Datenpunkte")
            print("   → Daher sind alle Durchschnitte identisch!")

        cur_raw.close()
        conn_raw.close()
        cur_analytics.close()
        conn_analytics.close()

    except Exception as e:
        print(f"❌ Fehler: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    diagnose_dsfir()
