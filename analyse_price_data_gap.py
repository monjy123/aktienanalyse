#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analysiert für alle Aktien:
- Wie viele Jahre Fundamentaldaten vorhanden sind
- Wie viele Jahre Price-Daten vorhanden sind
- Bei wie vielen Aktien eine Diskrepanz besteht
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

def analyze_price_data_gap():
    try:
        conn = get_connection(db_name="analytics")
        cur = conn.cursor(dictionary=True)

        print("=" * 80)
        print("ANALYSE: Fundamentaldaten vs. Price-Daten")
        print("=" * 80)
        print("\nLade Daten...\n")

        # Analyse-Query: Für jede ISIN vergleiche Fundamentaldaten-Jahre mit Price-Daten-Jahren
        cur.execute("""
            SELECT
                isin,
                ANY_VALUE(ticker) as ticker,
                ANY_VALUE(company_name) as company_name,
                ANY_VALUE(stock_index) as stock_index,
                COUNT(DISTINCT CASE WHEN period = 'FY' THEN YEAR(date) END) as fy_jahre_total,
                COUNT(DISTINCT CASE WHEN period = 'FY' AND price IS NOT NULL THEN YEAR(date) END) as fy_jahre_mit_price,
                COUNT(DISTINCT CASE WHEN period = 'FY' AND eps IS NOT NULL AND eps > 0 THEN YEAR(date) END) as fy_jahre_mit_eps,
                COUNT(DISTINCT CASE WHEN period = 'FY' AND price IS NOT NULL AND eps IS NOT NULL AND eps > 0 THEN YEAR(date) END) as fy_jahre_kgv_berechenbar,
                MIN(CASE WHEN period = 'FY' THEN date END) as erste_fy_datum,
                MAX(CASE WHEN period = 'FY' THEN date END) as letzte_fy_datum,
                MIN(CASE WHEN period = 'FY' AND price IS NOT NULL THEN date END) as erste_price_datum,
                MAX(CASE WHEN period = 'FY' AND price IS NOT NULL THEN date END) as letzte_price_datum
            FROM fmp_filtered_numbers
            GROUP BY isin
            HAVING fy_jahre_total > 0
            ORDER BY (fy_jahre_total - fy_jahre_mit_price) DESC, fy_jahre_total DESC
        """)

        results = cur.fetchall()

        # Statistiken berechnen
        total_stocks = len(results)
        stocks_with_gap = sum(1 for r in results if r['fy_jahre_total'] > r['fy_jahre_mit_price'])
        stocks_no_price = sum(1 for r in results if r['fy_jahre_mit_price'] == 0)
        stocks_gap_5plus = sum(1 for r in results if (r['fy_jahre_total'] - r['fy_jahre_mit_price']) >= 5)
        stocks_kgv_1_or_less = sum(1 for r in results if r['fy_jahre_kgv_berechenbar'] <= 1)

        print("=" * 80)
        print("ÜBERSICHT")
        print("=" * 80)
        print(f"Gesamt analysierte Aktien:                {total_stocks:>6}")
        print(f"  - Mit Diskrepanz (FY > Price Jahre):    {stocks_with_gap:>6} ({stocks_with_gap/total_stocks*100:.1f}%)")
        print(f"  - Davon Gap >= 5 Jahre:                 {stocks_gap_5plus:>6} ({stocks_gap_5plus/total_stocks*100:.1f}%)")
        print(f"  - Ohne Price-Daten (0 Jahre):           {stocks_no_price:>6} ({stocks_no_price/total_stocks*100:.1f}%)")
        print(f"  - KGV nur für <= 1 Jahr berechenbar:    {stocks_kgv_1_or_less:>6} ({stocks_kgv_1_or_less/total_stocks*100:.1f}%)")

        # Top 30 Aktien mit größter Diskrepanz
        print("\n" + "=" * 80)
        print("TOP 30 AKTIEN MIT GRÖSSTER DISKREPANZ")
        print("=" * 80)
        print(f"{'Ticker':<12} {'Company':<30} {'Index':<10} {'FY Jahre':<10} {'Price J.':<10} {'Gap':<8} {'KGV J.'}")
        print("-" * 120)

        for i, row in enumerate(results[:30], 1):
            gap = row['fy_jahre_total'] - row['fy_jahre_mit_price']
            if gap <= 0:
                break

            ticker = row['ticker'][:11] if row['ticker'] else 'N/A'
            company = row['company_name'][:29] if row['company_name'] else 'N/A'
            index = row['stock_index'][:9] if row['stock_index'] else 'N/A'

            print(f"{ticker:<12} {company:<30} {index:<10} "
                  f"{row['fy_jahre_total']:<10} {row['fy_jahre_mit_price']:<10} "
                  f"{gap:<8} {row['fy_jahre_kgv_berechenbar']}")

        # Detaillierte Statistik nach Gap-Größe
        print("\n" + "=" * 80)
        print("VERTEILUNG NACH GAP-GRÖSSE")
        print("=" * 80)

        gap_distribution = {}
        for r in results:
            gap = r['fy_jahre_total'] - r['fy_jahre_mit_price']
            if gap not in gap_distribution:
                gap_distribution[gap] = 0
            gap_distribution[gap] += 1

        print(f"{'Gap (Jahre)':<15} {'Anzahl Aktien':<20} {'Prozent'}")
        print("-" * 60)
        for gap in sorted(gap_distribution.keys(), reverse=True)[:20]:
            count = gap_distribution[gap]
            pct = count / total_stocks * 100
            print(f"{gap:<15} {count:<20} {pct:.1f}%")

        # Beispiele für verschiedene Gap-Kategorien
        print("\n" + "=" * 80)
        print("BEISPIELE NACH KATEGORIEN")
        print("=" * 80)

        categories = [
            ("Gap >= 10 Jahre", lambda r: (r['fy_jahre_total'] - r['fy_jahre_mit_price']) >= 10),
            ("Gap 5-9 Jahre", lambda r: 5 <= (r['fy_jahre_total'] - r['fy_jahre_mit_price']) < 10),
            ("Gap 1-4 Jahre", lambda r: 1 <= (r['fy_jahre_total'] - r['fy_jahre_mit_price']) < 5),
            ("Kein Gap", lambda r: (r['fy_jahre_total'] - r['fy_jahre_mit_price']) == 0),
        ]

        for cat_name, cat_filter in categories:
            matching = [r for r in results if cat_filter(r)]
            print(f"\n{cat_name}: {len(matching)} Aktien")
            if matching:
                print(f"  Beispiele:")
                for r in matching[:3]:
                    gap = r['fy_jahre_total'] - r['fy_jahre_mit_price']
                    print(f"    - {r['ticker']}: {r['company_name'][:40]} "
                          f"(FY: {r['fy_jahre_total']}, Price: {r['fy_jahre_mit_price']}, Gap: {gap})")

        # Besonders kritische Fälle (wie DSFIR.AS)
        print("\n" + "=" * 80)
        print("KRITISCHE FÄLLE (>= 10 FY Jahre aber <= 2 KGV-berechenbare Jahre)")
        print("=" * 80)

        critical = [r for r in results
                   if r['fy_jahre_total'] >= 10
                   and r['fy_jahre_kgv_berechenbar'] <= 2]

        print(f"Gefunden: {len(critical)} kritische Fälle\n")
        print(f"{'Ticker':<12} {'Company':<35} {'FY J.':<8} {'Price J.':<10} {'KGV J.':<8} {'Erste FY':<12} {'Erste Price'}")
        print("-" * 120)

        for r in critical[:20]:
            ticker = r['ticker'][:11] if r['ticker'] else 'N/A'
            company = r['company_name'][:34] if r['company_name'] else 'N/A'
            erste_fy = str(r['erste_fy_datum'])[:10] if r['erste_fy_datum'] else 'N/A'
            erste_price = str(r['erste_price_datum'])[:10] if r['erste_price_datum'] else 'N/A'

            print(f"{ticker:<12} {company:<35} {r['fy_jahre_total']:<8} "
                  f"{r['fy_jahre_mit_price']:<10} {r['fy_jahre_kgv_berechenbar']:<8} "
                  f"{erste_fy:<12} {erste_price}")

        cur.close()
        conn.close()

        print("\n" + "=" * 80)
        print("FAZIT")
        print("=" * 80)
        if stocks_gap_5plus > total_stocks * 0.1:
            print(f"⚠️  Signifikantes Problem: {stocks_gap_5plus} Aktien ({stocks_gap_5plus/total_stocks*100:.1f}%)")
            print("    haben einen Gap von >= 5 Jahren.")
            print("\n💡 EMPFEHLUNG: Systematische Lösung implementieren!")
            print("    - Historische Price-Daten nachladen")
            print("    - Oder: Mindestanzahl Jahre für Durchschnittsberechnung einführen")
            print("    - Oder: Transparente Anzeige im Frontend")
        else:
            print(f"✅ Überschaubares Problem: Nur {stocks_gap_5plus} Aktien mit Gap >= 5 Jahren")
            print("    Eine individuelle Behandlung könnte ausreichend sein.")

    except Exception as e:
        print(f"❌ Fehler: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_price_data_gap()
