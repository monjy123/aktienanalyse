#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Befüllt tickerdb.tickerlist.eodhd_ticker aus livedb.tick_table.
Matching-Reihenfolge:
1. ISIN (exakt)
2. ticker/yf_ticker
3. Name (fuzzy)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import get_connection


def main():
    conn = get_connection(autocommit=False)
    cur = conn.cursor()

    # 1. Update via ISIN (exakt)
    print("1. Update via ISIN...")
    cur.execute("""
        UPDATE tickerdb.tickerlist tl
        JOIN livedb.tick_table tt ON tt.ISIN = tl.isin
        SET tl.eodhd_ticker = tt.ticker_eod
        WHERE tt.ticker_eod IS NOT NULL
          AND (tl.eodhd_ticker IS NULL OR tl.eodhd_ticker = '')
    """)
    print(f"   {cur.rowcount} Zeilen aktualisiert")
    conn.commit()

    # 2. Update via ticker (für verbleibende)
    print("2. Update via ticker für verbleibende...")
    cur.execute("""
        UPDATE tickerdb.tickerlist tl
        JOIN livedb.tick_table tt
            ON (tt.ticker_yf = tl.ticker OR tt.ticker_yf = tl.yf_ticker)
        SET tl.eodhd_ticker = tt.ticker_eod
        WHERE tt.ticker_eod IS NOT NULL
          AND (tl.eodhd_ticker IS NULL OR tl.eodhd_ticker = '')
    """)
    print(f"   {cur.rowcount} Zeilen aktualisiert")
    conn.commit()

    # 3. Update via yf_ticker Match
    print("3. Update via yf_ticker...")
    cur.execute("""
        UPDATE tickerdb.tickerlist tl
        JOIN livedb.tick_table tt ON tt.ticker_yf = tl.yf_ticker
        SET tl.eodhd_ticker = tt.ticker_eod
        WHERE tt.ticker_eod IS NOT NULL
          AND tl.yf_ticker IS NOT NULL
          AND (tl.eodhd_ticker IS NULL OR tl.eodhd_ticker = '')
    """)
    print(f"   {cur.rowcount} Zeilen aktualisiert")
    conn.commit()

    # Statistik
    print("\n=== Ergebnis ===")
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(eodhd_ticker IS NOT NULL AND eodhd_ticker != '') AS befuellt,
            SUM(eodhd_ticker IS NULL OR eodhd_ticker = '') AS leer
        FROM tickerdb.tickerlist
    """)
    total, befuellt, leer = cur.fetchone()
    print(f"Total: {total}")
    print(f"Befüllt: {befuellt} ({100*befuellt/total:.1f}%)")
    print(f"Leer: {leer}")

    if leer > 0:
        print(f"\n=== Zeilen ohne eodhd_ticker (erste 20) ===")
        cur.execute("""
            SELECT isin, ticker, name
            FROM tickerdb.tickerlist
            WHERE eodhd_ticker IS NULL OR eodhd_ticker = ''
            LIMIT 20
        """)
        for r in cur.fetchall():
            print(f"  {r}")

    cur.close()
    conn.close()
    print("\nFertig!")


if __name__ == "__main__":
    main()
