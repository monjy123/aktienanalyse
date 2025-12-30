#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Debug-Skript zur Prüfung der Join-Konsistenz zwischen:
- raw_data.eodhd_financial_statements
- tickerdb.tickerlist
- raw_data.yf_prices
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import get_connection

QUERIES = [
    # 1. Übersicht der Tabellen
    ("1. Anzahl Einträge pro Tabelle", """
        SELECT 'eodhd_financial_statements' AS tabelle, COUNT(DISTINCT ticker_eod) AS ticker, COUNT(*) AS rows FROM raw_data.eodhd_financial_statements
        UNION ALL
        SELECT 'tickerlist', COUNT(DISTINCT eodhd_ticker), COUNT(*) FROM tickerdb.tickerlist WHERE eodhd_ticker IS NOT NULL
        UNION ALL
        SELECT 'yf_prices', COUNT(DISTINCT isin), COUNT(*) FROM raw_data.yf_prices
    """),

    # 2. Ticker in eodhd_financial_statements OHNE Match in tickerlist
    ("2. EOD-Ticker OHNE Match in tickerlist (erste 20)", """
        SELECT DISTINCT fs.ticker_eod
        FROM raw_data.eodhd_financial_statements fs
        LEFT JOIN tickerdb.tickerlist tl ON tl.eodhd_ticker = fs.ticker_eod
        WHERE tl.id IS NULL
        LIMIT 20
    """),

    # 3. Anzahl EOD-Ticker ohne Match
    ("3. Anzahl EOD-Ticker ohne Match in tickerlist", """
        SELECT COUNT(DISTINCT fs.ticker_eod) AS ticker_ohne_match
        FROM raw_data.eodhd_financial_statements fs
        LEFT JOIN tickerdb.tickerlist tl ON tl.eodhd_ticker = fs.ticker_eod
        WHERE tl.id IS NULL
    """),

    # 4. tickerlist-Einträge mit ISIN aber ohne yf_prices
    ("4. Tickerlist-ISINs OHNE Kursdaten in yf_prices (erste 20)", """
        SELECT DISTINCT tl.isin, tl.eodhd_ticker, tl.yf_ticker
        FROM tickerdb.tickerlist tl
        LEFT JOIN raw_data.yf_prices yp ON yp.isin = tl.isin
        WHERE tl.eodhd_ticker IS NOT NULL
          AND tl.isin IS NOT NULL
          AND yp.id IS NULL
        LIMIT 20
    """),

    # 5. Anzahl ISINs ohne Kursdaten
    ("5. Anzahl tickerlist-ISINs ohne Kursdaten", """
        SELECT COUNT(DISTINCT tl.isin) AS isin_ohne_kurse
        FROM tickerdb.tickerlist tl
        LEFT JOIN raw_data.yf_prices yp ON yp.isin = tl.isin
        WHERE tl.eodhd_ticker IS NOT NULL
          AND tl.isin IS NOT NULL
          AND yp.id IS NULL
    """),

    # 6. Vollständige Join-Kette: wie viele Datensätze haben ALLE Daten?
    ("6. Join-Kette Statistik", """
        SELECT
            COUNT(DISTINCT fs.ticker_eod) AS total_eod_ticker,
            COUNT(DISTINCT CASE WHEN tl.id IS NOT NULL THEN fs.ticker_eod END) AS mit_tickerlist,
            COUNT(DISTINCT CASE WHEN tl.isin IS NOT NULL THEN fs.ticker_eod END) AS mit_isin,
            COUNT(DISTINCT CASE WHEN yp.isin IS NOT NULL THEN fs.ticker_eod END) AS mit_kursdaten
        FROM raw_data.eodhd_financial_statements fs
        LEFT JOIN tickerdb.tickerlist tl ON tl.eodhd_ticker = fs.ticker_eod
        LEFT JOIN (SELECT DISTINCT isin FROM raw_data.yf_prices) yp ON yp.isin = tl.isin
    """),

    # 7. Beispiel für funktionierenden Join (sollte ISIN + Kurse haben)
    ("7. Beispiele mit vollständigem Join (erste 10)", """
        SELECT DISTINCT
            fs.ticker_eod,
            tl.isin,
            tl.yf_ticker,
            (SELECT COUNT(*) FROM raw_data.yf_prices WHERE isin = tl.isin) AS anzahl_kurse
        FROM raw_data.eodhd_financial_statements fs
        JOIN tickerdb.tickerlist tl ON tl.eodhd_ticker = fs.ticker_eod
        JOIN raw_data.yf_prices yp ON yp.isin = tl.isin
        LIMIT 10
    """),

    # 8. Prüfe ob eodhd_ticker evtl. anders formatiert ist
    ("8. Formatunterschiede eodhd_ticker vs ticker_eod (Stichprobe)", """
        SELECT
            fs.ticker_eod AS in_financial_statements,
            tl.eodhd_ticker AS in_tickerlist,
            LENGTH(fs.ticker_eod) AS len_fs,
            LENGTH(tl.eodhd_ticker) AS len_tl
        FROM raw_data.eodhd_financial_statements fs
        JOIN tickerdb.tickerlist tl
            ON TRIM(LOWER(tl.eodhd_ticker)) = TRIM(LOWER(fs.ticker_eod))
        WHERE tl.eodhd_ticker != fs.ticker_eod
        LIMIT 10
    """),

    # 9. Jahre in den Daten
    ("9. Verfügbare Jahre in eodhd_financial_statements vs yf_prices", """
        SELECT 'eodhd_financial_statements' AS tabelle,
               MIN(YEAR(period)) AS von_jahr,
               MAX(YEAR(period)) AS bis_jahr
        FROM raw_data.eodhd_financial_statements
        WHERE period_type = 'Y'
        UNION ALL
        SELECT 'yf_prices', MIN(YEAR(date)), MAX(YEAR(date))
        FROM raw_data.yf_prices
    """),
]


def main():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("DEBUG: Join-Konsistenz prüfen")
    print("=" * 70)

    for title, sql in QUERIES:
        print(f"\n{title}")
        print("-" * 60)
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if rows:
                # Header
                cols = [desc[0] for desc in cur.description]
                print(" | ".join(f"{c:20}" for c in cols))
                print("-" * 60)
                for row in rows:
                    print(" | ".join(f"{str(v):20}" for v in row))
            else:
                print("(keine Ergebnisse)")
        except Exception as e:
            print(f"FEHLER: {e}")

    cur.close()
    conn.close()
    print("\n" + "=" * 70)
    print("Debug abgeschlossen.")


if __name__ == "__main__":
    main()
