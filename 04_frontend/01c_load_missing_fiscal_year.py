#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lädt fehlende fiscal_year_end Daten von yfinance nach.

Findet alle Aktien ohne fiscal_year_end und lädt die Daten nach.
"""

import sys
import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from tqdm import tqdm
from mysql.connector import Error
from db import get_connection


# Threading Konfiguration
MAX_WORKERS = 10


def get_fiscal_year_end(ticker_yf):
    """Hole fiscal_year_end von yfinance API."""
    if not ticker_yf:
        return None

    try:
        stock = yf.Ticker(ticker_yf)
        info = stock.info

        # Prüfe ob gültige Daten zurückkamen
        if not info:
            return None

        # Fiskaljahr-Ende behandeln
        fiscal_year_end = info.get("fiscalYearEnd") or info.get("lastFiscalYearEnd")

        if fiscal_year_end and isinstance(fiscal_year_end, int):
            # Unix-Timestamp -> Monat extrahieren
            dt = datetime.datetime.fromtimestamp(fiscal_year_end)
            months = {
                1: "Januar", 2: "Februar", 3: "März", 4: "April",
                5: "Mai", 6: "Juni", 7: "Juli", 8: "August",
                9: "September", 10: "Oktober", 11: "November", 12: "Dezember"
            }
            fiscal_year_end = months.get(dt.month)

        return fiscal_year_end

    except Exception as e:
        return None


def process_ticker(isin, ticker_yf, company_name):
    """Verarbeite einen Ticker - hole fiscal_year_end."""
    fiscal_year_end = get_fiscal_year_end(ticker_yf)

    return {
        "isin": isin,
        "company_name": company_name,
        "fiscal_year_end": fiscal_year_end
    }


def main():
    print("=" * 60)
    print("FEHLENDE FISCAL_YEAR_END LADEN")
    print("=" * 60)

    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor(dictionary=True)

        # Ticker ohne fiscal_year_end laden
        print("Lade Ticker ohne fiscal_year_end...")
        cur.execute("""
            SELECT ci.isin, ci.company_name, ci.stock_index, tl.yf_ticker
            FROM analytics.company_info ci
            LEFT JOIN tickerdb.tickerlist tl ON ci.isin = tl.isin
            WHERE (ci.fiscal_year_end IS NULL OR ci.fiscal_year_end = '')
            AND tl.yf_ticker IS NOT NULL AND tl.yf_ticker != ''
        """)

        tickers = cur.fetchall()
        print(f"  → {len(tickers)} Ticker ohne fiscal_year_end gefunden")

        if len(tickers) == 0:
            print("\n✅ Alle Ticker haben bereits fiscal_year_end!")
            return

        # Nach Index gruppieren
        by_index = {}
        for row in tickers:
            idx = row['stock_index'] or 'Unbekannt'
            if idx not in by_index:
                by_index[idx] = []
            by_index[idx].append(row)

        print("\nVerteilung:")
        for idx, rows in sorted(by_index.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"  {idx:20} {len(rows):>4} ohne fiscal_year_end")

        # Parallel verarbeiten
        print(f"\nLade fiscal_year_end von yfinance (max {MAX_WORKERS} parallel)...")
        results = []
        success_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_ticker, row['isin'], row['yf_ticker'], row['company_name']): row['isin']
                for row in tickers
            }

            with tqdm(total=len(futures), desc="yfinance API") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)

                    if result["fiscal_year_end"]:
                        success_count += 1

                    pbar.update(1)

        # In DB speichern
        print(f"\nSpeichere {success_count} fiscal_year_end Einträge...")
        update_count = 0

        for data in tqdm(results, desc="Speichern"):
            if data["fiscal_year_end"]:
                try:
                    cur.execute("""
                        UPDATE analytics.company_info
                        SET fiscal_year_end = %s
                        WHERE isin = %s
                    """, (data["fiscal_year_end"], data["isin"]))
                    update_count += 1
                except Error as e:
                    print(f"  Fehler bei {data['company_name']} ({data['isin']}): {e}")

        conn.commit()

        print("\n" + "=" * 60)
        print("FERTIG")
        print("=" * 60)
        print(f"\n✅ {update_count} fiscal_year_end Einträge aktualisiert")

        # Finale Statistik
        cur.execute("""
            SELECT
                stock_index,
                COUNT(*) as total,
                SUM(CASE WHEN fiscal_year_end IS NOT NULL THEN 1 ELSE 0 END) as with_fiscal_year
            FROM analytics.company_info
            GROUP BY stock_index
            ORDER BY total DESC
        """)
        stats = cur.fetchall()

        print("\n📊 fiscal_year_end Status nach Index:")
        for row in stats:
            pct = (row['with_fiscal_year'] / row['total'] * 100) if row['total'] > 0 else 0
            print(f"  {row['stock_index']:20} {row['with_fiscal_year']:>4}/{row['total']:>4} ({pct:>5.1f}%)")

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
