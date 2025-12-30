#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retry-Script: Lädt NUR fehlende Stammdaten nach.

Holt nur Ticker wo sector IS NULL und versucht diese erneut via yfinance.
Langsamer (weniger parallel, mit Pausen) um Rate Limiting zu vermeiden.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from tqdm import tqdm
from mysql.connector import Error
from db import get_connection


# Langsamer um Rate Limiting zu vermeiden
BATCH_SIZE = 20
PAUSE_BETWEEN_BATCHES = 2  # Sekunden


def get_yf_info(ticker_yf):
    """Hole Stammdaten von yfinance API."""
    if not ticker_yf:
        return None

    try:
        time.sleep(0.3)  # Kleine Pause pro Request
        stock = yf.Ticker(ticker_yf)
        info = stock.info

        if not info:
            return None

        return {
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "country": info.get("country"),
            "currency": info.get("currency"),
            "description": info.get("longBusinessSummary"),
            "fiscal_year_end": info.get("fiscalYearEnd"),
        }

    except Exception as e:
        return None


def main():
    print("=" * 60)
    print("COMPANY INFO RETRY (nur fehlende Einträge)")
    print("=" * 60)

    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Nur Ticker OHNE Sector laden
        print("Lade Ticker ohne Sector...")
        cur.execute("""
            SELECT ci.isin, t.yf_ticker, ci.company_name, ci.stock_index
            FROM analytics.company_info ci
            JOIN tickerdb.tickerlist t ON ci.isin = t.isin
            WHERE ci.sector IS NULL
              AND t.yf_ticker IS NOT NULL
              AND t.yf_ticker != ''
        """)
        missing = cur.fetchall()
        print(f"  → {len(missing)} Ticker ohne Sector gefunden")

        if not missing:
            print("\nAlle Einträge haben bereits Sector-Daten!")
            return

        # Nach Index gruppieren
        by_index = {}
        for isin, yf_ticker, name, stock_index in missing:
            by_index.setdefault(stock_index, []).append((isin, yf_ticker, name))

        print("\nFehlend nach Index:")
        for idx, items in sorted(by_index.items(), key=lambda x: -len(x[1])):
            print(f"  {idx:20} {len(items):>4}")

        # Verarbeiten in Batches
        print(f"\nLade Daten (Batch-Größe: {BATCH_SIZE}, Pause: {PAUSE_BETWEEN_BATCHES}s)...")

        success_count = 0
        total_processed = 0

        for i in tqdm(range(0, len(missing), BATCH_SIZE), desc="Batches"):
            batch = missing[i:i+BATCH_SIZE]

            for isin, yf_ticker, name, stock_index in batch:
                yf_data = get_yf_info(yf_ticker)
                total_processed += 1

                if yf_data and yf_data.get("sector"):
                    cur.execute("""
                        UPDATE analytics.company_info
                        SET sector = %s,
                            industry = %s,
                            country = %s,
                            currency = %s,
                            description = %s,
                            fiscal_year_end = %s,
                            updated_at = NOW()
                        WHERE isin = %s
                    """, (
                        yf_data["sector"],
                        yf_data["industry"],
                        yf_data["country"],
                        yf_data["currency"],
                        yf_data["description"],
                        yf_data["fiscal_year_end"],
                        isin
                    ))
                    success_count += 1

            # Commit nach jedem Batch
            conn.commit()

            # Pause zwischen Batches
            if i + BATCH_SIZE < len(missing):
                time.sleep(PAUSE_BETWEEN_BATCHES)

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        print(f"\n📊 Verarbeitet:            {total_processed:,}")
        print(f"✅ Erfolgreich geladen:    {success_count:,}")
        print(f"❌ Weiterhin fehlend:      {total_processed - success_count:,}")

        cur.execute("SELECT COUNT(*) FROM analytics.company_info WHERE sector IS NOT NULL")
        with_sector = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.company_info")
        total = cur.fetchone()[0]

        print(f"\n📈 Gesamt mit Sector:      {with_sector:,}/{total:,} ({with_sector*100/total:.1f}%)")

        # Details nach Index
        print("\nNach Index:")
        cur.execute("""
            SELECT stock_index,
                   COUNT(*) as total,
                   SUM(CASE WHEN sector IS NOT NULL THEN 1 ELSE 0 END) as with_sector
            FROM analytics.company_info
            GROUP BY stock_index
            ORDER BY total DESC
        """)
        for row in cur.fetchall():
            pct = row[2]*100/row[1] if row[1] > 0 else 0
            print(f"  {row[0]:20} {row[2]:>4}/{row[1]:<4} ({pct:.0f}%)")

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
