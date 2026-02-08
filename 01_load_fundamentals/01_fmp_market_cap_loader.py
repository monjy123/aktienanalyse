#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FMP Historical Market Capitalization Loader

Lädt historische Marktkapitalisierung von FMP API für alle ISINs in tickerlist.
Verwendet Threading für parallele API-Requests.

Workflow:
1. ISINs aus tickerlist laden
2. ISIN → Ticker mappen via FMP API (nutzt bestehendes Mapping falls vorhanden)
3. Historical Market Cap laden (parallel)
4. In fmp_historical_market_cap speichern
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, str(Path(__file__).parent.parent))

from tqdm import tqdm
from mysql.connector import Error as MySQLError
from db import get_connection
from utils.fmp_api import api_request

# Thread-lokaler Storage für DB-Connections
thread_local = threading.local()

# Anzahl paralleler Threads (FMP erlaubt typischerweise 10-30 parallele Requests)
MAX_WORKERS = 10

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "fmp_market_cap_loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL Statements
# =============================================================================

CREATE_MARKET_CAP_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_historical_market_cap (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(12),
    stock_index VARCHAR(50),
    company_name VARCHAR(255),

    -- Daten
    date DATE NOT NULL,
    market_cap BIGINT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_entry (ticker, date),
    INDEX idx_ticker (ticker),
    INDEX idx_isin (isin),
    INDEX idx_stock_index (stock_index),
    INDEX idx_date (date),
    INDEX idx_ticker_date (ticker, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# =============================================================================
# API Functions (nutzt utils.fmp_api)
# =============================================================================

def search_isin(isin):
    """Suche Ticker für ISIN via FMP API."""
    data = api_request("/stable/search-isin", {"isin": isin}, rate_limit=0)
    if data and len(data) > 0:
        return data[0]  # Erstes Ergebnis
    return None


def get_historical_market_cap(ticker):
    """Lade historische Marktkapitalisierung (30+ Jahre)."""
    return api_request("/stable/historical-market-capitalization", {
        "symbol": ticker,
        "from": "1990-01-01"  # 30+ Jahre historische Daten
    }, rate_limit=0)  # Kein Rate Limiting bei Threading


# =============================================================================
# Data Processing
# =============================================================================

def get_existing_mapping(cur, isin):
    """Hole bestehendes Ticker-Mapping aus der DB."""
    cur.execute("""
        SELECT ticker, company_name, exchange
        FROM raw_data.fmp_ticker_mapping
        WHERE isin = %s
        LIMIT 1
    """, (isin,))
    result = cur.fetchone()
    if result:
        return {
            "symbol": result[0],
            "name": result[1],
            "exchange": result[2]
        }
    return None


def save_market_cap(cur, ticker, isin, stock_index, company_name, records):
    """Speichere Historical Market Cap."""
    if not records:
        return 0

    sql = """
    INSERT INTO raw_data.fmp_historical_market_cap (
        ticker, isin, stock_index, company_name,
        date, market_cap
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        isin = VALUES(isin),
        stock_index = VALUES(stock_index),
        company_name = VALUES(company_name),
        market_cap = VALUES(market_cap),
        updated_at = NOW()
    """

    count = 0
    for r in records:
        if not r.get("date"):
            continue

        values = (
            ticker,
            isin,
            stock_index,
            company_name,
            r.get("date"),
            r.get("marketCap"),
        )

        try:
            cur.execute(sql, values)
            count += 1
        except MySQLError as e:
            logger.warning(f"Fehler beim Speichern {ticker} {r.get('date')}: {e}")

    return count


# =============================================================================
# Main
# =============================================================================

def fetch_market_cap_for_ticker(isin, stock_index, existing_mappings):
    """
    Fetch Market Cap für einen Ticker (wird in Thread ausgeführt).
    Gibt (isin, stock_index, ticker, company_name, data, error) zurück.
    """
    # 1. Zuerst bestehendes Mapping prüfen
    mapping = existing_mappings.get(isin)

    # Falls kein Mapping existiert, via API suchen
    if not mapping:
        mapping = search_isin(isin)
        if not mapping:
            return (isin, stock_index, None, None, None, "Kein Ticker gefunden")

    ticker = mapping.get("symbol")
    company_name = mapping.get("name")

    if not ticker:
        return (isin, stock_index, None, None, None, "Leerer Ticker")

    # 2. Historical Market Cap laden
    market_cap_data = get_historical_market_cap(ticker)

    if not market_cap_data:
        return (isin, stock_index, ticker, company_name, None, "Keine Daten")

    return (isin, stock_index, ticker, company_name, market_cap_data, None)


def main():
    print("=" * 60)
    print("FMP HISTORICAL MARKET CAP LOADER")
    print(f"(mit {MAX_WORKERS} parallelen Threads)")
    print("=" * 60)

    if not FMP_API_KEY:
        print("FEHLER: FMP_API_KEY nicht in .env gefunden!")
        return

    conn = None
    cur = None

    try:
        # DB Verbindung
        print("\nVerbinde zur Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()
        print("Verbunden!")

        # Tabelle erstellen
        print("\nErstelle Tabelle...")
        cur.execute(CREATE_MARKET_CAP_TABLE)
        conn.commit()
        print("Tabelle erstellt!")

        # ISINs laden
        print("\nLade ISINs aus tickerlist...")
        cur.execute("""
            SELECT DISTINCT isin, stock_index
            FROM tickerdb.tickerlist
            WHERE isin IS NOT NULL AND isin != ''
        """)
        isins = cur.fetchall()
        print(f"{len(isins)} ISINs geladen!")

        # Bestehende Mappings vorladen (spart API-Calls)
        print("\nLade bestehende Ticker-Mappings...")
        cur.execute("""
            SELECT isin, ticker, company_name, exchange
            FROM raw_data.fmp_ticker_mapping
            WHERE isin IS NOT NULL
        """)
        existing_mappings = {}
        for row in cur.fetchall():
            existing_mappings[row[0]] = {
                "symbol": row[1],
                "name": row[2],
                "exchange": row[3]
            }
        print(f"{len(existing_mappings)} Mappings geladen!")

        # Statistik
        success_count = 0
        failed_isins = []
        total_records = 0

        # Market Cap laden mit Threading
        print("\n" + "=" * 60)
        print("Lade Historical Market Capitalization (parallel)...")
        print("=" * 60)

        # ThreadPoolExecutor für parallele API-Requests
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Alle Tasks submitten
            futures = {
                executor.submit(fetch_market_cap_for_ticker, isin, stock_index, existing_mappings): (isin, stock_index)
                for isin, stock_index in isins
            }

            # Ergebnisse verarbeiten mit Progress Bar
            with tqdm(total=len(futures), desc="Market Cap") as pbar:
                for future in as_completed(futures):
                    try:
                        isin, stock_index, ticker, company_name, data, error = future.result()

                        if error:
                            failed_isins.append((isin, stock_index, error))
                        elif data:
                            # Daten in DB speichern (sequentiell, da MySQL-Cursor nicht thread-safe)
                            count = save_market_cap(cur, ticker, isin, stock_index, company_name, data)
                            success_count += 1
                            total_records += count

                            # Commit nach jedem erfolgreichen Save
                            if success_count % 50 == 0:
                                conn.commit()

                    except Exception as e:
                        isin, stock_index = futures[future]
                        failed_isins.append((isin, stock_index, str(e)))
                        logger.error(f"Fehler bei {isin}: {e}")

                    pbar.update(1)

        # Finaler Commit
        conn.commit()

        # Finale Statistik aus DB
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        # Anzahl Ticker mit Daten
        cur.execute("""
            SELECT COUNT(DISTINCT ticker)
            FROM raw_data.fmp_historical_market_cap
        """)
        ticker_with_data = cur.fetchone()[0]

        # Gesamt Zeilen
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_historical_market_cap")
        total_rows = cur.fetchone()[0]

        # Datum Range
        cur.execute("""
            SELECT MIN(date), MAX(date)
            FROM raw_data.fmp_historical_market_cap
        """)
        date_range = cur.fetchone()

        # Ausgabe
        total_isins = len(isins)
        print(f"\n📊 Gesamt ISINs:           {total_isins:,}")
        print(f"✅ Erfolgreich geladen:    {success_count:,} ({success_count*100/total_isins:.1f}%)")
        print(f"❌ Fehlgeschlagen:         {len(failed_isins):,} ({len(failed_isins)*100/total_isins:.1f}%)")
        print(f"\n📈 Ticker mit Daten:       {ticker_with_data:,}")
        print(f"📄 Gesamt Datensätze:      {total_rows:,}")

        if date_range[0] and date_range[1]:
            print(f"📅 Zeitraum:               {date_range[0]} bis {date_range[1]}")

        print("\n📊 Datensätze pro Index:")
        cur.execute("""
            SELECT stock_index, COUNT(*) as cnt, COUNT(DISTINCT ticker) as tickers
            FROM raw_data.fmp_historical_market_cap
            GROUP BY stock_index
            ORDER BY cnt DESC
        """)
        for stock_idx, cnt, tickers in cur.fetchall():
            print(f"   {stock_idx:20} {cnt:>10,} Zeilen ({tickers} Ticker)")

        if failed_isins:
            print(f"\n⚠️  Fehlgeschlagene ISINs: {len(failed_isins):,} (erste 20):")
            for isin, idx, reason in failed_isins[:20]:
                print(f"   {isin} ({idx}): {reason}")

    except MySQLError as e:
        print(f"Datenbankfehler: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Fehler: {e}")
        logger.exception("Unerwarteter Fehler")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("\nVerbindung geschlossen.")


if __name__ == "__main__":
    main()
