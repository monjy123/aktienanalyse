#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FMP Revenue Segments Loader

Lädt Revenue Product Segmentation und Revenue Geographic Segmentation
von FMP API für alle ISINs in tickerlist.
Verwendet Threading für parallele API-Requests.

Workflow:
1. ISINs aus tickerlist laden
2. ISIN → Ticker mappen (nutzt bestehendes Mapping)
3. Product & Geographic Segments laden (parallel) - Annual + Quarterly
4. In separate Tabellen speichern
"""

import sys
import os
import time
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from tqdm import tqdm
from dotenv import load_dotenv
from mysql.connector import Error as MySQLError
from db import get_connection

# .env laden
load_dotenv(Path(__file__).parent.parent / ".env")

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE_URL = "https://financialmodelingprep.com"

# Anzahl paralleler Threads
MAX_WORKERS = 10

# Thread-lokaler Storage für Sessions
thread_local = threading.local()

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "fmp_revenue_segments_loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL Statements
# =============================================================================

CREATE_PRODUCT_SEGMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_revenue_product_segments (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(12),
    stock_index VARCHAR(50),
    company_name VARCHAR(255),

    -- Zeitraum
    date DATE NOT NULL,
    fiscal_year INT,
    period VARCHAR(10),
    reported_currency VARCHAR(10),

    -- Segment Daten (EAV-Struktur)
    segment_name VARCHAR(255) NOT NULL,
    revenue DOUBLE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_entry (ticker, date, period, segment_name),
    INDEX idx_ticker (ticker),
    INDEX idx_isin (isin),
    INDEX idx_stock_index (stock_index),
    INDEX idx_date (date),
    INDEX idx_period (period),
    INDEX idx_segment_name (segment_name),
    INDEX idx_ticker_date (ticker, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_GEOGRAPHIC_SEGMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_revenue_geographic_segments (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(12),
    stock_index VARCHAR(50),
    company_name VARCHAR(255),

    -- Zeitraum
    date DATE NOT NULL,
    fiscal_year INT,
    period VARCHAR(10),
    reported_currency VARCHAR(10),

    -- Segment Daten (EAV-Struktur)
    segment_name VARCHAR(255) NOT NULL,
    revenue DOUBLE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_entry (ticker, date, period, segment_name),
    INDEX idx_ticker (ticker),
    INDEX idx_isin (isin),
    INDEX idx_stock_index (stock_index),
    INDEX idx_date (date),
    INDEX idx_period (period),
    INDEX idx_segment_name (segment_name),
    INDEX idx_ticker_date (ticker, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# =============================================================================
# API Functions
# =============================================================================

def get_session():
    """Thread-lokale requests Session für Connection Pooling."""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def api_request(endpoint, params=None, max_retries=3):
    """API Request mit Retry und Rate Limiting."""
    if params is None:
        params = {}
    params["apikey"] = FMP_API_KEY

    url = f"{FMP_BASE_URL}{endpoint}"
    session = get_session()

    for attempt in range(max_retries):
        try:
            time.sleep(0.1)  # Rate Limiting: 100ms Pause pro Request
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(f"API Fehler (Versuch {attempt+1}): {e}. Warte {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"API Fehler nach {max_retries} Versuchen: {e}")
                return None
    return None


def search_isin(isin):
    """Suche Ticker für ISIN via FMP API."""
    data = api_request(f"/stable/search-isin", {"isin": isin})
    if data and len(data) > 0:
        return data[0]
    return None


def get_product_segments(ticker, period="annual"):
    """Lade Revenue Product Segmentation."""
    return api_request(f"/stable/revenue-product-segmentation", {
        "symbol": ticker,
        "period": period
    })


def get_geographic_segments(ticker, period="annual"):
    """Lade Revenue Geographic Segmentation."""
    return api_request(f"/stable/revenue-geographic-segmentation", {
        "symbol": ticker,
        "period": period
    })


# =============================================================================
# Data Processing
# =============================================================================

def flatten_segments(records):
    """
    Flacht die Segment-Daten ab.
    Input: [{"date": "2024-09-28", "period": "FY", "data": {"iPhone": 123, "Mac": 456}}]
    Output: [{"date": "2024-09-28", "period": "FY", "segment_name": "iPhone", "revenue": 123}, ...]
    """
    flattened = []
    if not records:
        return flattened

    for record in records:
        date = record.get("date")
        period = record.get("period")
        fiscal_year = record.get("fiscalYear")
        currency = record.get("reportedCurrency")
        data = record.get("data", {})

        if not date or not data:
            continue

        for segment_name, revenue in data.items():
            flattened.append({
                "date": date,
                "period": period,
                "fiscal_year": fiscal_year,
                "reported_currency": currency,
                "segment_name": segment_name,
                "revenue": revenue
            })

    return flattened


def save_segments(cur, table_name, ticker, isin, stock_index, company_name, records):
    """Speichere Segment-Daten."""
    if not records:
        return 0

    sql = f"""
    INSERT INTO raw_data.{table_name} (
        ticker, isin, stock_index, company_name,
        date, fiscal_year, period, reported_currency,
        segment_name, revenue
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        isin = VALUES(isin),
        stock_index = VALUES(stock_index),
        company_name = VALUES(company_name),
        fiscal_year = VALUES(fiscal_year),
        reported_currency = VALUES(reported_currency),
        revenue = VALUES(revenue),
        updated_at = NOW()
    """

    count = 0
    for r in records:
        if not r.get("date") or not r.get("segment_name"):
            continue

        values = (
            ticker,
            isin,
            stock_index,
            company_name,
            r.get("date"),
            r.get("fiscal_year"),
            r.get("period"),
            r.get("reported_currency"),
            r.get("segment_name"),
            r.get("revenue"),
        )

        try:
            cur.execute(sql, values)
            count += 1
        except MySQLError as e:
            logger.warning(f"Fehler beim Speichern {ticker} {r.get('date')} {r.get('segment_name')}: {e}")

    return count


# =============================================================================
# Main
# =============================================================================

def fetch_segments_for_ticker(isin, stock_index, existing_mappings):
    """
    Fetch alle Segment-Daten für einen Ticker (wird in Thread ausgeführt).
    Lädt sowohl Annual als auch Quarterly für beide Segment-Typen.
    """
    # 1. Mapping holen
    mapping = existing_mappings.get(isin)
    if not mapping:
        mapping = search_isin(isin)
        if not mapping:
            return (isin, stock_index, None, None, None, None, "Kein Ticker gefunden")

    ticker = mapping.get("symbol")
    company_name = mapping.get("name")

    if not ticker:
        return (isin, stock_index, None, None, None, None, "Leerer Ticker")

    # 2. Alle Segment-Daten laden (4 API-Calls pro Ticker)
    product_annual = get_product_segments(ticker, "annual")
    product_quarterly = get_product_segments(ticker, "quarter")
    geo_annual = get_geographic_segments(ticker, "annual")
    geo_quarterly = get_geographic_segments(ticker, "quarter")

    # 3. Flatten
    product_data = flatten_segments(product_annual) + flatten_segments(product_quarterly)
    geo_data = flatten_segments(geo_annual) + flatten_segments(geo_quarterly)

    if not product_data and not geo_data:
        return (isin, stock_index, ticker, company_name, None, None, "Keine Daten")

    return (isin, stock_index, ticker, company_name, product_data, geo_data, None)


def main():
    print("=" * 60)
    print("FMP REVENUE SEGMENTS LOADER")
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

        # Tabellen erstellen
        print("\nErstelle Tabellen...")
        cur.execute(CREATE_PRODUCT_SEGMENTS_TABLE)
        cur.execute(CREATE_GEOGRAPHIC_SEGMENTS_TABLE)
        conn.commit()
        print("Tabellen erstellt!")

        # ISINs laden
        print("\nLade ISINs aus tickerlist...")
        cur.execute("""
            SELECT DISTINCT isin, stock_index
            FROM tickerdb.tickerlist
            WHERE isin IS NOT NULL AND isin != ''
        """)
        isins = cur.fetchall()
        print(f"{len(isins)} ISINs geladen!")

        # Bestehende Mappings vorladen
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
        product_records = 0
        geo_records = 0

        # Segments laden mit Threading
        print("\n" + "=" * 60)
        print("Lade Revenue Segments (Product & Geographic, Annual & Quarterly)...")
        print("=" * 60)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_segments_for_ticker, isin, stock_index, existing_mappings): (isin, stock_index)
                for isin, stock_index in isins
            }

            with tqdm(total=len(futures), desc="Revenue Segments") as pbar:
                for future in as_completed(futures):
                    try:
                        isin, stock_index, ticker, company_name, product_data, geo_data, error = future.result()

                        if error:
                            failed_isins.append((isin, stock_index, error))
                        else:
                            # Product Segments speichern
                            if product_data:
                                count = save_segments(cur, "fmp_revenue_product_segments",
                                                      ticker, isin, stock_index, company_name, product_data)
                                product_records += count

                            # Geographic Segments speichern
                            if geo_data:
                                count = save_segments(cur, "fmp_revenue_geographic_segments",
                                                      ticker, isin, stock_index, company_name, geo_data)
                                geo_records += count

                            if product_data or geo_data:
                                success_count += 1

                            # Commit alle 50 Ticker
                            if success_count % 50 == 0:
                                conn.commit()

                    except Exception as e:
                        isin, stock_index = futures[future]
                        failed_isins.append((isin, stock_index, str(e)))
                        logger.error(f"Fehler bei {isin}: {e}")

                    pbar.update(1)

        # Finaler Commit
        conn.commit()

        # Finale Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        # Product Segments Stats
        cur.execute("SELECT COUNT(DISTINCT ticker) FROM raw_data.fmp_revenue_product_segments")
        product_tickers = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_revenue_product_segments")
        product_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT segment_name) FROM raw_data.fmp_revenue_product_segments")
        product_unique_segments = cur.fetchone()[0]

        # Geographic Segments Stats
        cur.execute("SELECT COUNT(DISTINCT ticker) FROM raw_data.fmp_revenue_geographic_segments")
        geo_tickers = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_revenue_geographic_segments")
        geo_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT segment_name) FROM raw_data.fmp_revenue_geographic_segments")
        geo_unique_segments = cur.fetchone()[0]

        total_isins = len(isins)
        print(f"\n📊 Gesamt ISINs:              {total_isins:,}")
        print(f"✅ Erfolgreich geladen:       {success_count:,} ({success_count*100/total_isins:.1f}%)")
        print(f"❌ Fehlgeschlagen:            {len(failed_isins):,}")

        print(f"\n📦 PRODUCT SEGMENTS:")
        print(f"   Ticker mit Daten:          {product_tickers:,}")
        print(f"   Gesamt Datensätze:         {product_total:,}")
        print(f"   Unique Segment-Namen:      {product_unique_segments:,}")

        print(f"\n🌍 GEOGRAPHIC SEGMENTS:")
        print(f"   Ticker mit Daten:          {geo_tickers:,}")
        print(f"   Gesamt Datensätze:         {geo_total:,}")
        print(f"   Unique Segment-Namen:      {geo_unique_segments:,}")

        # Top Product Segments
        print("\n📦 Top 10 Product Segments:")
        cur.execute("""
            SELECT segment_name, COUNT(*) as cnt
            FROM raw_data.fmp_revenue_product_segments
            GROUP BY segment_name
            ORDER BY cnt DESC
            LIMIT 10
        """)
        for seg_name, cnt in cur.fetchall():
            print(f"   {seg_name:40} {cnt:>6,}")

        # Top Geographic Segments
        print("\n🌍 Top 10 Geographic Segments:")
        cur.execute("""
            SELECT segment_name, COUNT(*) as cnt
            FROM raw_data.fmp_revenue_geographic_segments
            GROUP BY segment_name
            ORDER BY cnt DESC
            LIMIT 10
        """)
        for seg_name, cnt in cur.fetchall():
            print(f"   {seg_name:40} {cnt:>6,}")

        # Period breakdown
        print("\n📅 Datensätze nach Period:")
        cur.execute("""
            SELECT 'Product' as type, period, COUNT(*) as cnt
            FROM raw_data.fmp_revenue_product_segments
            GROUP BY period
            UNION ALL
            SELECT 'Geographic' as type, period, COUNT(*) as cnt
            FROM raw_data.fmp_revenue_geographic_segments
            GROUP BY period
            ORDER BY type, period
        """)
        for seg_type, period, cnt in cur.fetchall():
            print(f"   {seg_type:12} {period or 'NULL':8} {cnt:>8,}")

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
