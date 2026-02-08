#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FMP Historical Sector Performance Loader

Lädt historische Sector Performance (averageChange) für alle 11 Sektoren
von FMP API für NASDAQ und NYSE.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from tqdm import tqdm
from mysql.connector import Error as MySQLError
from db import get_connection
from utils.fmp_api import api_request

# Die 11 Sektoren (FMP-Bezeichnungen)
FMP_SECTORS = [
    "Energy",
    "Basic Materials",
    "Industrials",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Healthcare",
    "Financial Services",
    "Technology",
    "Communication Services",
    "Utilities",
    "Real Estate"
]

# Exchanges
EXCHANGES = ["NASDAQ", "NYSE"]

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "fmp_sector_performance_loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL Statements
# =============================================================================

CREATE_SECTOR_PERFORMANCE_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_historical_sector_performance (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    sector VARCHAR(50) NOT NULL,
    exchange VARCHAR(20) NOT NULL,

    -- Daten
    date DATE NOT NULL,
    average_change DOUBLE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_entry (sector, exchange, date),
    INDEX idx_sector (sector),
    INDEX idx_exchange (exchange),
    INDEX idx_date (date),
    INDEX idx_sector_exchange (sector, exchange),
    INDEX idx_sector_date (sector, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# =============================================================================
# API Functions (nutzt utils.fmp_api)
# =============================================================================

def get_historical_sector_performance(sector, exchange):
    """Lade historische Sector Performance (alle verfügbaren Daten)."""
    return api_request("/stable/historical-sector-performance", {
        "sector": sector,
        "exchange": exchange,
        "from": "1990-01-01"  # Maximale Historie anfordern
    }, rate_limit=0.1)


# =============================================================================
# Data Processing
# =============================================================================

def save_sector_performance(cur, sector, exchange, records):
    """Speichere Sector Performance Daten."""
    if not records:
        return 0

    sql = """
    INSERT INTO raw_data.fmp_historical_sector_performance (
        sector, exchange, date, average_change
    ) VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        average_change = VALUES(average_change),
        updated_at = NOW()
    """

    count = 0
    for r in records:
        if not r.get("date"):
            continue

        values = (
            sector,
            exchange,
            r.get("date"),
            r.get("averageChange"),
        )

        try:
            cur.execute(sql, values)
            count += 1
        except MySQLError as e:
            logger.warning(f"Fehler beim Speichern {sector} {exchange} {r.get('date')}: {e}")

    return count


# =============================================================================
# Main
# =============================================================================

def main():
    print("=" * 60)
    print("FMP HISTORICAL SECTOR PERFORMANCE LOADER")
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
        cur.execute(CREATE_SECTOR_PERFORMANCE_TABLE)
        conn.commit()
        print("Tabelle erstellt!")

        # Statistik
        total_records = 0
        sector_stats = {}

        # Sector Performance laden
        total_combinations = len(FMP_SECTORS) * len(EXCHANGES)
        print("\n" + "=" * 60)
        print(f"Lade Historical Sector Performance für {len(FMP_SECTORS)} Sektoren x {len(EXCHANGES)} Exchanges...")
        print("=" * 60)

        combinations = [(sector, exchange) for sector in FMP_SECTORS for exchange in EXCHANGES]

        for sector, exchange in tqdm(combinations, desc="Sector Performance"):
            perf_data = get_historical_sector_performance(sector, exchange)

            if perf_data:
                count = save_sector_performance(cur, sector, exchange, perf_data)
                sector_stats[(sector, exchange)] = count
                total_records += count
                conn.commit()
            else:
                sector_stats[(sector, exchange)] = 0
                logger.warning(f"Keine Daten für Sektor: {sector} ({exchange})")

        # Finale Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        # Gesamt Zeilen
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_historical_sector_performance")
        total_rows = cur.fetchone()[0]

        # Datum Range
        cur.execute("""
            SELECT MIN(date), MAX(date)
            FROM raw_data.fmp_historical_sector_performance
        """)
        date_range = cur.fetchone()

        print(f"\n📊 Gesamt Sektoren:        {len(FMP_SECTORS)}")
        print(f"📊 Gesamt Exchanges:       {len(EXCHANGES)}")
        print(f"📄 Gesamt Datensätze:      {total_rows:,}")

        if date_range[0] and date_range[1]:
            print(f"📅 Zeitraum:               {date_range[0]} bis {date_range[1]}")

        print("\n📊 Datensätze pro Sektor & Exchange:")
        cur.execute("""
            SELECT sector, exchange, COUNT(*) as cnt, MIN(date) as min_date, MAX(date) as max_date
            FROM raw_data.fmp_historical_sector_performance
            GROUP BY sector, exchange
            ORDER BY sector, exchange
        """)
        for sector, exchange, cnt, min_date, max_date in cur.fetchall():
            print(f"   {sector:25} {exchange:8} {cnt:>6,} Zeilen  ({min_date} - {max_date})")

        # Aktuelle Performance Werte
        print("\n📈 Aktuelle Performance (neueste Daten):")
        cur.execute("""
            SELECT s.sector, s.exchange, s.average_change, s.date
            FROM raw_data.fmp_historical_sector_performance s
            INNER JOIN (
                SELECT sector, exchange, MAX(date) as max_date
                FROM raw_data.fmp_historical_sector_performance
                GROUP BY sector, exchange
            ) latest ON s.sector = latest.sector
                    AND s.exchange = latest.exchange
                    AND s.date = latest.max_date
            ORDER BY s.exchange, s.average_change DESC
        """)
        for sector, exchange, avg_change, date in cur.fetchall():
            change_str = f"{avg_change:+.2f}%" if avg_change else "N/A"
            print(f"   {exchange:8} {sector:25} {change_str:>10}  ({date})")

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
