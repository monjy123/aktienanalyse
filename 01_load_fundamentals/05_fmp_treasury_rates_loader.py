#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FMP Treasury Rates Loader

Lädt historische US Treasury Rates von FMP API.
Enthält Zinssätze für verschiedene Laufzeiten (1M bis 30Y).
"""

import sys
import os
import time
import logging
from pathlib import Path
from datetime import datetime

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

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "fmp_treasury_rates_loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL Statements
# =============================================================================

CREATE_TREASURY_RATES_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_treasury_rates (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Datum
    date DATE NOT NULL,

    -- Treasury Rates nach Laufzeit
    month1 DOUBLE,
    month2 DOUBLE,
    month3 DOUBLE,
    month6 DOUBLE,
    year1 DOUBLE,
    year2 DOUBLE,
    year3 DOUBLE,
    year5 DOUBLE,
    year7 DOUBLE,
    year10 DOUBLE,
    year20 DOUBLE,
    year30 DOUBLE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_date (date),
    INDEX idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# =============================================================================
# API Functions
# =============================================================================

def api_request(endpoint, params=None, max_retries=3):
    """API Request mit Retry und Rate Limiting."""
    if params is None:
        params = {}
    params["apikey"] = FMP_API_KEY

    url = f"{FMP_BASE_URL}{endpoint}"

    for attempt in range(max_retries):
        try:
            time.sleep(0.1)  # Rate Limiting
            response = requests.get(url, params=params, timeout=30)
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


def get_treasury_rates():
    """Lade historische Treasury Rates (alle verfügbaren Daten)."""
    today = datetime.now().strftime("%Y-%m-%d")
    return api_request(f"/stable/treasury-rates", {
        "from": "1990-01-01",
        "to": today
    })


# =============================================================================
# Data Processing
# =============================================================================

def save_treasury_rates(cur, records):
    """Speichere Treasury Rates Daten."""
    if not records:
        return 0

    sql = """
    INSERT INTO raw_data.fmp_treasury_rates (
        date, month1, month2, month3, month6,
        year1, year2, year3, year5, year7, year10, year20, year30
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        month1 = VALUES(month1),
        month2 = VALUES(month2),
        month3 = VALUES(month3),
        month6 = VALUES(month6),
        year1 = VALUES(year1),
        year2 = VALUES(year2),
        year3 = VALUES(year3),
        year5 = VALUES(year5),
        year7 = VALUES(year7),
        year10 = VALUES(year10),
        year20 = VALUES(year20),
        year30 = VALUES(year30),
        updated_at = NOW()
    """

    count = 0
    for r in tqdm(records, desc="Speichere"):
        if not r.get("date"):
            continue

        values = (
            r.get("date"),
            r.get("month1"),
            r.get("month2"),
            r.get("month3"),
            r.get("month6"),
            r.get("year1"),
            r.get("year2"),
            r.get("year3"),
            r.get("year5"),
            r.get("year7"),
            r.get("year10"),
            r.get("year20"),
            r.get("year30"),
        )

        try:
            cur.execute(sql, values)
            count += 1
        except MySQLError as e:
            logger.warning(f"Fehler beim Speichern {r.get('date')}: {e}")

    return count


# =============================================================================
# Main
# =============================================================================

def main():
    print("=" * 60)
    print("FMP TREASURY RATES LOADER")
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
        cur.execute(CREATE_TREASURY_RATES_TABLE)
        conn.commit()
        print("Tabelle erstellt!")

        # Treasury Rates laden
        print("\n" + "=" * 60)
        print("Lade Treasury Rates...")
        print("=" * 60)

        rates_data = get_treasury_rates()

        if not rates_data:
            print("Keine Daten von API erhalten!")
            return

        print(f"\n{len(rates_data):,} Datensätze von API erhalten")

        # Speichern
        count = save_treasury_rates(cur, rates_data)
        conn.commit()

        # Finale Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        # Gesamt Zeilen
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_treasury_rates")
        total_rows = cur.fetchone()[0]

        # Datum Range
        cur.execute("""
            SELECT MIN(date), MAX(date)
            FROM raw_data.fmp_treasury_rates
        """)
        date_range = cur.fetchone()

        print(f"\n📄 Gesamt Datensätze:      {total_rows:,}")

        if date_range[0] and date_range[1]:
            print(f"📅 Zeitraum:               {date_range[0]} bis {date_range[1]}")

        # Aktuelle Rates
        print("\n📈 Aktuelle Treasury Rates (neueste Daten):")
        cur.execute("""
            SELECT date, month1, month3, month6, year1, year2, year5, year10, year30
            FROM raw_data.fmp_treasury_rates
            ORDER BY date DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            date, m1, m3, m6, y1, y2, y5, y10, y30 = row
            print(f"   Datum:    {date}")
            print(f"   1 Monat:  {m1:.2f}%" if m1 else "   1 Monat:  N/A")
            print(f"   3 Monate: {m3:.2f}%" if m3 else "   3 Monate: N/A")
            print(f"   6 Monate: {m6:.2f}%" if m6 else "   6 Monate: N/A")
            print(f"   1 Jahr:   {y1:.2f}%" if y1 else "   1 Jahr:   N/A")
            print(f"   2 Jahre:  {y2:.2f}%" if y2 else "   2 Jahre:  N/A")
            print(f"   5 Jahre:  {y5:.2f}%" if y5 else "   5 Jahre:  N/A")
            print(f"   10 Jahre: {y10:.2f}%" if y10 else "   10 Jahre: N/A")
            print(f"   30 Jahre: {y30:.2f}%" if y30 else "   30 Jahre: N/A")

        # Yield Curve (10Y - 2Y Spread)
        print("\n📊 Yield Curve (10Y - 2Y Spread) - letzte 5 Tage:")
        cur.execute("""
            SELECT date, year10, year2, (year10 - year2) as spread
            FROM raw_data.fmp_treasury_rates
            WHERE year10 IS NOT NULL AND year2 IS NOT NULL
            ORDER BY date DESC
            LIMIT 5
        """)
        for date, y10, y2, spread in cur.fetchall():
            spread_str = f"{spread:+.2f}%" if spread else "N/A"
            print(f"   {date}:  10Y={y10:.2f}%  2Y={y2:.2f}%  Spread={spread_str}")

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
