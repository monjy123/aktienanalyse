#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FMP Economic Indicators Loader

Lädt historische Economic Indicators von FMP API.
Enthält GDP, CPI, Unemployment Rate, Fed Funds, etc.
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

# Thread-lokaler Storage für Sessions
thread_local = threading.local()

# Anzahl paralleler Threads
MAX_WORKERS = 5

# Alle Economic Indicators
ECONOMIC_INDICATORS = [
    "GDP",
    "realGDP",
    "nominalPotentialGDP",
    "realGDPPerCapita",
    "federalFunds",
    "CPI",
    "inflationRate",
    "inflation",
    "retailSales",
    "consumerSentiment",
    "durableGoods",
    "unemploymentRate",
    "totalNonfarmPayroll",
    "initialClaims",
    "industrialProductionTotalIndex",
    "newPrivatelyOwnedHousingUnitsStartedTotalUnits",
    "totalVehicleSales",
    "retailMoneyFunds",
    "smoothedUSRecessionProbabilities",
    "3MonthOr90DayRatesAndYieldsCertificatesOfDeposit",
    "commercialBankInterestRateOnCreditCardPlansAllAccounts",
    "30YearFixedRateMortgageAverage",
    "15YearFixedRateMortgageAverage"
]

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "fmp_economic_indicators_loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL Statements
# =============================================================================

CREATE_ECONOMIC_INDICATORS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_economic_indicators (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    indicator_name VARCHAR(100) NOT NULL,

    -- Daten
    date DATE NOT NULL,
    value DOUBLE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_entry (indicator_name, date),
    INDEX idx_indicator (indicator_name),
    INDEX idx_date (date),
    INDEX idx_indicator_date (indicator_name, date)
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
            time.sleep(0.1)  # Rate Limiting
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


def get_economic_indicator(indicator_name):
    """Lade historische Economic Indicator Daten (alle verfügbaren Daten)."""
    today = datetime.now().strftime("%Y-%m-%d")
    return api_request(f"/stable/economic-indicators", {
        "name": indicator_name,
        "from": "1900-01-01",  # Maximale Historie
        "to": today
    })


# =============================================================================
# Data Processing
# =============================================================================

def fetch_indicator(indicator_name):
    """Fetch Indicator Daten (wird in Thread ausgeführt)."""
    data = get_economic_indicator(indicator_name)
    if data:
        return (indicator_name, data, None)
    return (indicator_name, None, "Keine Daten")


def save_economic_indicator(cur, indicator_name, records):
    """Speichere Economic Indicator Daten."""
    if not records:
        return 0

    sql = """
    INSERT INTO raw_data.fmp_economic_indicators (
        indicator_name, date, value
    ) VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        value = VALUES(value),
        updated_at = NOW()
    """

    count = 0
    for r in records:
        if not r.get("date"):
            continue

        values = (
            indicator_name,
            r.get("date"),
            r.get("value"),
        )

        try:
            cur.execute(sql, values)
            count += 1
        except MySQLError as e:
            logger.warning(f"Fehler beim Speichern {indicator_name} {r.get('date')}: {e}")

    return count


# =============================================================================
# Main
# =============================================================================

def main():
    print("=" * 60)
    print("FMP ECONOMIC INDICATORS LOADER")
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
        cur.execute(CREATE_ECONOMIC_INDICATORS_TABLE)
        conn.commit()
        print("Tabelle erstellt!")

        # Statistik
        indicator_stats = {}
        failed_indicators = []

        # Economic Indicators laden
        print("\n" + "=" * 60)
        print(f"Lade {len(ECONOMIC_INDICATORS)} Economic Indicators...")
        print("=" * 60)

        # Threading für parallele API-Requests
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_indicator, indicator): indicator
                for indicator in ECONOMIC_INDICATORS
            }

            with tqdm(total=len(futures), desc="Economic Indicators") as pbar:
                for future in as_completed(futures):
                    try:
                        indicator_name, data, error = future.result()

                        if error:
                            failed_indicators.append((indicator_name, error))
                            indicator_stats[indicator_name] = 0
                        elif data:
                            count = save_economic_indicator(cur, indicator_name, data)
                            indicator_stats[indicator_name] = count
                            conn.commit()

                    except Exception as e:
                        indicator = futures[future]
                        failed_indicators.append((indicator, str(e)))
                        logger.error(f"Fehler bei {indicator}: {e}")

                    pbar.update(1)

        # Finaler Commit
        conn.commit()

        # Finale Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        # Gesamt Zeilen
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_economic_indicators")
        total_rows = cur.fetchone()[0]

        # Unique Indicators
        cur.execute("SELECT COUNT(DISTINCT indicator_name) FROM raw_data.fmp_economic_indicators")
        unique_indicators = cur.fetchone()[0]

        print(f"\n📊 Angeforderte Indicators: {len(ECONOMIC_INDICATORS)}")
        print(f"✅ Erfolgreich geladen:     {unique_indicators}")
        print(f"📄 Gesamt Datensätze:       {total_rows:,}")

        # Details pro Indicator
        print("\n📊 Datensätze pro Indicator:")
        cur.execute("""
            SELECT indicator_name, COUNT(*) as cnt, MIN(date) as min_date, MAX(date) as max_date
            FROM raw_data.fmp_economic_indicators
            GROUP BY indicator_name
            ORDER BY indicator_name
        """)
        for indicator, cnt, min_date, max_date in cur.fetchall():
            # Kürze lange Namen
            display_name = indicator[:45] + "..." if len(indicator) > 45 else indicator
            print(f"   {display_name:48} {cnt:>6,} ({min_date} - {max_date})")

        # Aktuelle Werte für wichtige Indicators
        print("\n📈 Aktuelle Werte (wichtige Indicators):")
        key_indicators = ["GDP", "CPI", "unemploymentRate", "federalFunds", "inflationRate"]
        for ind in key_indicators:
            cur.execute("""
                SELECT value, date
                FROM raw_data.fmp_economic_indicators
                WHERE indicator_name = %s
                ORDER BY date DESC
                LIMIT 1
            """, (ind,))
            row = cur.fetchone()
            if row:
                value, date = row
                print(f"   {ind:25} {value:>12,.2f}  ({date})")

        if failed_indicators:
            print(f"\n⚠️  Fehlgeschlagene Indicators: {len(failed_indicators)}")
            for ind, reason in failed_indicators:
                print(f"   {ind}: {reason}")

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
