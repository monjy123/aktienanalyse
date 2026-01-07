#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Einmaliges Laden von Sektor-Informationen für NIKKEI 225 von FMP API.

Da yfinance für japanische Aktien keine Sektor-Informationen liefert,
werden diese einmalig von FMP Company Profile geladen.

Zukünftig: Das reguläre 01_load_company_info.py Skript mit yfinance
wird die FMP-Daten NICHT überschreiben (wegen COALESCE in UPDATE).

Nutzung: python 04_frontend/01d_load_fmp_sector_nikkei.py
"""

import sys
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from tqdm import tqdm
from dotenv import load_dotenv
from mysql.connector import Error
from db import get_connection

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"

# Threading Konfiguration
MAX_WORKERS = 5


def get_fmp_company_profile(ticker_fmp):
    """
    Hole Company Profile von FMP API.

    Returns:
        dict mit sector, industry, country, currency oder None bei Fehler
    """
    if not ticker_fmp:
        return None

    try:
        url = f"{FMP_BASE_URL}/profile/{ticker_fmp}"
        params = {"apikey": FMP_API_KEY}

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        # FMP gibt eine Liste zurück, erstes Element ist das Profil
        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        profile = data[0]

        return {
            "sector": profile.get("sector"),
            "industry": profile.get("industry"),
            "country": profile.get("country"),
            "currency": profile.get("currency"),
        }

    except requests.exceptions.RequestException as e:
        print(f"  API-Fehler für {ticker_fmp}: {e}")
        return None
    except Exception as e:
        print(f"  Fehler für {ticker_fmp}: {e}")
        return None


def process_ticker(isin, ticker_fmp):
    """Verarbeite einen Ticker - hole FMP Daten."""
    fmp_data = get_fmp_company_profile(ticker_fmp)

    return {
        "isin": isin,
        "ticker": ticker_fmp,
        "sector": fmp_data.get("sector") if fmp_data else None,
        "industry": fmp_data.get("industry") if fmp_data else None,
        "country": fmp_data.get("country") if fmp_data else None,
        "currency": fmp_data.get("currency") if fmp_data else None,
    }


def update_company_info(cur, data):
    """
    Update nur Sektor-Informationen in company_info.

    WICHTIG: Verwendet COALESCE, damit NULL-Werte die bestehenden Daten nicht überschreiben.
    """
    sql = """
    UPDATE analytics.company_info
    SET
        sector = COALESCE(%s, sector),
        industry = COALESCE(%s, industry),
        country = COALESCE(%s, country),
        currency = COALESCE(%s, currency),
        updated_at = NOW()
    WHERE isin = %s
    """

    cur.execute(sql, (
        data["sector"],
        data["industry"],
        data["country"],
        data["currency"],
        data["isin"],
    ))

    return cur.rowcount > 0


def main():
    print("=" * 60)
    print("FMP SEKTOR-DATEN FÜR NIKKEI 225 LADEN (EINMALIG)")
    print("=" * 60)

    if not FMP_API_KEY:
        print("\nFEHLER: FMP_API_KEY nicht in .env gefunden!")
        return

    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Ticker aus tickerlist laden (nur NIKKEI 225)
        print("Lade NIKKEI 225 Ticker aus tickerlist...")
        cur.execute("""
            SELECT DISTINCT isin, yf_ticker
            FROM tickerdb.tickerlist
            WHERE stock_index = 'NIKKEI 225'
              AND isin IS NOT NULL AND isin != ''
              AND yf_ticker IS NOT NULL AND yf_ticker != ''
        """)
        tickers = cur.fetchall()
        print(f"  → {len(tickers)} NIKKEI 225 Ticker geladen")

        # Prüfe aktuelle Sektor-Abdeckung
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN sector IS NOT NULL AND sector != '' THEN 1 ELSE 0 END) as with_sector
            FROM analytics.company_info
            WHERE stock_index = 'NIKKEI 225'
        """)
        before_stats = cur.fetchone()
        print(f"  → Aktuell: {before_stats[1]}/{before_stats[0]} mit Sektor")

        # Parallel von FMP laden
        print(f"\nLade Sektor-Daten von FMP API (max {MAX_WORKERS} parallel)...")
        print("⚠️  Bitte Geduld, dies kann einige Minuten dauern...")

        results = []
        success_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_ticker, isin, ticker_fmp): (isin, ticker_fmp)
                for isin, ticker_fmp in tickers
            }

            with tqdm(total=len(futures), desc="FMP API") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)

                    if result["sector"]:
                        success_count += 1

                    pbar.update(1)

                    # Rate Limiting: 5 Requests/Sekunde (Free Plan)
                    time.sleep(0.2)

        print(f"\n  → {success_count}/{len(results)} Ticker mit Sektor von FMP")

        # In DB speichern
        print("\nSpeichere in analytics.company_info...")
        updated_count = 0

        for data in tqdm(results, desc="Speichern"):
            try:
                if update_company_info(cur, data):
                    updated_count += 1
            except Error as e:
                print(f"  Fehler bei {data['isin']}: {e}")

        conn.commit()
        print(f"  → {updated_count} Einträge aktualisiert")

        # Statistik nach Update
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN sector IS NOT NULL AND sector != '' THEN 1 ELSE 0 END) as with_sector
            FROM analytics.company_info
            WHERE stock_index = 'NIKKEI 225'
        """)
        after_stats = cur.fetchone()

        print(f"\n📊 NIKKEI 225:")
        print(f"   Vorher:  {before_stats[1]}/{before_stats[0]} mit Sektor ({before_stats[1]*100/before_stats[0]:.1f}%)")
        print(f"   Nachher: {after_stats[1]}/{after_stats[0]} mit Sektor ({after_stats[1]*100/after_stats[0]:.1f}%)")
        print(f"   Gewinn:  +{after_stats[1] - before_stats[1]} Sektoren")

        # Zeige Sektor-Verteilung
        cur.execute("""
            SELECT sector, COUNT(*) as cnt
            FROM analytics.company_info
            WHERE stock_index = 'NIKKEI 225' AND sector IS NOT NULL
            GROUP BY sector
            ORDER BY cnt DESC
        """)
        sector_stats = cur.fetchall()

        if sector_stats:
            print("\n🏭 Sektor-Verteilung NIKKEI 225:")
            for sector, cnt in sector_stats:
                print(f"   {sector:30} {cnt:>3}")

        print("\n✅ Daten erfolgreich von FMP geladen!")
        print("ℹ️  Zukünftige Updates mit yfinance werden diese Daten NICHT überschreiben.")

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
