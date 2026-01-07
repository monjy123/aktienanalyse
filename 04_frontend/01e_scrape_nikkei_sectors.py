#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraping von NIKKEI 225 Sektor-Informationen von Wikipedia.

Scraped die Liste von Wikipedia und mappt die Sektoren auf die
bestehenden Kategorien in der Datenbank (Yahoo Finance Schema).

Nutzung: python 04_frontend/01e_scrape_nikkei_sectors.py
"""

import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from mysql.connector import Error
from db import get_connection


# Wikipedia-URL für NIKKEI 225 Komponenten
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/Nikkei_225"

# Sektor-Mapping: Wikipedia/Japanisch -> Yahoo Finance Standard
# Basierend auf: https://en.wikipedia.org/wiki/Nikkei_225
SECTOR_MAPPING = {
    # Technology
    "Information & Communication": "Technology",
    "Electric Appliances": "Technology",
    "Precision Instruments": "Technology",
    "Technology": "Technology",

    # Industrials
    "Machinery": "Industrials",
    "Transportation Equipment": "Industrials",
    "Construction": "Industrials",
    "Marine Transportation": "Industrials",
    "Air Transportation": "Industrials",
    "Land Transportation": "Industrials",
    "Warehousing & Harbor Transportation Services": "Industrials",
    "Metal Products": "Industrials",

    # Consumer Cyclical
    "Retail Trade": "Consumer Cyclical",
    "Textiles & Apparel": "Consumer Cyclical",
    "Other Products": "Consumer Cyclical",
    "Services": "Consumer Cyclical",
    "Automobile": "Consumer Cyclical",

    # Consumer Defensive
    "Foods": "Consumer Defensive",
    "Fishery, Agriculture & Forestry": "Consumer Defensive",

    # Basic Materials
    "Chemicals": "Basic Materials",
    "Glass & Ceramics Products": "Basic Materials",
    "Paper & Pulp": "Basic Materials",
    "Nonferrous Metals": "Basic Materials",
    "Steel": "Basic Materials",
    "Iron & Steel": "Basic Materials",
    "Rubber Products": "Basic Materials",
    "Mining": "Basic Materials",

    # Energy
    "Oil & Coal Products": "Energy",
    "Electric Power & Gas": "Energy",
    "Petroleum": "Energy",

    # Utilities
    "Utilities": "Utilities",

    # Financial Services
    "Banks": "Financial Services",
    "Securities & Commodity Futures": "Financial Services",
    "Insurance": "Financial Services",
    "Other Financing Business": "Financial Services",
    "Financial Services": "Financial Services",

    # Healthcare
    "Pharmaceutical": "Healthcare",
    "Pharmaceuticals": "Healthcare",

    # Real Estate
    "Real Estate": "Real Estate",

    # Communication Services
    "Communication Services": "Communication Services",
}


def scrape_nikkei_225_from_wikipedia():
    """
    Scrape NIKKEI 225 Komponenten von Wikipedia.

    Returns:
        list of dict: [{"ticker": "6857.T", "name": "ADVANTEST", "sector": "Technology"}, ...]
    """
    print(f"Lade Wikipedia-Seite: {WIKIPEDIA_URL}")

    try:
        # User-Agent Header setzen, damit Wikipedia nicht blockt
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(WIKIPEDIA_URL, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Finde die Tabelle mit den Komponenten
        # Die Wikipedia-Seite hat mehrere Tabellen, die richtige hat "Code" und "Company" Spalten
        tables = soup.find_all('table', class_='wikitable')

        companies = []

        for table in tables:
            headers = [th.get_text(strip=True) for th in table.find_all('th')]

            # Prüfe ob es die richtige Tabelle ist (sollte "Code", "Company" und "Sector" haben)
            if 'Code' not in headers or 'Company' not in headers:
                continue

            # Finde Index der Spalten
            try:
                code_idx = headers.index('Code')
                company_idx = headers.index('Company')
                sector_idx = headers.index('Sector') if 'Sector' in headers else headers.index('Industry')
            except ValueError:
                continue

            # Parse Zeilen
            rows = table.find_all('tr')[1:]  # Skip header

            for row in rows:
                cols = row.find_all('td')
                if len(cols) <= max(code_idx, company_idx, sector_idx):
                    continue

                # Code (Ticker ohne .T)
                code = cols[code_idx].get_text(strip=True)
                if not code or not code.isdigit():
                    continue

                ticker = f"{code}.T"

                # Company Name
                company = cols[company_idx].get_text(strip=True)

                # Sector
                sector_raw = cols[sector_idx].get_text(strip=True)
                sector_mapped = SECTOR_MAPPING.get(sector_raw, sector_raw)

                companies.append({
                    "ticker": ticker,
                    "name": company,
                    "sector_raw": sector_raw,
                    "sector": sector_mapped
                })

        return companies

    except requests.exceptions.RequestException as e:
        print(f"FEHLER beim Laden der Wikipedia-Seite: {e}")
        return []
    except Exception as e:
        print(f"FEHLER beim Parsen: {e}")
        return []


def update_company_info_by_ticker(cur, ticker, sector, industry=None):
    """
    Update Sektor-Information für ein Unternehmen anhand des Tickers.

    WICHTIG: Verwendet COALESCE, damit NULL-Werte die bestehenden Daten nicht überschreiben.
    """
    sql = """
    UPDATE analytics.company_info ci
    JOIN tickerdb.tickerlist tl ON ci.isin = tl.isin
    SET
        ci.sector = COALESCE(%s, ci.sector),
        ci.industry = COALESCE(%s, ci.industry),
        ci.updated_at = NOW()
    WHERE tl.yf_ticker = %s
    """

    cur.execute(sql, (sector, industry, ticker))
    return cur.rowcount > 0


def main():
    print("=" * 60)
    print("NIKKEI 225 SEKTOR-DATEN VON WIKIPEDIA LADEN")
    print("=" * 60)

    # Wikipedia scrapen
    companies = scrape_nikkei_225_from_wikipedia()

    if not companies:
        print("\nKeine Daten von Wikipedia geladen. Abbruch.")
        return

    print(f"\n✓ {len(companies)} Unternehmen von Wikipedia geladen")

    # Zeige Sektor-Verteilung (vor Mapping)
    print("\nSektor-Verteilung (Wikipedia Original):")
    sector_counts_raw = {}
    for company in companies:
        sector = company["sector_raw"]
        sector_counts_raw[sector] = sector_counts_raw.get(sector, 0) + 1

    for sector, count in sorted(sector_counts_raw.items(), key=lambda x: -x[1]):
        mapped = SECTOR_MAPPING.get(sector, sector)
        if mapped != sector:
            print(f"  {sector:40} -> {mapped:25} ({count:3} Unternehmen)")
        else:
            print(f"  {sector:40}    {' '*25} ({count:3} Unternehmen)")

    # Zeige Sektor-Verteilung (nach Mapping)
    print("\nSektor-Verteilung (Nach Mapping - Yahoo Finance Schema):")
    sector_counts = {}
    for company in companies:
        sector = company["sector"]
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        print(f"  {sector:30} {count:3} Unternehmen")

    # In Datenbank speichern
    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Prüfe aktuelle Sektor-Abdeckung
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN sector IS NOT NULL AND sector != '' THEN 1 ELSE 0 END) as with_sector
            FROM analytics.company_info ci
            JOIN tickerdb.tickerlist tl ON ci.isin = tl.isin
            WHERE tl.stock_index = 'NIKKEI 225'
        """)
        before_stats = cur.fetchone()
        print(f"  → Aktuell: {before_stats[1]}/{before_stats[0]} mit Sektor")

        # Update durchführen
        print("\nSpeichere Sektor-Daten in Datenbank...")
        updated_count = 0
        not_found = []

        for company in tqdm(companies, desc="Speichern"):
            try:
                if update_company_info_by_ticker(cur, company["ticker"], company["sector"], company["sector_raw"]):
                    updated_count += 1
                else:
                    not_found.append(company["ticker"])
            except Error as e:
                print(f"  Fehler bei {company['ticker']}: {e}")

        conn.commit()
        print(f"\n  → {updated_count} Einträge aktualisiert")

        if not_found:
            print(f"\n⚠️  {len(not_found)} Ticker in DB nicht gefunden:")
            for ticker in not_found[:10]:
                print(f"     {ticker}")
            if len(not_found) > 10:
                print(f"     ... und {len(not_found) - 10} weitere")

        # Statistik nach Update
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN sector IS NOT NULL AND sector != '' THEN 1 ELSE 0 END) as with_sector
            FROM analytics.company_info ci
            JOIN tickerdb.tickerlist tl ON ci.isin = tl.isin
            WHERE tl.stock_index = 'NIKKEI 225'
        """)
        after_stats = cur.fetchone()

        print(f"\n📊 NIKKEI 225:")
        print(f"   Vorher:  {before_stats[1]}/{before_stats[0]} mit Sektor ({before_stats[1]*100/before_stats[0] if before_stats[0] > 0 else 0:.1f}%)")
        print(f"   Nachher: {after_stats[1]}/{after_stats[0]} mit Sektor ({after_stats[1]*100/after_stats[0] if after_stats[0] > 0 else 0:.1f}%)")
        print(f"   Gewinn:  +{after_stats[1] - before_stats[1]} Sektoren")

        # Zeige Sektor-Verteilung in DB
        cur.execute("""
            SELECT ci.sector, COUNT(*) as cnt
            FROM analytics.company_info ci
            JOIN tickerdb.tickerlist tl ON ci.isin = tl.isin
            WHERE tl.stock_index = 'NIKKEI 225' AND ci.sector IS NOT NULL
            GROUP BY ci.sector
            ORDER BY cnt DESC
        """)
        sector_stats = cur.fetchall()

        if sector_stats:
            print("\n🏭 Sektor-Verteilung NIKKEI 225 (in Datenbank):")
            for sector, cnt in sector_stats:
                print(f"   {sector:30} {cnt:>3}")

        print("\n✅ Daten erfolgreich von Wikipedia geladen!")
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
