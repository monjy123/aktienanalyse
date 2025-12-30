#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Befüllt analytics.company_info mit Stammdaten.

Quellen:
- tickerlist: isin, ticker, company_name, stock_index
- yfinance API: sector, industry, country, currency, description, fiscal_year_end

Update-Frequenz: Jährlich oder bei Bedarf
"""

import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from tqdm import tqdm
from mysql.connector import Error
from db import get_connection

try:
    from deep_translator import GoogleTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    TRANSLATOR_AVAILABLE = False
    print("WARNUNG: deep-translator nicht installiert. Beschreibungen werden nicht übersetzt.")
    print("Installiere mit: pip install deep-translator")


# Threading Konfiguration
MAX_WORKERS = 10


def translate_to_german(text):
    """Übersetze Text von Englisch nach Deutsch."""
    if not text or not TRANSLATOR_AVAILABLE:
        return text

    try:
        # Google Translate ist kostenlos über deep-translator
        translator = GoogleTranslator(source='en', target='de')

        # Teile lange Texte in kleinere Chunks (Google Translate Limit: 5000 Zeichen)
        if len(text) > 4500:
            chunks = []
            sentences = text.split('. ')
            current_chunk = ''

            for sentence in sentences:
                if len(current_chunk) + len(sentence) + 2 < 4500:
                    current_chunk += sentence + '. '
                else:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = sentence + '. '

            if current_chunk:
                chunks.append(current_chunk)

            translated_chunks = [translator.translate(chunk) for chunk in chunks]
            return ' '.join(translated_chunks)
        else:
            return translator.translate(text)

    except Exception as e:
        # Bei Fehler Original-Text zurückgeben
        print(f"Übersetzungsfehler: {e}")
        return text


def get_yf_info(ticker_yf):
    """Hole Stammdaten von yfinance API."""
    if not ticker_yf:
        return None

    try:
        stock = yf.Ticker(ticker_yf)
        info = stock.info

        # Prüfe ob gültige Daten zurückkamen
        if not info or info.get("trailingPegRatio") is None and info.get("sector") is None:
            # Manchmal gibt yfinance leere Dicts zurück
            if not info.get("shortName") and not info.get("longName"):
                return None

        # Beschreibung holen und übersetzen
        description = info.get("longBusinessSummary")
        if description:
            description_de = translate_to_german(description)
        else:
            description_de = None

        # Fiskaljahr-Ende behandeln
        # yfinance gibt manchmal einen Unix-Timestamp zurück, manchmal einen Monatsnamen
        fiscal_year_end = info.get("fiscalYearEnd") or info.get("lastFiscalYearEnd")
        if fiscal_year_end and isinstance(fiscal_year_end, int):
            # Unix-Timestamp -> Monat extrahieren
            import datetime
            dt = datetime.datetime.fromtimestamp(fiscal_year_end)
            months = {
                1: "Januar", 2: "Februar", 3: "März", 4: "April",
                5: "Mai", 6: "Juni", 7: "Juli", 8: "August",
                9: "September", 10: "Oktober", 11: "November", 12: "Dezember"
            }
            fiscal_year_end = months.get(dt.month)

        return {
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "country": info.get("country"),
            "currency": info.get("currency"),
            "description": description_de,
            "fiscal_year_end": fiscal_year_end,
        }

    except Exception as e:
        return None


def process_ticker(isin, ticker_yf, name, stock_index):
    """Verarbeite einen Ticker - hole yfinance Daten."""
    yf_data = get_yf_info(ticker_yf)

    return {
        "isin": isin,
        "ticker": ticker_yf,
        "company_name": name,
        "stock_index": stock_index,
        "sector": yf_data.get("sector") if yf_data else None,
        "industry": yf_data.get("industry") if yf_data else None,
        "country": yf_data.get("country") if yf_data else None,
        "currency": yf_data.get("currency") if yf_data else None,
        "description": yf_data.get("description") if yf_data else None,
        "fiscal_year_end": yf_data.get("fiscal_year_end") if yf_data else None,
    }


def save_company_info(cur, data):
    """Speichere Stammdaten in company_info."""
    sql = """
    INSERT INTO analytics.company_info (
        isin, ticker, company_name, sector, industry,
        country, currency, description, fiscal_year_end, stock_index
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        ticker = VALUES(ticker),
        company_name = VALUES(company_name),
        sector = COALESCE(VALUES(sector), sector),
        industry = COALESCE(VALUES(industry), industry),
        country = COALESCE(VALUES(country), country),
        currency = COALESCE(VALUES(currency), currency),
        description = COALESCE(VALUES(description), description),
        fiscal_year_end = COALESCE(VALUES(fiscal_year_end), fiscal_year_end),
        stock_index = VALUES(stock_index),
        updated_at = NOW()
    """

    cur.execute(sql, (
        data["isin"],
        data["ticker"],
        data["company_name"],
        data["sector"],
        data["industry"],
        data["country"],
        data["currency"],
        data["description"],
        data["fiscal_year_end"],
        data["stock_index"],
    ))


def main():
    print("=" * 60)
    print("COMPANY INFO LADEN (Stammdaten)")
    print("=" * 60)

    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Ticker aus tickerlist laden
        print("Lade Ticker aus tickerlist...")
        cur.execute("""
            SELECT DISTINCT isin, yf_ticker, name, stock_index
            FROM tickerdb.tickerlist
            WHERE isin IS NOT NULL AND isin != ''
        """)
        tickers = cur.fetchall()
        print(f"  → {len(tickers)} Ticker geladen")

        # Statistik: Wie viele haben yf_ticker?
        with_yf = sum(1 for t in tickers if t[1])
        print(f"  → {with_yf} mit yf_ticker (werden bei yfinance abgefragt)")
        print(f"  → {len(tickers) - with_yf} ohne yf_ticker (nur Basisdaten)")

        # Parallel verarbeiten
        print(f"\nLade Stammdaten von yfinance (max {MAX_WORKERS} parallel)...")
        results = []
        success_count = 0
        yf_success_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_ticker, isin, yf_ticker, name, stock_index): isin
                for isin, yf_ticker, name, stock_index in tickers
            }

            with tqdm(total=len(futures), desc="yfinance API") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)

                    if result["sector"]:
                        yf_success_count += 1

                    pbar.update(1)

        # In DB speichern
        print("\nSpeichere in analytics.company_info...")
        for data in tqdm(results, desc="Speichern"):
            try:
                save_company_info(cur, data)
                success_count += 1
            except Error as e:
                print(f"  Fehler bei {data['isin']}: {e}")

        conn.commit()

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("SELECT COUNT(*) FROM analytics.company_info")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM analytics.company_info WHERE sector IS NOT NULL")
        with_sector = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM analytics.company_info WHERE description IS NOT NULL")
        with_desc = cur.fetchone()[0]

        cur.execute("""
            SELECT stock_index, COUNT(*) as cnt
            FROM analytics.company_info
            GROUP BY stock_index
            ORDER BY cnt DESC
        """)
        index_stats = cur.fetchall()

        cur.execute("""
            SELECT sector, COUNT(*) as cnt
            FROM analytics.company_info
            WHERE sector IS NOT NULL
            GROUP BY sector
            ORDER BY cnt DESC
            LIMIT 10
        """)
        sector_stats = cur.fetchall()

        print(f"\n📊 Gesamt Einträge:        {total:,}")
        print(f"✅ Mit Sektor:             {with_sector:,} ({with_sector*100/total:.1f}%)")
        print(f"📝 Mit Beschreibung:       {with_desc:,} ({with_desc*100/total:.1f}%)")

        print("\n📈 Nach Index:")
        for idx, cnt in index_stats:
            print(f"   {idx:20} {cnt:>6,}")

        print("\n🏭 Top 10 Sektoren:")
        for sector, cnt in sector_stats:
            print(f"   {sector:30} {cnt:>6,}")

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
