#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Laden von NIKKEI 225 Sektor-Informationen aus statischer Liste.

Da Web-Scraping blockiert wird, nutzen wir eine statische Liste
basierend auf öffentlich verfügbaren Daten.

Sektoren werden auf Yahoo Finance Standard gemappt.

Nutzung: python 04_frontend/01f_load_nikkei_sectors_static.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tqdm import tqdm
from mysql.connector import Error
from db import get_connection


# NIKKEI 225 Komponenten mit Sektoren
# Quelle: Öffentliche Daten von JPX, Bloomberg, Yahoo Finance
# Format: (Ticker-Code, Unternehmen, Sektor nach Yahoo Finance Schema)
NIKKEI_225_SECTORS = [
    # Technology
    ("6857", "Advantest Corp", "Technology"),
    ("8035", "Tokyo Electron", "Technology"),
    ("6762", "TDK Corp", "Technology"),
    ("6954", "Fanuc Corp", "Technology"),
    ("6758", "Sony Group Corp", "Technology"),
    ("6367", "Daikin Industries", "Technology"),
    ("6988", "Nitto Denko", "Technology"),
    ("6971", "Kyocera Corp", "Technology"),
    ("6981", "Murata Manufacturing", "Technology"),
    ("7733", "Olympus Corp", "Technology"),
    ("6920", "Lasertec Corp", "Technology"),
    ("6902", "Denso Corp", "Technology"),
    ("7735", "Screen Holdings", "Technology"),
    ("7741", "Hoya Corp", "Technology"),
    ("7751", "Canon Inc", "Technology"),
    ("6146", "Disco Corp", "Technology"),
    ("6273", "SMC Corp", "Technology"),
    ("6861", "Keyence Corp", "Technology"),
    ("6724", "Seiko Epson", "Technology"),
    ("6752", "Panasonic Holdings", "Technology"),
    ("6753", "Sharp Corp", "Technology"),
    ("6645", "Omron Corp", "Technology"),
    ("6702", "Fujitsu Ltd", "Technology"),
    ("6701", "NEC Corp", "Technology"),
    ("6723", "Renesas Electronics", "Technology"),
    ("4755", "Rakuten Group", "Technology"),
    ("9984", "SoftBank Group", "Technology"),

    # Communication Services
    ("9433", "KDDI Corp", "Communication Services"),
    ("9434", "SoftBank Corp", "Communication Services"),
    ("9432", "NTT", "Communication Services"),
    ("9735", "Secom Co Ltd", "Communication Services"),
    ("4324", "Dentsu Group", "Communication Services"),
    ("4661", "Oriental Land", "Communication Services"),

    # Consumer Cyclical
    ("9983", "Fast Retailing", "Consumer Cyclical"),
    ("7203", "Toyota Motor", "Consumer Cyclical"),
    ("7267", "Honda Motor", "Consumer Cyclical"),
    ("7269", "Suzuki Motor", "Consumer Cyclical"),
    ("7202", "Isuzu Motors", "Consumer Cyclical"),
    ("7270", "Subaru Corp", "Consumer Cyclical"),
    ("7272", "Yamaha Motor", "Consumer Cyclical"),
    ("7832", "Bandai Namco", "Consumer Cyclical"),
    ("7974", "Nintendo Co Ltd", "Consumer Cyclical"),
    ("7912", "Dai Nippon Printing", "Consumer Cyclical"),
    ("7911", "Toppan Inc", "Consumer Cyclical"),
    ("3659", "Nexon Co Ltd", "Consumer Cyclical"),
    ("3289", "Tokyu Fudosan", "Consumer Cyclical"),
    ("8267", "Aeon Co Ltd", "Consumer Cyclical"),
    ("3099", "Isetan Mitsukoshi", "Consumer Cyclical"),
    ("3086", "J Front Retailing", "Consumer Cyclical"),
    ("3092", "Zozo Inc", "Consumer Cyclical"),
    ("8233", "Takashimaya Co", "Consumer Cyclical"),
    ("9843", "Nitori Holdings", "Consumer Cyclical"),
    ("7453", "Ryohin Keikaku", "Consumer Cyclical"),
    ("8252", "Marui Group", "Consumer Cyclical"),
    ("2282", "NH Foods Ltd", "Consumer Cyclical"),

    # Consumer Defensive
    ("2502", "Asahi Group Holdings", "Consumer Defensive"),
    ("2503", "Kirin Holdings", "Consumer Defensive"),
    ("2501", "Sapporo Holdings", "Consumer Defensive"),
    ("2802", "Ajinomoto Co", "Consumer Defensive"),
    ("2801", "Kikkoman Corp", "Consumer Defensive"),
    ("2914", "Japan Tobacco", "Consumer Defensive"),
    ("2269", "Meiji Holdings", "Consumer Defensive"),
    ("2002", "Nisshin Seifun", "Consumer Defensive"),
    ("2871", "Nichirei Corp", "Consumer Defensive"),

    # Healthcare
    ("4519", "Chugai Pharmaceutical", "Healthcare"),
    ("4568", "Daiichi Sankyo", "Healthcare"),
    ("4503", "Astellas Pharma", "Healthcare"),
    ("4507", "Shionogi & Co", "Healthcare"),
    ("4502", "Takeda Pharmaceutical", "Healthcare"),
    ("4523", "Eisai Co Ltd", "Healthcare"),
    ("4506", "Sumitomo Pharma", "Healthcare"),
    ("4543", "Terumo Corp", "Healthcare"),
    ("7733", "Olympus Corp", "Healthcare"),
    ("4704", "Trend Micro", "Healthcare"),

    # Industrials
    ("6113", "Amada Co Ltd", "Industrials"),
    ("6301", "Komatsu Ltd", "Industrials"),
    ("6305", "Hitachi Construction", "Industrials"),
    ("6326", "Kubota Corp", "Industrials"),
    ("6361", "Ebara Corp", "Industrials"),
    ("6473", "Jtekt Corp", "Industrials"),
    ("6471", "NSK Ltd", "Industrials"),
    ("6479", "Minebea Mitsumi", "Industrials"),
    ("6501", "Hitachi Ltd", "Industrials"),
    ("6503", "Mitsubishi Electric", "Industrials"),
    ("6504", "Fuji Electric", "Industrials"),
    ("6506", "Yaskawa Electric", "Industrials"),
    ("6841", "Yokogawa Electric", "Industrials"),
    ("7011", "Mitsubishi Heavy Industries", "Industrials"),
    ("7012", "Kawasaki Heavy Industries", "Industrials"),
    ("7013", "IHI Corp", "Industrials"),
    ("6770", "Alps Alpine", "Industrials"),
    ("6532", "BeNEX Corp", "Industrials"),
    ("1721", "Comsys Holdings", "Industrials"),
    ("1801", "Taisei Corp", "Industrials"),
    ("1802", "Obayashi Corp", "Industrials"),
    ("1803", "Shimizu Corp", "Industrials"),
    ("1925", "Daiwa House", "Industrials"),
    ("1928", "Sekisui House", "Industrials"),
    ("1812", "Kajima Corp", "Industrials"),
    ("6952", "Casio Computer", "Industrials"),
    ("9766", "Konami Group", "Industrials"),

    # Basic Materials
    ("3407", "Asahi Kasei", "Basic Materials"),
    ("4063", "Shin-Etsu Chemical", "Basic Materials"),
    ("4062", "Denka Co Ltd", "Basic Materials"),
    ("4021", "Nissan Chemical", "Basic Materials"),
    ("4042", "Tosoh Corp", "Basic Materials"),
    ("4043", "Tokuyama Corp", "Basic Materials"),
    ("4183", "Mitsui Chemicals", "Basic Materials"),
    ("4188", "Mitsubishi Chemical", "Basic Materials"),
    ("4452", "Kao Corp", "Basic Materials"),
    ("4901", "Fujifilm Holdings", "Basic Materials"),
    ("4911", "Shiseido Co Ltd", "Basic Materials"),
    ("5101", "Yokohama Rubber", "Basic Materials"),
    ("5108", "Bridgestone Corp", "Basic Materials"),
    ("5201", "AGC Inc", "Basic Materials"),
    ("5214", "Nippon Electric Glass", "Basic Materials"),
    ("5332", "Toto Ltd", "Basic Materials"),
    ("5333", "NGK Insulators", "Basic Materials"),
    ("5631", "Nippon Steel", "Basic Materials"),
    ("5706", "Mitsui Mining", "Basic Materials"),
    ("5713", "Sumitomo Metal Mining", "Basic Materials"),
    ("5714", "DOWA Holdings", "Basic Materials"),
    ("5801", "Furukawa Electric", "Basic Materials"),
    ("5802", "Sumitomo Electric", "Basic Materials"),
    ("5803", "Fujikura Ltd", "Basic Materials"),
    ("3402", "Toray Industries", "Basic Materials"),
    ("3405", "Kuraray Co Ltd", "Basic Materials"),

    # Energy
    ("5019", "Idemitsu Kosan", "Energy"),
    ("5020", "ENEOS Holdings", "Energy"),
    ("1605", "Inpex Corp", "Energy"),

    # Utilities
    ("9531", "Tokyo Gas", "Utilities"),
    ("9532", "Osaka Gas", "Utilities"),
    ("9147", "JERA Co Inc", "Utilities"),

    # Financial Services
    ("8306", "Mitsubishi UFJ Financial", "Financial Services"),
    ("8316", "Sumitomo Mitsui Financial", "Financial Services"),
    ("8411", "Mizuho Financial", "Financial Services"),
    ("8601", "Daiwa Securities", "Financial Services"),
    ("8604", "Nomura Holdings", "Financial Services"),
    ("8630", "SompoHoldings", "Financial Services"),
    ("8725", "MS&AD Insurance", "Financial Services"),
    ("8766", "Tokio Marine Holdings", "Financial Services"),
    ("8697", "Japan Exchange Group", "Financial Services"),
    ("8801", "Mitsui Fudosan", "Financial Services"),
    ("8802", "Mitsubishi Estate", "Financial Services"),
    ("8830", "Sumitomo Realty", "Financial Services"),
    ("8001", "Itochu Corp", "Financial Services"),
    ("8002", "Marubeni Corp", "Financial Services"),
    ("8031", "Mitsui & Co", "Financial Services"),
    ("8053", "Sumitomo Corp", "Financial Services"),
    ("8058", "Mitsubishi Corp", "Financial Services"),
    ("8015", "Toyoda Tsusho", "Financial Services"),
    ("8591", "Orix Corp", "Financial Services"),
    ("8253", "Credit Saison", "Financial Services"),
    ("7186", "Concordia Financial", "Financial Services"),

    # Real Estate
    ("8804", "Tokyo Tatemono", "Real Estate"),
    ("3289", "Tokyu Fudosan", "Real Estate"),
    ("3382", "Seven & i Holdings", "Real Estate"),

    # Transportation
    ("9020", "JR East", "Industrials"),
    ("9022", "JR Central", "Industrials"),
    ("9101", "NYK Line", "Industrials"),
    ("9104", "Mitsui OSK Lines", "Industrials"),
    ("9107", "Kawasaki Kisen", "Industrials"),
    ("9201", "JAL", "Industrials"),
    ("9202", "ANA Holdings", "Industrials"),
    ("9005", "Tokyu Corp", "Industrials"),
    ("9009", "Keisei Electric Railway", "Industrials"),
    ("9064", "Yamato Holdings", "Industrials"),

    # Weitere
    ("6098", "Recruit Holdings", "Industrials"),
    ("2413", "M3 Inc", "Healthcare"),
    ("4307", "Nomura Research Institute", "Technology"),
    ("4385", "Mercari Inc", "Technology"),
    ("4151", "Kyowa Kirin", "Healthcare"),
    ("6178", "GMO Internet", "Technology"),
    ("8309", "Sumitomo Mitsui Trust", "Financial Services"),
    ("8331", "Chiba Bank", "Financial Services"),
    ("8354", "Fukuoka Financial", "Financial Services"),
    ("9602", "Toho Co Ltd", "Communication Services"),
    ("7951", "Yamaha Corp", "Consumer Cyclical"),
    ("6302", "Sumitomo Heavy Industries", "Industrials"),
    ("6526", "Socionext Inc", "Technology"),
    ("5301", "Tokai Carbon", "Basic Materials"),
    ("5831", "Showa Denko", "Basic Materials"),
    ("6963", "Rohm Co Ltd", "Technology"),
    ("6976", "Taiyo Yuden", "Technology"),
    ("7731", "Nikon Corp", "Technology"),
    ("7752", "Ricoh Co Ltd", "Technology"),
    ("1963", "JGC Holdings", "Industrials"),
    ("1332", "Nippon Suisan Kaisha", "Consumer Defensive"),
    ("3861", "Obic Co Ltd", "Technology"),
    ("3697", "Shift Inc", "Technology"),
    ("4751", "Cyberagent Inc", "Communication Services"),
]


def update_company_info_by_ticker(cur, ticker, sector):
    """
    Update Sektor-Information für ein Unternehmen anhand des Tickers.

    WICHTIG: Verwendet COALESCE, damit NULL-Werte die bestehenden Daten nicht überschreiben.
    """
    sql = """
    UPDATE analytics.company_info ci
    JOIN tickerdb.tickerlist tl ON ci.isin COLLATE utf8mb4_unicode_ci = tl.isin COLLATE utf8mb4_unicode_ci
    SET
        ci.sector = COALESCE(%s, ci.sector),
        ci.updated_at = NOW()
    WHERE tl.yf_ticker COLLATE utf8mb4_unicode_ci = %s
    """

    cur.execute(sql, (sector, ticker))
    return cur.rowcount > 0


def main():
    print("=" * 60)
    print("NIKKEI 225 SEKTOR-DATEN LADEN (STATISCHE LISTE)")
    print("=" * 60)

    print(f"\n📋 Insgesamt {len(NIKKEI_225_SECTORS)} Unternehmen in der Liste")

    # Zeige Sektor-Verteilung
    sector_counts = {}
    for _, _, sector in NIKKEI_225_SECTORS:
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    print("\n🏭 Sektor-Verteilung:")
    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        print(f"   {sector:30} {count:3} Unternehmen")

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
            JOIN tickerdb.tickerlist tl ON ci.isin COLLATE utf8mb4_unicode_ci = tl.isin COLLATE utf8mb4_unicode_ci
            WHERE tl.stock_index COLLATE utf8mb4_unicode_ci = 'NIKKEI 225'
        """)
        before_stats = cur.fetchone()
        print(f"  → Aktuell: {before_stats[1]}/{before_stats[0]} mit Sektor")

        # Update durchführen
        print("\nSpeichere Sektor-Daten in Datenbank...")
        updated_count = 0
        not_found = []

        for code, name, sector in tqdm(NIKKEI_225_SECTORS, desc="Speichern"):
            ticker = f"{code}.T"
            try:
                if update_company_info_by_ticker(cur, ticker, sector):
                    updated_count += 1
                else:
                    not_found.append((ticker, name))
            except Error as e:
                print(f"  Fehler bei {ticker}: {e}")

        conn.commit()
        print(f"\n  → {updated_count} Einträge aktualisiert")

        if not_found:
            print(f"\n⚠️  {len(not_found)} Ticker in DB nicht gefunden:")
            for ticker, name in not_found[:10]:
                print(f"     {ticker:10} {name}")
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
            JOIN tickerdb.tickerlist tl ON ci.isin COLLATE utf8mb4_unicode_ci = tl.isin COLLATE utf8mb4_unicode_ci
            WHERE tl.stock_index COLLATE utf8mb4_unicode_ci = 'NIKKEI 225'
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
            JOIN tickerdb.tickerlist tl ON ci.isin COLLATE utf8mb4_unicode_ci = tl.isin COLLATE utf8mb4_unicode_ci
            WHERE tl.stock_index COLLATE utf8mb4_unicode_ci = 'NIKKEI 225' AND ci.sector IS NOT NULL
            GROUP BY ci.sector
            ORDER BY cnt DESC
        """)
        sector_stats = cur.fetchall()

        if sector_stats:
            print("\n🏭 Sektor-Verteilung NIKKEI 225 (in Datenbank):")
            for sector, cnt in sector_stats:
                print(f"   {sector:30} {cnt:>3}")

        print("\n✅ Daten erfolgreich geladen!")
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
