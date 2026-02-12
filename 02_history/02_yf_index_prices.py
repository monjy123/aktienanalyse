#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lädt historische Index-Kurse über yfinance in raw_data.index_prices.
Wird für die Beta-Berechnung benötigt (Benchmark-Renditen).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import yfinance as yf
from mysql.connector import Error as MySQLError
from db import get_connection

START_DATE = "1970-01-01"

INDEX_MAPPING = {
    "DAX":        "^GDAXI",
    "MDAX":       "^MDAXI",
    "S&P 500":    "^GSPC",
    "STOXX600":   "^STOXX",
    "FTSE 100":   "^FTSE",
    "Nikkei 225": "^N225",
}


def load_index_prices():
    try:
        conn = get_connection(db_name="raw_data", autocommit=False)
        cur = conn.cursor()
    except MySQLError as e:
        print(f"DB-Verbindung fehlgeschlagen: {e}")
        return

    # Tabelle anlegen falls nicht vorhanden
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data.index_prices (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            stock_index VARCHAR(50) NOT NULL,
            yf_ticker VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            close DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_index_date (stock_index, date),
            KEY idx_date (date)
        )
    """)
    conn.commit()

    tickers = list(INDEX_MAPPING.values())
    index_names = list(INDEX_MAPPING.keys())
    print(f"Lade {len(tickers)} Indizes: {', '.join(index_names)}")

    try:
        data = yf.download(
            tickers,
            start=START_DATE,
            progress=True,
            group_by="ticker",
            auto_adjust=False,
        )

        if data.empty:
            print("Keine Daten erhalten.")
            return

    except Exception as e:
        print(f"yfinance Download fehlgeschlagen: {e}")
        return

    insert_sql = """
        INSERT INTO raw_data.index_prices (stock_index, yf_ticker, date, close)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            close = VALUES(close)
    """

    total_rows = 0

    for index_name, yf_ticker in INDEX_MAPPING.items():
        try:
            df = data[yf_ticker].reset_index()
        except KeyError:
            cols = [col for col in data.columns if col[0] == yf_ticker]
            if not cols:
                print(f"  {index_name} ({yf_ticker}): Keine Daten")
                continue
            df = data.loc[:, cols]
            df.columns = [c[1] for c in df.columns]
            df = df.reset_index()

        df.rename(columns={"Date": "date", "Close": "close"}, inplace=True)
        df = df[["date", "close"]].dropna(subset=["close"])

        rows = [
            (index_name, yf_ticker, row["date"].date(), float(row["close"]))
            for _, row in df.iterrows()
        ]

        if not rows:
            print(f"  {index_name} ({yf_ticker}): Keine gültigen Zeilen")
            continue

        # Batch-Insert
        for i in range(0, len(rows), 5000):
            cur.executemany(insert_sql, rows[i:i+5000])
        conn.commit()

        total_rows += len(rows)
        print(f"  {index_name:15} ({yf_ticker:8}): {len(rows):>7,} Zeilen  |  {df['date'].min().date()} bis {df['date'].max().date()}")

    cur.close()
    conn.close()
    print(f"\nGesamt: {total_rows:,} Zeilen gespeichert.")


if __name__ == "__main__":
    load_index_prices()
