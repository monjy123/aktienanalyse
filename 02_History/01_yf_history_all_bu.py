#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Neues Skript zum Laden von historischen Kursdaten in die neue Tabelle `yf_prices`.
- ISIN wird übernommen
- Ticker kommen aus tickerdb.tickerlist
- Spaltenstruktur ist an die neue Tabelle angepasst
"""

import yfinance as yf
import mysql.connector
from datetime import datetime
import pandas as pd
import time

# Micro-Batching Helper

def execute_in_chunks(cursor, sql, data, chunk_size=5000):
    for i in range(0, len(data), chunk_size):
        subset = data[i:i+chunk_size]
        cursor.executemany(sql, subset)

# ⚙️ Konfiguration
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "monjy",
    "password": "Emst4558!!",
    "database": "raw_data"
}

BATCH_SIZE = 50
START_DATE = "1970-01-01"

# 📡 DB-Verbindung
conn = mysql.connector.connect(**DB_CONFIG)
cur = conn.cursor(dictionary=True)

# 🧾 Ticker + ISIN aus neuer Tabelle laden
cur.execute("""
    SELECT isin, yf_ticker, stock_index
    FROM tickerdb.tickerlist
    WHERE yf_ticker IS NOT NULL;
""")
records = cur.fetchall()

print(f"📈 {len(records)} Einträge gefunden.")

# Gruppiere nach yf_ticker
ticker_map = {}
for r in records:
    ticker_map[r["yf_ticker"]] = r

all_tickers = list(ticker_map.keys())

# 🔁 Verarbeitung in Batches
for i in range(0, len(all_tickers), BATCH_SIZE):
    batch = all_tickers[i:i+BATCH_SIZE]
    print(f"\n📦 Lade Batch {i//BATCH_SIZE + 1} ({len(batch)} Ticker)...")

    try:
        data = yf.download(batch, start=START_DATE, progress=False, group_by='ticker', auto_adjust=False)
        if data.empty:
            print("⚠️ Keine Daten erhalten.")
            continue

        all_rows = []

        for ticker in batch:
            info = ticker_map[ticker]
            isin = info["isin"]
            stock_index = info["stock_index"]

            try:
                df = data[ticker].reset_index()
            except KeyError:
                cols = [col for col in data.columns if col[0] == ticker]
                if not cols:
                    continue
                df = data.loc[:, cols]
                df.columns = [c[1] for c in df.columns]
                df = df.reset_index()

            df['isin'] = isin
            df['ticker_yf'] = ticker
            df['stock_index'] = stock_index

            df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Adj Close': 'adj_close',
                'Volume': 'volume'
            }, inplace=True)

            all_rows.append(df)

        if not all_rows:
            print("⚠️ Keine gültigen Tickerdaten in diesem Batch.")
            continue

        merged = pd.concat(all_rows)
        merged.dropna(subset=["date", "close"], inplace=True)

        # SQL angepasst an neue Tabelle
        insert_sql = """
            INSERT INTO yf_prices (isin, ticker_yf, date, open, high, low, close, adj_close, volume, stock_index)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                open=VALUES(open),
                high=VALUES(high),
                low=VALUES(low),
                close=VALUES(close),
                adj_close=VALUES(adj_close),
                volume=VALUES(volume),
                stock_index=VALUES(stock_index),
                updated_at=NOW();
        """

        rows = [
            (
                row["isin"],
                row["ticker_yf"],
                row["date"].date(),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("adj_close"),
                int(row["volume"]) if not pd.isna(row["volume"]) else None,
                row["stock_index"]
            )
            for _, row in merged.iterrows()
        ]

        execute_in_chunks(cur, insert_sql, rows, chunk_size=5000)
        conn.commit()

        print(f"✅ Batch {i//BATCH_SIZE + 1}: {len(rows)} Kurszeilen gespeichert.")
        time.sleep(3)

    except Exception as e:
        print(f"❌ Fehler in Batch {i//BATCH_SIZE + 1}: {e}")
        conn.rollback()
        time.sleep(10)

cur.close()
conn.close()
print("\n🎉 Alle Kursdaten erfolgreich geladen.")
