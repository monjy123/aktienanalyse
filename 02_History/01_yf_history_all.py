#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lädt historische Kursdaten über yfinance in raw_data.yf_prices
- ISIN wird übernommen
- Yahoo-Ticker kommen aus tickerdb.tickerlist
- Struktur passt exakt zur Tabelle yf_prices
"""

import time
import pandas as pd
import yfinance as yf
from mysql.connector import Error as MySQLError
from db import get_connection

BATCH_SIZE = 50
START_DATE = "1970-01-01"


# -----------------------------------------------------------
# Helper: Micro-Batching
# -----------------------------------------------------------
def execute_in_chunks(cursor, sql, data, chunk_size=5000):
    for i in range(0, len(data), chunk_size):
        cursor.executemany(sql, data[i:i+chunk_size])


# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
def load_history():
    try:
        conn = get_connection(db_name="raw_data", autocommit=False)
        cur = conn.cursor(dictionary=True)
    except MySQLError as e:
        print("❌ DB-Verbindung fehlgeschlagen:", e)
        return

    # Ticker laden
    cur.execute("""
        SELECT isin, yf_ticker, stock_index
        FROM tickerdb.tickerlist
        WHERE yf_ticker IS NOT NULL;
    """)

    records = cur.fetchall()
    print(f"📈 {len(records)} gültige Ticker gefunden.")

    ticker_map = {r["yf_ticker"]: r for r in records}
    all_tickers = list(ticker_map.keys())

    insert_sql = """
        INSERT INTO yf_prices
        (isin, ticker_yf, date, open, high, low, close, adj_close, volume, stock_index)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            adj_close = VALUES(adj_close),
            volume = VALUES(volume),
            stock_index = VALUES(stock_index),
            updated_at = NOW();
    """

    # Batch-Verarbeitung
    for i in range(0, len(all_tickers), BATCH_SIZE):
        batch = all_tickers[i:i + BATCH_SIZE]
        print(f"\n📦 Lade Batch {i // BATCH_SIZE + 1} ({len(batch)} Ticker)...")

        try:
            data = yf.download(
                batch,
                start=START_DATE,
                progress=False,
                group_by="ticker",
                auto_adjust=False
            )

            if data.empty:
                print("⚠️ Keine Daten im Batch erhalten.")
                continue

            all_rows = []

            for ticker in batch:
                if ticker not in ticker_map:
                    continue

                isin = ticker_map[ticker]["isin"]
                stock_index = ticker_map[ticker]["stock_index"]

                # yfinance-Format fixen
                try:
                    df = data[ticker].reset_index()
                except KeyError:
                    cols = [col for col in data.columns if col[0] == ticker]
                    if not cols:
                        continue
                    df = data.loc[:, cols]
                    df.columns = [c[1] for c in df.columns]
                    df = df.reset_index()

                df.rename(columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Adj Close": "adj_close",
                    "Volume": "volume"
                }, inplace=True)

                df["isin"] = isin
                df["ticker_yf"] = ticker
                df["stock_index"] = stock_index

                all_rows.append(df)

            if not all_rows:
                print("⚠️ Batch enthält keine gültigen Daten.")
                continue

            merged = pd.concat(all_rows)
            merged.dropna(subset=["date", "close"], inplace=True)

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
                    int(row["volume"]) if pd.notna(row["volume"]) else None,
                    row["stock_index"]
                )
                for _, row in merged.iterrows()
            ]

            execute_in_chunks(cur, insert_sql, rows)
            conn.commit()

            print(f"✅ Batch {i // BATCH_SIZE + 1}: {len(rows)} Zeilen gespeichert.")
            time.sleep(2)

        except Exception as e:
            print(f"❌ Fehler in Batch {i // BATCH_SIZE + 1}: {e}")
            conn.rollback()
            time.sleep(5)

    cur.close()
    conn.close()
    print("\n🎉 Alle Kursdaten erfolgreich geladen.")


if __name__ == "__main__":
    load_history()
