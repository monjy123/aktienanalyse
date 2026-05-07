#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lädt historische Kursdaten über yfinance in raw_data.yf_prices
- ISIN wird übernommen
- Yahoo-Ticker kommen aus tickerdb.tickerlist
- Struktur passt exakt zur Tabelle yf_prices
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

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

    # Sammle Ticker, die im Bulk-Download keine Daten geliefert haben → Einzel-Retry am Ende
    failed_tickers = []

    def df_to_rows(df, ticker, isin, stock_index):
        df = df.rename(columns={
            "Date": "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
        })
        df["isin"] = isin
        df["ticker_yf"] = ticker
        df["stock_index"] = stock_index
        if ticker.endswith('.L'):
            for col in ['open', 'high', 'low', 'close', 'adj_close']:
                if col in df.columns:
                    df[col] = df[col] / 100.0
        df = df.dropna(subset=["date", "close"])
        return [
            (
                row["isin"], row["ticker_yf"], row["date"].date(),
                row.get("open"), row.get("high"), row.get("low"),
                row.get("close"), row.get("adj_close"),
                int(row["volume"]) if pd.notna(row["volume"]) else None,
                row["stock_index"],
            )
            for _, row in df.iterrows()
        ]

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
                print("⚠️ Keine Daten im Batch erhalten — Ticker für Einzel-Retry vormerken.")
                failed_tickers.extend(batch)
                continue

            all_rows = []
            tickers_with_data = set()

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
                        failed_tickers.append(ticker)
                        continue
                    df = data.loc[:, cols]
                    df.columns = [c[1] for c in df.columns]
                    df = df.reset_index()

                rows_for_ticker = df_to_rows(df, ticker, isin, stock_index)
                if rows_for_ticker:
                    all_rows.extend(rows_for_ticker)
                    tickers_with_data.add(ticker)
                else:
                    failed_tickers.append(ticker)

            if not all_rows:
                print("⚠️ Batch enthält keine gültigen Daten.")
                continue

            execute_in_chunks(cur, insert_sql, all_rows)
            conn.commit()

            print(f"✅ Batch {i // BATCH_SIZE + 1}: {len(all_rows)} Zeilen gespeichert "
                  f"({len(tickers_with_data)}/{len(batch)} Ticker).")
            time.sleep(2)

        except Exception as e:
            print(f"❌ Fehler in Batch {i // BATCH_SIZE + 1}: {e} — Ticker für Einzel-Retry vormerken.")
            conn.rollback()
            failed_tickers.extend(batch)
            time.sleep(5)

    # ----- Einzel-Retry für stille yfinance-Fehler (z.B. TypeError 'NoneType') -----
    if failed_tickers:
        # Duplikate raus, Reihenfolge erhalten
        seen = set()
        retry_list = [t for t in failed_tickers if t in ticker_map and not (t in seen or seen.add(t))]
        print(f"\n🔁 Einzel-Retry für {len(retry_list)} Ticker ohne Bulk-Daten…")
        recovered, still_failed = 0, []
        for ticker in retry_list:
            isin = ticker_map[ticker]["isin"]
            stock_index = ticker_map[ticker]["stock_index"]
            df = None
            for attempt in range(3):
                try:
                    h = yf.Ticker(ticker).history(start=START_DATE, auto_adjust=False)
                    if not h.empty:
                        df = h.reset_index()
                        break
                except Exception as e:
                    if attempt == 2:
                        print(f"  ✗ {ticker}: {type(e).__name__}: {e}")
                time.sleep(2 * (attempt + 1))
            if df is None or df.empty:
                still_failed.append(ticker)
                continue
            rows_for_ticker = df_to_rows(df, ticker, isin, stock_index)
            if not rows_for_ticker:
                still_failed.append(ticker)
                continue
            try:
                execute_in_chunks(cur, insert_sql, rows_for_ticker)
                conn.commit()
                recovered += 1
                print(f"  ✓ {ticker}: {len(rows_for_ticker)} Zeilen gespeichert.")
            except Exception as e:
                conn.rollback()
                still_failed.append(ticker)
                print(f"  ✗ {ticker} DB-Fehler: {e}")
        print(f"\n🔁 Einzel-Retry: {recovered} erholt, {len(still_failed)} weiterhin ohne Daten.")
        if still_failed:
            print(f"   Endgültig fehlgeschlagen: {', '.join(still_failed[:20])}"
                  f"{' …' if len(still_failed) > 20 else ''}")

    cur.close()
    conn.close()
    print("\n🎉 Alle Kursdaten erfolgreich geladen.")


if __name__ == "__main__":
    load_history()
