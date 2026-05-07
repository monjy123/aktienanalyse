#!/usr/bin/env python3
"""One-shot: refresh stale prices for user holdings.

Findet alle Holdings mit live_metrics.price_date < CURDATE() und holt für
deren Ticker frische Kurse von yfinance. Schreibt nach raw_data.yf_prices
und aktualisiert analytics.live_metrics.price/price_date.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import yfinance as yf
from db import get_connection

conn = get_connection(autocommit=False)
cur = conn.cursor(dictionary=True)

cur.execute("""
    SELECT lm.isin, lm.ticker, lm.price AS old_price, lm.price_date AS old_date,
           lm.stock_index
    FROM analytics.live_metrics lm
    JOIN (SELECT DISTINCT isin FROM analytics.user_holdings) uh ON lm.isin = uh.isin
    WHERE lm.price_date < CURDATE()
    ORDER BY lm.price_date ASC
""")
stale = cur.fetchall()
print(f"Stale holdings: {len(stale)}")

upsert_yfp = """
    INSERT INTO raw_data.yf_prices
        (isin, ticker_yf, date, open, high, low, close, adj_close, volume, stock_index)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open), high=VALUES(high), low=VALUES(low),
        close=VALUES(close), adj_close=VALUES(adj_close),
        volume=VALUES(volume), updated_at=NOW()
"""
update_lm = """
    UPDATE analytics.live_metrics
    SET price = %s, price_date = %s
    WHERE isin = %s
"""

ok, fail = 0, 0
for row in stale:
    isin = row['isin']
    ticker = row['ticker']
    try:
        hist = yf.Ticker(ticker).history(start=str(row['old_date']))
        if hist.empty:
            print(f"  ⚠  {ticker} ({isin}): keine Daten von yfinance")
            fail += 1
            continue

        # UK-Aktien (.L): Pence → Pfund
        scale = 100.0 if ticker.endswith('.L') else 1.0

        rows_yfp = []
        for dt, r in hist.iterrows():
            d = dt.date()
            rows_yfp.append((
                isin, ticker, d,
                float(r['Open'])/scale  if r['Open']  == r['Open'] else None,
                float(r['High'])/scale  if r['High']  == r['High'] else None,
                float(r['Low'])/scale   if r['Low']   == r['Low']  else None,
                float(r['Close'])/scale if r['Close'] == r['Close'] else None,
                float(r['Close'])/scale if r['Close'] == r['Close'] else None,  # adj_close = close
                int(r['Volume']) if r['Volume'] == r['Volume'] else None,
                row['stock_index'],
            ))
        cur.executemany(upsert_yfp, rows_yfp)

        # neuesten Close + Datum nehmen
        last = hist.iloc[-1]
        new_price = float(last['Close']) / scale
        new_date = hist.index[-1].date()
        cur.execute(update_lm, (new_price, new_date, isin))

        old_p = float(row['old_price']) if row['old_price'] is not None else 0.0
        diff_pct = (new_price - old_p) / old_p * 100 if old_p else 0.0
        print(f"  ✓ {ticker:10s} {row['old_date']} {old_p:>9.2f} → {new_date} {new_price:>9.2f}  ({diff_pct:+.2f}%)")
        ok += 1
    except Exception as e:
        print(f"  ✗ {ticker} ({isin}): {type(e).__name__}: {e}")
        fail += 1

conn.commit()
cur.close()
conn.close()
print(f"\nFertig: {ok} aktualisiert, {fail} fehlgeschlagen.")
