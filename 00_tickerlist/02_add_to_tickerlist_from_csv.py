import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import pandas as pd
from db import get_connection
from mysql.connector import Error as MySQLError

# ============================================================
# CONFIG
# ============================================================

DB_SCHEMA = "TICKER"        # <- nutzt DB_NAME_TICKER aus .env
TABLE_NAME = "tickerlist"

FILE_CONFIG = {
    "STOXX600_clean.csv": {"stock_index": "STOXX600", "exchange": None},
    "DAX_clean.csv":      {"stock_index": "DAX",      "exchange": "Xetra"},
    "MDAX_clean.csv":     {"stock_index": "MDAX",     "exchange": "Xetra"},
    "SP500_clean.csv":    {"stock_index": "S&P 500",  "exchange": "New York Stock Exchange"},
    "FTSE100_clean.csv":  {"stock_index": "FTSE 100", "exchange": "London Stock Exchange"},
    "NIKKEI225_clean.csv":{"stock_index": "Nikkei 225", "exchange": "Tokyo Stock Exchange"},
}

INSERT_SQL = f"""
INSERT INTO `{TABLE_NAME}` 
(isin, stock_index, name, ticker, yf_ticker, eodhd_ticker, exchange, finanzen_name, marketscreener_name)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    stock_index = VALUES(stock_index),
    ticker = VALUES(ticker),
    yf_ticker = VALUES(yf_ticker),
    eodhd_ticker = VALUES(eodhd_ticker),
    exchange = VALUES(exchange),
    finanzen_name = VALUES(finanzen_name),
    marketscreener_name = VALUES(marketscreener_name),
    updated_at = CURRENT_TIMESTAMP;
"""

# ============================================================
# IMPORT LOGIK
# ============================================================

def import_csv_files():
    try:
        con = get_connection(db_name=DB_SCHEMA, autocommit=False)
    except MySQLError as e:
        print("❌ DB-Verbindung fehlgeschlagen:", e)
        return

    cur = con.cursor()

    for filename, cfg in FILE_CONFIG.items():

        # Stelle sicher, dass CSVs relativ zum Skriptpfad gefunden werden
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(SCRIPT_DIR, filename)

        if not os.path.exists(full_path):
            print(f"⚠ Datei fehlt: {full_path}")
            continue

        df = pd.read_csv(full_path, dtype=str)



        # Fehlende Spalten hinzufügen
        for col in ["Emittententicker", "Name", "Börse", "ISIN"]:
            if col not in df.columns:
                df[col] = None

        stock_index = cfg["stock_index"]

        for _, row in df.iterrows():
            isin = row["ISIN"]
            name = row["Name"]
            ticker = row["Emittententicker"]

            exchange = (
                row["Börse"] if cfg["exchange"] is None else cfg["exchange"]
            )

            vals = (
                isin,
                stock_index,
                name,
                ticker,
                None,  # yf_ticker
                None,  # eodhd_ticker
                exchange,
                None,  # finanzen_name
                None   # marketscreener_name
            )

            try:
                cur.execute(INSERT_SQL, vals)
            except MySQLError as e:
                print(f"❌ Fehler bei ISIN {isin}: {e}")

        con.commit()
        print(f"✔ Import abgeschlossen: {filename}")

    cur.close()
    con.close()
    print("\n==> Alle CSVs vollständig importiert.")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import_csv_files()
