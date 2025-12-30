import os
import pandas as pd
import mysql.connector
from mysql.connector import Error as MySQLError

# ============================================================
# DB CONFIG
# ============================================================

DB_HOST = os.getenv("MYDB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("MYDB_PORT", "3306"))
DB_USER = os.getenv("MYDB_USER", "monjy")
DB_PASSWORD = os.getenv("MYDB_PASSWORD", "Emst4558!!")
DB_NAME = "tickerdb"   # <--- final korrekt
TABLE_NAME = "tickerlist"

# ============================================================
# CSV → INDEX + EXCHANGE
# ============================================================

FILE_CONFIG = {
    "STOXX600_clean.csv": {
        "stock_index": "STOXX600",
        "exchange": None
    },
    "DAX_clean.csv": {
        "stock_index": "DAX",
        "exchange": "Xetra"
    },
    "MDAX_clean.csv": {
        "stock_index": "MDAX",
        "exchange": "Xetra"
    },
    "SP500_clean.csv": {
        "stock_index": "S&P 500",
        "exchange": "New York Stock Exchange"
    },
    "FTSE100_clean.csv": {
        "stock_index": "FTSE 100",
        "exchange": "London Stock Exchange"
    },
    "NIKKEI225_clean.csv": {
        "stock_index": "Nikkei 225",
        "exchange": "Tokyo Stock Exchange"
    }
}

# ============================================================
# SQL STATEMENT
# ============================================================

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
        con = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=False
        )
    except MySQLError as e:
        print("❌ DB-Verbindung fehlgeschlagen:", e)
        return

    cur = con.cursor()

    for filename, cfg in FILE_CONFIG.items():

        if not os.path.exists(filename):
            print(f"⚠ Datei fehlt: {filename}")
            continue

        print(f"\n==> Verarbeite {filename}")

        df = pd.read_csv(filename, dtype=str)

        for col in ["Emittententicker", "Name", "Börse", "ISIN"]:
            if col not in df.columns:
                df[col] = None

        stock_index = cfg["stock_index"]

        for _, row in df.iterrows():
            isin = row["ISIN"]
            name = row["Name"]
            ticker = row["Emittententicker"]

            if cfg["exchange"] is None:
                exchange = row["Börse"] if pd.notna(row["Börse"]) else None
            else:
                exchange = cfg["exchange"]

            vals = (
                isin,
                stock_index,
                name,
                ticker,
                None,
                None,
                exchange,
                None,
                None
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
