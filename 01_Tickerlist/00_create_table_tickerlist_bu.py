import os
import mysql.connector
from mysql.connector import Error as MySQLError

DB_HOST = os.getenv("MYDB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("MYDB_PORT", "3306"))
DB_USER = os.getenv("MYDB_USER", "monjy")
DB_PASSWORD = os.getenv("MYDB_PASSWORD", "Emst4558!!")
DB_NAME = "tickerdb"   # <--- final korrekt
TABLE_NAME = "tickerlist"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
    id INT AUTO_INCREMENT PRIMARY KEY,

    isin VARCHAR(32) NOT NULL,
    stock_index VARCHAR(255) NOT NULL,

    name VARCHAR(255),
    ticker VARCHAR(255),
    yf_ticker VARCHAR(255),
    eodhd_ticker VARCHAR(255),
    exchange VARCHAR(255),
    finanzen_name VARCHAR(255),
    marketscreener_name VARCHAR(255),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_isin_stockindex (isin, stock_index)
);
"""

def main():
    try:
        con = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True
        )
        cur = con.cursor()
        cur.execute(CREATE_TABLE_SQL)
        print(f"✔ Tabelle '{TABLE_NAME}' erfolgreich erstellt.")
        cur.close()
        con.close()

    except MySQLError as e:
        print("❌ Fehler bei der Tabellenerstellung:", e)

if __name__ == "__main__":
    main()
