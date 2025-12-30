import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import get_connection
from mysql.connector import Error as MySQLError

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
        con = get_connection(db_name="ticker")  # <<< WICHTIG
        cur = con.cursor()
        cur.execute(CREATE_TABLE_SQL)
        print(f"✔ Tabelle '{TABLE_NAME}' wurde in 'tickerdb' erstellt.")
        cur.close()
        con.close()

    except MySQLError as e:
        print("❌ Fehler:", e)

if __name__ == "__main__":
    main()
