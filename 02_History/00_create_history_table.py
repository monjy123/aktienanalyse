#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
create_historie_table.py
Erstellt die SQL-Tabelle `yf_prices` im Schema `raw_data`.
"""

from mysql.connector import Error as MySQLError
from db import get_connection


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `yf_prices` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `isin` varchar(20) NOT NULL,
  `ticker_yf` varchar(20) NOT NULL,
  `date` date NOT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `adj_close` double DEFAULT NULL,
  `volume` bigint DEFAULT NULL,
  `stock_index` varchar(50) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_isin_ticker_date` (`isin`, `ticker_yf`, `date`),
  KEY `idx_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""


def create_table():
    try:
        con = get_connection(db_name="raw_data")
        cur = con.cursor()
        cur.execute(CREATE_TABLE_SQL)
        print("✔ Tabelle `yf_prices` erfolgreich erstellt.")
        cur.close()
        con.close()
    except MySQLError as e:
        print("❌ Fehler beim Erstellen der Tabelle:", e)


if __name__ == "__main__":
    create_table()
