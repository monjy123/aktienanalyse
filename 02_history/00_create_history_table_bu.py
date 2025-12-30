#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
create_historie_table.py
Erstellt die SQL-Tabelle `yf_prices` (angepasst) mit den gewünschten Änderungen:
- Spalte `isin` vor `ticker_yf`
- Entfernt: `currency`
- Umbenannt: `source_index` -> `stock_index`
"""

import mysql.connector
from mysql.connector import errorcode

def create_table(connection):
    cursor = connection.cursor()

    create_table_sql = """
    CREATE TABLE `yf_prices` (
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

    try:
        cursor.execute(create_table_sql)
        print("Tabelle `yf_prices` wurde erfolgreich erstellt.")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Tabelle `yf_prices` existiert bereits.")
        else:
            print(f"Fehler beim Erstellen der Tabelle: {err}")
    finally:
        cursor.close()


def main():
    # Beispielhafte DB-Verbindung – bitte anpassen!
    config = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "monjy",
        "password": "Emst4558!!",
        "database": "raw_data"
    }
    

    try:
        connection = mysql.connector.connect(**config)
        create_table(connection)
    except mysql.connector.Error as err:
        print(f"Fehler bei der Verbindung zur Datenbank: {err}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    main()
