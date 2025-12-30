#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fügt Margen-Spalten zur bestehenden analytics.live_metrics Tabelle hinzu.
Einmalig ausführen, falls die Tabelle bereits existiert.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

ALTER_COLUMNS = [
    # Margen (aktuell aus calcu_numbers)
    ("profit_margin", "DOUBLE DEFAULT NULL COMMENT 'Gewinnmarge = Net Income / Revenue in %'"),
    ("operating_margin", "DOUBLE DEFAULT NULL COMMENT 'Operative Marge = Operating Income / Revenue in %'"),

    # Gewinnmarge Durchschnitte
    ("profit_margin_avg_3y", "DOUBLE DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Gewinnmarge'"),
    ("profit_margin_avg_5y", "DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge'"),
    ("profit_margin_avg_10y", "DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Gewinnmarge'"),
    ("profit_margin_avg_5y_2019", "DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge (2015-2019)'"),

    # Operative Marge Durchschnitte
    ("operating_margin_avg_3y", "DOUBLE DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Operative Marge'"),
    ("operating_margin_avg_5y", "DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge'"),
    ("operating_margin_avg_10y", "DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Operative Marge'"),
    ("operating_margin_avg_5y_2019", "DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge (2015-2019)'"),
]


def main():
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        print("Füge neue Margen-Spalten zu analytics.live_metrics hinzu...")

        for col_name, col_def in ALTER_COLUMNS:
            try:
                sql = f"ALTER TABLE analytics.live_metrics ADD COLUMN {col_name} {col_def}"
                cur.execute(sql)
                print(f"  + Spalte '{col_name}' hinzugefügt")
            except Error as e:
                if e.errno == 1060:  # Duplicate column name
                    print(f"  - Spalte '{col_name}' existiert bereits")
                else:
                    raise

        conn.commit()
        print("\nSpalten erfolgreich hinzugefügt!")

    except Error as e:
        print(f"Datenbankfehler: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
