#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fügt Margen-Spalten zur bestehenden analytics.calcu_numbers Tabelle hinzu.
Einmalig ausführen, falls die Tabelle bereits existiert.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

ALTER_SQL = """
-- Margen (pro Jahr)
ALTER TABLE analytics.calcu_numbers
ADD COLUMN IF NOT EXISTS profit_margin DOUBLE DEFAULT NULL COMMENT 'Gewinnmarge = Net Income / Revenue in %' AFTER net_debt_ebitda,
ADD COLUMN IF NOT EXISTS operating_margin DOUBLE DEFAULT NULL COMMENT 'Operative Marge = Operating Income / Revenue in %' AFTER profit_margin,

-- Gewinnmarge Durchschnitte
ADD COLUMN IF NOT EXISTS profit_margin_avg_3y DOUBLE DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Gewinnmarge' AFTER operating_margin,
ADD COLUMN IF NOT EXISTS profit_margin_avg_5y DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge' AFTER profit_margin_avg_3y,
ADD COLUMN IF NOT EXISTS profit_margin_avg_10y DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Gewinnmarge' AFTER profit_margin_avg_5y,
ADD COLUMN IF NOT EXISTS profit_margin_avg_5y_2019 DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge (2015-2019)' AFTER profit_margin_avg_10y,

-- Operative Marge Durchschnitte
ADD COLUMN IF NOT EXISTS operating_margin_avg_3y DOUBLE DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Operative Marge' AFTER profit_margin_avg_5y_2019,
ADD COLUMN IF NOT EXISTS operating_margin_avg_5y DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge' AFTER operating_margin_avg_3y,
ADD COLUMN IF NOT EXISTS operating_margin_avg_10y DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Operative Marge' AFTER operating_margin_avg_5y,
ADD COLUMN IF NOT EXISTS operating_margin_avg_5y_2019 DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge (2015-2019)' AFTER operating_margin_avg_10y
"""

# MySQL < 8.0.16 unterstützt IF NOT EXISTS nicht bei ADD COLUMN
# Daher einzelne ALTERs mit Fehlerbehandlung
ALTER_COLUMNS = [
    ("profit_margin", "DOUBLE DEFAULT NULL COMMENT 'Gewinnmarge = Net Income / Revenue in %'"),
    ("operating_margin", "DOUBLE DEFAULT NULL COMMENT 'Operative Marge = Operating Income / Revenue in %'"),
    ("profit_margin_avg_3y", "DOUBLE DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Gewinnmarge'"),
    ("profit_margin_avg_5y", "DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge'"),
    ("profit_margin_avg_10y", "DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Gewinnmarge'"),
    ("profit_margin_avg_5y_2019", "DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge (2015-2019)'"),
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

        print("Füge neue Spalten zu analytics.calcu_numbers hinzu...")

        for col_name, col_def in ALTER_COLUMNS:
            try:
                sql = f"ALTER TABLE analytics.calcu_numbers ADD COLUMN {col_name} {col_def}"
                cur.execute(sql)
                print(f"  + Spalte '{col_name}' hinzugefügt")
            except Error as e:
                if e.errno == 1060:  # Duplicate column name
                    print(f"  - Spalte '{col_name}' existiert bereits")
                else:
                    raise

        conn.commit()
        print("\nSpalten erfolgreich hinzugefügt!")
        print("Führe nun 05_fill_calcu_numbers.py aus, um die Daten zu aktualisieren.")

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
