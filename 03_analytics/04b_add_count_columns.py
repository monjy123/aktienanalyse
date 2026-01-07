#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erweitert die Tabelle analytics.calcu_numbers um Count-Spalten
für PE und EV/EBIT Durchschnitte.

Diese Spalten speichern, wie viele Jahre tatsächlich für die
Berechnung der Durchschnitte verwendet wurden (nach Filterung
von NULL-Werten, Negativwerten und Ausreißern).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

# Liste der hinzuzufügenden Spalten
NEW_COLUMNS = [
    ("pe_avg_5y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 5J PE-Durchschnitt'"),
    ("pe_avg_10y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 10J PE-Durchschnitt'"),
    ("pe_avg_15y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 15J PE-Durchschnitt'"),
    ("pe_avg_20y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 20J PE-Durchschnitt'"),
    ("ev_ebit_avg_5y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 5J EV/EBIT-Durchschnitt'"),
    ("ev_ebit_avg_10y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 10J EV/EBIT-Durchschnitt'"),
    ("ev_ebit_avg_15y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 15J EV/EBIT-Durchschnitt'"),
    ("ev_ebit_avg_20y_count", "INT DEFAULT NULL COMMENT 'Anzahl Jahre für 20J EV/EBIT-Durchschnitt'"),
]


def main():
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        print("Erweitere Tabelle analytics.calcu_numbers um Count-Spalten...")

        # Prüfe welche Spalten bereits existieren
        cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'analytics'
              AND TABLE_NAME = 'calcu_numbers'
        """)
        existing_columns = {row[0] for row in cur.fetchall()}

        # Füge fehlende Spalten hinzu
        added_count = 0
        skipped_count = 0

        for col_name, col_def in NEW_COLUMNS:
            if col_name in existing_columns:
                print(f"  ⏭️  Spalte {col_name} existiert bereits, überspringe")
                skipped_count += 1
            else:
                alter_sql = f"ALTER TABLE analytics.calcu_numbers ADD COLUMN {col_name} {col_def}"
                cur.execute(alter_sql)
                print(f"  ✅ Spalte {col_name} hinzugefügt")
                added_count += 1

        conn.commit()

        print(f"\n✅ Tabelle erfolgreich erweitert! ({added_count} hinzugefügt, {skipped_count} übersprungen)")

        # Zeige alle count-Spalten
        cur.execute("""
            SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'analytics'
              AND TABLE_NAME = 'calcu_numbers'
              AND COLUMN_NAME LIKE '%_count'
            ORDER BY ORDINAL_POSITION
        """)

        print("\nCount-Spalten:")
        print(f"{'Spalte':<30} {'Typ':<15} {'Kommentar'}")
        print("-" * 80)
        for row in cur.fetchall():
            print(f"{row[0]:<30} {row[1]:<15} {row[2] or ''}")

    except Error as e:
        print(f"❌ Datenbankfehler: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
