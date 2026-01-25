#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migration: Fügt next_earnings_date Spalte hinzu.

Diese Migration:
1. Fügt next_earnings_date Spalte zu live_metrics hinzu
2. Fügt die Spalten-Konfiguration für Watchlist und Screener hinzu
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


def main():
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("MIGRATION: next_earnings_date hinzufügen")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Schritt 1: Spalte zu live_metrics hinzufügen (falls nicht vorhanden)
        print("\n1. Prüfe/Erstelle Spalte in live_metrics...")
        try:
            cur.execute("""
                ALTER TABLE analytics.live_metrics
                ADD COLUMN next_earnings_date DATE COMMENT 'Nächstes Earnings-Datum von yfinance API'
                AFTER yf_operating_margin
            """)
            print("   ✓ Spalte next_earnings_date hinzugefügt")
        except Error as e:
            if "Duplicate column name" in str(e):
                print("   ℹ Spalte next_earnings_date existiert bereits")
            else:
                raise e

        # Schritt 2: Spalten-Konfiguration hinzufügen
        print("\n2. Füge Spalten-Konfiguration hinzu...")

        # Alle existierenden User-IDs holen
        cur.execute("SELECT DISTINCT user_id FROM analytics.user_column_settings")
        user_ids = [row[0] for row in cur.fetchall()]
        print(f"   Gefundene User-IDs: {user_ids}")

        insert_sql = """
        INSERT INTO analytics.user_column_settings
            (user_id, view_name, source_table, column_key, display_name, sort_order, is_visible, column_group, format_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            display_name = VALUES(display_name),
            column_group = VALUES(column_group),
            format_type = VALUES(format_type)
        """

        # Für jeden User und jede View hinzufügen
        for user_id in user_ids:
            for view in ['watchlist', 'screener']:
                cur.execute(insert_sql, (
                    user_id,
                    view,
                    'live_metrics',
                    'next_earnings_date',
                    'Nächste<br>Zahlen',
                    9,  # sort_order nach yf_payout_ratio
                    False,  # is_visible (muss manuell aktiviert werden)
                    'Stammdaten',
                    'date'
                ))
                print(f"   ✓ Spalten-Konfiguration für User {user_id}, {view} hinzugefügt")

        conn.commit()

        print("\n" + "=" * 60)
        print("MIGRATION ERFOLGREICH")
        print("=" * 60)
        print("""
Die Spalte 'Nächste Zahlen' ist jetzt verfügbar.

Um sie zu aktivieren:
1. Öffne Watchlist oder Screener
2. Klicke auf 'Spalten'
3. Aktiviere 'Nächste Zahlen' unter 'Stammdaten'

Die Daten werden beim nächsten Lauf von 02_load_live_metrics.py
automatisch von yfinance geladen.
""")

    except Error as e:
        print(f"\nDatenbankfehler: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
