#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Aktualisiert die Spaltennamen für konsistente deutsche Bezeichnungen.

Änderungen:
- Alle Begriffe auf Deutsch
- TTM im Namen statt in Klammern: "TTM KGV" statt "KGV (TTM)"
- YF (yfinance) in Klammern: "Gewinnmarge (YF)" statt "YF Profit Margin"
- Fehlende YF-Spalten hinzufügen
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# Neue display_name Werte für bestehende Spalten
NAME_UPDATES = {
    # Bewertung - TTM im Namen
    'ttm_pe': 'TTM KGV',
    'ttm_ev_ebit': 'TTM EV/EBIT',
    'fy_pe': 'FY KGV',
    'fy_ev_ebit': 'FY EV/EBIT',

    # Durchschnitte - konsistent
    'pe_avg_5y': 'KGV Ø 5J',
    'pe_avg_10y': 'KGV Ø 10J',
    'pe_avg_15y': 'KGV Ø 15J',
    'pe_avg_20y': 'KGV Ø 20J',
    'pe_avg_10y_2019': 'KGV Ø 10-19',

    'ev_ebit_avg_5y': 'EV/EBIT Ø 5J',
    'ev_ebit_avg_10y': 'EV/EBIT Ø 10J',
    'ev_ebit_avg_15y': 'EV/EBIT Ø 15J',
    'ev_ebit_avg_20y': 'EV/EBIT Ø 20J',
    'ev_ebit_avg_10y_2019': 'EV/EBIT Ø 10-19',

    # Margen - konsistent deutsch
    'profit_margin': 'Gewinnmarge',
    'operating_margin': 'Op. Marge',
    'profit_margin_avg_3y': 'Gewinnmarge Ø 3J',
    'profit_margin_avg_5y': 'Gewinnmarge Ø 5J',
    'profit_margin_avg_10y': 'Gewinnmarge Ø 10J',
    'profit_margin_avg_5y_2019': 'Gewinnmarge Ø 15-19',
    'operating_margin_avg_3y': 'Op. Marge Ø 3J',
    'operating_margin_avg_5y': 'Op. Marge Ø 5J',
    'operating_margin_avg_10y': 'Op. Marge Ø 10J',
    'operating_margin_avg_5y_2019': 'Op. Marge Ø 15-19',

    # Bilanz
    'equity_ratio': 'EK-Quote',
    'net_debt_ebitda': 'Nettoverschuldung/EBITDA',

    # Marktkapitalisierung
    'market_cap': 'Marktkapitalisierung',
}

# Neue YF-Spalten hinzufügen (für beide Views)
NEW_YF_COLUMNS = [
    # column_key, display_name, sort_order, is_visible, column_group, format_type
    ('yf_ttm_pe', 'TTM KGV (YF)', 27, False, 'Bewertung', 'number'),
    ('yf_forward_pe', 'Forward KGV (YF)', 28, False, 'Bewertung', 'number'),
    ('yf_profit_margin', 'Gewinnmarge (YF)', 90, False, 'Margen', 'percent'),
    ('yf_operating_margin', 'Op. Marge (YF)', 91, False, 'Margen', 'percent'),
    ('yf_payout_ratio', 'Ausschüttungsquote (YF)', 92, False, 'Margen', 'percent'),
]


def main():
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("SPALTENNAMEN AKTUALISIEREN")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # 1. Bestehende Namen aktualisieren
        print("\n1. Aktualisiere bestehende Spaltennamen...")
        update_count = 0

        for column_key, new_name in NAME_UPDATES.items():
            cur.execute("""
                UPDATE analytics.user_column_settings
                SET display_name = %s
                WHERE column_key = %s
            """, (new_name, column_key))
            if cur.rowcount > 0:
                print(f"   {column_key} -> '{new_name}' ({cur.rowcount} Zeilen)")
                update_count += cur.rowcount

        print(f"\n   {update_count} Spaltennamen aktualisiert")

        # 2. Neue YF-Spalten hinzufügen
        print("\n2. Füge neue YF-Spalten hinzu...")

        insert_sql = """
        INSERT INTO analytics.user_column_settings
            (view_name, source_table, column_key, display_name, sort_order, is_visible, column_group, format_type)
        VALUES (%s, 'live_metrics', %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            display_name = VALUES(display_name),
            column_group = VALUES(column_group),
            format_type = VALUES(format_type)
        """

        for col in NEW_YF_COLUMNS:
            # Für watchlist
            cur.execute(insert_sql, ('watchlist',) + col)
            # Für screener
            cur.execute(insert_sql, ('screener',) + col)
            print(f"   + {col[0]} -> '{col[1]}'")

        conn.commit()

        # 3. Übersicht anzeigen
        print("\n" + "=" * 60)
        print("AKTUALISIERTE SPALTENNAMEN")
        print("=" * 60)

        cur.execute("""
            SELECT column_group, column_key, display_name
            FROM analytics.user_column_settings
            WHERE view_name = 'watchlist'
            ORDER BY sort_order
        """)

        current_group = None
        for row in cur.fetchall():
            if row[0] != current_group:
                current_group = row[0]
                print(f"\n[{current_group}]")
            print(f"  {row[1]:<30} -> {row[2]}")

        print("\n" + "=" * 60)
        print("FERTIG!")
        print("=" * 60)

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
