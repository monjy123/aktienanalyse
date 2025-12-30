#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fügt die neuen PE/EV-EBIT Abweichungs-Spalten zu user_column_settings hinzu.

Diese Spalten zeigen die prozentuale Abweichung zwischen aktuellen Werten
und historischen Durchschnitten:
  +25% = 25% teurer als historischer Durchschnitt
  -15% = 15% günstiger als historischer Durchschnitt
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# Neue Spalten für die Abweichungen
NEW_COLUMNS = [
    # === YF TTM PE vs. historische Durchschnitte ===
    # view_name, source_table, column_key, display_name, sort_order, is_visible, column_group, format_type
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_5y', 'TTM PE vs Ø5J', 135, False, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_10y', 'TTM PE vs Ø10J', 136, True, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_15y', 'TTM PE vs Ø15J', 137, False, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_20y', 'TTM PE vs Ø20J', 138, False, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_10y_2019', 'TTM PE vs Ø10-19', 139, False, 'PE Abweichung', 'percent'),

    # === YF Forward PE vs. historische Durchschnitte ===
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_5y', 'Fwd PE vs Ø5J', 140, False, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_10y', 'Fwd PE vs Ø10J', 141, True, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_15y', 'Fwd PE vs Ø15J', 142, False, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_20y', 'Fwd PE vs Ø20J', 143, False, 'PE Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_10y_2019', 'Fwd PE vs Ø10-19', 144, False, 'PE Abweichung', 'percent'),

    # === TTM EV/EBIT vs. historische Durchschnitte ===
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_5y', 'EV/EBIT vs Ø5J', 150, False, 'EV/EBIT Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_10y', 'EV/EBIT vs Ø10J', 151, True, 'EV/EBIT Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_15y', 'EV/EBIT vs Ø15J', 152, False, 'EV/EBIT Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_20y', 'EV/EBIT vs Ø20J', 153, False, 'EV/EBIT Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_10y_2019', 'EV/EBIT vs Ø10-19', 154, False, 'EV/EBIT Abweichung', 'percent'),

    # === YF API Kennzahlen (falls noch nicht vorhanden) ===
    ('watchlist', 'live_metrics', 'yf_ttm_pe', 'YF TTM PE', 130, True, 'Bewertung', 'number'),
    ('watchlist', 'live_metrics', 'yf_forward_pe', 'YF Forward PE', 131, True, 'Bewertung', 'number'),
]


def main():
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("COLUMN SETTINGS: PE/EV-EBIT Abweichungen hinzufuegen")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        insert_sql = """
        INSERT INTO analytics.user_column_settings
            (view_name, source_table, column_key, display_name, sort_order, is_visible, column_group, format_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            display_name = VALUES(display_name),
            sort_order = VALUES(sort_order),
            column_group = VALUES(column_group),
            format_type = VALUES(format_type)
        """

        # Watchlist Spalten einfuegen
        print("\n1. Fuege Watchlist-Spalten ein...")
        for col in NEW_COLUMNS:
            cur.execute(insert_sql, col)
            print(f"   + {col[2]} ({col[3]})")

        # Screener: gleiche Spalten kopieren
        print("\n2. Kopiere fuer Screener...")
        screener_columns = [(
            'screener', col[1], col[2], col[3], col[4], col[5], col[6], col[7]
        ) for col in NEW_COLUMNS]

        for col in screener_columns:
            cur.execute(insert_sql, col)

        conn.commit()

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("""
            SELECT column_group, COUNT(*) as cnt
            FROM analytics.user_column_settings
            WHERE view_name = 'screener'
              AND column_group IN ('PE Abweichung', 'EV/EBIT Abweichung')
            GROUP BY column_group
        """)

        print("\nNeue Spaltengruppen (Screener):")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} Spalten")

        print("""
Neue Spalten fuer Filter verfuegbar:
------------------------------------
PE Abweichung (YF TTM PE vs. historische Durchschnitte):
  - yf_ttm_pe_vs_avg_5y, _10y, _15y, _20y, _10y_2019

PE Abweichung (YF Forward PE vs. historische Durchschnitte):
  - yf_fwd_pe_vs_avg_5y, _10y, _15y, _20y, _10y_2019

EV/EBIT Abweichung (TTM EV/EBIT vs. historische Durchschnitte):
  - ev_ebit_vs_avg_5y, _10y, _15y, _20y, _10y_2019

Beispiel-Filter:
  TTM PE vs Ø10J < -20  -> Aktien die 20%+ unter hist. Schnitt handeln
  Fwd PE vs Ø10J < 0    -> Aktien die unter hist. Schnitt handeln
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
