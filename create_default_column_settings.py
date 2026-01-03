#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt Default Column Settings für einen User
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

# Default-Spalten Definition (aus 04_create_column_settings.py)
DEFAULT_COLUMNS = [
    # === STAMMDATEN (company_info) ===
    # view_name, source_table, column_key, display_name, sort_order, is_visible, column_group, format_type
    ('watchlist', 'company_info', 'ticker', 'Ticker', 1, True, 'Stammdaten', 'text'),
    ('watchlist', 'company_info', 'company_name', 'Unternehmen', 2, True, 'Stammdaten', 'text'),
    ('watchlist', 'company_info', 'sector', 'Sektor', 3, False, 'Stammdaten', 'text'),
    ('watchlist', 'company_info', 'industry', 'Branche', 4, False, 'Stammdaten', 'text'),
    ('watchlist', 'company_info', 'country', 'Land', 5, False, 'Stammdaten', 'text'),
    ('watchlist', 'company_info', 'stock_index', 'Index', 6, False, 'Stammdaten', 'text'),
    ('watchlist', 'company_info', 'currency', 'Währung', 7, False, 'Stammdaten', 'text'),
    ('watchlist', 'live_metrics', 'yf_payout_ratio', 'Ausschüttungs<br>quote', 8, False, 'Stammdaten', 'percent'),

    # === KURSDATEN (live_metrics) ===
    ('watchlist', 'live_metrics', 'price', 'Kurs', 10, True, 'Kursdaten', 'currency'),
    ('watchlist', 'live_metrics', 'price_date', 'Kursdatum', 11, False, 'Kursdaten', 'text'),
    ('watchlist', 'live_metrics', 'market_cap', 'Markt. in Mrd.', 12, False, 'Kursdaten', 'billions'),

    # === BEWERTUNG FY (live_metrics) ===
    ('watchlist', 'live_metrics', 'fy_pe', 'KGV (FY)', 20, False, 'Bewertung', 'number'),
    ('watchlist', 'live_metrics', 'fy_ev_ebit', 'EV/EBIT (FY)', 21, False, 'Bewertung', 'number'),

    # === BEWERTUNG TTM (live_metrics) ===
    ('watchlist', 'live_metrics', 'ttm_pe', 'KGV (TTM)', 25, True, 'Bewertung', 'number'),
    ('watchlist', 'live_metrics', 'ttm_ev_ebit', 'EV/EBIT (TTM)', 26, True, 'Bewertung', 'number'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe', 'KGV TTM (YF)', 27, False, 'Bewertung', 'number'),
    ('watchlist', 'live_metrics', 'yf_forward_pe', 'KGV Forward (YF)', 28, False, 'Bewertung', 'number'),

    # === PE DURCHSCHNITTE (live_metrics) ===
    ('watchlist', 'live_metrics', 'pe_avg_5y', 'KGV Ø5J', 30, False, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'pe_avg_10y', 'KGV Ø10J', 31, True, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'pe_avg_15y', 'KGV Ø15J', 32, False, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'pe_avg_20y', 'KGV Ø20J', 33, False, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'pe_avg_10y_2019', 'KGV Ø10-19', 34, False, 'Durchschnitte', 'number'),

    # === EV/EBIT DURCHSCHNITTE (live_metrics) ===
    ('watchlist', 'live_metrics', 'ev_ebit_avg_5y', 'EV/EBIT Ø5J', 40, False, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'ev_ebit_avg_10y', 'EV/EBIT Ø10J', 41, True, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'ev_ebit_avg_15y', 'EV/EBIT Ø15J', 42, False, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'ev_ebit_avg_20y', 'EV/EBIT Ø20J', 43, False, 'Durchschnitte', 'number'),
    ('watchlist', 'live_metrics', 'ev_ebit_avg_10y_2019', 'EV/EBIT Ø10-19', 44, False, 'Durchschnitte', 'number'),

    # === UMSATZ WACHSTUM (live_metrics) ===
    ('watchlist', 'live_metrics', 'revenue_cagr_3y', 'Umsatz CAGR 3J', 50, False, 'Wachstum', 'percent'),
    ('watchlist', 'live_metrics', 'revenue_cagr_5y', 'Umsatz CAGR 5J', 51, True, 'Wachstum', 'percent'),
    ('watchlist', 'live_metrics', 'revenue_cagr_10y', 'Umsatz CAGR 10J', 52, False, 'Wachstum', 'percent'),

    # === EBIT WACHSTUM (live_metrics) ===
    ('watchlist', 'live_metrics', 'ebit_cagr_3y', 'EBIT CAGR 3J', 55, False, 'Wachstum', 'percent'),
    ('watchlist', 'live_metrics', 'ebit_cagr_5y', 'EBIT CAGR 5J', 56, False, 'Wachstum', 'percent'),
    ('watchlist', 'live_metrics', 'ebit_cagr_10y', 'EBIT CAGR 10J', 57, False, 'Wachstum', 'percent'),

    # === GEWINN WACHSTUM (live_metrics) ===
    ('watchlist', 'live_metrics', 'net_income_cagr_3y', 'Gewinn CAGR 3J', 60, False, 'Wachstum', 'percent'),
    ('watchlist', 'live_metrics', 'net_income_cagr_5y', 'Gewinn CAGR 5J', 61, False, 'Wachstum', 'percent'),
    ('watchlist', 'live_metrics', 'net_income_cagr_10y', 'Gewinn CAGR 10J', 62, False, 'Wachstum', 'percent'),

    # === BILANZ (live_metrics) ===
    ('watchlist', 'live_metrics', 'equity_ratio', 'EK-Quote', 70, True, 'Bilanz', 'percent'),
    ('watchlist', 'live_metrics', 'net_debt_ebitda', 'NetDebt/EBITDA', 71, True, 'Bilanz', 'number'),

    # === MARGEN (live_metrics) ===
    ('watchlist', 'live_metrics', 'profit_margin', 'Gewinn<br>marge', 80, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'operating_margin', 'Op. Marge', 81, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'profit_margin_avg_3y', 'Gewinn<br>marge Ø3J', 82, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'profit_margin_avg_5y', 'Gewinn<br>marge Ø5J', 83, True, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'profit_margin_avg_10y', 'Gewinn<br>marge Ø10J', 84, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'profit_margin_avg_5y_2019', 'Gewinn<br>marge Ø15-19', 85, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'operating_margin_avg_3y', 'Op. Marge Ø3J', 86, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'operating_margin_avg_5y', 'Op. Marge Ø5J', 87, True, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'operating_margin_avg_10y', 'Op. Marge Ø10J', 88, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'operating_margin_avg_5y_2019', 'Op. Marge Ø15-19', 89, False, 'Margen', 'percent'),

    # === YFINANCE METRIKEN - MARGEN (live_metrics) ===
    ('watchlist', 'live_metrics', 'yf_profit_margin', 'Gewinn<br>marge (YF)', 90, False, 'Margen', 'percent'),
    ('watchlist', 'live_metrics', 'yf_operating_margin', 'Op. Marge (YF)', 91, False, 'Margen', 'percent'),

    # === KGV ABWEICHUNGEN (live_metrics) ===
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_5y', 'KGV Abw. Ø5J', 100, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_10y', 'KGV Abw. Ø10J', 101, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_15y', 'KGV Abw. Ø15J', 102, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_20y', 'KGV Abw. Ø20J', 103, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_ttm_pe_vs_avg_10y_2019', 'KGV Abw. Ø10-19', 104, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_5y', 'Fwd KGV Abw. Ø5J', 105, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_10y', 'Fwd KGV Abw. Ø10J', 106, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_15y', 'Fwd KGV Abw. Ø15J', 107, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_20y', 'Fwd KGV Abw. Ø20J', 108, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'yf_fwd_pe_vs_avg_10y_2019', 'Fwd KGV Abw. Ø10-19', 109, False, 'KGV Abweichung', 'percent'),

    # === EV/EBIT ABWEICHUNGEN (live_metrics) ===
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_5y', 'EV/EBIT Abw. Ø5J', 110, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_10y', 'EV/EBIT Abw. Ø10J', 111, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_15y', 'EV/EBIT Abw. Ø15J', 112, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_20y', 'EV/EBIT Abw. Ø20J', 113, False, 'KGV Abweichung', 'percent'),
    ('watchlist', 'live_metrics', 'ev_ebit_vs_avg_10y_2019', 'EV/EBIT Abw. Ø10-19', 114, False, 'KGV Abweichung', 'percent'),
]


def create_column_settings_for_user(user_id):
    """Erstellt Default Column Settings für einen User."""

    conn = get_connection('analytics', autocommit=False)
    cur = conn.cursor(dictionary=True)

    try:
        # 1. Prüfen ob User existiert
        cur.execute("SELECT id, email, first_name, last_name FROM users WHERE id = %s", (user_id,))
        user = cur.fetchone()

        if not user:
            print(f"❌ Fehler: User mit ID {user_id} nicht gefunden!")
            return False

        print(f"\n📝 Erstelle Column Settings für:")
        print(f"   User: {user['first_name']} {user['last_name']} ({user['email']})")
        print()

        # 2. Bestehende Settings löschen
        cur.execute("DELETE FROM user_column_settings WHERE user_id = %s", (user_id,))
        deleted = cur.rowcount
        print(f"🗑️  {deleted} bestehende Column Settings gelöscht")

        # 3. Watchlist Column Settings einfügen
        print(f"\n📦 Füge Column Settings ein...")

        insert_sql = """
        INSERT INTO user_column_settings
            (user_id, view_name, source_table, column_key, display_name, sort_order, is_visible, column_group, format_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        watchlist_count = 0
        for col in DEFAULT_COLUMNS:
            cur.execute(insert_sql, (user_id,) + col)
            watchlist_count += 1

        print(f"   - Watchlist: {watchlist_count} Spalten")

        # 4. Screener Column Settings (kopiert von Watchlist)
        screener_count = 0
        for col in DEFAULT_COLUMNS:
            screener_col = (user_id, 'screener') + col[1:]  # view_name auf 'screener' ändern
            cur.execute(insert_sql, screener_col)
            screener_count += 1

        print(f"   - Screener: {screener_count} Spalten")

        conn.commit()

        # 5. Statistik anzeigen
        print(f"\n✅ Column Settings erfolgreich erstellt!")
        print(f"\n📊 Statistik:")

        cur.execute("""
            SELECT view_name, COUNT(*) as total, SUM(is_visible) as visible
            FROM user_column_settings
            WHERE user_id = %s
            GROUP BY view_name
        """, (user_id,))

        results = cur.fetchall()
        for row in results:
            print(f"   - {row['view_name']}: {row['total']} Spalten ({row['visible']} sichtbar)")

        # Sichtbare Spalten anzeigen
        cur.execute("""
            SELECT column_group, display_name
            FROM user_column_settings
            WHERE user_id = %s AND view_name = 'watchlist' AND is_visible = TRUE
            ORDER BY sort_order
        """, (user_id,))

        print(f"\n📋 Sichtbare Spalten (Watchlist):")
        current_group = None
        for row in cur.fetchall():
            if row['column_group'] != current_group:
                current_group = row['column_group']
                print(f"\n   [{current_group}]")
            display_name = row['display_name'].replace('<br>', ' ')
            print(f"     - {display_name}")

        return True

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Fehler beim Erstellen der Column Settings: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # Für Emanuel Schiller (ID: 3)
    USER_ID = 3

    create_column_settings_for_user(USER_ID)
