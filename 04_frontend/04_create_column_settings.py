#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die Tabelle user_column_settings für konfigurierbare Spaltenauswahl.

Ermöglicht:
- Auswahl welche Spalten in Watchlist/Screener angezeigt werden
- Reihenfolge der Spalten festlegen
- Spalten aus live_metrics UND company_info kombinierbar
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# =============================================================================
# Tabelle: user_column_settings
# =============================================================================
CREATE_COLUMN_SETTINGS = """
CREATE TABLE IF NOT EXISTS analytics.user_column_settings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    view_name VARCHAR(50) NOT NULL COMMENT 'watchlist oder screener',
    source_table VARCHAR(50) NOT NULL COMMENT 'live_metrics oder company_info',
    column_key VARCHAR(50) NOT NULL COMMENT 'Spaltenname in der Quelltabelle',
    display_name VARCHAR(100) NOT NULL COMMENT 'Anzeigename im Frontend',
    sort_order INT DEFAULT 0 COMMENT 'Reihenfolge (niedrig = links)',
    is_visible BOOLEAN DEFAULT TRUE COMMENT 'Spalte anzeigen?',
    column_group VARCHAR(50) COMMENT 'Gruppierung (Bewertung, Wachstum, etc.)',
    format_type VARCHAR(20) DEFAULT 'number' COMMENT 'number, percent, currency, billions, text',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_view_column (view_name, column_key),
    KEY idx_view_visible (view_name, is_visible, sort_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='Spalten-Konfiguration für Watchlist und Screener';
"""


# =============================================================================
# Default-Spalten Definition
# =============================================================================
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
    ('watchlist', 'live_metrics', 'next_earnings_date', 'Nächste<br>Zahlen', 9, False, 'Stammdaten', 'date'),

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


def main():
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("SPALTEN-KONFIGURATION ERSTELLEN")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Tabelle erstellen
        print("\n1. Erstelle analytics.user_column_settings...")
        cur.execute(CREATE_COLUMN_SETTINGS)
        print("   Tabelle erstellt")

        # Default-Spalten einfügen
        print("\n2. Füge Default-Konfiguration ein...")

        insert_sql = """
        INSERT INTO analytics.user_column_settings
            (view_name, source_table, column_key, display_name, sort_order, is_visible, column_group, format_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            display_name = VALUES(display_name),
            sort_order = VALUES(sort_order),
            is_visible = VALUES(is_visible),
            column_group = VALUES(column_group),
            format_type = VALUES(format_type)
        """

        for col in DEFAULT_COLUMNS:
            cur.execute(insert_sql, col)

        # Screener: gleiche Spalten wie Watchlist kopieren
        print("\n3. Kopiere Konfiguration für Screener...")
        screener_columns = [(
            'screener', col[1], col[2], col[3], col[4], col[5], col[6], col[7]
        ) for col in DEFAULT_COLUMNS]

        for col in screener_columns:
            cur.execute(insert_sql, col)

        conn.commit()

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("""
            SELECT view_name, COUNT(*) as total,
                   SUM(is_visible) as visible
            FROM analytics.user_column_settings
            GROUP BY view_name
        """)

        print("\n{:<15} {:>10} {:>10}".format("View", "Gesamt", "Sichtbar"))
        print("-" * 35)
        for row in cur.fetchall():
            print("{:<15} {:>10} {:>10}".format(row[0], row[1], row[2]))

        # Sichtbare Spalten anzeigen
        cur.execute("""
            SELECT column_group, display_name
            FROM analytics.user_column_settings
            WHERE view_name = 'watchlist' AND is_visible = TRUE
            ORDER BY sort_order
        """)

        print("\nDefault sichtbare Spalten (Watchlist):")
        print("-" * 40)
        current_group = None
        for row in cur.fetchall():
            if row[0] != current_group:
                current_group = row[0]
                print(f"\n  [{current_group}]")
            print(f"    - {row[1]}")

        print("""

Verwendung:
-----------
-- Spalte ausblenden:
UPDATE analytics.user_column_settings
SET is_visible = FALSE
WHERE view_name = 'watchlist' AND column_key = 'ttm_pe';

-- Spalte einblenden:
UPDATE analytics.user_column_settings
SET is_visible = TRUE
WHERE view_name = 'watchlist' AND column_key = 'sector';

-- Reihenfolge ändern (Sektor nach vorne):
UPDATE analytics.user_column_settings
SET sort_order = 3
WHERE view_name = 'watchlist' AND column_key = 'sector';

-- Sichtbare Spalten für Query abrufen:
SELECT column_key, source_table, display_name
FROM analytics.user_column_settings
WHERE view_name = 'watchlist' AND is_visible = TRUE
ORDER BY sort_order;
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
