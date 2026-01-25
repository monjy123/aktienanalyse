#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die 3 Frontend-Tabellen in der analytics Datenbank:
1. company_info     - Stammdaten (statisch, jährlich aktualisieren)
2. live_metrics     - Kennzahlen (täglich aktualisieren)
3. user_watchlist   - Benutzer-spezifisch (manuell)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# =============================================================================
# Tabelle 1: company_info (Stammdaten)
# =============================================================================
CREATE_COMPANY_INFO = """
CREATE TABLE IF NOT EXISTS analytics.company_info (
    isin VARCHAR(20) PRIMARY KEY,
    ticker VARCHAR(20),
    company_name VARCHAR(255),
    sector VARCHAR(100) COMMENT 'Sektor (z.B. Technology)',
    industry VARCHAR(255) COMMENT 'Branche (z.B. Software)',
    country VARCHAR(100) COMMENT 'Land des Hauptsitzes',
    currency VARCHAR(10) COMMENT 'Berichtswährung',
    description TEXT COMMENT 'Unternehmensbeschreibung',
    fiscal_year_end VARCHAR(20) COMMENT 'Fiskaljahr-Ende (z.B. December)',
    stock_index VARCHAR(50) COMMENT 'Index-Zugehörigkeit',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    KEY idx_ticker (ticker),
    KEY idx_sector (sector),
    KEY idx_country (country),
    KEY idx_stock_index (stock_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='Stammdaten - Update: jährlich oder bei Bedarf';
"""


# =============================================================================
# Tabelle 2: live_metrics (Kennzahlen - täglich)
# =============================================================================
CREATE_LIVE_METRICS = """
CREATE TABLE IF NOT EXISTS analytics.live_metrics (
    isin VARCHAR(20) PRIMARY KEY,
    ticker VARCHAR(20),
    company_name VARCHAR(255) COMMENT 'Für schnelle JOINs',
    stock_index VARCHAR(50),

    -- Kursdaten
    price DOUBLE COMMENT 'Aktueller Kurs',
    price_date DATE COMMENT 'Datum des Kurses',
    market_cap DOUBLE COMMENT 'Marktkapitalisierung',

    -- Bewertungskennzahlen (FY = Ganzjahr)
    fy_pe DOUBLE COMMENT 'FY KGV (Ganzjahresdaten)',
    fy_ev_ebit DOUBLE COMMENT 'FY EV/EBIT (Ganzjahresdaten)',

    -- Bewertungskennzahlen (TTM = Trailing Twelve Months)
    ttm_pe DOUBLE COMMENT 'TTM KGV (letzte 4 Quartale)',
    ttm_ev_ebit DOUBLE COMMENT 'TTM EV/EBIT (letzte 4 Quartale)',

    -- PE Durchschnitte (rollierend)
    pe_avg_5y DOUBLE COMMENT '5-Jahres-Durchschnitt PE',
    pe_avg_10y DOUBLE COMMENT '10-Jahres-Durchschnitt PE',
    pe_avg_15y DOUBLE COMMENT '15-Jahres-Durchschnitt PE',
    pe_avg_20y DOUBLE COMMENT '20-Jahres-Durchschnitt PE',
    pe_avg_10y_2019 DOUBLE COMMENT '10-Jahres-Durchschnitt PE (2010-2019, fix)',

    -- EV/EBIT Durchschnitte (rollierend)
    ev_ebit_avg_5y DOUBLE COMMENT '5-Jahres-Durchschnitt EV/EBIT',
    ev_ebit_avg_10y DOUBLE COMMENT '10-Jahres-Durchschnitt EV/EBIT',
    ev_ebit_avg_15y DOUBLE COMMENT '15-Jahres-Durchschnitt EV/EBIT',
    ev_ebit_avg_20y DOUBLE COMMENT '20-Jahres-Durchschnitt EV/EBIT',
    ev_ebit_avg_10y_2019 DOUBLE COMMENT '10-Jahres-Durchschnitt EV/EBIT (2010-2019, fix)',

    -- Wachstumsraten (CAGR)
    revenue_cagr_3y DOUBLE COMMENT 'Umsatz CAGR 3 Jahre in %',
    revenue_cagr_5y DOUBLE COMMENT 'Umsatz CAGR 5 Jahre in %',
    revenue_cagr_10y DOUBLE COMMENT 'Umsatz CAGR 10 Jahre in %',

    ebit_cagr_3y DOUBLE COMMENT 'EBIT CAGR 3 Jahre in %',
    ebit_cagr_5y DOUBLE COMMENT 'EBIT CAGR 5 Jahre in %',
    ebit_cagr_10y DOUBLE COMMENT 'EBIT CAGR 10 Jahre in %',

    net_income_cagr_3y DOUBLE COMMENT 'Gewinn CAGR 3 Jahre in %',
    net_income_cagr_5y DOUBLE COMMENT 'Gewinn CAGR 5 Jahre in %',
    net_income_cagr_10y DOUBLE COMMENT 'Gewinn CAGR 10 Jahre in %',

    -- Bilanz-Kennzahlen
    equity_ratio DOUBLE COMMENT 'Eigenkapitalquote in %',
    net_debt_ebitda DOUBLE COMMENT 'Net Debt / EBITDA',

    -- Margen (berechnet aus calcu_numbers)
    profit_margin DOUBLE COMMENT 'Gewinnmarge = Net Income / Revenue in %',
    operating_margin DOUBLE COMMENT 'Operative Marge = Operating Income / Revenue in %',
    profit_margin_avg_3y DOUBLE COMMENT '3-Jahres-Durchschnitt Gewinnmarge',
    profit_margin_avg_5y DOUBLE COMMENT '5-Jahres-Durchschnitt Gewinnmarge',
    profit_margin_avg_10y DOUBLE COMMENT '10-Jahres-Durchschnitt Gewinnmarge',
    profit_margin_avg_5y_2019 DOUBLE COMMENT '5-Jahres-Durchschnitt Gewinnmarge (2015-2019)',
    operating_margin_avg_3y DOUBLE COMMENT '3-Jahres-Durchschnitt Operative Marge',
    operating_margin_avg_5y DOUBLE COMMENT '5-Jahres-Durchschnitt Operative Marge',
    operating_margin_avg_10y DOUBLE COMMENT '10-Jahres-Durchschnitt Operative Marge',
    operating_margin_avg_5y_2019 DOUBLE COMMENT '5-Jahres-Durchschnitt Operative Marge (2015-2019)',

    -- yfinance API Kennzahlen (täglich aktualisiert)
    yf_ttm_pe DOUBLE COMMENT 'TTM PE von yfinance API (nicht berechnet)',
    yf_forward_pe DOUBLE COMMENT 'Forward PE von yfinance API',
    yf_payout_ratio DOUBLE COMMENT 'Payout Ratio von yfinance API in % (35.0 = 35%)',
    yf_profit_margin DOUBLE COMMENT 'Profit Margin von yfinance API in % (35.0 = 35%)',
    yf_operating_margin DOUBLE COMMENT 'Operating Margin von yfinance API in % (35.0 = 35%)',
    next_earnings_date DATE COMMENT 'Nächstes Earnings-Datum von yfinance API',

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    KEY idx_ticker (ticker),
    KEY idx_stock_index (stock_index),
    KEY idx_ttm_pe (ttm_pe),
    KEY idx_ttm_ev_ebit (ttm_ev_ebit)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='Live-Kennzahlen - Update: täglich';
"""


# =============================================================================
# Tabelle 3: user_watchlist (Benutzer-spezifisch)
# =============================================================================
CREATE_USER_WATCHLIST = """
CREATE TABLE IF NOT EXISTS analytics.user_watchlist (
    id INT AUTO_INCREMENT PRIMARY KEY,
    isin VARCHAR(20) NOT NULL UNIQUE,
    favorite INT DEFAULT 0 COMMENT '0=kein Favorit, 1-9=Favoriten-Stufen',
    notes TEXT COMMENT 'Persönliche Notizen',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    KEY idx_favorite (favorite)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='User Watchlist - Update: manuell';
"""


# =============================================================================
# Tabelle 4: user_favorite_labels (Favoriten-Namen)
# =============================================================================
CREATE_USER_FAVORITE_LABELS = """
CREATE TABLE IF NOT EXISTS analytics.user_favorite_labels (
    favorite_id INT PRIMARY KEY COMMENT '1-9',
    label VARCHAR(50) NOT NULL COMMENT 'Benutzerdefinierter Name',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='Benutzerdefinierte Namen für Favoriten-Stufen 1-9';
"""

INSERT_DEFAULT_LABELS = """
INSERT INTO analytics.user_favorite_labels (favorite_id, label) VALUES
    (1, 'Halte ich'),
    (2, 'Top Favorit'),
    (3, 'Beobachte ich'),
    (4, 'Favorit 4'),
    (5, 'Favorit 5'),
    (6, 'Favorit 6'),
    (7, 'Favorit 7'),
    (8, 'Favorit 8'),
    (9, 'Favorit 9')
ON DUPLICATE KEY UPDATE label = label;
"""


# =============================================================================
# Tabelle 5: user_favorite_filter (Favoriten-Filter)
# =============================================================================
CREATE_USER_FAVORITE_FILTER = """
CREATE TABLE IF NOT EXISTS analytics.user_favorite_filter (
    favorite_id INT PRIMARY KEY COMMENT '1-9',
    is_visible BOOLEAN DEFAULT TRUE COMMENT 'Sichtbar in Watchlist?',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='Filter: Welche Favoriten in der Watchlist angezeigt werden';
"""

INSERT_DEFAULT_FILTER = """
INSERT INTO analytics.user_favorite_filter (favorite_id, is_visible) VALUES
    (1, TRUE), (2, TRUE), (3, TRUE), (4, TRUE), (5, TRUE),
    (6, TRUE), (7, TRUE), (8, TRUE), (9, TRUE)
ON DUPLICATE KEY UPDATE is_visible = is_visible;
"""


def main():
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("FRONTEND TABELLEN ERSTELLEN")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Tabelle 1: company_info
        print("\n1. Erstelle analytics.company_info...")
        cur.execute(CREATE_COMPANY_INFO)
        print("   ✓ company_info erstellt (Stammdaten)")

        # Tabelle 2: live_metrics
        print("\n2. Erstelle analytics.live_metrics...")
        cur.execute(CREATE_LIVE_METRICS)
        print("   ✓ live_metrics erstellt (Kennzahlen)")

        # Tabelle 3: user_watchlist
        print("\n3. Erstelle analytics.user_watchlist...")
        cur.execute(CREATE_USER_WATCHLIST)
        print("   ✓ user_watchlist erstellt (Benutzer)")

        # Tabelle 4: user_favorite_labels
        print("\n4. Erstelle analytics.user_favorite_labels...")
        cur.execute(CREATE_USER_FAVORITE_LABELS)
        cur.execute(INSERT_DEFAULT_LABELS)
        print("   ✓ user_favorite_labels erstellt mit Default-Werten")

        # Tabelle 5: user_favorite_filter
        print("\n5. Erstelle analytics.user_favorite_filter...")
        cur.execute(CREATE_USER_FAVORITE_FILTER)
        cur.execute(INSERT_DEFAULT_FILTER)
        print("   ✓ user_favorite_filter erstellt mit Default-Werten")

        conn.commit()

        # Übersicht
        print("\n" + "=" * 60)
        print("FERTIG - Tabellen erstellt:")
        print("=" * 60)
        print("""
┌───────────────────────────┬──────────────────────────────────────┐
│ Tabelle                   │ Beschreibung                         │
├───────────────────────────┼──────────────────────────────────────┤
│ company_info              │ Stammdaten (jährlich aktualisieren)  │
│ live_metrics              │ Kennzahlen (täglich aktualisieren)   │
│ user_watchlist            │ Favoriten & Notizen (manuell)        │
│ user_favorite_labels      │ Namen für Favoriten 1-9              │
│ user_favorite_filter      │ Filter: Welche Favoriten anzeigen    │
└───────────────────────────┴──────────────────────────────────────┘

Nächste Schritte:
  1. python 01_load_company_info.py   → Stammdaten laden (yfinance)
  2. python 02_load_live_metrics.py   → Kennzahlen laden (calcu_numbers)
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
