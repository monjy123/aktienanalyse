#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die Tabelle analytics.calcu_numbers.
Enthält berechnete Kennzahlen basierend auf fmp_filtered_numbers.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS analytics.calcu_numbers (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation (übernommen von fmp_filtered_numbers)
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(20) NOT NULL,
    stock_index VARCHAR(255) NOT NULL,
    company_name VARCHAR(255),
    date DATE NOT NULL,
    period VARCHAR(10),

    -- Basis-Kennzahlen (Ganzjahr)
    fy_pe DOUBLE DEFAULT NULL COMMENT 'FY Price/Earnings = Price / EPS (Ganzjahresdaten)',
    fy_ev_ebit DOUBLE DEFAULT NULL COMMENT 'FY EV/EBIT (Ganzjahresdaten)',
    ebit DOUBLE DEFAULT NULL COMMENT 'Operating Income',
    ev DOUBLE DEFAULT NULL COMMENT 'Enterprise Value = Market Cap + Net Debt + Minority Interest',

    -- TTM-Kennzahlen (Trailing Twelve Months - rollierend aus 4 Quartalen)
    ttm_net_income DOUBLE DEFAULT NULL COMMENT 'TTM Net Income (Summe letzte 4 Quartale)',
    ttm_ebit DOUBLE DEFAULT NULL COMMENT 'TTM EBIT (Summe letzte 4 Quartale)',
    ttm_pe DOUBLE DEFAULT NULL COMMENT 'TTM Price/Earnings = Market Cap / TTM Net Income',
    ttm_ev_ebit DOUBLE DEFAULT NULL COMMENT 'TTM EV/EBIT = EV / TTM EBIT',

    -- PE Durchschnitte (rollierend)
    pe_avg_5y DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt PE',
    pe_avg_10y DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt PE',
    pe_avg_15y DOUBLE DEFAULT NULL COMMENT '15-Jahres-Durchschnitt PE',
    pe_avg_20y DOUBLE DEFAULT NULL COMMENT '20-Jahres-Durchschnitt PE',

    -- EV/EBIT Durchschnitte (rollierend)
    ev_ebit_avg_5y DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt EV/EBIT',
    ev_ebit_avg_10y DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt EV/EBIT',
    ev_ebit_avg_15y DOUBLE DEFAULT NULL COMMENT '15-Jahres-Durchschnitt EV/EBIT',
    ev_ebit_avg_20y DOUBLE DEFAULT NULL COMMENT '20-Jahres-Durchschnitt EV/EBIT',

    -- Wachstumsraten (CAGR)
    revenue_cagr_3y DOUBLE DEFAULT NULL COMMENT 'Umsatz CAGR 3 Jahre in %',
    revenue_cagr_5y DOUBLE DEFAULT NULL COMMENT 'Umsatz CAGR 5 Jahre in %',
    revenue_cagr_10y DOUBLE DEFAULT NULL COMMENT 'Umsatz CAGR 10 Jahre in %',

    ebit_cagr_3y DOUBLE DEFAULT NULL COMMENT 'EBIT CAGR 3 Jahre in %',
    ebit_cagr_5y DOUBLE DEFAULT NULL COMMENT 'EBIT CAGR 5 Jahre in %',
    ebit_cagr_10y DOUBLE DEFAULT NULL COMMENT 'EBIT CAGR 10 Jahre in %',

    net_income_cagr_3y DOUBLE DEFAULT NULL COMMENT 'Gewinn CAGR 3 Jahre in %',
    net_income_cagr_5y DOUBLE DEFAULT NULL COMMENT 'Gewinn CAGR 5 Jahre in %',
    net_income_cagr_10y DOUBLE DEFAULT NULL COMMENT 'Gewinn CAGR 10 Jahre in %',

    -- Bilanz-Kennzahlen
    equity_ratio DOUBLE DEFAULT NULL COMMENT 'Eigenkapitalquote = Total Equity / Total Assets in %',
    net_debt_ebitda DOUBLE DEFAULT NULL COMMENT 'Net Debt / EBITDA',

    -- Margen (pro Jahr)
    profit_margin DOUBLE DEFAULT NULL COMMENT 'Gewinnmarge = Net Income / Revenue in %',
    operating_margin DOUBLE DEFAULT NULL COMMENT 'Operative Marge = Operating Income / Revenue in %',

    -- Gewinnmarge Durchschnitte (rollierend)
    profit_margin_avg_3y DOUBLE DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Gewinnmarge',
    profit_margin_avg_5y DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge',
    profit_margin_avg_10y DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Gewinnmarge',
    profit_margin_avg_5y_2019 DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge (2015-2019)',

    -- Operative Marge Durchschnitte (rollierend)
    operating_margin_avg_3y DOUBLE DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Operative Marge',
    operating_margin_avg_5y DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge',
    operating_margin_avg_10y DOUBLE DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Operative Marge',
    operating_margin_avg_5y_2019 DOUBLE DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge (2015-2019)',

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_isin_index_date_period (isin, stock_index, date, period),
    KEY idx_ticker (ticker),
    KEY idx_isin (isin),
    KEY idx_stock_index (stock_index),
    KEY idx_date (date),
    KEY idx_period (period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""


def main():
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        print("Erstelle Tabelle analytics.calcu_numbers...")
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        print("Tabelle erfolgreich erstellt!")

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
