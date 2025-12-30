#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die Tabelle analytics.fmp_filtered_numbers.
UNIQUE KEY auf (isin, stock_index, date, period).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

CREATE_TABLE_SQL = """
CREATE TABLE analytics.fmp_filtered_numbers (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(20) NOT NULL,
    stock_index VARCHAR(255) NOT NULL,
    company_name VARCHAR(255),

    -- Zeitraum
    date DATE NOT NULL,
    period VARCHAR(10),

    -- Preisdaten
    price DOUBLE DEFAULT NULL,
    avg_price DOUBLE DEFAULT NULL,
    market_cap DOUBLE DEFAULT NULL,

    -- Balance Sheet
    total_assets DOUBLE DEFAULT NULL,
    total_liabilities DOUBLE DEFAULT NULL,
    total_equity DOUBLE DEFAULT NULL,
    total_stockholders_equity DOUBLE DEFAULT NULL,
    cash_and_cash_equivalents DOUBLE DEFAULT NULL,
    goodwill DOUBLE DEFAULT NULL,
    short_term_debt DOUBLE DEFAULT NULL,
    long_term_debt DOUBLE DEFAULT NULL,
    total_debt DOUBLE DEFAULT NULL,
    net_debt DOUBLE DEFAULT NULL,
    minority_interest DOUBLE DEFAULT NULL,

    -- Income Statement
    revenue DOUBLE DEFAULT NULL,
    cost_of_revenue DOUBLE DEFAULT NULL,
    gross_profit DOUBLE DEFAULT NULL,
    research_and_development_expenses DOUBLE DEFAULT NULL,
    general_and_administrative_expenses DOUBLE DEFAULT NULL,
    selling_and_marketing_expenses DOUBLE DEFAULT NULL,
    selling_general_and_administrative_expenses DOUBLE DEFAULT NULL,
    other_expenses DOUBLE DEFAULT NULL,
    operating_expenses DOUBLE DEFAULT NULL,
    cost_and_expenses DOUBLE DEFAULT NULL,
    operating_income DOUBLE DEFAULT NULL,
    interest_income DOUBLE DEFAULT NULL,
    interest_expense DOUBLE DEFAULT NULL,
    ebitda DOUBLE DEFAULT NULL,
    total_other_income_expenses_net DOUBLE DEFAULT NULL,
    income_before_tax DOUBLE DEFAULT NULL,
    income_tax_expense DOUBLE DEFAULT NULL,
    net_income DOUBLE DEFAULT NULL,
    eps DOUBLE DEFAULT NULL,
    eps_diluted DOUBLE DEFAULT NULL,
    weighted_average_shs_out DOUBLE DEFAULT NULL,
    weighted_average_shs_out_dil DOUBLE DEFAULT NULL,

    -- Cash Flow
    net_income_cf DOUBLE DEFAULT NULL,
    depreciation_and_amortization_cf DOUBLE DEFAULT NULL,
    stock_based_compensation DOUBLE DEFAULT NULL,
    capital_expenditure DOUBLE DEFAULT NULL,
    free_cash_flow DOUBLE DEFAULT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_isin_index_date_period (isin, stock_index, date, period),
    KEY idx_ticker (ticker),
    KEY idx_isin (isin),
    KEY idx_stock_index (stock_index),
    KEY idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""


def main():
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        print("Erstelle Tabelle analytics.fmp_filtered_numbers...")
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
