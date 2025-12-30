#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die Tabelle analytics.eodhd_filtered_numbers.
UNIQUE KEY auf (isin, stock_index, year).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

CREATE_TABLE_SQL = """
CREATE TABLE analytics.eodhd_filtered_numbers (
    id INT AUTO_INCREMENT PRIMARY KEY,

    isin VARCHAR(20) NOT NULL,
    stock_index VARCHAR(255) NOT NULL,
    ticker_eod VARCHAR(20) NOT NULL,
    ticker_yf VARCHAR(20) DEFAULT NULL,

    year INT NOT NULL,
    fiscal_year DATE,

    price DOUBLE DEFAULT NULL,
    avg_price DOUBLE DEFAULT NULL,

    -- Income Statement
    TotalRevenue DOUBLE DEFAULT NULL,
    CostOfRevenue DOUBLE DEFAULT NULL,
    GrossProfit DOUBLE DEFAULT NULL,
    OperatingExpense DOUBLE DEFAULT NULL,
    SellingGeneralAndAdministration DOUBLE DEFAULT NULL,
    SellingAndMarketingExpense DOUBLE DEFAULT NULL,
    ResearchAndDevelopment DOUBLE DEFAULT NULL,
    OtherOperatingExpenses DOUBLE DEFAULT NULL,
    OperatingIncome DOUBLE DEFAULT NULL,
    NonOperatingIncome DOUBLE DEFAULT NULL,
    NetInterestIncome DOUBLE DEFAULT NULL,
    InterestExpense DOUBLE DEFAULT NULL,
    InterestIncome DOUBLE DEFAULT NULL,
    PretaxIncome DOUBLE DEFAULT NULL,
    TaxProvision DOUBLE DEFAULT NULL,
    NetIncome DOUBLE DEFAULT NULL,
    EBITDA DOUBLE DEFAULT NULL,
    EBIT DOUBLE DEFAULT NULL,
    NetMinorityInterest DOUBLE DEFAULT NULL,
    CapitalExpenditure DOUBLE DEFAULT NULL,
    ChangeInWorkingCapital DOUBLE DEFAULT NULL,
    DepreciationAndAmortization DOUBLE DEFAULT NULL,
    StockBasedCompensation DOUBLE DEFAULT NULL,

    -- Balance Sheet
    totalAssets DOUBLE DEFAULT NULL,
    TotalDebt DOUBLE DEFAULT NULL,
    TotalLiabilities DOUBLE DEFAULT NULL,
    StockholdersEquity DOUBLE DEFAULT NULL,
    NetDebt DOUBLE DEFAULT NULL,
    CashAndEquivalents DOUBLE DEFAULT NULL,
    CashAndShortTermInvestments DOUBLE DEFAULT NULL,
    GoodWill DOUBLE DEFAULT NULL,
    SharesOutstanding DOUBLE DEFAULT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_isin_index_year (isin, stock_index, year),
    KEY idx_ticker_eod (ticker_eod),
    KEY idx_stock_index (stock_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""


def main():
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        print("Erstelle Tabelle analytics.eodhd_filtered_numbers...")
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
