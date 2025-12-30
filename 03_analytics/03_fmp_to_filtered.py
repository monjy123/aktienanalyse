#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Befuellt analytics.fmp_filtered_numbers aus raw_data.fmp_financial_statements.

Logik:
- Kopiert ausgewaehlte Spalten von fmp_financial_statements
- Ergaenzt price (letzter Kurs des Jahres), avg_price (Durchschnittskurs)
- Berechnet market_cap aus price * weighted_average_shs_out
- UNIQUE KEY ist (isin, stock_index, date, period)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# Schritt 1: Fundamentaldaten kopieren
INSERT_FUNDAMENTALS_SQL = """
INSERT INTO analytics.fmp_filtered_numbers (
    ticker, isin, stock_index, company_name,
    date, period,

    -- Balance Sheet
    total_assets, total_liabilities, total_equity, total_stockholders_equity,
    cash_and_cash_equivalents, goodwill, short_term_debt, long_term_debt,
    total_debt, net_debt, minority_interest,

    -- Income Statement
    revenue, cost_of_revenue, gross_profit,
    research_and_development_expenses, general_and_administrative_expenses,
    selling_and_marketing_expenses, selling_general_and_administrative_expenses,
    other_expenses, operating_expenses, cost_and_expenses, operating_income,
    interest_income, interest_expense, ebitda, total_other_income_expenses_net,
    income_before_tax, income_tax_expense, net_income,
    eps, eps_diluted, weighted_average_shs_out, weighted_average_shs_out_dil,

    -- Cash Flow
    net_income_cf, depreciation_and_amortization_cf, stock_based_compensation,
    capital_expenditure, free_cash_flow
)

SELECT
    fs.ticker,
    fs.isin,
    fs.stock_index,
    fs.company_name,
    fs.date,
    fs.period,

    -- Balance Sheet
    fs.total_assets,
    fs.total_liabilities,
    fs.total_equity,
    fs.total_stockholders_equity,
    fs.cash_and_cash_equivalents,
    fs.goodwill,
    fs.short_term_debt,
    fs.long_term_debt,
    fs.total_debt,
    fs.net_debt,
    fs.minority_interest,

    -- Income Statement
    fs.revenue,
    fs.cost_of_revenue,
    fs.gross_profit,
    fs.research_and_development_expenses,
    fs.general_and_administrative_expenses,
    fs.selling_and_marketing_expenses,
    fs.selling_general_and_administrative_expenses,
    fs.other_expenses,
    fs.operating_expenses,
    fs.cost_and_expenses,
    fs.operating_income,
    fs.interest_income,
    fs.interest_expense,
    fs.ebitda,
    fs.total_other_income_expenses_net,
    fs.income_before_tax,
    fs.income_tax_expense,
    fs.net_income,
    fs.eps,
    fs.eps_diluted,
    fs.weighted_average_shs_out,
    fs.weighted_average_shs_out_dil,

    -- Cash Flow
    fs.net_income_cf,
    fs.depreciation_and_amortization_cf,
    fs.stock_based_compensation,
    fs.capital_expenditure,
    fs.free_cash_flow

FROM raw_data.fmp_financial_statements fs
WHERE fs.isin IS NOT NULL
  AND fs.isin != ''
  AND fs.stock_index IS NOT NULL

ON DUPLICATE KEY UPDATE
    ticker = VALUES(ticker),
    company_name = VALUES(company_name),
    total_assets = VALUES(total_assets),
    total_liabilities = VALUES(total_liabilities),
    total_equity = VALUES(total_equity),
    total_stockholders_equity = VALUES(total_stockholders_equity),
    cash_and_cash_equivalents = VALUES(cash_and_cash_equivalents),
    goodwill = VALUES(goodwill),
    short_term_debt = VALUES(short_term_debt),
    long_term_debt = VALUES(long_term_debt),
    total_debt = VALUES(total_debt),
    net_debt = VALUES(net_debt),
    minority_interest = VALUES(minority_interest),
    revenue = VALUES(revenue),
    cost_of_revenue = VALUES(cost_of_revenue),
    gross_profit = VALUES(gross_profit),
    research_and_development_expenses = VALUES(research_and_development_expenses),
    general_and_administrative_expenses = VALUES(general_and_administrative_expenses),
    selling_and_marketing_expenses = VALUES(selling_and_marketing_expenses),
    selling_general_and_administrative_expenses = VALUES(selling_general_and_administrative_expenses),
    other_expenses = VALUES(other_expenses),
    operating_expenses = VALUES(operating_expenses),
    cost_and_expenses = VALUES(cost_and_expenses),
    operating_income = VALUES(operating_income),
    interest_income = VALUES(interest_income),
    interest_expense = VALUES(interest_expense),
    ebitda = VALUES(ebitda),
    total_other_income_expenses_net = VALUES(total_other_income_expenses_net),
    income_before_tax = VALUES(income_before_tax),
    income_tax_expense = VALUES(income_tax_expense),
    net_income = VALUES(net_income),
    eps = VALUES(eps),
    eps_diluted = VALUES(eps_diluted),
    weighted_average_shs_out = VALUES(weighted_average_shs_out),
    weighted_average_shs_out_dil = VALUES(weighted_average_shs_out_dil),
    net_income_cf = VALUES(net_income_cf),
    depreciation_and_amortization_cf = VALUES(depreciation_and_amortization_cf),
    stock_based_compensation = VALUES(stock_based_compensation),
    capital_expenditure = VALUES(capital_expenditure),
    free_cash_flow = VALUES(free_cash_flow),
    updated_at = NOW();
"""

# Schritt 2: avg_price hinzufuegen (Durchschnitt pro ISIN und Jahr)
UPDATE_AVG_PRICE_SQL = """
UPDATE analytics.fmp_filtered_numbers ffn
JOIN (
    SELECT isin, YEAR(date) AS year, AVG(close) AS avg_price
    FROM raw_data.yf_prices
    GROUP BY isin, YEAR(date)
) yp ON yp.isin = ffn.isin AND yp.year = YEAR(ffn.date)
SET ffn.avg_price = yp.avg_price;
"""

# Schritt 3: price (letzter Kurs des Jahres) hinzufuegen
# Optimiert mit ROW_NUMBER() Window Function (MySQL 8+)
UPDATE_LAST_PRICE_SQL = """
UPDATE analytics.fmp_filtered_numbers ffn
JOIN (
    SELECT isin, year, close AS price
    FROM (
        SELECT
            isin,
            YEAR(date) AS year,
            close,
            ROW_NUMBER() OVER (PARTITION BY isin, YEAR(date) ORDER BY date DESC) AS rn
        FROM raw_data.yf_prices
    ) ranked
    WHERE rn = 1
) yp ON yp.isin = ffn.isin AND yp.year = YEAR(ffn.date)
SET ffn.price = yp.price;
"""

# Schritt 4: market_cap berechnen (price * weighted_average_shs_out)
UPDATE_MARKET_CAP_SQL = """
UPDATE analytics.fmp_filtered_numbers
SET market_cap = price * weighted_average_shs_out
WHERE price IS NOT NULL
  AND weighted_average_shs_out IS NOT NULL;
"""


def main():
    import time
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("FMP Financial Statements -> Filtered Numbers")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Schritt 1: Fundamentaldaten
        print("\n" + "-" * 60)
        print("[1/4] FUNDAMENTALDATEN KOPIEREN")
        print("-" * 60)
        print("      Kopiere ausgewaehlte Spalten aus fmp_financial_statements...")
        start = time.time()
        cur.execute(INSERT_FUNDAMENTALS_SQL)
        rows1 = cur.rowcount
        conn.commit()
        elapsed = time.time() - start
        print(f"      --> {rows1} Zeilen in {elapsed:.1f}s")

        # Schritt 2: avg_price
        print("\n" + "-" * 60)
        print("[2/4] DURCHSCHNITTSPREISE BERECHNEN")
        print("-" * 60)
        print("      Berechne AVG(close) pro ISIN/Jahr aus yf_prices...")
        start = time.time()
        cur.execute(UPDATE_AVG_PRICE_SQL)
        rows2 = cur.rowcount
        conn.commit()
        elapsed = time.time() - start
        print(f"      --> {rows2} Zeilen aktualisiert in {elapsed:.1f}s")

        # Schritt 3: price (letzter Kurs)
        print("\n" + "-" * 60)
        print("[3/4] JAHRESENDKURSE BERECHNEN")
        print("-" * 60)
        print("      Ermittle letzten Schlusskurs pro ISIN/Jahr...")
        start = time.time()
        cur.execute(UPDATE_LAST_PRICE_SQL)
        rows3 = cur.rowcount
        conn.commit()
        elapsed = time.time() - start
        print(f"      --> {rows3} Zeilen aktualisiert in {elapsed:.1f}s")

        # Schritt 4: market_cap
        print("\n" + "-" * 60)
        print("[4/4] MARKET CAP BERECHNEN")
        print("-" * 60)
        print("      Berechne market_cap = price * weighted_average_shs_out...")
        start = time.time()
        cur.execute(UPDATE_MARKET_CAP_SQL)
        rows4 = cur.rowcount
        conn.commit()
        elapsed = time.time() - start
        print(f"      --> {rows4} Zeilen aktualisiert in {elapsed:.1f}s")

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("SELECT COUNT(*) FROM analytics.fmp_filtered_numbers")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT stock_index) FROM analytics.fmp_filtered_numbers")
        indices = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT isin) FROM analytics.fmp_filtered_numbers")
        isins = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.fmp_filtered_numbers WHERE price IS NOT NULL")
        with_price = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.fmp_filtered_numbers WHERE market_cap IS NOT NULL")
        with_mcap = cur.fetchone()[0]

        print(f"Total Zeilen:             {total:,}")
        print(f"Unterschiedliche ISINs:   {isins:,}")
        print(f"Unterschiedliche Indizes: {indices}")
        print(f"Mit Kursdaten:            {with_price:,} ({100*with_price/total:.1f}%)" if total > 0 else "Mit Kursdaten: 0")
        print(f"Mit Market Cap:           {with_mcap:,} ({100*with_mcap/total:.1f}%)" if total > 0 else "Mit Market Cap: 0")

        # Aufschluesselung nach Index
        print("\nAufschluesselung nach stock_index:")
        cur.execute("""
            SELECT stock_index, COUNT(*) as cnt, COUNT(DISTINCT isin) as isins
            FROM analytics.fmp_filtered_numbers
            GROUP BY stock_index
            ORDER BY cnt DESC
        """)
        for row in cur.fetchall():
            print(f"  {row[0]:20} {row[1]:6,} Zeilen, {row[2]:4} ISINs")

        # Aufschluesselung nach Period
        print("\nAufschluesselung nach period:")
        cur.execute("""
            SELECT period, COUNT(*) as cnt
            FROM analytics.fmp_filtered_numbers
            GROUP BY period
            ORDER BY cnt DESC
        """)
        for row in cur.fetchall():
            print(f"  {row[0]:10} {row[1]:6,} Zeilen")

    except Error as e:
        print(f"Datenbankfehler: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Unerwarteter Fehler: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("\nVerbindung geschlossen.")


if __name__ == "__main__":
    main()
