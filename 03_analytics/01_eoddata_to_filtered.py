#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Befüllt analytics.eodhd_filtered_numbers aus tickerlist + eodhd_financial_statements.

Logik:
- Startet von tickerdb.tickerlist (nur Einträge mit eodhd_ticker)
- Für jeden Match in raw_data.eodhd_financial_statements wird pivotiert
- UNIQUE KEY ist (isin, stock_index, year)
- Kursdaten (price, avg_price) werden separat per UPDATE hinzugefügt (Performance)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# Schritt 1: Fundamentaldaten pivotieren (OHNE Kursdaten - schnell)
INSERT_FUNDAMENTALS_SQL = """
INSERT INTO analytics.eodhd_filtered_numbers (
    isin, stock_index, ticker_eod, ticker_yf, year, fiscal_year,

    TotalRevenue, CostOfRevenue, GrossProfit, OperatingExpense,
    SellingGeneralAndAdministration, SellingAndMarketingExpense,
    ResearchAndDevelopment, OtherOperatingExpenses, OperatingIncome,
    NonOperatingIncome, NetInterestIncome, InterestExpense, InterestIncome,
    PretaxIncome, TaxProvision, NetIncome, EBITDA, EBIT, NetMinorityInterest,
    CapitalExpenditure, ChangeInWorkingCapital, DepreciationAndAmortization,
    StockBasedCompensation, totalAssets, TotalDebt, TotalLiabilities,
    StockholdersEquity, NetDebt, CashAndEquivalents, CashAndShortTermInvestments,
    GoodWill, SharesOutstanding
)

SELECT
    tl.isin,
    tl.stock_index,
    tl.eodhd_ticker AS ticker_eod,
    tl.yf_ticker,
    YEAR(fs.period) AS year,
    fs.period AS fiscal_year,

    MAX(CASE WHEN fs.metric = 'IS_totalRevenue' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_costOfRevenue' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_grossProfit' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_totalOperatingExpenses' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_sellingGeneralAdministrative' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_sellingAndMarketingExpenses' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_researchDevelopment' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_otherOperatingExpenses' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_operatingIncome' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_nonOperatingIncomeNetOther' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_netInterestIncome' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_interestExpense' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_interestIncome' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_incomeBeforeTax' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_taxProvision' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_netIncome' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_ebitda' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_ebit' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'IS_minorityInterest' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'CF_capitalExpenditures' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'CF_changeInWorkingCapital' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'CF_depreciation' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'CF_stockBasedCompensation' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_totalAssets' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_shortLongTermDebtTotal' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_totalLiab' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_totalStockholderEquity' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_netDebt' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_cashAndEquivalents' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_cashAndShortTermInvestments' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_goodWill' THEN fs.value END),
    MAX(CASE WHEN fs.metric = 'BS_commonStockSharesOutstanding' THEN fs.value END)

FROM tickerdb.tickerlist tl
INNER JOIN raw_data.eodhd_financial_statements fs
    ON fs.ticker_eod = tl.eodhd_ticker

WHERE tl.eodhd_ticker IS NOT NULL
  AND tl.eodhd_ticker != ''
  AND fs.period_type = 'Y'

GROUP BY tl.isin, tl.stock_index, tl.eodhd_ticker, tl.yf_ticker, YEAR(fs.period), fs.period

ON DUPLICATE KEY UPDATE
    ticker_eod = VALUES(ticker_eod),
    ticker_yf = VALUES(ticker_yf),
    fiscal_year = VALUES(fiscal_year),
    updated_at = NOW(),
    TotalRevenue = VALUES(TotalRevenue),
    CostOfRevenue = VALUES(CostOfRevenue),
    GrossProfit = VALUES(GrossProfit),
    OperatingExpense = VALUES(OperatingExpense),
    SellingGeneralAndAdministration = VALUES(SellingGeneralAndAdministration),
    SellingAndMarketingExpense = VALUES(SellingAndMarketingExpense),
    ResearchAndDevelopment = VALUES(ResearchAndDevelopment),
    OtherOperatingExpenses = VALUES(OtherOperatingExpenses),
    OperatingIncome = VALUES(OperatingIncome),
    NonOperatingIncome = VALUES(NonOperatingIncome),
    NetInterestIncome = VALUES(NetInterestIncome),
    InterestExpense = VALUES(InterestExpense),
    InterestIncome = VALUES(InterestIncome),
    PretaxIncome = VALUES(PretaxIncome),
    TaxProvision = VALUES(TaxProvision),
    NetIncome = VALUES(NetIncome),
    EBITDA = VALUES(EBITDA),
    EBIT = VALUES(EBIT),
    NetMinorityInterest = VALUES(NetMinorityInterest),
    CapitalExpenditure = VALUES(CapitalExpenditure),
    ChangeInWorkingCapital = VALUES(ChangeInWorkingCapital),
    DepreciationAndAmortization = VALUES(DepreciationAndAmortization),
    StockBasedCompensation = VALUES(StockBasedCompensation),
    totalAssets = VALUES(totalAssets),
    TotalDebt = VALUES(TotalDebt),
    TotalLiabilities = VALUES(TotalLiabilities),
    StockholdersEquity = VALUES(StockholdersEquity),
    NetDebt = VALUES(NetDebt),
    CashAndEquivalents = VALUES(CashAndEquivalents),
    CashAndShortTermInvestments = VALUES(CashAndShortTermInvestments),
    GoodWill = VALUES(GoodWill),
    SharesOutstanding = VALUES(SharesOutstanding);
"""

# Schritt 2: avg_price hinzufügen
UPDATE_AVG_PRICE_SQL = """
UPDATE analytics.eodhd_filtered_numbers efn
JOIN (
    SELECT isin, YEAR(date) AS year, AVG(close) AS avg_price
    FROM raw_data.yf_prices
    GROUP BY isin, YEAR(date)
) yp ON yp.isin = efn.isin AND yp.year = efn.year
SET efn.avg_price = yp.avg_price;
"""

# Schritt 3: price (letzter Kurs des Jahres) hinzufügen
UPDATE_LAST_PRICE_SQL = """
UPDATE analytics.eodhd_filtered_numbers efn
JOIN (
    SELECT yp1.isin, YEAR(yp1.date) AS year, yp1.close AS price
    FROM raw_data.yf_prices yp1
    INNER JOIN (
        SELECT isin, YEAR(date) AS year, MAX(date) AS max_date
        FROM raw_data.yf_prices
        GROUP BY isin, YEAR(date)
    ) yp_max ON yp1.isin = yp_max.isin
            AND YEAR(yp1.date) = yp_max.year
            AND yp1.date = yp_max.max_date
) yp ON yp.isin = efn.isin AND yp.year = efn.year
SET efn.price = yp.price;
"""


def main():
    import time
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("EODHD Financial Statements -> Filtered Numbers")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Schritt 1: Fundamentaldaten
        print("\n" + "-" * 60)
        print("[1/3] FUNDAMENTALDATEN PIVOTIEREN")
        print("-" * 60)
        print("      Joiner tickerlist mit eodhd_financial_statements...")
        print("      Pivotiere ~30 Kennzahlen pro ISIN/Jahr...")
        start = time.time()
        cur.execute(INSERT_FUNDAMENTALS_SQL)
        rows1 = cur.rowcount
        conn.commit()
        elapsed = time.time() - start
        print(f"      --> {rows1} Zeilen in {elapsed:.1f}s")

        # Schritt 2: avg_price
        print("\n" + "-" * 60)
        print("[2/3] DURCHSCHNITTSPREISE BERECHNEN")
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
        print("[3/3] JAHRESENDKURSE BERECHNEN")
        print("-" * 60)
        print("      Ermittle letzten Schlusskurs pro ISIN/Jahr...")
        start = time.time()
        cur.execute(UPDATE_LAST_PRICE_SQL)
        rows3 = cur.rowcount
        conn.commit()
        elapsed = time.time() - start
        print(f"      --> {rows3} Zeilen aktualisiert in {elapsed:.1f}s")

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("SELECT COUNT(*) FROM analytics.eodhd_filtered_numbers")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT stock_index) FROM analytics.eodhd_filtered_numbers")
        indices = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT isin) FROM analytics.eodhd_filtered_numbers")
        isins = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.eodhd_filtered_numbers WHERE price IS NOT NULL")
        with_price = cur.fetchone()[0]

        print(f"Total Zeilen:            {total:,}")
        print(f"Unterschiedliche ISINs:  {isins:,}")
        print(f"Unterschiedliche Indizes: {indices}")
        print(f"Mit Kursdaten:           {with_price:,} ({100*with_price/total:.1f}%)")

        # Aufschlüsselung nach Index
        print("\nAufschlüsselung nach stock_index:")
        cur.execute("""
            SELECT stock_index, COUNT(*) as cnt, COUNT(DISTINCT isin) as isins
            FROM analytics.eodhd_filtered_numbers
            GROUP BY stock_index
            ORDER BY cnt DESC
        """)
        for row in cur.fetchall():
            print(f"  {row[0]:20} {row[1]:6,} Zeilen, {row[2]:4} ISINs")

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
