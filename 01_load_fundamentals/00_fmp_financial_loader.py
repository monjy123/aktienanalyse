#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FMP Financial Statements Loader

Lädt Balance Sheet, Income Statement und Cash Flow von FMP API
und kombiniert sie zu einer Zeile pro Datum.

Workflow:
1. Ticker aus tickerlist laden (yf_ticker + isin)
2. Primär: yf_ticker → FMP-Ticker konvertieren und Heimatbörse abfragen
3. Fallback: Falls zu wenig Daten, ISIN-Suche nach Alternativen
4. 3 Statements laden und by date mergen
5. In fmp_financial_statements speichern
"""

import sys
import os
import time
import logging
import threading
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from tqdm import tqdm
from dotenv import load_dotenv
from mysql.connector import Error as MySQLError
from db import get_connection

# .env laden
load_dotenv(Path(__file__).parent.parent / ".env")

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE_URL = "https://financialmodelingprep.com"

# Threading Konfiguration
MAX_WORKERS = 5  # Anzahl paralleler Threads

# Minimum Datensätze für vollständige Daten (3 Jahre Annual)
MIN_ANNUAL_RECORDS = 3
MIN_QUARTERLY_RECORDS = 8  # ~2 Jahre Quarterly

# Yahoo Finance → FMP Exchange Suffix Mapping
# FMP nutzt teilweise andere Suffixes als Yahoo Finance
YF_TO_FMP_SUFFIX = {
    ".F": ".DE",      # Frankfurt → Xetra (FMP nutzt .DE für deutsche Börsen)
    ".DE": ".DE",     # Xetra bleibt
    ".L": ".L",       # London bleibt
    ".PA": ".PA",     # Paris bleibt
    ".AS": ".AS",     # Amsterdam bleibt
    ".MI": ".MI",     # Mailand bleibt
    ".MC": ".MC",     # Madrid bleibt
    ".SW": ".SW",     # Schweiz bleibt
    ".TO": ".TO",     # Toronto bleibt
    ".T": ".T",       # Tokyo bleibt
    ".HK": ".HK",     # Hong Kong bleibt
    ".AX": ".AX",     # Australien bleibt
    ".BR": ".BR",     # Brüssel bleibt
    ".CO": ".CO",     # Kopenhagen bleibt
    ".HE": ".HE",     # Helsinki bleibt
    ".ST": ".ST",     # Stockholm bleibt
    ".OL": ".OL",     # Oslo bleibt
    ".IS": ".IS",     # Istanbul bleibt
    ".VI": ".VI",     # Wien bleibt
    ".LS": ".LS",     # Lissabon bleibt
    ".IR": ".IR",     # Irland bleibt
}

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "fmp_loader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL Statements
# =============================================================================

CREATE_MAPPING_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_ticker_mapping (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(12),
    company_name VARCHAR(255),
    stock_index VARCHAR(50),
    exchange VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY unique_ticker_isin (ticker, isin),
    INDEX idx_ticker (ticker),
    INDEX idx_isin (isin),
    INDEX idx_stock_index (stock_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_FINANCIALS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_data.fmp_financial_statements (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(12),
    stock_index VARCHAR(50),
    company_name VARCHAR(255),

    -- Zeitraum
    date DATE NOT NULL,
    period VARCHAR(10),
    calendar_year INT,

    -- Balance Sheet
    total_assets DOUBLE,
    total_liabilities DOUBLE,
    total_equity DOUBLE,
    total_stockholders_equity DOUBLE,
    total_current_assets DOUBLE,
    total_current_liabilities DOUBLE,
    total_non_current_assets DOUBLE,
    total_non_current_liabilities DOUBLE,
    cash_and_cash_equivalents DOUBLE,
    cash_and_short_term_investments DOUBLE,
    short_term_investments DOUBLE,
    net_receivables DOUBLE,
    inventory DOUBLE,
    other_current_assets DOUBLE,
    property_plant_equipment_net DOUBLE,
    goodwill DOUBLE,
    intangible_assets DOUBLE,
    goodwill_and_intangible_assets DOUBLE,
    long_term_investments DOUBLE,
    other_non_current_assets DOUBLE,
    accounts_payables DOUBLE,
    short_term_debt DOUBLE,
    deferred_revenue DOUBLE,
    other_current_liabilities DOUBLE,
    long_term_debt DOUBLE,
    deferred_revenue_non_current DOUBLE,
    deferred_tax_liabilities_non_current DOUBLE,
    other_non_current_liabilities DOUBLE,
    total_debt DOUBLE,
    net_debt DOUBLE,
    capital_lease_obligations DOUBLE,
    common_stock DOUBLE,
    retained_earnings DOUBLE,
    accumulated_other_comprehensive_income_loss DOUBLE,
    other_total_stockholders_equity DOUBLE,
    total_liabilities_and_stockholders_equity DOUBLE,
    minority_interest DOUBLE,
    total_investments DOUBLE,
    tax_assets DOUBLE,
    tax_payables DOUBLE,
    preferred_stock DOUBLE,
    treasury_stock DOUBLE,

    -- Income Statement
    revenue DOUBLE,
    cost_of_revenue DOUBLE,
    gross_profit DOUBLE,
    gross_profit_ratio DOUBLE,
    research_and_development_expenses DOUBLE,
    general_and_administrative_expenses DOUBLE,
    selling_and_marketing_expenses DOUBLE,
    selling_general_and_administrative_expenses DOUBLE,
    other_expenses DOUBLE,
    operating_expenses DOUBLE,
    cost_and_expenses DOUBLE,
    operating_income DOUBLE,
    operating_income_ratio DOUBLE,
    interest_income DOUBLE,
    interest_expense DOUBLE,
    depreciation_and_amortization DOUBLE,
    ebitda DOUBLE,
    ebitda_ratio DOUBLE,
    total_other_income_expenses_net DOUBLE,
    income_before_tax DOUBLE,
    income_before_tax_ratio DOUBLE,
    income_tax_expense DOUBLE,
    net_income DOUBLE,
    net_income_ratio DOUBLE,
    eps DOUBLE,
    eps_diluted DOUBLE,
    weighted_average_shs_out DOUBLE,
    weighted_average_shs_out_dil DOUBLE,

    -- Cash Flow
    net_income_cf DOUBLE,
    depreciation_and_amortization_cf DOUBLE,
    deferred_income_tax DOUBLE,
    stock_based_compensation DOUBLE,
    change_in_working_capital DOUBLE,
    accounts_receivables DOUBLE,
    inventory_cf DOUBLE,
    accounts_payables_cf DOUBLE,
    other_working_capital DOUBLE,
    other_non_cash_items DOUBLE,
    net_cash_provided_by_operating_activities DOUBLE,
    investments_in_property_plant_and_equipment DOUBLE,
    acquisitions_net DOUBLE,
    purchases_of_investments DOUBLE,
    sales_maturities_of_investments DOUBLE,
    other_investing_activities DOUBLE,
    net_cash_used_for_investing_activities DOUBLE,
    debt_repayment DOUBLE,
    common_stock_issued DOUBLE,
    common_stock_repurchased DOUBLE,
    dividends_paid DOUBLE,
    other_financing_activities DOUBLE,
    net_cash_used_provided_by_financing_activities DOUBLE,
    effect_of_forex_changes_on_cash DOUBLE,
    net_change_in_cash DOUBLE,
    cash_at_end_of_period DOUBLE,
    cash_at_beginning_of_period DOUBLE,
    operating_cash_flow DOUBLE,
    capital_expenditure DOUBLE,
    free_cash_flow DOUBLE,

    -- Metadata
    filing_date DATE,
    accepted_date DATETIME,
    cik VARCHAR(20),
    link VARCHAR(500),
    final_link VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_entry (ticker, date, period),
    INDEX idx_ticker (ticker),
    INDEX idx_isin (isin),
    INDEX idx_stock_index (stock_index),
    INDEX idx_date (date),
    INDEX idx_ticker_date (ticker, date),
    INDEX idx_calendar_year (calendar_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# =============================================================================
# API Functions
# =============================================================================

def api_request(endpoint, params=None, max_retries=3):
    """API Request mit Retry und Rate Limiting."""
    if params is None:
        params = {}
    params["apikey"] = FMP_API_KEY

    url = f"{FMP_BASE_URL}{endpoint}"

    for attempt in range(max_retries):
        try:
            time.sleep(0.25)  # Rate limiting
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(f"API Fehler (Versuch {attempt+1}): {e}. Warte {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"API Fehler nach {max_retries} Versuchen: {e}")
                return None
    return None


def search_isin(isin):
    """Suche Ticker für ISIN via FMP API. Gibt alle Ergebnisse zurück."""
    data = api_request(f"/stable/search-isin", {"isin": isin})
    if data and len(data) > 0:
        return data  # Alle Ergebnisse für Fallback-Logik
    return None


def yf_ticker_to_fmp(yf_ticker):
    """Konvertiere Yahoo Finance Ticker zu FMP Format.

    Beispiele:
    - SAP.DE → SAP.DE (bleibt)
    - SAP.F → SAP.DE (Frankfurt → Xetra)
    - AAPL → AAPL (US ohne Suffix)
    """
    if not yf_ticker:
        return None

    # Finde das Suffix
    for yf_suffix, fmp_suffix in YF_TO_FMP_SUFFIX.items():
        if yf_ticker.endswith(yf_suffix):
            base_ticker = yf_ticker[:-len(yf_suffix)]
            return base_ticker + fmp_suffix

    # Kein bekanntes Suffix → vermutlich US-Ticker, unverändert zurückgeben
    return yf_ticker


def has_sufficient_data(statements, min_records):
    """Prüfe ob genug Datensätze vorhanden sind."""
    if not statements:
        return False
    return len(statements) >= min_records


def count_non_null_fields(record, key_fields):
    """Zähle nicht-null Felder für Qualitätsprüfung."""
    count = 0
    for field in key_fields:
        if record.get(field) is not None:
            count += 1
    return count


def evaluate_data_quality(balance_sheets, income_statements, cash_flows, period_type):
    """Bewerte die Datenqualität. Gibt (is_sufficient, record_count, quality_score) zurück."""
    min_records = MIN_ANNUAL_RECORDS if period_type == "annual" else MIN_QUARTERLY_RECORDS

    # Anzahl Datensätze
    bs_count = len(balance_sheets) if balance_sheets else 0
    inc_count = len(income_statements) if income_statements else 0
    cf_count = len(cash_flows) if cash_flows else 0

    record_count = max(bs_count, inc_count, cf_count)

    # Genug Datensätze?
    if record_count < min_records:
        return False, record_count, 0

    # Qualitätsprüfung: Wichtige Felder vorhanden?
    key_fields_bs = ["totalAssets", "totalLiabilities", "totalEquity"]
    key_fields_inc = ["revenue", "netIncome", "operatingIncome"]
    key_fields_cf = ["freeCashFlow", "operatingCashFlow"]

    quality_score = 0

    if balance_sheets and len(balance_sheets) > 0:
        quality_score += count_non_null_fields(balance_sheets[0], key_fields_bs)
    if income_statements and len(income_statements) > 0:
        quality_score += count_non_null_fields(income_statements[0], key_fields_inc)
    if cash_flows and len(cash_flows) > 0:
        quality_score += count_non_null_fields(cash_flows[0], key_fields_cf)

    # Mindestens 5 wichtige Felder sollten gefüllt sein
    is_sufficient = record_count >= min_records and quality_score >= 5

    return is_sufficient, record_count, quality_score


def get_balance_sheet(ticker, period="annual"):
    """Lade Balance Sheet."""
    return api_request(f"/stable/balance-sheet-statement?symbol={ticker}", {
        "period": period,
        "limit": 1000
    })


def get_income_statement(ticker, period="annual"):
    """Lade Income Statement."""
    return api_request(f"/stable/income-statement?symbol={ticker}", {
        "period": period,
        "limit": 1000
    })


def get_cash_flow(ticker, period="annual"):
    """Lade Cash Flow Statement."""
    return api_request(f"/stable/cash-flow-statement?symbol={ticker}", {
        "period": period,
        "limit": 1000
    })


# =============================================================================
# Data Processing
# =============================================================================

def merge_statements(balance_sheets, income_statements, cash_flows):
    """Merge 3 Statements by date zu einer Zeile pro Datum."""
    merged = defaultdict(dict)

    # Balance Sheet
    if balance_sheets:
        for bs in balance_sheets:
            key = (bs.get("date"), bs.get("period"))
            merged[key].update({
                "date": bs.get("date"),
                "period": bs.get("period"),
                "calendar_year": bs.get("calendarYear"),
                "filing_date": bs.get("fillingDate"),
                "accepted_date": bs.get("acceptedDate"),
                "cik": bs.get("cik"),
                "link": bs.get("link"),
                "final_link": bs.get("finalLink"),
                # Balance Sheet Felder
                "total_assets": bs.get("totalAssets"),
                "total_liabilities": bs.get("totalLiabilities"),
                "total_equity": bs.get("totalEquity"),
                "total_stockholders_equity": bs.get("totalStockholdersEquity"),
                "total_current_assets": bs.get("totalCurrentAssets"),
                "total_current_liabilities": bs.get("totalCurrentLiabilities"),
                "total_non_current_assets": bs.get("totalNonCurrentAssets"),
                "total_non_current_liabilities": bs.get("totalNonCurrentLiabilities"),
                "cash_and_cash_equivalents": bs.get("cashAndCashEquivalents"),
                "cash_and_short_term_investments": bs.get("cashAndShortTermInvestments"),
                "short_term_investments": bs.get("shortTermInvestments"),
                "net_receivables": bs.get("netReceivables"),
                "inventory": bs.get("inventory"),
                "other_current_assets": bs.get("otherCurrentAssets"),
                "property_plant_equipment_net": bs.get("propertyPlantEquipmentNet"),
                "goodwill": bs.get("goodwill"),
                "intangible_assets": bs.get("intangibleAssets"),
                "goodwill_and_intangible_assets": bs.get("goodwillAndIntangibleAssets"),
                "long_term_investments": bs.get("longTermInvestments"),
                "other_non_current_assets": bs.get("otherNonCurrentAssets"),
                "accounts_payables": bs.get("accountPayables"),
                "short_term_debt": bs.get("shortTermDebt"),
                "deferred_revenue": bs.get("deferredRevenue"),
                "other_current_liabilities": bs.get("otherCurrentLiabilities"),
                "long_term_debt": bs.get("longTermDebt"),
                "deferred_revenue_non_current": bs.get("deferredRevenueNonCurrent"),
                "deferred_tax_liabilities_non_current": bs.get("deferredTaxLiabilitiesNonCurrent"),
                "other_non_current_liabilities": bs.get("otherNonCurrentLiabilities"),
                "total_debt": bs.get("totalDebt"),
                "net_debt": bs.get("netDebt"),
                "capital_lease_obligations": bs.get("capitalLeaseObligations"),
                "common_stock": bs.get("commonStock"),
                "retained_earnings": bs.get("retainedEarnings"),
                "accumulated_other_comprehensive_income_loss": bs.get("accumulatedOtherComprehensiveIncomeLoss"),
                "other_total_stockholders_equity": bs.get("othertotalStockholdersEquity"),
                "total_liabilities_and_stockholders_equity": bs.get("totalLiabilitiesAndStockholdersEquity"),
                "minority_interest": bs.get("minorityInterest"),
                "total_investments": bs.get("totalInvestments"),
                "tax_assets": bs.get("taxAssets"),
                "tax_payables": bs.get("taxPayables"),
                "preferred_stock": bs.get("preferredStock"),
                "treasury_stock": bs.get("treasuryStock"),
            })

    # Income Statement
    if income_statements:
        for inc in income_statements:
            key = (inc.get("date"), inc.get("period"))
            merged[key].update({
                "date": inc.get("date"),
                "period": inc.get("period"),
                "calendar_year": inc.get("calendarYear"),
                # Income Statement Felder
                "revenue": inc.get("revenue"),
                "cost_of_revenue": inc.get("costOfRevenue"),
                "gross_profit": inc.get("grossProfit"),
                "gross_profit_ratio": inc.get("grossProfitRatio"),
                "research_and_development_expenses": inc.get("researchAndDevelopmentExpenses"),
                "general_and_administrative_expenses": inc.get("generalAndAdministrativeExpenses"),
                "selling_and_marketing_expenses": inc.get("sellingAndMarketingExpenses"),
                "selling_general_and_administrative_expenses": inc.get("sellingGeneralAndAdministrativeExpenses"),
                "other_expenses": inc.get("otherExpenses"),
                "operating_expenses": inc.get("operatingExpenses"),
                "cost_and_expenses": inc.get("costAndExpenses"),
                "operating_income": inc.get("operatingIncome"),
                "operating_income_ratio": inc.get("operatingIncomeRatio"),
                "interest_income": inc.get("interestIncome"),
                "interest_expense": inc.get("interestExpense"),
                "depreciation_and_amortization": inc.get("depreciationAndAmortization"),
                "ebitda": inc.get("ebitda"),
                "ebitda_ratio": inc.get("ebitdaratio"),
                "total_other_income_expenses_net": inc.get("totalOtherIncomeExpensesNet"),
                "income_before_tax": inc.get("incomeBeforeTax"),
                "income_before_tax_ratio": inc.get("incomeBeforeTaxRatio"),
                "income_tax_expense": inc.get("incomeTaxExpense"),
                "net_income": inc.get("netIncome"),
                "net_income_ratio": inc.get("netIncomeRatio"),
                "eps": inc.get("eps"),
                "eps_diluted": inc.get("epsdiluted"),
                "weighted_average_shs_out": inc.get("weightedAverageShsOut"),
                "weighted_average_shs_out_dil": inc.get("weightedAverageShsOutDil"),
            })

    # Cash Flow
    if cash_flows:
        for cf in cash_flows:
            key = (cf.get("date"), cf.get("period"))
            merged[key].update({
                "date": cf.get("date"),
                "period": cf.get("period"),
                "calendar_year": cf.get("calendarYear"),
                # Cash Flow Felder
                "net_income_cf": cf.get("netIncome"),
                "depreciation_and_amortization_cf": cf.get("depreciationAndAmortization"),
                "deferred_income_tax": cf.get("deferredIncomeTax"),
                "stock_based_compensation": cf.get("stockBasedCompensation"),
                "change_in_working_capital": cf.get("changeInWorkingCapital"),
                "accounts_receivables": cf.get("accountsReceivables"),
                "inventory_cf": cf.get("inventory"),
                "accounts_payables_cf": cf.get("accountsPayables"),
                "other_working_capital": cf.get("otherWorkingCapital"),
                "other_non_cash_items": cf.get("otherNonCashItems"),
                "net_cash_provided_by_operating_activities": cf.get("netCashProvidedByOperatingActivities"),
                "investments_in_property_plant_and_equipment": cf.get("investmentsInPropertyPlantAndEquipment"),
                "acquisitions_net": cf.get("acquisitionsNet"),
                "purchases_of_investments": cf.get("purchasesOfInvestments"),
                "sales_maturities_of_investments": cf.get("salesMaturitiesOfInvestments"),
                "other_investing_activities": cf.get("otherInvestingActivites"),
                "net_cash_used_for_investing_activities": cf.get("netCashUsedForInvestingActivites"),
                "debt_repayment": cf.get("debtRepayment"),
                "common_stock_issued": cf.get("commonStockIssued"),
                "common_stock_repurchased": cf.get("commonStockRepurchased"),
                "dividends_paid": cf.get("dividendsPaid"),
                "other_financing_activities": cf.get("otherFinancingActivites"),
                "net_cash_used_provided_by_financing_activities": cf.get("netCashUsedProvidedByFinancingActivities"),
                "effect_of_forex_changes_on_cash": cf.get("effectOfForexChangesOnCash"),
                "net_change_in_cash": cf.get("netChangeInCash"),
                "cash_at_end_of_period": cf.get("cashAtEndOfPeriod"),
                "cash_at_beginning_of_period": cf.get("cashAtBeginningOfPeriod"),
                "operating_cash_flow": cf.get("operatingCashFlow"),
                "capital_expenditure": cf.get("capitalExpenditure"),
                "free_cash_flow": cf.get("freeCashFlow"),
            })

    return list(merged.values())


def save_mapping(cur, ticker, isin, stock_index, company_name, exchange):
    """Speichere Ticker-ISIN Mapping."""
    sql = """
    INSERT INTO raw_data.fmp_ticker_mapping
        (ticker, isin, stock_index, company_name, exchange)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        company_name = VALUES(company_name),
        exchange = VALUES(exchange)
    """
    cur.execute(sql, (ticker, isin, stock_index, company_name, exchange))


def save_financials(cur, ticker, isin, stock_index, company_name, records):
    """Speichere Financial Statements."""
    if not records:
        return 0

    sql = """
    INSERT INTO raw_data.fmp_financial_statements (
        ticker, isin, stock_index, company_name,
        date, period, calendar_year,
        total_assets, total_liabilities, total_equity, total_stockholders_equity,
        total_current_assets, total_current_liabilities,
        total_non_current_assets, total_non_current_liabilities,
        cash_and_cash_equivalents, cash_and_short_term_investments,
        short_term_investments, net_receivables, inventory, other_current_assets,
        property_plant_equipment_net, goodwill, intangible_assets,
        goodwill_and_intangible_assets, long_term_investments, other_non_current_assets,
        accounts_payables, short_term_debt, deferred_revenue, other_current_liabilities,
        long_term_debt, deferred_revenue_non_current, deferred_tax_liabilities_non_current,
        other_non_current_liabilities, total_debt, net_debt, capital_lease_obligations,
        common_stock, retained_earnings, accumulated_other_comprehensive_income_loss,
        other_total_stockholders_equity, total_liabilities_and_stockholders_equity,
        minority_interest, total_investments, tax_assets, tax_payables,
        preferred_stock, treasury_stock,
        revenue, cost_of_revenue, gross_profit, gross_profit_ratio,
        research_and_development_expenses, general_and_administrative_expenses,
        selling_and_marketing_expenses, selling_general_and_administrative_expenses,
        other_expenses, operating_expenses, cost_and_expenses,
        operating_income, operating_income_ratio,
        interest_income, interest_expense, depreciation_and_amortization,
        ebitda, ebitda_ratio, total_other_income_expenses_net,
        income_before_tax, income_before_tax_ratio, income_tax_expense,
        net_income, net_income_ratio, eps, eps_diluted,
        weighted_average_shs_out, weighted_average_shs_out_dil,
        net_income_cf, depreciation_and_amortization_cf, deferred_income_tax,
        stock_based_compensation, change_in_working_capital, accounts_receivables,
        inventory_cf, accounts_payables_cf, other_working_capital, other_non_cash_items,
        net_cash_provided_by_operating_activities,
        investments_in_property_plant_and_equipment, acquisitions_net,
        purchases_of_investments, sales_maturities_of_investments, other_investing_activities,
        net_cash_used_for_investing_activities, debt_repayment,
        common_stock_issued, common_stock_repurchased, dividends_paid,
        other_financing_activities, net_cash_used_provided_by_financing_activities,
        effect_of_forex_changes_on_cash, net_change_in_cash,
        cash_at_end_of_period, cash_at_beginning_of_period,
        operating_cash_flow, capital_expenditure, free_cash_flow,
        filing_date, accepted_date, cik, link, final_link
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        isin = VALUES(isin),
        stock_index = VALUES(stock_index),
        company_name = VALUES(company_name),
        calendar_year = VALUES(calendar_year),
        total_assets = VALUES(total_assets),
        total_liabilities = VALUES(total_liabilities),
        total_equity = VALUES(total_equity),
        revenue = VALUES(revenue),
        net_income = VALUES(net_income),
        free_cash_flow = VALUES(free_cash_flow),
        updated_at = NOW()
    """

    count = 0
    for r in records:
        if not r.get("date"):
            continue

        values = (
            ticker, isin, stock_index, company_name,
            r.get("date"), r.get("period"), r.get("calendar_year"),
            r.get("total_assets"), r.get("total_liabilities"),
            r.get("total_equity"), r.get("total_stockholders_equity"),
            r.get("total_current_assets"), r.get("total_current_liabilities"),
            r.get("total_non_current_assets"), r.get("total_non_current_liabilities"),
            r.get("cash_and_cash_equivalents"), r.get("cash_and_short_term_investments"),
            r.get("short_term_investments"), r.get("net_receivables"),
            r.get("inventory"), r.get("other_current_assets"),
            r.get("property_plant_equipment_net"), r.get("goodwill"),
            r.get("intangible_assets"), r.get("goodwill_and_intangible_assets"),
            r.get("long_term_investments"), r.get("other_non_current_assets"),
            r.get("accounts_payables"), r.get("short_term_debt"),
            r.get("deferred_revenue"), r.get("other_current_liabilities"),
            r.get("long_term_debt"), r.get("deferred_revenue_non_current"),
            r.get("deferred_tax_liabilities_non_current"), r.get("other_non_current_liabilities"),
            r.get("total_debt"), r.get("net_debt"), r.get("capital_lease_obligations"),
            r.get("common_stock"), r.get("retained_earnings"),
            r.get("accumulated_other_comprehensive_income_loss"),
            r.get("other_total_stockholders_equity"),
            r.get("total_liabilities_and_stockholders_equity"),
            r.get("minority_interest"), r.get("total_investments"),
            r.get("tax_assets"), r.get("tax_payables"),
            r.get("preferred_stock"), r.get("treasury_stock"),
            r.get("revenue"), r.get("cost_of_revenue"),
            r.get("gross_profit"), r.get("gross_profit_ratio"),
            r.get("research_and_development_expenses"),
            r.get("general_and_administrative_expenses"),
            r.get("selling_and_marketing_expenses"),
            r.get("selling_general_and_administrative_expenses"),
            r.get("other_expenses"), r.get("operating_expenses"),
            r.get("cost_and_expenses"), r.get("operating_income"),
            r.get("operating_income_ratio"), r.get("interest_income"),
            r.get("interest_expense"), r.get("depreciation_and_amortization"),
            r.get("ebitda"), r.get("ebitda_ratio"),
            r.get("total_other_income_expenses_net"), r.get("income_before_tax"),
            r.get("income_before_tax_ratio"), r.get("income_tax_expense"),
            r.get("net_income"), r.get("net_income_ratio"),
            r.get("eps"), r.get("eps_diluted"),
            r.get("weighted_average_shs_out"), r.get("weighted_average_shs_out_dil"),
            r.get("net_income_cf"), r.get("depreciation_and_amortization_cf"),
            r.get("deferred_income_tax"), r.get("stock_based_compensation"),
            r.get("change_in_working_capital"), r.get("accounts_receivables"),
            r.get("inventory_cf"), r.get("accounts_payables_cf"),
            r.get("other_working_capital"), r.get("other_non_cash_items"),
            r.get("net_cash_provided_by_operating_activities"),
            r.get("investments_in_property_plant_and_equipment"),
            r.get("acquisitions_net"), r.get("purchases_of_investments"),
            r.get("sales_maturities_of_investments"), r.get("other_investing_activities"),
            r.get("net_cash_used_for_investing_activities"), r.get("debt_repayment"),
            r.get("common_stock_issued"), r.get("common_stock_repurchased"),
            r.get("dividends_paid"), r.get("other_financing_activities"),
            r.get("net_cash_used_provided_by_financing_activities"),
            r.get("effect_of_forex_changes_on_cash"), r.get("net_change_in_cash"),
            r.get("cash_at_end_of_period"), r.get("cash_at_beginning_of_period"),
            r.get("operating_cash_flow"), r.get("capital_expenditure"),
            r.get("free_cash_flow"), r.get("filing_date"),
            r.get("accepted_date"), r.get("cik"),
            r.get("link"), r.get("final_link"),
        )

        try:
            cur.execute(sql, values)
            count += 1
        except MySQLError as e:
            logger.warning(f"Fehler beim Speichern {ticker} {r.get('date')}: {e}")

    return count


# =============================================================================
# Main
# =============================================================================

def fetch_statements_for_ticker(ticker, period):
    """Lade alle 3 Statements für einen Ticker."""
    bs = get_balance_sheet(ticker, period)
    inc = get_income_statement(ticker, period)
    cf = get_cash_flow(ticker, period)
    return bs, inc, cf


def process_ticker(cur, isin, stock_index, period_type, ticker_cache=None, yf_ticker=None, company_name_db=None):
    """Verarbeite einen Ticker mit Priorität auf Heimatbörse.

    Neue Logik:
    1. Primär: yf_ticker → FMP konvertieren und Heimatbörse abfragen
    2. Prüfen ob genug Daten vorhanden
    3. Fallback: Falls zu wenig Daten, ISIN-Suche nach besseren Alternativen

    Args:
        cur: DB Cursor
        isin: ISIN der Aktie
        stock_index: Index-Zugehörigkeit
        period_type: "FY" oder "Q"
        ticker_cache: Dict mit bereits gemappten ISIN→Ticker für Konsistenz
        yf_ticker: Yahoo Finance Ticker aus tickerlist
        company_name_db: Firmenname aus tickerlist
    """
    period = "annual" if period_type == "FY" else "quarter"
    used_fallback = False

    # 1. Cache prüfen (für Quarterly-Durchlauf)
    if ticker_cache and isin in ticker_cache:
        ticker, company_name, exchange = ticker_cache[isin]
        bs, inc, cf = fetch_statements_for_ticker(ticker, period)

        if bs or inc or cf:
            save_mapping(cur, ticker, isin, stock_index, company_name, exchange)
            merged = merge_statements(bs, inc, cf)
            count = save_financials(cur, ticker, isin, stock_index, company_name, merged)
            return ticker, count

    # 2. PRIMÄR: yf_ticker → FMP Ticker konvertieren und Heimatbörse abfragen
    ticker = None
    company_name = company_name_db
    exchange = None
    bs, inc, cf = None, None, None

    if yf_ticker:
        fmp_ticker = yf_ticker_to_fmp(yf_ticker)
        if fmp_ticker:
            logger.debug(f"Versuche Heimatbörse: {yf_ticker} → {fmp_ticker}")
            bs, inc, cf = fetch_statements_for_ticker(fmp_ticker, period)

            # Datenqualität prüfen
            is_sufficient, record_count, quality_score = evaluate_data_quality(bs, inc, cf, period)

            if is_sufficient:
                ticker = fmp_ticker
                exchange = "home_exchange"  # Heimatbörse
                logger.info(f"✓ Heimatbörse OK: {fmp_ticker} ({record_count} Records, Score: {quality_score})")
            else:
                logger.debug(f"Heimatbörse unzureichend: {fmp_ticker} ({record_count} Records, Score: {quality_score})")

    # 3. FALLBACK: ISIN-Suche falls Heimatbörse nicht ausreicht
    if not ticker and isin:
        logger.debug(f"Fallback: ISIN-Suche für {isin}")
        isin_results = search_isin(isin)

        if isin_results:
            best_ticker = None
            best_data = (None, None, None)
            best_score = -1
            best_count = 0
            best_mapping = None

            # Alle ISIN-Ergebnisse durchprobieren
            for mapping in isin_results:
                test_ticker = mapping.get("symbol")
                if not test_ticker:
                    continue

                # Überspringe den bereits getesteten Heimatbörsen-Ticker
                if yf_ticker:
                    fmp_home = yf_ticker_to_fmp(yf_ticker)
                    if test_ticker == fmp_home:
                        continue

                test_bs, test_inc, test_cf = fetch_statements_for_ticker(test_ticker, period)
                is_sufficient, record_count, quality_score = evaluate_data_quality(test_bs, test_inc, test_cf, period)

                logger.debug(f"  ISIN-Alternative: {test_ticker} ({record_count} Records, Score: {quality_score})")

                # Bessere Alternative gefunden?
                if quality_score > best_score or (quality_score == best_score and record_count > best_count):
                    best_score = quality_score
                    best_count = record_count
                    best_ticker = test_ticker
                    best_data = (test_bs, test_inc, test_cf)
                    best_mapping = mapping

            # Beste Alternative verwenden
            if best_ticker and best_score >= 5:
                ticker = best_ticker
                bs, inc, cf = best_data
                company_name = best_mapping.get("name") or company_name
                exchange = best_mapping.get("exchange")
                used_fallback = True
                logger.info(f"→ Fallback OK: {ticker} ({best_count} Records, Score: {best_score})")

    # 4. Kein Ticker gefunden
    if not ticker:
        return None, "Kein Ticker mit ausreichend Daten gefunden"

    # 5. Cache aktualisieren
    if ticker_cache is not None and period_type == "FY":
        ticker_cache[isin] = (ticker, company_name, exchange)

    # 6. Mapping und Daten speichern
    save_mapping(cur, ticker, isin, stock_index, company_name, exchange)

    if not bs and not inc and not cf:
        return ticker, "Keine Daten"

    merged = merge_statements(bs, inc, cf)
    count = save_financials(cur, ticker, isin, stock_index, company_name, merged)

    result_info = f"{count}" if not used_fallback else f"{count} (Fallback)"
    return ticker, result_info


def process_worker(isin, stock_index, period_type, ticker_cache, cache_lock, yf_ticker=None, company_name=None):
    """Worker-Funktion für Threading - jeder Thread hat eigene DB-Connection.

    Args:
        isin: ISIN der Aktie
        stock_index: Index-Zugehörigkeit
        period_type: "FY" oder "Q"
        ticker_cache: Thread-safe Dict mit ISIN→Ticker Mappings
        cache_lock: Lock für Cache-Zugriff
        yf_ticker: Yahoo Finance Ticker aus tickerlist (für Heimatbörse)
        company_name: Firmenname aus tickerlist
    """
    conn = None
    cur = None
    try:
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Cache nur für Quarterly verwenden (bei FY wird neu ermittelt)
        cached_ticker_data = None
        if period_type == "Q":
            with cache_lock:
                cached_ticker_data = ticker_cache.get(isin)

        ticker, result = process_ticker(
            cur, isin, stock_index, period_type,
            ticker_cache=ticker_cache if period_type == "Q" else None,
            yf_ticker=yf_ticker,
            company_name_db=company_name
        )

        # Bei Annual: Cache mit gefundenem Ticker aktualisieren
        if period_type == "FY" and ticker:
            with cache_lock:
                # Cache nur aktualisieren wenn noch nicht vorhanden
                if isin not in ticker_cache:
                    ticker_cache[isin] = (ticker, company_name, "cached")

        conn.commit()
        return isin, ticker, result

    except Exception as e:
        logger.error(f"Fehler bei {isin}: {e}")
        if conn:
            conn.rollback()
        return isin, None, str(e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def main():
    print("=" * 60)
    print("FMP FINANCIAL STATEMENTS LOADER (THREADED)")
    print(f"Max Workers: {MAX_WORKERS}")
    print("=" * 60)

    if not FMP_API_KEY:
        print("FEHLER: FMP_API_KEY nicht in .env gefunden!")
        return

    conn = None
    cur = None

    try:
        # DB Verbindung für Setup
        print("\nVerbinde zur Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()
        print("Verbunden!")

        # Tabellen erstellen
        print("\nErstelle Tabellen...")
        cur.execute(CREATE_MAPPING_TABLE)
        cur.execute(CREATE_FINANCIALS_TABLE)
        conn.commit()
        print("Tabellen erstellt!")

        # Ticker laden (ISIN, stock_index, yf_ticker, name)
        print("\nLade Ticker aus tickerlist...")
        cur.execute("""
            SELECT DISTINCT isin, stock_index, yf_ticker, name
            FROM tickerdb.tickerlist
            WHERE isin IS NOT NULL AND isin != ''
        """)
        tickers = cur.fetchall()
        print(f"{len(tickers)} Ticker geladen!")

        # Statistik: Wie viele haben yf_ticker?
        with_yf = sum(1 for t in tickers if t[2])
        print(f"  → {with_yf} mit yf_ticker (Heimatbörse primär)")
        print(f"  → {len(tickers) - with_yf} nur mit ISIN (Fallback)")

        # Setup-Connection schließen
        cur.close()
        conn.close()

        # Thread-safe Strukturen
        failed_isins = []
        ticker_cache = {}
        cache_lock = threading.Lock()

        # Annual Daten (parallel)
        print("\n" + "=" * 60)
        print("SCHRITT 1: Jährliche Daten (Annual) - PARALLEL")
        print("  Priorität: yf_ticker (Heimatbörse) → Fallback: ISIN-Suche")
        print("=" * 60)

        home_exchange_count = 0
        fallback_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    process_worker,
                    isin, stock_index, "FY", ticker_cache, cache_lock,
                    yf_ticker=yf_ticker, company_name=name
                ): (isin, stock_index, yf_ticker, name)
                for isin, stock_index, yf_ticker, name in tickers
            }

            with tqdm(total=len(futures), desc="Annual") as pbar:
                for future in as_completed(futures):
                    isin, ticker, result = future.result()
                    if ticker is None:
                        failed_isins.append((isin, futures[future][1], "Kein Ticker"))
                    elif result == "Keine Daten":
                        failed_isins.append((isin, futures[future][1], "Keine Daten"))
                    elif "(Fallback)" in str(result):
                        fallback_count += 1
                    else:
                        home_exchange_count += 1
                    pbar.update(1)

        print(f"\n→ {len(ticker_cache)} Ticker im Cache für Quarterly-Durchlauf")
        print(f"  → {home_exchange_count} via Heimatbörse (yf_ticker)")
        print(f"  → {fallback_count} via ISIN-Fallback")

        # Quarterly Daten (parallel)
        print("\n" + "=" * 60)
        print("SCHRITT 2: Quartalsdaten (Quarterly) - PARALLEL")
        print("  Nutzt gecachte Ticker aus Annual-Durchlauf")
        print("=" * 60)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    process_worker,
                    isin, stock_index, "Q", ticker_cache, cache_lock,
                    yf_ticker=yf_ticker, company_name=name
                ): (isin, stock_index)
                for isin, stock_index, yf_ticker, name in tickers
            }

            with tqdm(total=len(futures), desc="Quarterly") as pbar:
                for future in as_completed(futures):
                    future.result()  # Errors werden geloggt
                    pbar.update(1)

        # Finale Statistik aus DB
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        conn = get_connection()
        cur = conn.cursor()

        # Anzahl gemappter Ticker
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_ticker_mapping")
        mapped = cur.fetchone()[0]

        # Anzahl Ticker mit Daten (haben mind. 1 Zeile)
        cur.execute("""
            SELECT COUNT(DISTINCT ticker)
            FROM raw_data.fmp_financial_statements
        """)
        ticker_with_data = cur.fetchone()[0]

        # Gesamt Zeilen
        cur.execute("SELECT COUNT(*) FROM raw_data.fmp_financial_statements")
        total_rows = cur.fetchone()[0]

        # Annual vs Quarterly
        cur.execute("""
            SELECT
                CASE WHEN period = 'FY' THEN 'Annual' ELSE 'Quarterly' END as period_type,
                COUNT(*) as cnt
            FROM raw_data.fmp_financial_statements
            GROUP BY CASE WHEN period = 'FY' THEN 'Annual' ELSE 'Quarterly' END
        """)
        period_stats = cur.fetchall()

        # ISINs ohne Ticker
        total_tickers = len(tickers)
        tickers_ohne_mapping = total_tickers - mapped

        # Ausgabe
        print(f"\n📊 Gesamt Ticker:          {total_tickers:,}")
        print(f"✅ Ticker gemapped:        {mapped:,} ({mapped*100/total_tickers:.1f}%)")
        print(f"❌ Kein Ticker gefunden:   {tickers_ohne_mapping:,} ({tickers_ohne_mapping*100/total_tickers:.1f}%)")
        print(f"📈 Ticker mit Daten:       {ticker_with_data:,}")
        print(f"\n📄 Gesamt Datensätze:      {total_rows:,}")

        for period_type, cnt in period_stats:
            print(f"   - {period_type:12} {cnt:>8,} Zeilen")

        print("\n📊 Datensätze pro Index:")
        cur.execute("""
            SELECT stock_index, COUNT(*) as cnt
            FROM raw_data.fmp_financial_statements
            GROUP BY stock_index
            ORDER BY cnt DESC
        """)
        for stock_idx, cnt in cur.fetchall():
            print(f"   {stock_idx:20} {cnt:>8,} Zeilen")

        if failed_isins:
            print(f"\n⚠️  Fehlgeschlagene ISINs: {len(failed_isins):,} (erste 20):")
            for isin, idx, reason in failed_isins[:20]:
                print(f"   {isin} ({idx}): {reason}")

        cur.close()
        conn.close()

    except MySQLError as e:
        print(f"Datenbankfehler: {e}")
        logger.exception("Datenbankfehler")
    except Exception as e:
        print(f"Fehler: {e}")
        logger.exception("Unerwarteter Fehler")

    print("\nFertig.")


if __name__ == "__main__":
    main()
