# Database Schema Overview

Dieses Dokument beschreibt alle relevanten Tabellen aus den Datenbanken:

- `raw_data`
- `tickerdb` (bzw. `tickert`)
- `analytics`

Es dient als Referenz für Entwicklung, SQL-Queries, Data Pipelines, ETL-Prozesse und AI-gestützte Tools (Continue, GPT etc.).

---

# 📂 1) Datenbank: `raw_data`

## 1.1 Tabelle: `eodhd_financial_statements`

Granulare Finanzkennzahlen (Income Statement, Balance Sheet, Cashflow) aus EODHD.

| Spalte | Typ | Beschreibung |
|--------|------|--------------|
| id | bigint, PK | Auto-Increment |
| ticker_eod | varchar(20), NOT NULL | EODHD-Ticker |
| ticker_yf | varchar(20), NULL | Alternativ: Yahoo-Ticker |
| company_name | varchar(255) | Unternehmensname |
| indizes | varchar(255) | Index-Zugehörigkeit |
| statement_type | enum('income','balance_sheet','cash_flow') | Typ |
| period | date | Periode (Jahr oder Quartal) |
| period_type | enum('Y','Q') | Art der Periode |
| metric | varchar(255) | Kennzahl |
| value | double | Wert |

**Keys:**

- `PRIMARY KEY (id)`
- `UNIQUE KEY uniq_record (ticker_eod, statement_type, period, period_type, metric)`
- `KEY idx_ticker_period (ticker_eod, period)`


---

## 1.2 Tabelle: `yf_prices`

Alle historischen Yahoo-Finanzdaten (OHLCV) je ISIN & YF-Ticker.

| Spalte | Typ |
|--------|-----|
| id | bigint, PK |
| isin | varchar(20), NOT NULL |
| ticker_yf | varchar(20), NOT NULL |
| date | date, NOT NULL |
| open | double |
| high | double |
| low | double |
| close | double |
| adj_close | double |
| volume | bigint |
| stock_index | varchar(50) |
| created_at | timestamp |
| updated_at | datetime |

**Keys:**

- `UNIQUE KEY uniq_isin_ticker_date (isin, ticker_yf, date)`
- `KEY idx_date (date)`


---

# 📂 2) Datenbank: `tickerdb` (oder `tickert`)

## Tabelle: `tickerlist`

Zentrale Stammdaten aller Aktien aus allen Indizes.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| isin | varchar(32), NOT NULL |
| stock_index | varchar(255), NOT NULL |
| name | varchar(255) |
| ticker | varchar(255) |
| yf_ticker | varchar(255) |
| eodhd_ticker | varchar(255) |
| exchange | varchar(255) |
| finanzen_name | varchar(255) |
| marketscreener_name | varchar(255) |
| created_at | timestamp |
| updated_at | timestamp |

**Keys:**

- `UNIQUE KEY unique_isin_stockindex (isin, stock_index)`


---

# 📂 3) Datenbank: `analytics`

## Tabelle: `eodhd_filtered_numbers`

Jährlich gruppierte Kennzahlen (aus `eodhd_financial_statements`) inkl. Preisen.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| ticker_eod | varchar(20), NOT NULL |
| year | int, NOT NULL |
| fiscal_year | date |
| ticker | varchar(20) |
| price | double |
| avg_price | double |
| TotalRevenue | double |
| CostOfRevenue | double |
| GrossProfit | double |
| OperatingExpense | double |
| SellingGeneralAndAdministration | double |
| SellingAndMarketingExpense | double |
| ResearchAndDevelopment | double |
| OtherOperatingExpenses | double |
| OperatingIncome | double |
| NonOperatingIncome | double |
| NetInterestIncome | double |
| InterestExpense | double |
| InterestIncome | double |
| PretaxIncome | double |
| TaxProvision | double |
| NetIncome | double |
| EBITDA | double |
| EBIT | double |
| NetMinorityInterest | double |
| CapitalExpenditure | double |
| ChangeInWorkingCapital | double |
| DepreciationAndAmortization | double |
| StockBasedCompensation | double |
| totalAssets | double |
| TotalDebt | double |
| TotalLiabilities | double |
| StockholdersEquity | double |
| NetDebt | double |
| CashAndEquivalents | double |
| CashAndShortTermInvestments | double |
| GoodWill | double |
| SharesOutstanding | double |
| created_at | timestamp |
| updated_at | datetime |

**Keys:**

- `UNIQUE KEY uq_tickereod_year (ticker_eod, year)`


---

# 🧩 Beziehungen (Relations)

## Ticker → Historische Kurse
- `tickerdb.tickerlist.yf_ticker` → `raw_data.yf_prices.ticker_yf`
- via ISIN: `tickerdb.tickerlist.isin` → `raw_data.yf_prices.isin`

## Finanzdaten → Jahreskennzahlen
- `raw_data.eodhd_financial_statements` → `analytics.eodhd_filtered_numbers`
  - Matching: (`ticker_eod`, `year`)



