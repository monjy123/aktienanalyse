# Database Schema Overview

Primäre Schemas: `tickerdb` (Stammdaten), `raw_data` (Ingestion), `analytics` (Transform/Berechnung). FMP ist die aktuelle Hauptquelle für Fundamentals; EODHD-Pfade sind als Legacy noch vorhanden.

---

# 1) Datenbank: `raw_data`

## 1.1 `fmp_financial_statements`
Konsolidierte FMP-Financial-Statements (Balance Sheet, Income Statement, Cash Flow) je Datum/Periode.

- Kernspalten: `ticker`, `isin`, `stock_index`, `company_name`, `date`, `period`, `calendar_year`, Filing-Metadaten (`filing_date`, `accepted_date`, `cik`, `link`, `final_link`).
- Enthält die vollen FMP-Felder zu Bilanz, GuV, Cashflow (siehe Loader `01_load_fundamentals/00_fmp_financial_loader.py` für Feldliste).
- Keys: `UNIQUE (ticker, date, period)`, Indizes auf `ticker`, `isin`, `stock_index`, `date`, `calendar_year`.

## 1.2 `fmp_ticker_mapping`
Mapping Yahoo/FMP/ISIN pro Wertpapier.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| id | int, PK | |
| ticker | varchar(20) | FMP-Ticker |
| isin | varchar(12) | ISIN |
| company_name | varchar(255) | Firmenname |
| stock_index | varchar(50) | Index-Zuordnung |
| exchange | varchar(100) | Heimatbörse |
| created_at | timestamp | |

Keys: `UNIQUE (ticker, isin)`, Indizes auf `ticker`, `isin`, `stock_index`.

## 1.3 `fmp_historical_market_cap`
Historische Marktkapitalisierung.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| ticker | varchar(20) |
| isin | varchar(12) |
| stock_index | varchar(50) |
| company_name | varchar(255) |
| date | date |
| market_cap | bigint |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (ticker, date)`, Indizes auf `ticker`, `isin`, `stock_index`, `date`.

## 1.4 `fmp_revenue_product_segments`
Produkt-Segmente (EAV-Struktur) je Datum/Periode.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| ticker | varchar(20) |
| isin | varchar(12) |
| stock_index | varchar(50) |
| company_name | varchar(255) |
| date | date |
| fiscal_year | int |
| period | varchar(10) |
| reported_currency | varchar(10) |
| segment_name | varchar(255) |
| revenue | double |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (ticker, date, period, segment_name)`, Indizes auf `ticker`, `isin`, `stock_index`, `date`, `period`, `segment_name`.

## 1.5 `fmp_revenue_geographic_segments`
Analog zu Produktsegmenten, aber nach Region (`segment_name`, `revenue`). Gleiche Keys/Indizes wie 1.4.

## 1.6 `fmp_historical_sector_pe`
Historische PE-Ratios nach Sektor/Börse.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| sector | varchar(50) |
| exchange | varchar(20) |
| date | date |
| pe | double |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (sector, exchange, date)`, Indizes auf `sector`, `exchange`, `date`.

## 1.7 `fmp_historical_sector_performance`
Durchschnittliche Sektor-Performance je Tag.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| sector | varchar(50) |
| exchange | varchar(20) |
| date | date |
| average_change | double |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (sector, exchange, date)`, Indizes wie bei 1.6.

## 1.8 `fmp_treasury_rates`
US Treasury Rates je Laufzeit.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| date | date |
| month1 ... year30 | double | Laufzeitspalten (1M–30Y) |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (date)`, Index `date`.

## 1.9 `fmp_economic_indicators`
Historische Makro-Indikatoren.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| indicator_name | varchar(100) |
| date | date |
| value | double |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (indicator_name, date)`, Indizes auf `indicator_name`, `date`.

## 1.10 `yf_prices`
Yahoo-Finanzdaten (OHLCV) je ISIN/Ticker.

| Spalte | Typ |
|--------|-----|
| id | bigint, PK |
| isin | varchar(20) |
| ticker_yf | varchar(20) |
| date | date |
| open | double |
| high | double |
| low | double |
| close | double |
| adj_close | double |
| volume | bigint |
| stock_index | varchar(50) |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (isin, ticker_yf, date)`, Index `date`.

## 1.11 Legacy: `eodhd_financial_statements`
Früher importierte EODHD-Rohdaten (Metric/Value). Wird nur noch für Legacy-Skripte genutzt.

---

# 2) Datenbank: `tickerdb`

## 2.1 `tickerlist`
Zentrale Stammdaten und Quell-Mapping.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| isin | varchar(32) |
| stock_index | varchar(255) |
| name | varchar(255) |
| ticker | varchar(255) |
| yf_ticker | varchar(255) |
| eodhd_ticker | varchar(255) |
| exchange | varchar(255) |
| finanzen_name | varchar(255) |
| marketscreener_name | varchar(255) |
| created_at | timestamp |
| updated_at | timestamp |

Keys: `UNIQUE (isin, stock_index)`.

---

# 3) Datenbank: `analytics`

## 3.1 `fmp_filtered_numbers`
Gefilterte/angereicherte Fundamentals aus FMP + Kursdaten.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| id | int, PK | |
| ticker | varchar(20) | |
| isin | varchar(20) | |
| stock_index | varchar(255) | |
| company_name | varchar(255) | |
| date | date | Periode (FY/Q) |
| period | varchar(10) | |
| price | double | Schlusskurs Jahresende |
| avg_price | double | Durchschnittskurs pro Jahr |
| market_cap | double | price * weighted_average_shs_out |
| total_assets | double | |
| total_liabilities | double | |
| total_equity | double | |
| total_stockholders_equity | double | |
| cash_and_cash_equivalents | double | |
| goodwill | double | |
| short_term_debt | double | |
| long_term_debt | double | |
| total_debt | double | |
| net_debt | double | |
| minority_interest | double | |
| revenue | double | |
| cost_of_revenue | double | |
| gross_profit | double | |
| research_and_development_expenses | double | |
| general_and_administrative_expenses | double | |
| selling_and_marketing_expenses | double | |
| selling_general_and_administrative_expenses | double | |
| other_expenses | double | |
| operating_expenses | double | |
| cost_and_expenses | double | |
| operating_income | double | |
| interest_income | double | |
| interest_expense | double | |
| ebitda | double | |
| total_other_income_expenses_net | double | |
| income_before_tax | double | |
| income_tax_expense | double | |
| net_income | double | |
| eps | double | |
| eps_diluted | double | |
| weighted_average_shs_out | double | |
| weighted_average_shs_out_dil | double | |
| net_income_cf | double | |
| depreciation_and_amortization_cf | double | |
| stock_based_compensation | double | |
| capital_expenditure | double | |
| free_cash_flow | double | |
| created_at | timestamp | |
| updated_at | datetime | |

Keys: `UNIQUE (isin, stock_index, date, period)`, Indizes auf `ticker`, `isin`, `stock_index`, `date`.

## 3.2 `calcu_numbers`
Berechnete Kennzahlen auf Basis `fmp_filtered_numbers`.

| Spalte | Typ |
|--------|-----|
| id | int, PK |
| ticker | varchar(20) |
| isin | varchar(20) |
| stock_index | varchar(255) |
| company_name | varchar(255) |
| date | date |
| period | varchar(10) |
| pe | double |
| ebit | double |
| ev | double |
| ev_ebit | double |
| pe_avg_5y | double |
| pe_avg_10y | double |
| pe_avg_15y | double |
| pe_avg_20y | double |
| ev_ebit_avg_5y | double |
| ev_ebit_avg_10y | double |
| ev_ebit_avg_15y | double |
| ev_ebit_avg_20y | double |
| revenue_cagr_3y | double |
| revenue_cagr_5y | double |
| revenue_cagr_10y | double |
| ebit_cagr_3y | double |
| ebit_cagr_5y | double |
| ebit_cagr_10y | double |
| net_income_cagr_3y | double |
| net_income_cagr_5y | double |
| net_income_cagr_10y | double |
| equity_ratio | double |
| net_debt_ebitda | double |
| profit_margin | double | Gewinnmarge = Net Income / Revenue in % |
| operating_margin | double | Operative Marge = Operating Income / Revenue in % |
| profit_margin_avg_3y | double | 3-Jahres-Durchschnitt Gewinnmarge |
| profit_margin_avg_5y | double | 5-Jahres-Durchschnitt Gewinnmarge |
| profit_margin_avg_10y | double | 10-Jahres-Durchschnitt Gewinnmarge |
| profit_margin_avg_5y_2019 | double | 5-Jahres-Durchschnitt Gewinnmarge (2015-2019) |
| operating_margin_avg_3y | double | 3-Jahres-Durchschnitt Operative Marge |
| operating_margin_avg_5y | double | 5-Jahres-Durchschnitt Operative Marge |
| operating_margin_avg_10y | double | 10-Jahres-Durchschnitt Operative Marge |
| operating_margin_avg_5y_2019 | double | 5-Jahres-Durchschnitt Operative Marge (2015-2019) |
| created_at | timestamp |
| updated_at | datetime |

Keys: `UNIQUE (isin, stock_index, date, period)`, Indizes auf `ticker`, `isin`, `stock_index`, `date`, `period`.

## 3.3 Frontend-Tabellen

### 3.3.1 `company_info`
Stammdaten für Website/Screener. Update: jährlich oder bei Bedarf.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| isin | varchar(20), PK | Primärschlüssel |
| ticker | varchar(20) | FMP/YF Ticker |
| company_name | varchar(255) | Unternehmensname |
| sector | varchar(100) | Sektor (z.B. Technology) |
| industry | varchar(255) | Branche (z.B. Software) |
| country | varchar(100) | Land des Hauptsitzes |
| currency | varchar(10) | Berichtswährung |
| description | text | Unternehmensbeschreibung |
| fiscal_year_end | varchar(20) | Fiskaljahr-Ende (z.B. December) |
| stock_index | varchar(50) | Index-Zugehörigkeit |
| created_at | timestamp | |
| updated_at | datetime | |

Quellen: `tickerlist` (Basisdaten), yfinance API (sector, industry, country, description, fiscal_year_end).

### 3.3.2 `live_metrics`
Aktuelle Kennzahlen für Website/Screener. Update: täglich.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| isin | varchar(20), PK | Primärschlüssel |
| ticker | varchar(20) | |
| company_name | varchar(255) | Für schnelle JOINs |
| stock_index | varchar(50) | |
| price | double | Aktueller Kurs |
| price_date | date | Datum des Kurses |
| market_cap | double | Marktkapitalisierung |
| pe | double | Aktuelles KGV |
| ev_ebit | double | Aktuelles EV/EBIT |
| pe_avg_5y | double | 5-Jahres-Durchschnitt PE |
| pe_avg_10y | double | 10-Jahres-Durchschnitt PE |
| pe_avg_15y | double | 15-Jahres-Durchschnitt PE |
| pe_avg_20y | double | 20-Jahres-Durchschnitt PE |
| pe_avg_10y_2019 | double | 10J-Durchschnitt PE (2010-2019, fix) |
| ev_ebit_avg_5y | double | 5-Jahres-Durchschnitt EV/EBIT |
| ev_ebit_avg_10y | double | 10-Jahres-Durchschnitt EV/EBIT |
| ev_ebit_avg_15y | double | 15-Jahres-Durchschnitt EV/EBIT |
| ev_ebit_avg_20y | double | 20-Jahres-Durchschnitt EV/EBIT |
| ev_ebit_avg_10y_2019 | double | 10J-Durchschnitt EV/EBIT (2010-2019, fix) |
| revenue_cagr_3y | double | Umsatz CAGR 3J |
| revenue_cagr_5y | double | Umsatz CAGR 5J |
| revenue_cagr_10y | double | Umsatz CAGR 10J |
| ebit_cagr_3y | double | EBIT CAGR 3J |
| ebit_cagr_5y | double | EBIT CAGR 5J |
| ebit_cagr_10y | double | EBIT CAGR 10J |
| net_income_cagr_3y | double | Gewinn CAGR 3J |
| net_income_cagr_5y | double | Gewinn CAGR 5J |
| net_income_cagr_10y | double | Gewinn CAGR 10J |
| equity_ratio | double | Eigenkapitalquote in % |
| net_debt_ebitda | double | Net Debt / EBITDA |
| yf_ttm_pe | double | TTM PE von yfinance API |
| yf_forward_pe | double | Forward PE von yfinance API |
| yf_payout_ratio | double | Payout Ratio in % |
| yf_profit_margin | double | Profit Margin in % |
| yf_operating_margin | double | Operating Margin in % |
| yf_ttm_pe_vs_avg_5y | double | YF TTM PE vs. 5J-Durchschnitt in % |
| yf_ttm_pe_vs_avg_10y | double | YF TTM PE vs. 10J-Durchschnitt in % |
| yf_ttm_pe_vs_avg_15y | double | YF TTM PE vs. 15J-Durchschnitt in % |
| yf_ttm_pe_vs_avg_20y | double | YF TTM PE vs. 20J-Durchschnitt in % |
| yf_ttm_pe_vs_avg_10y_2019 | double | YF TTM PE vs. 10J-Durchschnitt (2010-2019) in % |
| yf_fwd_pe_vs_avg_5y | double | YF Forward PE vs. 5J-Durchschnitt in % |
| yf_fwd_pe_vs_avg_10y | double | YF Forward PE vs. 10J-Durchschnitt in % |
| yf_fwd_pe_vs_avg_15y | double | YF Forward PE vs. 15J-Durchschnitt in % |
| yf_fwd_pe_vs_avg_20y | double | YF Forward PE vs. 20J-Durchschnitt in % |
| yf_fwd_pe_vs_avg_10y_2019 | double | YF Forward PE vs. 10J-Durchschnitt (2010-2019) in % |
| ev_ebit_vs_avg_5y | double | TTM EV/EBIT vs. 5J-Durchschnitt in % |
| ev_ebit_vs_avg_10y | double | TTM EV/EBIT vs. 10J-Durchschnitt in % |
| ev_ebit_vs_avg_15y | double | TTM EV/EBIT vs. 15J-Durchschnitt in % |
| ev_ebit_vs_avg_20y | double | TTM EV/EBIT vs. 20J-Durchschnitt in % |
| ev_ebit_vs_avg_10y_2019 | double | TTM EV/EBIT vs. 10J-Durchschnitt (2010-2019) in % |
| created_at | timestamp | |
| updated_at | datetime | |

Abweichungs-Berechnung: `((aktueller_wert / durchschnitt) - 1) * 100`
- Positiv (+25%): Aktie handelt 25% teurer als historischer Durchschnitt
- Negativ (-15%): Aktie handelt 15% günstiger als historischer Durchschnitt

Quellen: `calcu_numbers` (aktuellste Zeile + 2019er Daten), `fmp_filtered_numbers` (Preise), `yfinance API` (TTM/Forward PE).

### 3.3.3 `user_watchlist`
Benutzer-spezifische Favoriten und Notizen. Update: manuell.

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| id | int, PK | Auto-Increment |
| isin | varchar(20), UNIQUE | |
| favorite | int | 0=kein, 1=halte ich, 2=top Favorit, 3=beobachte |
| notes | text | Persönliche Notizen |
| created_at | timestamp | |
| updated_at | datetime | |

## 3.4 Legacy: `eodhd_filtered_numbers`
Pivot aus `eodhd_financial_statements` (1 Zeile je ISIN/Index/Jahr) mit Kurs/AVG-Price. Strukturen siehe `03_analytics/00_create_table_eodtofiltered.py`.

---

# Beziehungen (Auszug)
- `tickerdb.tickerlist` ↔ `raw_data.yf_prices` über `isin`/`yf_ticker`.
- `raw_data.fmp_financial_statements` → `analytics.fmp_filtered_numbers` (Copy ausgewählter Felder + Preis/Market Cap).
- `analytics.fmp_filtered_numbers` → `analytics.calcu_numbers` (berechnete Kennzahlen).
- Legacy: `raw_data.eodhd_financial_statements` → `analytics.eodhd_filtered_numbers`.

## Frontend-Datenfluss
```
tickerlist + yfinance API  →  company_info      (Stammdaten)
calcu_numbers + fmp_filtered_numbers  →  live_metrics  (Kennzahlen)
tickerlist  →  user_watchlist  (ISINs für Favoriten/Notizen)
```

Website-Query (Beispiel):
```sql
SELECT ci.company_name, ci.sector, lm.pe, lm.ev_ebit, uw.favorite, uw.notes
FROM analytics.live_metrics lm
JOIN analytics.company_info ci ON lm.isin = ci.isin
JOIN analytics.user_watchlist uw ON lm.isin = uw.isin
WHERE uw.favorite > 0
ORDER BY uw.favorite, lm.pe;
```

