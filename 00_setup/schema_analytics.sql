-- MySQL dump 10.13  Distrib 8.0.45, for Linux (x86_64)
--
-- Host: localhost    Database: analytics
-- ------------------------------------------------------
-- Server version	8.0.45-0ubuntu0.24.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `analyst_estimates`
--

DROP TABLE IF EXISTS `analyst_estimates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `analyst_estimates` (
  `id` int NOT NULL AUTO_INCREMENT,
  `isin` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `period` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `period_type` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `period_end_date` date DEFAULT NULL,
  `metric` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `estimate_value` decimal(18,4) DEFAULT NULL,
  `prior_year_value` decimal(18,4) DEFAULT NULL,
  `actual_value` decimal(18,4) DEFAULT NULL,
  `currency` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `unit` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `num_analysts` int DEFAULT NULL,
  `release_date` date DEFAULT NULL,
  `source` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT 'finanzen.net',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_isin_period_metric` (`isin`,`period`,`metric`),
  KEY `idx_isin` (`isin`),
  KEY `idx_metric` (`metric`),
  KEY `idx_period_type` (`period_type`)
) ENGINE=InnoDB AUTO_INCREMENT=566595 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `calcu_numbers`
--

DROP TABLE IF EXISTS `calcu_numbers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `calcu_numbers` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) NOT NULL,
  `isin` varchar(20) NOT NULL,
  `stock_index` varchar(255) NOT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `date` date NOT NULL,
  `period` varchar(10) DEFAULT NULL,
  `fy_pe` double DEFAULT NULL COMMENT 'FY Price/Earnings = Price / EPS (Ganzjahresdaten)',
  `fy_ev_ebit` double DEFAULT NULL COMMENT 'FY EV/EBIT (Ganzjahresdaten)',
  `ebit` double DEFAULT NULL COMMENT 'Operating Income',
  `ev` double DEFAULT NULL COMMENT 'Enterprise Value = Market Cap + Net Debt + Minority Interest',
  `ttm_net_income` double DEFAULT NULL COMMENT 'TTM Net Income (Summe letzte 4 Quartale)',
  `ttm_ebit` double DEFAULT NULL COMMENT 'TTM EBIT (Summe letzte 4 Quartale)',
  `ttm_pe` double DEFAULT NULL COMMENT 'TTM Price/Earnings = Market Cap / TTM Net Income',
  `ttm_ev_ebit` double DEFAULT NULL COMMENT 'TTM EV/EBIT = EV / TTM EBIT',
  `pe_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt PE',
  `pe_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt PE',
  `pe_avg_15y` double DEFAULT NULL COMMENT '15-Jahres-Durchschnitt PE',
  `pe_avg_20y` double DEFAULT NULL COMMENT '20-Jahres-Durchschnitt PE',
  `ev_ebit_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt EV/EBIT',
  `ev_ebit_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt EV/EBIT',
  `ev_ebit_avg_15y` double DEFAULT NULL COMMENT '15-Jahres-Durchschnitt EV/EBIT',
  `ev_ebit_avg_20y` double DEFAULT NULL COMMENT '20-Jahres-Durchschnitt EV/EBIT',
  `revenue_cagr_3y` double DEFAULT NULL COMMENT 'Umsatz CAGR 3 Jahre in %',
  `revenue_cagr_5y` double DEFAULT NULL COMMENT 'Umsatz CAGR 5 Jahre in %',
  `revenue_cagr_10y` double DEFAULT NULL COMMENT 'Umsatz CAGR 10 Jahre in %',
  `ebit_cagr_3y` double DEFAULT NULL COMMENT 'EBIT CAGR 3 Jahre in %',
  `ebit_cagr_5y` double DEFAULT NULL COMMENT 'EBIT CAGR 5 Jahre in %',
  `ebit_cagr_10y` double DEFAULT NULL COMMENT 'EBIT CAGR 10 Jahre in %',
  `net_income_cagr_3y` double DEFAULT NULL COMMENT 'Gewinn CAGR 3 Jahre in %',
  `net_income_cagr_5y` double DEFAULT NULL COMMENT 'Gewinn CAGR 5 Jahre in %',
  `net_income_cagr_10y` double DEFAULT NULL COMMENT 'Gewinn CAGR 10 Jahre in %',
  `equity_ratio` double DEFAULT NULL COMMENT 'Eigenkapitalquote = Total Equity / Total Assets in %',
  `net_debt_ebitda` double DEFAULT NULL COMMENT 'Net Debt / EBITDA',
  `profit_margin` double DEFAULT NULL COMMENT 'Gewinnmarge = Net Income / Revenue in %',
  `operating_margin` double DEFAULT NULL COMMENT 'Operative Marge = Operating Income / Revenue in %',
  `profit_margin_avg_3y` double DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Gewinnmarge',
  `profit_margin_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge',
  `profit_margin_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Gewinnmarge',
  `profit_margin_avg_5y_2019` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge (2015-2019)',
  `operating_margin_avg_3y` double DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Operative Marge',
  `operating_margin_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge',
  `operating_margin_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Operative Marge',
  `operating_margin_avg_5y_2019` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge (2015-2019)',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `pe_avg_5y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 5J PE-Durchschnitt',
  `pe_avg_10y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 10J PE-Durchschnitt',
  `pe_avg_15y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 15J PE-Durchschnitt',
  `pe_avg_20y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 20J PE-Durchschnitt',
  `ev_ebit_avg_5y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 5J EV/EBIT-Durchschnitt',
  `ev_ebit_avg_10y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 10J EV/EBIT-Durchschnitt',
  `ev_ebit_avg_15y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 15J EV/EBIT-Durchschnitt',
  `ev_ebit_avg_20y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 20J EV/EBIT-Durchschnitt',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_isin_index_date_period` (`isin`,`stock_index`,`date`,`period`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_stock_index` (`stock_index`),
  KEY `idx_date` (`date`),
  KEY `idx_period` (`period`)
) ENGINE=InnoDB AUTO_INCREMENT=157615 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `company_info`
--

DROP TABLE IF EXISTS `company_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `company_info` (
  `isin` varchar(20) NOT NULL,
  `ticker` varchar(20) DEFAULT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `sector` varchar(100) DEFAULT NULL COMMENT 'Sektor (z.B. Technology)',
  `industry` varchar(255) DEFAULT NULL COMMENT 'Branche (z.B. Software)',
  `country` varchar(100) DEFAULT NULL COMMENT 'Land des Hauptsitzes',
  `currency` varchar(10) DEFAULT NULL COMMENT 'Berichtswährung',
  `description` text COMMENT 'Unternehmensbeschreibung',
  `fiscal_year_end` varchar(20) DEFAULT NULL COMMENT 'Fiskaljahr-Ende (z.B. December)',
  `stock_index` varchar(50) DEFAULT NULL COMMENT 'Index-Zugehörigkeit',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`isin`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_sector` (`sector`),
  KEY `idx_country` (`country`),
  KEY `idx_stock_index` (`stock_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Stammdaten - Update: jährlich oder bei Bedarf';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `earnings_calendar`
--

DROP TABLE IF EXISTS `earnings_calendar`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `earnings_calendar` (
  `id` int NOT NULL AUTO_INCREMENT,
  `isin` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `period` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `period_end_date` date DEFAULT NULL,
  `release_date` date DEFAULT NULL,
  `event_type` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `eps_estimate` decimal(12,4) DEFAULT NULL,
  `eps_currency` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_estimated` tinyint(1) DEFAULT '0',
  `source` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT 'finanzen.net',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_isin_period_event` (`isin`,`period`,`event_type`),
  KEY `idx_isin` (`isin`),
  KEY `idx_release_date` (`release_date`)
) ENGINE=InnoDB AUTO_INCREMENT=153443 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `eodhd_filtered_numbers`
--

DROP TABLE IF EXISTS `eodhd_filtered_numbers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `eodhd_filtered_numbers` (
  `id` int NOT NULL AUTO_INCREMENT,
  `isin` varchar(20) NOT NULL,
  `stock_index` varchar(255) NOT NULL,
  `ticker_eod` varchar(20) NOT NULL,
  `ticker_yf` varchar(20) DEFAULT NULL,
  `year` int NOT NULL,
  `fiscal_year` date DEFAULT NULL,
  `price` double DEFAULT NULL,
  `avg_price` double DEFAULT NULL,
  `TotalRevenue` double DEFAULT NULL,
  `CostOfRevenue` double DEFAULT NULL,
  `GrossProfit` double DEFAULT NULL,
  `OperatingExpense` double DEFAULT NULL,
  `SellingGeneralAndAdministration` double DEFAULT NULL,
  `SellingAndMarketingExpense` double DEFAULT NULL,
  `ResearchAndDevelopment` double DEFAULT NULL,
  `OtherOperatingExpenses` double DEFAULT NULL,
  `OperatingIncome` double DEFAULT NULL,
  `NonOperatingIncome` double DEFAULT NULL,
  `NetInterestIncome` double DEFAULT NULL,
  `InterestExpense` double DEFAULT NULL,
  `InterestIncome` double DEFAULT NULL,
  `PretaxIncome` double DEFAULT NULL,
  `TaxProvision` double DEFAULT NULL,
  `NetIncome` double DEFAULT NULL,
  `EBITDA` double DEFAULT NULL,
  `EBIT` double DEFAULT NULL,
  `NetMinorityInterest` double DEFAULT NULL,
  `CapitalExpenditure` double DEFAULT NULL,
  `ChangeInWorkingCapital` double DEFAULT NULL,
  `DepreciationAndAmortization` double DEFAULT NULL,
  `StockBasedCompensation` double DEFAULT NULL,
  `totalAssets` double DEFAULT NULL,
  `TotalDebt` double DEFAULT NULL,
  `TotalLiabilities` double DEFAULT NULL,
  `StockholdersEquity` double DEFAULT NULL,
  `NetDebt` double DEFAULT NULL,
  `CashAndEquivalents` double DEFAULT NULL,
  `CashAndShortTermInvestments` double DEFAULT NULL,
  `GoodWill` double DEFAULT NULL,
  `SharesOutstanding` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_isin_index_year` (`isin`,`stock_index`,`year`),
  KEY `idx_ticker_eod` (`ticker_eod`),
  KEY `idx_stock_index` (`stock_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `historical_fundamentals`
--

DROP TABLE IF EXISTS `historical_fundamentals`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `historical_fundamentals` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) NOT NULL,
  `isin` varchar(20) NOT NULL,
  `stock_index` varchar(255) NOT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `date` date NOT NULL,
  `period` varchar(10) DEFAULT NULL,
  `price` double DEFAULT NULL,
  `avg_price` double DEFAULT NULL,
  `market_cap` double DEFAULT NULL,
  `total_assets` double DEFAULT NULL,
  `total_liabilities` double DEFAULT NULL,
  `total_equity` double DEFAULT NULL,
  `total_stockholders_equity` double DEFAULT NULL,
  `cash_and_cash_equivalents` double DEFAULT NULL,
  `goodwill` double DEFAULT NULL,
  `short_term_debt` double DEFAULT NULL,
  `long_term_debt` double DEFAULT NULL,
  `total_debt` double DEFAULT NULL,
  `net_debt` double DEFAULT NULL,
  `minority_interest` double DEFAULT NULL,
  `revenue` double DEFAULT NULL,
  `cost_of_revenue` double DEFAULT NULL,
  `gross_profit` double DEFAULT NULL,
  `research_and_development_expenses` double DEFAULT NULL,
  `general_and_administrative_expenses` double DEFAULT NULL,
  `selling_and_marketing_expenses` double DEFAULT NULL,
  `selling_general_and_administrative_expenses` double DEFAULT NULL,
  `other_expenses` double DEFAULT NULL,
  `operating_expenses` double DEFAULT NULL,
  `cost_and_expenses` double DEFAULT NULL,
  `operating_income` double DEFAULT NULL,
  `interest_income` double DEFAULT NULL,
  `interest_expense` double DEFAULT NULL,
  `ebitda` double DEFAULT NULL,
  `total_other_income_expenses_net` double DEFAULT NULL,
  `income_before_tax` double DEFAULT NULL,
  `income_tax_expense` double DEFAULT NULL,
  `net_income` double DEFAULT NULL,
  `eps` double DEFAULT NULL,
  `eps_diluted` double DEFAULT NULL,
  `weighted_average_shs_out` double DEFAULT NULL,
  `weighted_average_shs_out_dil` double DEFAULT NULL,
  `net_income_cf` double DEFAULT NULL,
  `depreciation_and_amortization_cf` double DEFAULT NULL,
  `stock_based_compensation` double DEFAULT NULL,
  `capital_expenditure` double DEFAULT NULL,
  `free_cash_flow` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_isin_index_date_period` (`isin`,`stock_index`,`date`,`period`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_stock_index` (`stock_index`),
  KEY `idx_date` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=198578 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `live_metrics`
--

DROP TABLE IF EXISTS `live_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `live_metrics` (
  `isin` varchar(20) NOT NULL,
  `ticker` varchar(20) DEFAULT NULL,
  `company_name` varchar(255) DEFAULT NULL COMMENT 'Für schnelle JOINs',
  `stock_index` varchar(50) DEFAULT NULL,
  `price` double DEFAULT NULL COMMENT 'Aktueller Kurs',
  `price_date` date DEFAULT NULL COMMENT 'Datum des Kurses',
  `market_cap` double DEFAULT NULL COMMENT 'Marktkapitalisierung',
  `fy_pe` double DEFAULT NULL COMMENT 'FY KGV (Ganzjahresdaten)',
  `fy_ev_ebit` double DEFAULT NULL COMMENT 'FY EV/EBIT (Ganzjahresdaten)',
  `ttm_pe` double DEFAULT NULL COMMENT 'TTM KGV (letzte 4 Quartale)',
  `ttm_ev_ebit` double DEFAULT NULL COMMENT 'TTM EV/EBIT (letzte 4 Quartale)',
  `pe_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt PE',
  `pe_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt PE',
  `pe_avg_15y` double DEFAULT NULL COMMENT '15-Jahres-Durchschnitt PE',
  `pe_avg_20y` double DEFAULT NULL COMMENT '20-Jahres-Durchschnitt PE',
  `pe_avg_10y_2019` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt PE (2010-2019, fix)',
  `ev_ebit_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt EV/EBIT',
  `ev_ebit_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt EV/EBIT',
  `ev_ebit_avg_15y` double DEFAULT NULL COMMENT '15-Jahres-Durchschnitt EV/EBIT',
  `ev_ebit_avg_20y` double DEFAULT NULL COMMENT '20-Jahres-Durchschnitt EV/EBIT',
  `ev_ebit_avg_10y_2019` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt EV/EBIT (2010-2019, fix)',
  `revenue_cagr_3y` double DEFAULT NULL COMMENT 'Umsatz CAGR 3 Jahre in %',
  `revenue_cagr_5y` double DEFAULT NULL COMMENT 'Umsatz CAGR 5 Jahre in %',
  `revenue_cagr_10y` double DEFAULT NULL COMMENT 'Umsatz CAGR 10 Jahre in %',
  `ebit_cagr_3y` double DEFAULT NULL COMMENT 'EBIT CAGR 3 Jahre in %',
  `ebit_cagr_5y` double DEFAULT NULL COMMENT 'EBIT CAGR 5 Jahre in %',
  `ebit_cagr_10y` double DEFAULT NULL COMMENT 'EBIT CAGR 10 Jahre in %',
  `net_income_cagr_3y` double DEFAULT NULL COMMENT 'Gewinn CAGR 3 Jahre in %',
  `net_income_cagr_5y` double DEFAULT NULL COMMENT 'Gewinn CAGR 5 Jahre in %',
  `net_income_cagr_10y` double DEFAULT NULL COMMENT 'Gewinn CAGR 10 Jahre in %',
  `equity_ratio` double DEFAULT NULL COMMENT 'Eigenkapitalquote in %',
  `net_debt_ebitda` double DEFAULT NULL COMMENT 'Net Debt / EBITDA',
  `profit_margin` double DEFAULT NULL COMMENT 'Gewinnmarge = Net Income / Revenue in %',
  `operating_margin` double DEFAULT NULL COMMENT 'Operative Marge = Operating Income / Revenue in %',
  `profit_margin_avg_3y` double DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Gewinnmarge',
  `profit_margin_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge',
  `profit_margin_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Gewinnmarge',
  `profit_margin_avg_5y_2019` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Gewinnmarge (2015-2019)',
  `operating_margin_avg_3y` double DEFAULT NULL COMMENT '3-Jahres-Durchschnitt Operative Marge',
  `operating_margin_avg_5y` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge',
  `operating_margin_avg_10y` double DEFAULT NULL COMMENT '10-Jahres-Durchschnitt Operative Marge',
  `operating_margin_avg_5y_2019` double DEFAULT NULL COMMENT '5-Jahres-Durchschnitt Operative Marge (2015-2019)',
  `yf_ttm_pe` double DEFAULT NULL COMMENT 'TTM PE von yfinance API (nicht berechnet)',
  `yf_forward_pe` double DEFAULT NULL COMMENT 'Forward PE von yfinance API',
  `yf_payout_ratio` double DEFAULT NULL COMMENT 'Payout Ratio von yfinance API in % (35.0 = 35%)',
  `yf_profit_margin` double DEFAULT NULL COMMENT 'Profit Margin von yfinance API in % (35.0 = 35%)',
  `yf_operating_margin` double DEFAULT NULL COMMENT 'Operating Margin von yfinance API in % (35.0 = 35%)',
  `next_earnings_date` date DEFAULT NULL COMMENT 'Nächstes Earnings-Datum von yfinance API',
  `yf_ttm_pe_vs_avg_5y` double DEFAULT NULL COMMENT 'YF TTM PE vs. 5J-Durchschnitt in %',
  `yf_ttm_pe_vs_avg_10y` double DEFAULT NULL COMMENT 'YF TTM PE vs. 10J-Durchschnitt in %',
  `yf_ttm_pe_vs_avg_15y` double DEFAULT NULL COMMENT 'YF TTM PE vs. 15J-Durchschnitt in %',
  `yf_ttm_pe_vs_avg_20y` double DEFAULT NULL COMMENT 'YF TTM PE vs. 20J-Durchschnitt in %',
  `yf_ttm_pe_vs_avg_10y_2019` double DEFAULT NULL COMMENT 'YF TTM PE vs. 10J-Durchschnitt (2010-2019) in %',
  `yf_fwd_pe_vs_avg_5y` double DEFAULT NULL COMMENT 'YF Forward PE vs. 5J-Durchschnitt in %',
  `yf_fwd_pe_vs_avg_10y` double DEFAULT NULL COMMENT 'YF Forward PE vs. 10J-Durchschnitt in %',
  `yf_fwd_pe_vs_avg_15y` double DEFAULT NULL COMMENT 'YF Forward PE vs. 15J-Durchschnitt in %',
  `yf_fwd_pe_vs_avg_20y` double DEFAULT NULL COMMENT 'YF Forward PE vs. 20J-Durchschnitt in %',
  `yf_fwd_pe_vs_avg_10y_2019` double DEFAULT NULL COMMENT 'YF Forward PE vs. 10J-Durchschnitt (2010-2019) in %',
  `ev_ebit_vs_avg_5y` double DEFAULT NULL COMMENT 'TTM EV/EBIT vs. 5J-Durchschnitt in %',
  `ev_ebit_vs_avg_10y` double DEFAULT NULL COMMENT 'TTM EV/EBIT vs. 10J-Durchschnitt in %',
  `ev_ebit_vs_avg_15y` double DEFAULT NULL COMMENT 'TTM EV/EBIT vs. 15J-Durchschnitt in %',
  `ev_ebit_vs_avg_20y` double DEFAULT NULL COMMENT 'TTM EV/EBIT vs. 20J-Durchschnitt in %',
  `ev_ebit_vs_avg_10y_2019` double DEFAULT NULL COMMENT 'TTM EV/EBIT vs. 10J-Durchschnitt (2010-2019) in %',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `pe_avg_5y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 5J PE-Durchschnitt',
  `pe_avg_10y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 10J PE-Durchschnitt',
  `pe_avg_15y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 15J PE-Durchschnitt',
  `pe_avg_20y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 20J PE-Durchschnitt',
  `ev_ebit_avg_5y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 5J EV/EBIT-Durchschnitt',
  `ev_ebit_avg_10y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 10J EV/EBIT-Durchschnitt',
  `ev_ebit_avg_15y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 15J EV/EBIT-Durchschnitt',
  `ev_ebit_avg_20y_count` int DEFAULT NULL COMMENT 'Anzahl Jahre für 20J EV/EBIT-Durchschnitt',
  PRIMARY KEY (`isin`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_stock_index` (`stock_index`),
  KEY `idx_ttm_pe` (`ttm_pe`),
  KEY `idx_ttm_ev_ebit` (`ttm_ev_ebit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Live-Kennzahlen - Update: täglich';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_column_settings`
--

DROP TABLE IF EXISTS `user_column_settings`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `user_column_settings` (
  `id` int NOT NULL AUTO_INCREMENT,
  `view_name` varchar(50) NOT NULL COMMENT 'watchlist oder screener',
  `source_table` varchar(50) NOT NULL COMMENT 'live_metrics oder company_info',
  `column_key` varchar(50) NOT NULL COMMENT 'Spaltenname in der Quelltabelle',
  `display_name` varchar(100) NOT NULL COMMENT 'Anzeigename im Frontend',
  `sort_order` int DEFAULT '0' COMMENT 'Reihenfolge (niedrig = links)',
  `is_visible` tinyint(1) DEFAULT '1' COMMENT 'Spalte anzeigen?',
  `column_group` varchar(50) DEFAULT NULL COMMENT 'Gruppierung (Bewertung, Wachstum, etc.)',
  `format_type` varchar(20) DEFAULT 'number' COMMENT 'number, percent, currency, billions, text',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `user_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_user_view_column` (`user_id`,`view_name`,`column_key`),
  KEY `idx_view_visible` (`view_name`,`is_visible`,`sort_order`),
  CONSTRAINT `fk_column_settings_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1214 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Spalten-Konfiguration für Watchlist und Screener';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_dcf_scenarios`
--

DROP TABLE IF EXISTS `user_dcf_scenarios`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `user_dcf_scenarios` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `isin` varchar(20) NOT NULL,
  `scenario_name` varchar(100) DEFAULT 'Standard',
  `forecast_years` int DEFAULT '5',
  `revenue_growth_y1` double DEFAULT NULL,
  `revenue_growth_y2` double DEFAULT NULL,
  `revenue_growth_y3` double DEFAULT NULL,
  `revenue_growth_y4` double DEFAULT NULL,
  `revenue_growth_y5` double DEFAULT NULL,
  `revenue_growth_y10` double DEFAULT NULL,
  `revenue_growth_y9` double DEFAULT NULL,
  `revenue_growth_y8` double DEFAULT NULL,
  `revenue_growth_y7` double DEFAULT NULL,
  `revenue_growth_y6` double DEFAULT NULL,
  `ebit_margin` double DEFAULT NULL COMMENT 'EBIT-Marge in Prozent',
  `tax_rate` double DEFAULT '25' COMMENT 'Steuersatz in Prozent',
  `capex_percent` double DEFAULT '3' COMMENT 'CapEx als % vom Umsatz',
  `wc_change_percent` double DEFAULT '0' COMMENT 'Working Capital Veränderung als % vom Umsatz',
  `depreciation_percent` double DEFAULT '3' COMMENT 'Abschreibungen als % vom Umsatz',
  `terminal_growth` double DEFAULT '2' COMMENT 'Ewige Wachstumsrate in Prozent',
  `wacc` double DEFAULT '9' COMMENT 'WACC in Prozent',
  `fair_value_per_share` double DEFAULT NULL COMMENT 'Berechneter Fair Value je Aktie',
  `enterprise_value` double DEFAULT NULL COMMENT 'Berechneter Enterprise Value',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_user_isin` (`user_id`,`isin`),
  KEY `idx_user_scenarios` (`user_id`),
  CONSTRAINT `user_dcf_scenarios_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='User DCF valuation scenarios';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_favorite_filter`
--

DROP TABLE IF EXISTS `user_favorite_filter`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `user_favorite_filter` (
  `favorite_id` int NOT NULL COMMENT '1-9',
  `is_visible` tinyint(1) DEFAULT '1' COMMENT 'Sichtbar in Watchlist?',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `user_id` int NOT NULL,
  PRIMARY KEY (`user_id`,`favorite_id`),
  CONSTRAINT `fk_favorite_filter_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Filter: Welche Favoriten in der Watchlist angezeigt werden';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_favorite_labels`
--

DROP TABLE IF EXISTS `user_favorite_labels`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `user_favorite_labels` (
  `favorite_id` int NOT NULL COMMENT '1-9',
  `label` varchar(50) NOT NULL COMMENT 'Benutzerdefinierter Name',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `user_id` int NOT NULL,
  PRIMARY KEY (`user_id`,`favorite_id`),
  CONSTRAINT `fk_favorite_labels_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Benutzerdefinierte Namen für Favoriten-Stufen 1-9';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_watchlist`
--

DROP TABLE IF EXISTS `user_watchlist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `user_watchlist` (
  `id` int NOT NULL AUTO_INCREMENT,
  `isin` varchar(20) NOT NULL,
  `favorite` int DEFAULT '0' COMMENT '0=kein Favorit, 1-9=Favoriten-Stufen',
  `notes` text COMMENT 'Persönliche Notizen',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `user_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_user_isin` (`user_id`,`isin`),
  KEY `idx_favorite` (`favorite`),
  CONSTRAINT `fk_watchlist_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1461 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='User Watchlist - Update: manuell';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email` varchar(255) NOT NULL,
  `password_hash` varchar(255) NOT NULL COMMENT 'bcrypt hash',
  `first_name` varchar(100) NOT NULL,
  `last_name` varchar(100) NOT NULL,
  `role` enum('admin','user') DEFAULT 'user',
  `is_approved` tinyint(1) DEFAULT '0' COMMENT 'Admin approval required',
  `is_active` tinyint(1) DEFAULT '1' COMMENT 'Account enabled/disabled',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `last_login` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`),
  KEY `idx_email` (`email`),
  KEY `idx_role` (`role`),
  KEY `idx_approved` (`is_approved`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='User authentication and authorization';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-02-10 13:44:57
