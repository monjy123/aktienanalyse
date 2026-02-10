-- MySQL dump 10.13  Distrib 8.0.45, for Linux (x86_64)
--
-- Host: localhost    Database: raw_data
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
-- Table structure for table `fmp_economic_indicators`
--

DROP TABLE IF EXISTS `fmp_economic_indicators`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_economic_indicators` (
  `id` int NOT NULL AUTO_INCREMENT,
  `indicator_name` varchar(100) NOT NULL,
  `date` date NOT NULL,
  `value` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`indicator_name`,`date`),
  KEY `idx_indicator` (`indicator_name`),
  KEY `idx_date` (`date`),
  KEY `idx_indicator_date` (`indicator_name`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=25102 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_financial_statements`
--

DROP TABLE IF EXISTS `fmp_financial_statements`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_financial_statements` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) NOT NULL,
  `isin` varchar(12) DEFAULT NULL,
  `stock_index` varchar(50) DEFAULT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `date` date NOT NULL,
  `period` varchar(10) DEFAULT NULL,
  `calendar_year` int DEFAULT NULL,
  `total_assets` double DEFAULT NULL,
  `total_liabilities` double DEFAULT NULL,
  `total_equity` double DEFAULT NULL,
  `total_stockholders_equity` double DEFAULT NULL,
  `total_current_assets` double DEFAULT NULL,
  `total_current_liabilities` double DEFAULT NULL,
  `total_non_current_assets` double DEFAULT NULL,
  `total_non_current_liabilities` double DEFAULT NULL,
  `cash_and_cash_equivalents` double DEFAULT NULL,
  `cash_and_short_term_investments` double DEFAULT NULL,
  `short_term_investments` double DEFAULT NULL,
  `net_receivables` double DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `other_current_assets` double DEFAULT NULL,
  `property_plant_equipment_net` double DEFAULT NULL,
  `goodwill` double DEFAULT NULL,
  `intangible_assets` double DEFAULT NULL,
  `goodwill_and_intangible_assets` double DEFAULT NULL,
  `long_term_investments` double DEFAULT NULL,
  `other_non_current_assets` double DEFAULT NULL,
  `accounts_payables` double DEFAULT NULL,
  `short_term_debt` double DEFAULT NULL,
  `deferred_revenue` double DEFAULT NULL,
  `other_current_liabilities` double DEFAULT NULL,
  `long_term_debt` double DEFAULT NULL,
  `deferred_revenue_non_current` double DEFAULT NULL,
  `deferred_tax_liabilities_non_current` double DEFAULT NULL,
  `other_non_current_liabilities` double DEFAULT NULL,
  `total_debt` double DEFAULT NULL,
  `net_debt` double DEFAULT NULL,
  `capital_lease_obligations` double DEFAULT NULL,
  `common_stock` double DEFAULT NULL,
  `retained_earnings` double DEFAULT NULL,
  `accumulated_other_comprehensive_income_loss` double DEFAULT NULL,
  `other_total_stockholders_equity` double DEFAULT NULL,
  `total_liabilities_and_stockholders_equity` double DEFAULT NULL,
  `minority_interest` double DEFAULT NULL,
  `total_investments` double DEFAULT NULL,
  `tax_assets` double DEFAULT NULL,
  `tax_payables` double DEFAULT NULL,
  `preferred_stock` double DEFAULT NULL,
  `treasury_stock` double DEFAULT NULL,
  `revenue` double DEFAULT NULL,
  `cost_of_revenue` double DEFAULT NULL,
  `gross_profit` double DEFAULT NULL,
  `gross_profit_ratio` double DEFAULT NULL,
  `research_and_development_expenses` double DEFAULT NULL,
  `general_and_administrative_expenses` double DEFAULT NULL,
  `selling_and_marketing_expenses` double DEFAULT NULL,
  `selling_general_and_administrative_expenses` double DEFAULT NULL,
  `other_expenses` double DEFAULT NULL,
  `operating_expenses` double DEFAULT NULL,
  `cost_and_expenses` double DEFAULT NULL,
  `operating_income` double DEFAULT NULL,
  `operating_income_ratio` double DEFAULT NULL,
  `interest_income` double DEFAULT NULL,
  `interest_expense` double DEFAULT NULL,
  `depreciation_and_amortization` double DEFAULT NULL,
  `ebitda` double DEFAULT NULL,
  `ebitda_ratio` double DEFAULT NULL,
  `total_other_income_expenses_net` double DEFAULT NULL,
  `income_before_tax` double DEFAULT NULL,
  `income_before_tax_ratio` double DEFAULT NULL,
  `income_tax_expense` double DEFAULT NULL,
  `net_income` double DEFAULT NULL,
  `net_income_ratio` double DEFAULT NULL,
  `eps` double DEFAULT NULL,
  `eps_diluted` double DEFAULT NULL,
  `weighted_average_shs_out` double DEFAULT NULL,
  `weighted_average_shs_out_dil` double DEFAULT NULL,
  `net_income_cf` double DEFAULT NULL,
  `depreciation_and_amortization_cf` double DEFAULT NULL,
  `deferred_income_tax` double DEFAULT NULL,
  `stock_based_compensation` double DEFAULT NULL,
  `change_in_working_capital` double DEFAULT NULL,
  `accounts_receivables` double DEFAULT NULL,
  `inventory_cf` double DEFAULT NULL,
  `accounts_payables_cf` double DEFAULT NULL,
  `other_working_capital` double DEFAULT NULL,
  `other_non_cash_items` double DEFAULT NULL,
  `net_cash_provided_by_operating_activities` double DEFAULT NULL,
  `investments_in_property_plant_and_equipment` double DEFAULT NULL,
  `acquisitions_net` double DEFAULT NULL,
  `purchases_of_investments` double DEFAULT NULL,
  `sales_maturities_of_investments` double DEFAULT NULL,
  `other_investing_activities` double DEFAULT NULL,
  `net_cash_used_for_investing_activities` double DEFAULT NULL,
  `debt_repayment` double DEFAULT NULL,
  `common_stock_issued` double DEFAULT NULL,
  `common_stock_repurchased` double DEFAULT NULL,
  `dividends_paid` double DEFAULT NULL,
  `other_financing_activities` double DEFAULT NULL,
  `net_cash_used_provided_by_financing_activities` double DEFAULT NULL,
  `effect_of_forex_changes_on_cash` double DEFAULT NULL,
  `net_change_in_cash` double DEFAULT NULL,
  `cash_at_end_of_period` double DEFAULT NULL,
  `cash_at_beginning_of_period` double DEFAULT NULL,
  `operating_cash_flow` double DEFAULT NULL,
  `capital_expenditure` double DEFAULT NULL,
  `free_cash_flow` double DEFAULT NULL,
  `filing_date` date DEFAULT NULL,
  `accepted_date` datetime DEFAULT NULL,
  `cik` varchar(20) DEFAULT NULL,
  `link` varchar(500) DEFAULT NULL,
  `final_link` varchar(500) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`ticker`,`date`,`period`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_stock_index` (`stock_index`),
  KEY `idx_date` (`date`),
  KEY `idx_ticker_date` (`ticker`,`date`),
  KEY `idx_calendar_year` (`calendar_year`)
) ENGINE=InnoDB AUTO_INCREMENT=171447 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_historical_market_cap`
--

DROP TABLE IF EXISTS `fmp_historical_market_cap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_historical_market_cap` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) NOT NULL,
  `isin` varchar(12) DEFAULT NULL,
  `stock_index` varchar(50) DEFAULT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `date` date NOT NULL,
  `market_cap` bigint DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`ticker`,`date`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_stock_index` (`stock_index`),
  KEY `idx_date` (`date`),
  KEY `idx_ticker_date` (`ticker`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=6881707 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_historical_sector_pe`
--

DROP TABLE IF EXISTS `fmp_historical_sector_pe`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_historical_sector_pe` (
  `id` int NOT NULL AUTO_INCREMENT,
  `sector` varchar(50) NOT NULL,
  `exchange` varchar(20) NOT NULL,
  `date` date NOT NULL,
  `pe` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`sector`,`exchange`,`date`),
  KEY `idx_sector` (`sector`),
  KEY `idx_exchange` (`exchange`),
  KEY `idx_date` (`date`),
  KEY `idx_sector_exchange` (`sector`,`exchange`),
  KEY `idx_sector_date` (`sector`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=134277 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_historical_sector_performance`
--

DROP TABLE IF EXISTS `fmp_historical_sector_performance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_historical_sector_performance` (
  `id` int NOT NULL AUTO_INCREMENT,
  `sector` varchar(50) NOT NULL,
  `exchange` varchar(20) NOT NULL,
  `date` date NOT NULL,
  `average_change` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`sector`,`exchange`,`date`),
  KEY `idx_sector` (`sector`),
  KEY `idx_exchange` (`exchange`),
  KEY `idx_date` (`date`),
  KEY `idx_sector_exchange` (`sector`,`exchange`),
  KEY `idx_sector_date` (`sector`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=134277 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_revenue_geographic_segments`
--

DROP TABLE IF EXISTS `fmp_revenue_geographic_segments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_revenue_geographic_segments` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) NOT NULL,
  `isin` varchar(12) DEFAULT NULL,
  `stock_index` varchar(50) DEFAULT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `date` date NOT NULL,
  `fiscal_year` int DEFAULT NULL,
  `period` varchar(10) DEFAULT NULL,
  `reported_currency` varchar(10) DEFAULT NULL,
  `segment_name` varchar(255) NOT NULL,
  `revenue` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`ticker`,`date`,`period`,`segment_name`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_stock_index` (`stock_index`),
  KEY `idx_date` (`date`),
  KEY `idx_period` (`period`),
  KEY `idx_segment_name` (`segment_name`),
  KEY `idx_ticker_date` (`ticker`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=49072 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_revenue_product_segments`
--

DROP TABLE IF EXISTS `fmp_revenue_product_segments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_revenue_product_segments` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) NOT NULL,
  `isin` varchar(12) DEFAULT NULL,
  `stock_index` varchar(50) DEFAULT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `date` date NOT NULL,
  `fiscal_year` int DEFAULT NULL,
  `period` varchar(10) DEFAULT NULL,
  `reported_currency` varchar(10) DEFAULT NULL,
  `segment_name` varchar(255) NOT NULL,
  `revenue` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`ticker`,`date`,`period`,`segment_name`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_stock_index` (`stock_index`),
  KEY `idx_date` (`date`),
  KEY `idx_period` (`period`),
  KEY `idx_segment_name` (`segment_name`),
  KEY `idx_ticker_date` (`ticker`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=94026 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_ticker_mapping`
--

DROP TABLE IF EXISTS `fmp_ticker_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_ticker_mapping` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) NOT NULL,
  `isin` varchar(12) DEFAULT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `stock_index` varchar(50) DEFAULT NULL,
  `exchange` varchar(100) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_ticker_isin` (`ticker`,`isin`),
  KEY `idx_ticker` (`ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_stock_index` (`stock_index`)
) ENGINE=InnoDB AUTO_INCREMENT=3041 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fmp_treasury_rates`
--

DROP TABLE IF EXISTS `fmp_treasury_rates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fmp_treasury_rates` (
  `id` int NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `month1` double DEFAULT NULL,
  `month2` double DEFAULT NULL,
  `month3` double DEFAULT NULL,
  `month6` double DEFAULT NULL,
  `year1` double DEFAULT NULL,
  `year2` double DEFAULT NULL,
  `year3` double DEFAULT NULL,
  `year5` double DEFAULT NULL,
  `year7` double DEFAULT NULL,
  `year10` double DEFAULT NULL,
  `year20` double DEFAULT NULL,
  `year30` double DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_date` (`date`),
  KEY `idx_date` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=9007 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ticker_history`
--

DROP TABLE IF EXISTS `ticker_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ticker_history` (
  `id` int NOT NULL AUTO_INCREMENT,
  `isin` varchar(20) NOT NULL COMMENT 'ISIN des Unternehmens (bleibt konstant)',
  `old_yf_ticker` varchar(20) NOT NULL COMMENT 'Alter yfinance Ticker',
  `new_yf_ticker` varchar(20) DEFAULT NULL COMMENT 'Neuer yfinance Ticker (NULL wenn delisted)',
  `change_date` date NOT NULL COMMENT 'Datum der Änderung',
  `valid_from` date DEFAULT NULL COMMENT 'Ab wann war der alte Ticker gültig',
  `valid_until` date DEFAULT NULL COMMENT 'Bis wann war der alte Ticker gültig',
  `change_type` enum('merger','acquisition','rename','ticker_change','spinoff','delisting','other') NOT NULL DEFAULT 'other',
  `notes` text COMMENT 'Beschreibung der Änderung (z.B. "Fusion mit Firmenich")',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_isin_old_ticker` (`isin`,`old_yf_ticker`),
  KEY `idx_isin` (`isin`),
  KEY `idx_old_ticker` (`old_yf_ticker`),
  KEY `idx_new_ticker` (`new_yf_ticker`),
  KEY `idx_change_date` (`change_date`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Historische Ticker-Zuordnungen für Fusionen, Umbenennungen, etc.';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `yf_prices`
--

DROP TABLE IF EXISTS `yf_prices`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `yf_prices` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `isin` varchar(20) NOT NULL,
  `ticker_yf` varchar(20) NOT NULL,
  `date` date NOT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `adj_close` double DEFAULT NULL,
  `volume` bigint DEFAULT NULL,
  `stock_index` varchar(50) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_isin_ticker_date` (`isin`,`ticker_yf`,`date`),
  KEY `idx_date` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=329348438 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-02-10 13:44:44
