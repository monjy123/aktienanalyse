-- Migration: KI-Extraktor liefert zusätzlich Berichtsmetadaten
-- Stand 2026-05-11
--
-- Neue Spalte für Anzahl ausstehender Aktien (point-in-time, nicht weighted average).
-- Idempotent: prüft via information_schema ob Spalte schon existiert (MySQL 8.0 kennt
-- "ADD COLUMN IF NOT EXISTS" erst ab 8.0.29 — der hier nicht — daher Workaround).

USE analytics;

SET @col_exists := (
  SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'ki_extracted_fundamentals'
    AND COLUMN_NAME = 'shares_outstanding_eop'
);

SET @sql := IF(@col_exists = 0,
  "ALTER TABLE ki_extracted_fundamentals
     ADD COLUMN shares_outstanding_eop DOUBLE DEFAULT NULL
       COMMENT 'Anzahl ausstehender Aktien zum Periodenende (point-in-time)'
       AFTER weighted_average_shs_out_dil",
  "SELECT 'shares_outstanding_eop existiert bereits' AS info"
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
