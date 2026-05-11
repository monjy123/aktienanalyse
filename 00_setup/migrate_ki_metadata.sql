-- Migration: KI-Extraktor liefert zusätzlich Berichtsmetadaten
-- Stand 2026-05-11
--
-- Neue Spalte für Anzahl ausstehender Aktien (point-in-time, nicht weighted average)

USE analytics;

ALTER TABLE ki_extracted_fundamentals
  ADD COLUMN IF NOT EXISTS shares_outstanding_eop DOUBLE DEFAULT NULL
  COMMENT 'Anzahl ausstehender Aktien zum Periodenende (point-in-time)'
  AFTER weighted_average_shs_out_dil;
