#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die Tabelle raw_data.ticker_history für historische Ticker-Zuordnungen.

Diese Tabelle ermöglicht es, historische Preisdaten von alten Tickern zu laden,
wenn ein Unternehmen fusioniert wurde, umbenannt wurde oder den Ticker gewechselt hat.

Beispiele:
- DSM.AS → DSFIR.AS (Fusion: DSM + Firmenich)
- FB → META (Umbenennung: Facebook → Meta)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_data.ticker_history (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Identifikation
    isin VARCHAR(20) NOT NULL COMMENT 'ISIN des Unternehmens (bleibt konstant)',
    old_yf_ticker VARCHAR(20) NOT NULL COMMENT 'Alter yfinance Ticker',
    new_yf_ticker VARCHAR(20) COMMENT 'Neuer yfinance Ticker (NULL wenn delisted)',

    -- Zeitliche Information
    change_date DATE NOT NULL COMMENT 'Datum der Änderung',
    valid_from DATE COMMENT 'Ab wann war der alte Ticker gültig',
    valid_until DATE COMMENT 'Bis wann war der alte Ticker gültig',

    -- Art der Änderung
    change_type ENUM(
        'merger',           -- Fusion mit anderem Unternehmen
        'acquisition',      -- Übernahme durch anderes Unternehmen
        'rename',           -- Umbenennung
        'ticker_change',    -- Ticker-Wechsel ohne Namensänderung
        'spinoff',          -- Abspaltung
        'delisting',        -- Börsenrückzug
        'other'             -- Sonstiges
    ) NOT NULL DEFAULT 'other',

    -- Beschreibung
    notes TEXT COMMENT 'Beschreibung der Änderung (z.B. "Fusion mit Firmenich")',

    -- Metadaten
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Indizes
    KEY idx_isin (isin),
    KEY idx_old_ticker (old_yf_ticker),
    KEY idx_new_ticker (new_yf_ticker),
    KEY idx_change_date (change_date),
    UNIQUE KEY uq_isin_old_ticker (isin, old_yf_ticker)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='Historische Ticker-Zuordnungen für Fusionen, Umbenennungen, etc.';
"""


def main():
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(db_name="raw_data", autocommit=False)
        cur = conn.cursor()

        print("Erstelle Tabelle raw_data.ticker_history...")
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        print("✅ Tabelle erfolgreich erstellt!")

        # Zeige Tabellenstruktur
        cur.execute("DESCRIBE raw_data.ticker_history")
        print("\nTabellenstruktur:")
        print(f"{'Field':<20} {'Type':<30} {'Null':<6} {'Key':<6} {'Default':<15}")
        print("-" * 85)
        for row in cur.fetchall():
            print(f"{row[0]:<20} {row[1]:<30} {row[2]:<6} {row[3]:<6} {str(row[4] or ''):<15}")

    except Error as e:
        print(f"❌ Datenbankfehler: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
