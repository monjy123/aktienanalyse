"""
Erstellt die Tabellen für finanzen.net Daten:
- earnings_calendar: Veröffentlichungstermine für Quartalszahlen
- analyst_estimates: Analystenschätzungen (EPS, Umsatz, EBIT, etc.)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import get_connection
from mysql.connector import Error as MySQLError


# Tabelle 1: Earnings Calendar (Termine)
CREATE_EARNINGS_CALENDAR = """
CREATE TABLE IF NOT EXISTS `earnings_calendar` (
    id INT AUTO_INCREMENT PRIMARY KEY,

    isin VARCHAR(32) NOT NULL,

    -- Periode (z.B. "Q1 2026", "FY 2026")
    period VARCHAR(32) NOT NULL,
    period_end_date DATE,              -- Ende der Berichtsperiode (z.B. 31.03.2026)

    -- Termin
    release_date DATE,                 -- Veröffentlichungsdatum
    event_type VARCHAR(32),            -- 'earnings', 'hauptversammlung', 'dividende'

    -- EPS-Schätzung für diesen Termin
    eps_estimate DECIMAL(12,4),
    eps_currency VARCHAR(8),

    -- Metadaten
    is_estimated BOOLEAN DEFAULT FALSE, -- (e)* markiert
    source VARCHAR(32) DEFAULT 'finanzen.net',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_isin_period_event (isin, period, event_type),
    INDEX idx_isin (isin),
    INDEX idx_release_date (release_date)
);
"""


# Tabelle 2: Analyst Estimates (Schätzungen)
CREATE_ANALYST_ESTIMATES = """
CREATE TABLE IF NOT EXISTS `analyst_estimates` (
    id INT AUTO_INCREMENT PRIMARY KEY,

    isin VARCHAR(32) NOT NULL,

    -- Periode
    period VARCHAR(32) NOT NULL,       -- "Q1 2026", "Q2 2026", "FY 2026", "FY 2027"
    period_type VARCHAR(16) NOT NULL,  -- 'quarter' oder 'fiscal_year'
    period_end_date DATE,              -- Ende der Periode

    -- Kennzahl
    metric VARCHAR(32) NOT NULL,       -- 'eps', 'revenue', 'ebit', 'ebitda', 'dividend', 'net_income', etc.

    -- Werte
    estimate_value DECIMAL(18,4),      -- Schätzwert
    prior_year_value DECIMAL(18,4),    -- Vorjahreswert
    actual_value DECIMAL(18,4),        -- Tatsächlicher Wert (falls veröffentlicht)

    currency VARCHAR(8),               -- EUR, USD, JPY, etc.
    unit VARCHAR(16),                  -- 'per_share', 'millions', etc.

    -- Analysten-Info
    num_analysts INT,                  -- Anzahl der Analysten

    -- Metadaten
    release_date DATE,                 -- Veröffentlichungsdatum
    source VARCHAR(32) DEFAULT 'finanzen.net',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_isin_period_metric (isin, period, metric),
    INDEX idx_isin (isin),
    INDEX idx_metric (metric),
    INDEX idx_period_type (period_type)
);
"""


def main():
    try:
        con = get_connection(db_name="analytics")
        cur = con.cursor()

        # Earnings Calendar
        cur.execute(CREATE_EARNINGS_CALENDAR)
        print("Tabelle 'earnings_calendar' erstellt.")

        # Analyst Estimates
        cur.execute(CREATE_ANALYST_ESTIMATES)
        print("Tabelle 'analyst_estimates' erstellt.")

        con.commit()
        cur.close()
        con.close()

        print("\nTabellen erfolgreich erstellt!")

    except MySQLError as e:
        print(f"Fehler: {e}")


if __name__ == "__main__":
    main()
