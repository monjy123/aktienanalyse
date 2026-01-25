#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die user_dcf_scenarios Tabelle für DCF-Bewertungen.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# =============================================================================
# Tabelle: user_dcf_scenarios (DCF-Bewertungsszenarien)
# =============================================================================
CREATE_DCF_SCENARIOS_TABLE = """
CREATE TABLE IF NOT EXISTS analytics.user_dcf_scenarios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    isin VARCHAR(20) NOT NULL,
    scenario_name VARCHAR(100) DEFAULT 'Standard',

    -- Prognose-Zeitraum
    forecast_years INT DEFAULT 5,

    -- Umsatzwachstum pro Jahr (%)
    revenue_growth_y1 DOUBLE,
    revenue_growth_y2 DOUBLE,
    revenue_growth_y3 DOUBLE,
    revenue_growth_y4 DOUBLE,
    revenue_growth_y5 DOUBLE,
    revenue_growth_y6 DOUBLE,
    revenue_growth_y7 DOUBLE,
    revenue_growth_y8 DOUBLE,
    revenue_growth_y9 DOUBLE,
    revenue_growth_y10 DOUBLE,

    -- Operative Annahmen (%)
    ebit_margin DOUBLE COMMENT 'EBIT-Marge in Prozent',
    tax_rate DOUBLE DEFAULT 25.0 COMMENT 'Steuersatz in Prozent',
    capex_percent DOUBLE DEFAULT 3.0 COMMENT 'CapEx als % vom Umsatz',
    wc_change_percent DOUBLE DEFAULT 0.0 COMMENT 'Working Capital Veränderung als % vom Umsatz',
    depreciation_percent DOUBLE DEFAULT 3.0 COMMENT 'Abschreibungen als % vom Umsatz',

    -- Terminal Value Annahmen
    terminal_growth DOUBLE DEFAULT 2.0 COMMENT 'Ewige Wachstumsrate in Prozent',
    wacc DOUBLE DEFAULT 9.0 COMMENT 'WACC in Prozent',

    -- Gecachte Ergebnisse
    fair_value_per_share DOUBLE COMMENT 'Berechneter Fair Value je Aktie',
    enterprise_value DOUBLE COMMENT 'Berechneter Enterprise Value',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_user_isin (user_id, isin),
    INDEX idx_user_scenarios (user_id),

    FOREIGN KEY (user_id) REFERENCES analytics.users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='User DCF valuation scenarios';
"""


def create_dcf_scenarios_table():
    """Erstellt die user_dcf_scenarios Tabelle."""
    try:
        conn = get_connection('analytics')
        cur = conn.cursor()

        print("Erstelle user_dcf_scenarios Tabelle...")
        cur.execute(CREATE_DCF_SCENARIOS_TABLE)
        print("✓ user_dcf_scenarios Tabelle erstellt")

        # Migration: Füge neue Wachstumsfelder hinzu falls sie fehlen
        migration_columns = [
            ("revenue_growth_y6", "DOUBLE"),
            ("revenue_growth_y7", "DOUBLE"),
            ("revenue_growth_y8", "DOUBLE"),
            ("revenue_growth_y9", "DOUBLE"),
            ("revenue_growth_y10", "DOUBLE"),
        ]

        for col_name, col_type in migration_columns:
            try:
                cur.execute(f"""
                    ALTER TABLE analytics.user_dcf_scenarios
                    ADD COLUMN {col_name} {col_type} AFTER revenue_growth_y5
                """)
                print(f"✓ Spalte {col_name} hinzugefügt")
            except Exception:
                # Spalte existiert bereits
                pass

        conn.commit()
        cur.close()
        conn.close()

        print("\n✓ DCF-Tabelle erfolgreich erstellt/aktualisiert!")

    except Error as e:
        print(f"✗ Fehler beim Erstellen der DCF-Tabelle: {e}")
        sys.exit(1)


if __name__ == "__main__":
    create_dcf_scenarios_table()
