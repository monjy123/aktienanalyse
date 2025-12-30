#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erweitert analytics.live_metrics um prozentuale Abweichungen:
- YF TTM PE vs. historische PE-Durchschnitte
- YF Forward PE vs. historische PE-Durchschnitte
- TTM EV/EBIT vs. historische EV/EBIT-Durchschnitte

Formel: ((aktueller_wert / durchschnitt) - 1) * 100
Beispiel: TTM PE = 25, Ø10J PE = 20 -> +25% (teurer als historischer Schnitt)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


ALTER_STATEMENTS = [
    # === YF TTM PE vs. historische Durchschnitte ===
    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_ttm_pe_vs_avg_5y DOUBLE
       COMMENT 'YF TTM PE vs. 5J-Durchschnitt in %' AFTER yf_operating_margin""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_ttm_pe_vs_avg_10y DOUBLE
       COMMENT 'YF TTM PE vs. 10J-Durchschnitt in %' AFTER yf_ttm_pe_vs_avg_5y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_ttm_pe_vs_avg_15y DOUBLE
       COMMENT 'YF TTM PE vs. 15J-Durchschnitt in %' AFTER yf_ttm_pe_vs_avg_10y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_ttm_pe_vs_avg_20y DOUBLE
       COMMENT 'YF TTM PE vs. 20J-Durchschnitt in %' AFTER yf_ttm_pe_vs_avg_15y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_ttm_pe_vs_avg_10y_2019 DOUBLE
       COMMENT 'YF TTM PE vs. 10J-Durchschnitt (2010-2019) in %' AFTER yf_ttm_pe_vs_avg_20y""",

    # === YF Forward PE vs. historische Durchschnitte ===
    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_fwd_pe_vs_avg_5y DOUBLE
       COMMENT 'YF Forward PE vs. 5J-Durchschnitt in %' AFTER yf_ttm_pe_vs_avg_10y_2019""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_fwd_pe_vs_avg_10y DOUBLE
       COMMENT 'YF Forward PE vs. 10J-Durchschnitt in %' AFTER yf_fwd_pe_vs_avg_5y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_fwd_pe_vs_avg_15y DOUBLE
       COMMENT 'YF Forward PE vs. 15J-Durchschnitt in %' AFTER yf_fwd_pe_vs_avg_10y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_fwd_pe_vs_avg_20y DOUBLE
       COMMENT 'YF Forward PE vs. 20J-Durchschnitt in %' AFTER yf_fwd_pe_vs_avg_15y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN yf_fwd_pe_vs_avg_10y_2019 DOUBLE
       COMMENT 'YF Forward PE vs. 10J-Durchschnitt (2010-2019) in %' AFTER yf_fwd_pe_vs_avg_20y""",

    # === TTM EV/EBIT vs. historische Durchschnitte ===
    """ALTER TABLE analytics.live_metrics
       ADD COLUMN ev_ebit_vs_avg_5y DOUBLE
       COMMENT 'TTM EV/EBIT vs. 5J-Durchschnitt in %' AFTER yf_fwd_pe_vs_avg_10y_2019""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN ev_ebit_vs_avg_10y DOUBLE
       COMMENT 'TTM EV/EBIT vs. 10J-Durchschnitt in %' AFTER ev_ebit_vs_avg_5y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN ev_ebit_vs_avg_15y DOUBLE
       COMMENT 'TTM EV/EBIT vs. 15J-Durchschnitt in %' AFTER ev_ebit_vs_avg_10y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN ev_ebit_vs_avg_20y DOUBLE
       COMMENT 'TTM EV/EBIT vs. 20J-Durchschnitt in %' AFTER ev_ebit_vs_avg_15y""",

    """ALTER TABLE analytics.live_metrics
       ADD COLUMN ev_ebit_vs_avg_10y_2019 DOUBLE
       COMMENT 'TTM EV/EBIT vs. 10J-Durchschnitt (2010-2019) in %' AFTER ev_ebit_vs_avg_20y""",
]


def main():
    conn = None
    cur = None

    try:
        print("=" * 60)
        print("LIVE_METRICS ERWEITERN: Prozentuale Abweichungen")
        print("=" * 60)

        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        success = 0
        skipped = 0

        for stmt in ALTER_STATEMENTS:
            # Spaltenname extrahieren
            col_name = stmt.split("ADD COLUMN")[1].split()[0].strip()

            try:
                cur.execute(stmt)
                print(f"  + {col_name} hinzugefuegt")
                success += 1
            except Error as e:
                if "Duplicate column name" in str(e):
                    print(f"  - {col_name} existiert bereits")
                    skipped += 1
                else:
                    print(f"  ! Fehler bei {col_name}: {e}")

        conn.commit()

        print("\n" + "=" * 60)
        print("FERTIG")
        print("=" * 60)
        print(f"\nNeu hinzugefuegt: {success}")
        print(f"Bereits vorhanden: {skipped}")

        print("""
Neue Spalten:
-------------
PE-Abweichungen (YF TTM PE vs. Durchschnitte):
  - yf_ttm_pe_vs_avg_5y, _10y, _15y, _20y, _10y_2019

PE-Abweichungen (YF Forward PE vs. Durchschnitte):
  - yf_fwd_pe_vs_avg_5y, _10y, _15y, _20y, _10y_2019

EV/EBIT-Abweichungen (TTM EV/EBIT vs. Durchschnitte):
  - ev_ebit_vs_avg_5y, _10y, _15y, _20y, _10y_2019

Interpretation:
  +25% = 25% teurer als historischer Durchschnitt
  -15% = 15% guenstiger als historischer Durchschnitt
""")

    except Error as e:
        print(f"\nDatenbankfehler: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
