#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Befüllt raw_data.ticker_history mit bekannten Ticker-Änderungen.

Quellen:
- Manuelle Recherche
- Bekannte Fälle aus der Analyse (kritische Fälle mit wenig Price-Daten)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection

# Bekannte Ticker-Änderungen
TICKER_CHANGES = [
    # DSM Firmenich (Fusion Mai 2023)
    {
        'isin': 'CH1216478797',
        'old_yf_ticker': 'DSM.AS',
        'new_yf_ticker': 'DSFIR.AS',
        'change_date': '2023-05-08',
        'valid_from': '1989-11-15',  # DSM IPO
        'valid_until': '2023-05-07',
        'change_type': 'merger',
        'notes': 'Fusion von Royal DSM (DSM.AS) mit Firmenich zu DSM-Firmenich AG'
    },

    # Stellantis (Fusion Januar 2021: PSA + FCA)
    {
        'isin': 'NL00150001Q9',
        'old_yf_ticker': 'PEUGY',  # PSA (Peugeot)
        'new_yf_ticker': 'STLA',
        'change_date': '2021-01-18',
        'valid_from': '2014-06-02',
        'valid_until': '2021-01-17',
        'change_type': 'merger',
        'notes': 'Fusion von PSA (Peugeot) mit Fiat Chrysler (FCA) zu Stellantis'
    },

    # Meta Platforms (Facebook Rename Oktober 2021)
    {
        'isin': 'US30303M1027',
        'old_yf_ticker': 'FB',
        'new_yf_ticker': 'META',
        'change_date': '2021-10-28',
        'valid_from': '2012-05-18',  # Facebook IPO
        'valid_until': '2021-10-27',
        'change_type': 'rename',
        'notes': 'Umbenennung von Facebook Inc. zu Meta Platforms Inc.'
    },

    # Alphabet (Google Restructuring Oktober 2015)
    {
        'isin': 'US02079K3059',
        'old_yf_ticker': 'GOOG',
        'new_yf_ticker': 'GOOGL',
        'change_date': '2015-10-02',
        'valid_from': '2004-08-19',  # Google IPO
        'valid_until': '2015-10-01',
        'change_type': 'ticker_change',
        'notes': 'Ticker-Änderung im Rahmen der Alphabet-Umstrukturierung (Class A shares)'
    },

    # Weitere bekannte Fälle können hier hinzugefügt werden
]


def populate_ticker_history():
    """Fügt bekannte Ticker-Änderungen in die Datenbank ein."""
    conn = None
    cur = None

    try:
        print("Verbinde mit Datenbank...")
        conn = get_connection(db_name="raw_data", autocommit=False)
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO ticker_history (
                isin, old_yf_ticker, new_yf_ticker, change_date,
                valid_from, valid_until, change_type, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                new_yf_ticker = VALUES(new_yf_ticker),
                change_date = VALUES(change_date),
                valid_from = VALUES(valid_from),
                valid_until = VALUES(valid_until),
                change_type = VALUES(change_type),
                notes = VALUES(notes),
                updated_at = NOW()
        """

        print(f"\nFüge {len(TICKER_CHANGES)} Ticker-Änderungen ein...\n")

        for change in TICKER_CHANGES:
            cur.execute(insert_sql, (
                change['isin'],
                change['old_yf_ticker'],
                change['new_yf_ticker'],
                change['change_date'],
                change.get('valid_from'),
                change.get('valid_until'),
                change['change_type'],
                change['notes']
            ))

            print(f"✅ {change['old_yf_ticker']} → {change['new_yf_ticker']}")
            print(f"   {change['notes']}")
            print(f"   Datum: {change['change_date']}\n")

        conn.commit()

        # Zeige Statistik
        cur.execute("SELECT COUNT(*) FROM ticker_history")
        total = cur.fetchone()[0]

        cur.execute("SELECT change_type, COUNT(*) FROM ticker_history GROUP BY change_type")
        stats = cur.fetchall()

        print("=" * 70)
        print("STATISTIK")
        print("=" * 70)
        print(f"Gesamt Einträge: {total}")
        print(f"\nNach Änderungstyp:")
        for change_type, count in stats:
            print(f"  - {change_type}: {count}")

        print("\n✅ Ticker-History erfolgreich befüllt!")

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
    populate_ticker_history()
