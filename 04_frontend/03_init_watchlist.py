#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Initialisiert analytics.user_watchlist mit allen ISINs aus tickerlist.

Trägt alle ISINs ein mit:
- favorite = 0 (kein Favorit)
- notes = NULL (keine Notizen)

Später können Favoriten und Notizen manuell gesetzt werden.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tqdm import tqdm
from mysql.connector import Error
from db import get_connection


def main():
    print("=" * 60)
    print("USER WATCHLIST INITIALISIEREN")
    print("=" * 60)

    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor()

        # Alle ISINs aus tickerlist laden
        print("\nLade ISINs aus tickerlist...")
        cur.execute("""
            SELECT DISTINCT isin
            FROM tickerdb.tickerlist
            WHERE isin IS NOT NULL AND isin != ''
        """)
        isins = [row[0] for row in cur.fetchall()]
        print(f"  → {len(isins)} ISINs gefunden")

        # In user_watchlist eintragen
        print("\nTrage ISINs in user_watchlist ein...")

        insert_sql = """
        INSERT INTO analytics.user_watchlist (isin, favorite, notes)
        VALUES (%s, 0, NULL)
        ON DUPLICATE KEY UPDATE
            isin = isin
        """

        success_count = 0
        for isin in tqdm(isins, desc="Eintragen"):
            try:
                cur.execute(insert_sql, (isin,))
                success_count += 1
            except Error as e:
                print(f"  Fehler bei {isin}: {e}")

        conn.commit()

        # Statistik
        print("\n" + "=" * 60)
        print("FERTIG - STATISTIK")
        print("=" * 60)

        cur.execute("SELECT COUNT(*) FROM analytics.user_watchlist")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM analytics.user_watchlist WHERE favorite > 0")
        with_favorite = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM analytics.user_watchlist WHERE notes IS NOT NULL")
        with_notes = cur.fetchone()[0]

        print(f"\n📊 Gesamt Einträge:        {total:,}")
        print(f"⭐ Mit Favorit:            {with_favorite:,}")
        print(f"📝 Mit Notizen:            {with_notes:,}")

        print("""
Nächste Schritte:
  - Favoriten setzen:  UPDATE analytics.user_watchlist SET favorite = 1 WHERE isin = 'DE0007164600';
  - Notizen setzen:    UPDATE analytics.user_watchlist SET notes = 'Meine Notiz' WHERE isin = 'DE0007164600';

  Favoriten-Stufen:
    1 = halte ich aktuell
    2 = top Favorit
    3 = beobachte ich
    0 = kein Favorit
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

    print("\nFertig.")


if __name__ == "__main__":
    main()
