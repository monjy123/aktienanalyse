#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lädt historische Preisdaten von alten Tickern für Unternehmen,
die fusioniert wurden oder ihren Ticker geändert haben.

Nutzt die ticker_history Tabelle um zu identifizieren, welche
historischen Daten nachgeladen werden müssen.

Beispiel:
- DSFIR.AS hat nur Daten ab 2023-04-18
- Über ticker_history finden wir DSM.AS als alten Ticker
- Wir laden DSM.AS Daten für 1989-2023 nach
- Diese werden mit ISIN CH1216478797 in yf_prices gespeichert
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import time
import pandas as pd
import yfinance as yf
from mysql.connector import Error as MySQLError
from db import get_connection
from datetime import datetime, timedelta


def execute_in_chunks(cursor, sql, data, chunk_size=5000):
    """Helper für Micro-Batching"""
    for i in range(0, len(data), chunk_size):
        cursor.executemany(sql, data[i:i+chunk_size])


def load_historical_prices_for_old_tickers():
    """Lädt historische Preise von alten Tickern"""
    try:
        conn = get_connection(db_name="raw_data", autocommit=False)
        cur = conn.cursor(dictionary=True)
    except MySQLError as e:
        print("❌ DB-Verbindung fehlgeschlagen:", e)
        return

    print("=" * 70)
    print("HISTORISCHE PREISE VON ALTEN TICKERN LADEN")
    print("=" * 70)

    # Hole alle Ticker-Änderungen
    cur.execute("""
        SELECT
            th.id,
            th.isin,
            th.old_yf_ticker,
            th.new_yf_ticker,
            th.change_date,
            th.valid_from,
            th.valid_until,
            th.change_type,
            th.notes
        FROM ticker_history th
        WHERE th.old_yf_ticker IS NOT NULL
        ORDER BY th.change_date DESC
    """)

    ticker_changes = cur.fetchall()
    print(f"\nGefunden: {len(ticker_changes)} Ticker-Änderungen\n")

    if not ticker_changes:
        print("Keine Ticker-Änderungen gefunden. Beende.")
        cur.close()
        conn.close()
        return

    # Insert SQL vorbereiten
    insert_sql = """
        INSERT INTO yf_prices
        (isin, ticker_yf, date, open, high, low, close, adj_close, volume, stock_index)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            adj_close = VALUES(adj_close),
            volume = VALUES(volume),
            updated_at = NOW()
    """

    total_inserted = 0
    total_skipped = 0

    # Verarbeite jede Ticker-Änderung
    for change in ticker_changes:
        print("-" * 70)
        print(f"Verarbeite: {change['old_yf_ticker']} → {change['new_yf_ticker']}")
        print(f"ISIN: {change['isin']}")
        print(f"Änderung: {change['change_date']} ({change['change_type']})")
        print(f"Info: {change['notes']}")

        # Prüfe, welche Daten bereits vorhanden sind
        cur.execute("""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(*) as count
            FROM yf_prices
            WHERE isin = %s
        """, (change['isin'],))

        existing_data = cur.fetchone()

        # Bestimme Zeitraum für das Laden
        # Start: valid_from oder 20 Jahre vor dem change_date
        if change['valid_from']:
            start_date = change['valid_from']
            start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, datetime) else str(start_date)
        else:
            # Fallback: 20 Jahre vor der Änderung
            change_dt = change['change_date'] if isinstance(change['change_date'], datetime) else datetime.strptime(str(change['change_date']), '%Y-%m-%d')
            start_date = (change_dt - timedelta(days=365*20))
            start_date_str = start_date.strftime('%Y-%m-%d')

        # Ende: change_date - 1 Tag
        end_date = change['valid_until'] or change['change_date']
        end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime) else str(end_date)

        print(f"\nZielzeitraum: {start_date_str} bis {end_date_str}")

        if existing_data and existing_data['min_date']:
            print(f"Vorhandene Daten: {existing_data['min_date']} bis {existing_data['max_date']} "
                  f"({existing_data['count']} Einträge)")

            # Konvertiere zu date objects für Vergleich
            start_date_obj = start_date if isinstance(start_date, datetime) else datetime.strptime(start_date_str, '%Y-%m-%d')
            start_date_obj = start_date_obj.date() if isinstance(start_date_obj, datetime) else start_date_obj

            # Prüfe ob wir Daten vor dem vorhandenen Minimum brauchen
            if existing_data['min_date'] <= start_date_obj:
                print("✅ Historische Daten bereits vorhanden, überspringe.")
                total_skipped += 1
                continue

            # Lade nur den fehlenden Zeitraum
            end_date_str = (existing_data['min_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"📥 Lade fehlende Daten: {start_date_str} bis {end_date_str}")
        else:
            print(f"📥 Keine Daten vorhanden, lade vollständigen Zeitraum")

        # Lade Daten von yfinance
        try:
            print(f"   Lade {change['old_yf_ticker']} von yfinance...")
            ticker_data = yf.download(
                change['old_yf_ticker'],
                start=start_date_str,
                end=end_date_str,
                progress=False,
                auto_adjust=False
            )

            if ticker_data.empty:
                print(f"   ⚠️  Keine Daten verfügbar für {change['old_yf_ticker']}")
                continue

            # Bereite Daten für Insert vor
            rows_to_insert = []
            for date, row in ticker_data.iterrows():
                rows_to_insert.append((
                    change['isin'],
                    change['old_yf_ticker'],  # Behalte den alten Ticker für Nachvollziehbarkeit
                    date.strftime('%Y-%m-%d'),
                    float(row['Open']) if pd.notna(row['Open']) else None,
                    float(row['High']) if pd.notna(row['High']) else None,
                    float(row['Low']) if pd.notna(row['Low']) else None,
                    float(row['Close']) if pd.notna(row['Close']) else None,
                    float(row['Adj Close']) if pd.notna(row['Adj Close']) else None,
                    int(row['Volume']) if pd.notna(row['Volume']) else None,
                    None  # stock_index wird später bei Bedarf ergänzt
                ))

            if rows_to_insert:
                print(f"   💾 Speichere {len(rows_to_insert)} Datensätze...")
                execute_in_chunks(cur, insert_sql, rows_to_insert)
                conn.commit()
                total_inserted += len(rows_to_insert)
                print(f"   ✅ {len(rows_to_insert)} Einträge gespeichert")
            else:
                print(f"   ⚠️  Keine Daten zum Speichern")

            # Kurze Pause um yfinance API nicht zu überlasten
            time.sleep(0.5)

        except Exception as e:
            print(f"   ❌ Fehler beim Laden von {change['old_yf_ticker']}: {e}")
            conn.rollback()
            continue

    cur.close()
    conn.close()

    print("\n" + "=" * 70)
    print("ZUSAMMENFASSUNG")
    print("=" * 70)
    print(f"Verarbeitet: {len(ticker_changes)} Ticker-Änderungen")
    print(f"Eingefügt:   {total_inserted:,} neue Datensätze")
    print(f"Übersprungen: {total_skipped} (Daten bereits vorhanden)")
    print("\n✅ Historische Preise erfolgreich nachgeladen!")


if __name__ == "__main__":
    load_historical_prices_for_old_tickers()
