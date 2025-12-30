#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Übersetzt fehlende Beschreibungen (z.B. Nikkei 225) nachträglich.

Dieses Script findet alle englischen Beschreibungen und übersetzt sie ins Deutsche.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tqdm import tqdm
from mysql.connector import Error
from db import get_connection

try:
    from deep_translator import GoogleTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    TRANSLATOR_AVAILABLE = False
    print("FEHLER: deep-translator nicht installiert!")
    print("Installiere mit: pip install deep-translator")
    sys.exit(1)


def translate_to_german(text):
    """Übersetze Text von Englisch nach Deutsch."""
    if not text:
        return text

    try:
        translator = GoogleTranslator(source='en', target='de')

        # Teile lange Texte in kleinere Chunks (Google Translate Limit: 5000 Zeichen)
        if len(text) > 4500:
            chunks = []
            sentences = text.split('. ')
            current_chunk = ''

            for sentence in sentences:
                if len(current_chunk) + len(sentence) + 2 < 4500:
                    current_chunk += sentence + '. '
                else:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = sentence + '. '

            if current_chunk:
                chunks.append(current_chunk)

            translated_chunks = [translator.translate(chunk) for chunk in chunks]
            return ' '.join(translated_chunks)
        else:
            return translator.translate(text)

    except Exception as e:
        print(f"Übersetzungsfehler: {e}")
        return text


def is_english(text):
    """Prüfe, ob Text wahrscheinlich Englisch ist (vereinfachte Heuristik)."""
    if not text:
        return False

    # Häufige englische Wörter, die in deutschen Texten selten vorkommen
    english_indicators = [
        ' the ', ' and ', ' is ', ' are ', ' was ', ' were ',
        ' has ', ' have ', ' with ', ' from ', ' that ',
        'Company', 'Corporation', 'operates', 'provides',
        'engaged in', 'manufactures'
    ]

    text_lower = text.lower()
    matches = sum(1 for indicator in english_indicators if indicator.lower() in text_lower)

    # Wenn mindestens 3 englische Indikatoren gefunden werden
    return matches >= 3


def main():
    print("=" * 60)
    print("FEHLENDE BESCHREIBUNGEN ÜBERSETZEN")
    print("=" * 60)

    conn = None
    cur = None

    try:
        print("\nVerbinde mit Datenbank...")
        conn = get_connection(autocommit=False)
        cur = conn.cursor(dictionary=True)

        # Alle Beschreibungen laden
        print("Lade Beschreibungen...")
        cur.execute("""
            SELECT isin, company_name, description, stock_index
            FROM analytics.company_info
            WHERE description IS NOT NULL AND description != ''
        """)

        all_descriptions = cur.fetchall()
        print(f"  → {len(all_descriptions)} Beschreibungen gefunden")

        # Englische Beschreibungen identifizieren
        print("\nIdentifiziere englische Beschreibungen...")
        english_descriptions = []

        for row in tqdm(all_descriptions, desc="Prüfung"):
            if is_english(row['description']):
                english_descriptions.append(row)

        print(f"\n  → {len(english_descriptions)} englische Beschreibungen gefunden")

        if len(english_descriptions) == 0:
            print("\n✅ Alle Beschreibungen sind bereits übersetzt!")
            return

        # Nach Index gruppieren
        by_index = {}
        for row in english_descriptions:
            idx = row['stock_index'] or 'Unbekannt'
            if idx not in by_index:
                by_index[idx] = []
            by_index[idx].append(row)

        print("\nVerteilung:")
        for idx, rows in sorted(by_index.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"  {idx:20} {len(rows):>4} Beschreibungen")

        # Übersetzen
        print(f"\nÜbersetze {len(english_descriptions)} Beschreibungen...")
        translated_count = 0

        for row in tqdm(english_descriptions, desc="Übersetzung"):
            try:
                description_de = translate_to_german(row['description'])

                # In Datenbank aktualisieren
                cur.execute("""
                    UPDATE analytics.company_info
                    SET description = %s
                    WHERE isin = %s
                """, (description_de, row['isin']))

                translated_count += 1

                # Alle 10 Einträge committen
                if translated_count % 10 == 0:
                    conn.commit()

            except Exception as e:
                print(f"\nFehler bei {row['company_name']} ({row['isin']}): {e}")

        # Finaler Commit
        conn.commit()

        print("\n" + "=" * 60)
        print("FERTIG")
        print("=" * 60)
        print(f"\n✅ {translated_count} Beschreibungen übersetzt")

        # Finale Statistik
        cur.execute("""
            SELECT stock_index, COUNT(*) as cnt
            FROM analytics.company_info
            WHERE description IS NOT NULL
            GROUP BY stock_index
            ORDER BY cnt DESC
        """)
        stats = cur.fetchall()

        print("\n📊 Beschreibungen nach Index:")
        for row in stats:
            print(f"  {row['stock_index']:20} {row['cnt']:>4}")

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
