#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migriert alle User-Daten von Admin User (ID: 1) zu Emanuel Schiller (ID: 3)
und löscht anschließend den alten Admin User Account
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

def migrate_user_data(from_user_id, to_user_id):
    """Migriert alle User-Daten von einem User zum anderen."""

    conn = get_connection('analytics', autocommit=False)
    cur = conn.cursor(dictionary=True)

    try:
        # 1. Prüfen ob beide User existieren
        cur.execute("SELECT id, email, first_name, last_name FROM users WHERE id IN (%s, %s)", (from_user_id, to_user_id))
        users = cur.fetchall()

        if len(users) != 2:
            print(f"❌ Fehler: Nicht beide User gefunden!")
            return False

        from_user = next(u for u in users if u['id'] == from_user_id)
        to_user = next(u for u in users if u['id'] == to_user_id)

        print(f"\n📋 Migration von User-Daten:")
        print(f"   Von: {from_user['first_name']} {from_user['last_name']} (ID: {from_user_id}, {from_user['email']})")
        print(f"   Zu:  {to_user['first_name']} {to_user['last_name']} (ID: {to_user_id}, {to_user['email']})")
        print()

        # 2. Alle bestehenden Daten des Ziel-Users löschen
        print("🗑️  Lösche bestehende Daten des Ziel-Users...")

        tables_to_clear = [
            'user_favorite_labels',
            'user_favorite_filter',
            'user_column_settings'
        ]

        for table in tables_to_clear:
            cur.execute(f"DELETE FROM {table} WHERE user_id = %s", (to_user_id,))
            deleted = cur.rowcount
            print(f"   - {table}: {deleted} Einträge gelöscht")

        # 3. Daten vom Quell-User zum Ziel-User kopieren
        print(f"\n📦 Kopiere Daten von User {from_user_id} zu User {to_user_id}...")

        # user_favorite_labels
        cur.execute("""
            INSERT INTO user_favorite_labels (user_id, favorite_id, label)
            SELECT %s, favorite_id, label
            FROM user_favorite_labels
            WHERE user_id = %s
        """, (to_user_id, from_user_id))
        print(f"   - user_favorite_labels: {cur.rowcount} Einträge kopiert")

        # user_favorite_filter
        cur.execute("""
            INSERT INTO user_favorite_filter (user_id, favorite_id, is_visible)
            SELECT %s, favorite_id, is_visible
            FROM user_favorite_filter
            WHERE user_id = %s
        """, (to_user_id, from_user_id))
        print(f"   - user_favorite_filter: {cur.rowcount} Einträge kopiert")

        # user_column_settings (mit INSERT IGNORE um Duplikate zu vermeiden)
        cur.execute("""
            INSERT IGNORE INTO user_column_settings
                (user_id, view_name, source_table, column_key, display_name,
                 sort_order, is_visible, column_group, format_type)
            SELECT %s, view_name, source_table, column_key, display_name,
                   sort_order, is_visible, column_group, format_type
            FROM user_column_settings
            WHERE user_id = %s
        """, (to_user_id, from_user_id))
        print(f"   - user_column_settings: {cur.rowcount} Einträge kopiert")

        # 4. Prüfe ob es weitere Tabellen mit user_id gibt
        print(f"\n🔍 Suche nach weiteren User-bezogenen Daten...")

        # Prüfe ob stock_data_favorites Tabelle existiert
        cur.execute("SHOW TABLES LIKE 'stock_data_favorites'")
        if cur.fetchone():
            cur.execute("SELECT COUNT(*) as count FROM stock_data_favorites WHERE user_id = %s", (from_user_id,))
            favorites_count = cur.fetchone()['count']

            if favorites_count > 0:
                cur.execute("""
                    UPDATE stock_data_favorites
                    SET user_id = %s
                    WHERE user_id = %s
                """, (to_user_id, from_user_id))
                print(f"   - stock_data_favorites: {favorites_count} Favoriten übertragen")
        else:
            print(f"   - stock_data_favorites: Tabelle nicht gefunden (übersprungen)")

        # 5. Alten User löschen
        print(f"\n🗑️  Lösche alten User (ID: {from_user_id})...")

        # Zuerst alle abhängigen Daten löschen
        for table in tables_to_clear:
            cur.execute(f"DELETE FROM {table} WHERE user_id = %s", (from_user_id,))

        # Dann den User selbst löschen
        cur.execute("DELETE FROM users WHERE id = %s", (from_user_id,))

        print(f"   ✅ User '{from_user['first_name']} {from_user['last_name']}' gelöscht")

        # Commit
        conn.commit()

        print(f"\n✅ Migration erfolgreich abgeschlossen!")
        print(f"   Alle Daten von '{from_user['first_name']} {from_user['last_name']}' wurden zu '{to_user['first_name']} {to_user['last_name']}' übertragen.")
        print(f"   Der alte User-Account wurde gelöscht.")

        return True

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Fehler bei der Migration: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Von Admin User (ID: 1) zu Emanuel Schiller (ID: 3)
    FROM_USER_ID = 1
    TO_USER_ID = 3

    migrate_user_data(FROM_USER_ID, TO_USER_ID)
