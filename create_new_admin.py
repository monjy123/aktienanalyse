#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt neuen Admin-Account
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import bcrypt
from db import get_connection

def create_admin_user(email, password, first_name, last_name):
    """Erstellt Admin-Account mit sofortiger Freigabe."""

    # Passwort hashen
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    conn = get_connection('analytics', autocommit=False)
    cur = conn.cursor()

    try:
        # Prüfen ob Email bereits existiert
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        existing = cur.fetchone()

        if existing:
            print(f"❌ Fehler: User mit Email '{email}' existiert bereits (ID: {existing[0]})")
            return False

        # Admin-User erstellen (direkt freigegeben)
        cur.execute("""
            INSERT INTO users
            (email, password_hash, first_name, last_name, role, is_approved, is_active)
            VALUES (%s, %s, %s, %s, 'admin', TRUE, TRUE)
        """, (email, password_hash, first_name, last_name))

        user_id = cur.lastrowid

        # Default-Daten initialisieren (Favoriten-Labels, Filter, Column Settings)
        # 1. Favoriten-Labels erstellen (1-9)
        for i in range(1, 10):
            cur.execute("""
                INSERT INTO user_favorite_labels (user_id, favorite_id, label)
                VALUES (%s, %s, %s)
            """, (user_id, i, f'Favorit {i}'))

        # 2. Favoriten-Filter erstellen (alle sichtbar)
        for i in range(1, 10):
            cur.execute("""
                INSERT INTO user_favorite_filter (user_id, favorite_id, is_visible)
                VALUES (%s, %s, TRUE)
            """, (user_id, i))

        # 3. Column Settings vom ersten Admin kopieren (falls vorhanden)
        cur.execute("SELECT MIN(id) FROM users WHERE role = 'admin' AND id != %s", (user_id,))
        result = cur.fetchone()

        if result and result[0]:
            first_admin_id = result[0]
            # Kopiere Column Settings mit INSERT IGNORE um Duplikate zu vermeiden
            cur.execute("""
                INSERT IGNORE INTO user_column_settings
                    (user_id, view_name, source_table, column_key, display_name,
                     sort_order, is_visible, column_group, format_type)
                SELECT %s, view_name, source_table, column_key, display_name,
                       sort_order, is_visible, column_group, format_type
                FROM user_column_settings
                WHERE user_id = %s
            """, (user_id, first_admin_id))

        conn.commit()

        print(f"\n✅ Admin-Account erfolgreich erstellt!")
        print(f"   ID:       {user_id}")
        print(f"   Email:    {email}")
        print(f"   Name:     {first_name} {last_name}")
        print(f"   Rolle:    admin")
        print(f"   Status:   ✅ Freigegeben & Aktiv")
        print(f"\nSie können sich jetzt mit diesen Zugangsdaten anmelden!")

        return True

    except Exception as e:
        conn.rollback()
        print(f"❌ Fehler beim Erstellen des Admin-Accounts: {e}")
        return False
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Neue Admin-Daten
    EMAIL = "emanuelschiller@icloud.com"
    PASSWORD = "Emst4558"
    FIRST_NAME = "Emanuel"
    LAST_NAME = "Schiller"

    create_admin_user(EMAIL, PASSWORD, FIRST_NAME, LAST_NAME)
