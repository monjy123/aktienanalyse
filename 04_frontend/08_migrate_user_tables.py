#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migriert bestehende User-Tabellen für Multi-User-Support:
1. Erstellt ersten Admin-User
2. Fügt user_id Spalte zu bestehenden Tabellen hinzu
3. Migriert bestehende Daten zum Admin-User
4. Setzt Foreign Keys und Constraints
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import bcrypt
import getpass
from mysql.connector import Error
from db import get_connection


def create_first_admin():
    """Erstellt den ersten Admin-User mit interaktiven Eingaben."""
    print("\n" + "="*60)
    print("ERSTELLE ERSTEN ADMIN-USER")
    print("="*60)

    # Eingaben sammeln
    print("\nBitte geben Sie die Admin-Zugangsdaten ein:")
    email = input("E-Mail: ").strip()

    while not email or '@' not in email:
        print("✗ Ungültige E-Mail")
        email = input("E-Mail: ").strip()

    first_name = input("Vorname: ").strip()
    while not first_name:
        first_name = input("Vorname: ").strip()

    last_name = input("Nachname: ").strip()
    while not last_name:
        last_name = input("Nachname: ").strip()

    # Passwort sicher eingeben
    while True:
        password = getpass.getpass("Passwort (min. 8 Zeichen): ")
        if len(password) < 8:
            print("✗ Passwort muss mindestens 8 Zeichen lang sein")
            continue

        password_confirm = getpass.getpass("Passwort bestätigen: ")
        if password != password_confirm:
            print("✗ Passwörter stimmen nicht überein")
            continue

        break

    # Passwort hashen
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    # User in Datenbank erstellen
    try:
        conn = get_connection('analytics')
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO users
            (email, password_hash, first_name, last_name, role, is_approved, is_active)
            VALUES (%s, %s, %s, %s, 'admin', TRUE, TRUE)
        """, (email, password_hash, first_name, last_name))

        admin_id = cur.lastrowid
        conn.commit()

        print(f"\n✓ Admin-User erstellt (ID: {admin_id})")
        print(f"  E-Mail: {email}")
        print(f"  Name: {first_name} {last_name}")

        cur.close()
        conn.close()

        return admin_id

    except Error as e:
        print(f"✗ Fehler beim Erstellen des Admin-Users: {e}")
        sys.exit(1)


def migrate_user_watchlist(admin_id):
    """Migriert user_watchlist Tabelle."""
    try:
        conn = get_connection('analytics', autocommit=False)
        cur = conn.cursor()

        print("\nMigriere user_watchlist...")

        # 1. Prüfen ob user_id Spalte bereits existiert
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'analytics'
            AND TABLE_NAME = 'user_watchlist'
            AND COLUMN_NAME = 'user_id'
        """)
        if cur.fetchone()[0] > 0:
            print("  ℹ user_id Spalte existiert bereits, überspringe Migration")
            cur.close()
            conn.close()
            return

        # 2. user_id Spalte hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_watchlist
            ADD COLUMN user_id INT NULL COMMENT 'Foreign key to users.id'
        """)

        # 3. Bestehende Daten dem Admin zuordnen
        cur.execute("""
            UPDATE analytics.user_watchlist
            SET user_id = %s
        """, (admin_id,))
        updated_rows = cur.rowcount

        # 4. user_id NOT NULL setzen
        cur.execute("""
            ALTER TABLE analytics.user_watchlist
            MODIFY COLUMN user_id INT NOT NULL
        """)

        # 5. Foreign Key hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_watchlist
            ADD CONSTRAINT fk_watchlist_user
            FOREIGN KEY (user_id) REFERENCES analytics.users(id) ON DELETE CASCADE
        """)

        # 6. Alte UNIQUE Constraint auf isin entfernen (falls vorhanden)
        try:
            cur.execute("""
                ALTER TABLE analytics.user_watchlist
                DROP INDEX isin
            """)
        except Error:
            pass  # Index existiert möglicherweise nicht

        # 7. Neue UNIQUE Constraint (user_id, isin)
        cur.execute("""
            ALTER TABLE analytics.user_watchlist
            ADD UNIQUE KEY unique_user_isin (user_id, isin)
        """)

        conn.commit()
        print(f"  ✓ {updated_rows} Einträge migriert")

        cur.close()
        conn.close()

    except Error as e:
        print(f"  ✗ Fehler: {e}")
        sys.exit(1)


def migrate_user_favorite_labels(admin_id):
    """Migriert user_favorite_labels Tabelle."""
    try:
        conn = get_connection('analytics', autocommit=False)
        cur = conn.cursor()

        print("\nMigriere user_favorite_labels...")

        # 1. Prüfen ob user_id Spalte bereits existiert
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'analytics'
            AND TABLE_NAME = 'user_favorite_labels'
            AND COLUMN_NAME = 'user_id'
        """)
        if cur.fetchone()[0] > 0:
            print("  ℹ user_id Spalte existiert bereits, überspringe Migration")
            cur.close()
            conn.close()
            return

        # 2. user_id Spalte hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_labels
            ADD COLUMN user_id INT NULL COMMENT 'Foreign key to users.id'
        """)

        # 3. Bestehende Daten dem Admin zuordnen
        cur.execute("""
            UPDATE analytics.user_favorite_labels
            SET user_id = %s
        """, (admin_id,))
        updated_rows = cur.rowcount

        # 4. user_id NOT NULL setzen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_labels
            MODIFY COLUMN user_id INT NOT NULL
        """)

        # 5. Alte PRIMARY KEY entfernen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_labels
            DROP PRIMARY KEY
        """)

        # 6. Neue PRIMARY KEY (user_id, favorite_id)
        cur.execute("""
            ALTER TABLE analytics.user_favorite_labels
            ADD PRIMARY KEY (user_id, favorite_id)
        """)

        # 7. Foreign Key hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_labels
            ADD CONSTRAINT fk_favorite_labels_user
            FOREIGN KEY (user_id) REFERENCES analytics.users(id) ON DELETE CASCADE
        """)

        conn.commit()
        print(f"  ✓ {updated_rows} Einträge migriert")

        cur.close()
        conn.close()

    except Error as e:
        print(f"  ✗ Fehler: {e}")
        sys.exit(1)


def migrate_user_favorite_filter(admin_id):
    """Migriert user_favorite_filter Tabelle."""
    try:
        conn = get_connection('analytics', autocommit=False)
        cur = conn.cursor()

        print("\nMigriere user_favorite_filter...")

        # 1. Prüfen ob user_id Spalte bereits existiert
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'analytics'
            AND TABLE_NAME = 'user_favorite_filter'
            AND COLUMN_NAME = 'user_id'
        """)
        if cur.fetchone()[0] > 0:
            print("  ℹ user_id Spalte existiert bereits, überspringe Migration")
            cur.close()
            conn.close()
            return

        # 2. user_id Spalte hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_filter
            ADD COLUMN user_id INT NULL COMMENT 'Foreign key to users.id'
        """)

        # 3. Bestehende Daten dem Admin zuordnen
        cur.execute("""
            UPDATE analytics.user_favorite_filter
            SET user_id = %s
        """, (admin_id,))
        updated_rows = cur.rowcount

        # 4. user_id NOT NULL setzen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_filter
            MODIFY COLUMN user_id INT NOT NULL
        """)

        # 5. Alte PRIMARY KEY entfernen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_filter
            DROP PRIMARY KEY
        """)

        # 6. Neue PRIMARY KEY (user_id, favorite_id)
        cur.execute("""
            ALTER TABLE analytics.user_favorite_filter
            ADD PRIMARY KEY (user_id, favorite_id)
        """)

        # 7. Foreign Key hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_favorite_filter
            ADD CONSTRAINT fk_favorite_filter_user
            FOREIGN KEY (user_id) REFERENCES analytics.users(id) ON DELETE CASCADE
        """)

        conn.commit()
        print(f"  ✓ {updated_rows} Einträge migriert")

        cur.close()
        conn.close()

    except Error as e:
        print(f"  ✗ Fehler: {e}")
        sys.exit(1)


def migrate_user_column_settings(admin_id):
    """Migriert user_column_settings Tabelle."""
    try:
        conn = get_connection('analytics', autocommit=False)
        cur = conn.cursor()

        print("\nMigriere user_column_settings...")

        # 1. Prüfen ob user_id Spalte bereits existiert
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'analytics'
            AND TABLE_NAME = 'user_column_settings'
            AND COLUMN_NAME = 'user_id'
        """)
        if cur.fetchone()[0] > 0:
            print("  ℹ user_id Spalte existiert bereits, überspringe Migration")
            cur.close()
            conn.close()
            return

        # 2. user_id Spalte hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_column_settings
            ADD COLUMN user_id INT NULL COMMENT 'Foreign key to users.id'
        """)

        # 3. Bestehende Daten dem Admin zuordnen
        cur.execute("""
            UPDATE analytics.user_column_settings
            SET user_id = %s
        """, (admin_id,))
        updated_rows = cur.rowcount

        # 4. user_id NOT NULL setzen
        cur.execute("""
            ALTER TABLE analytics.user_column_settings
            MODIFY COLUMN user_id INT NOT NULL
        """)

        # 5. Foreign Key hinzufügen
        cur.execute("""
            ALTER TABLE analytics.user_column_settings
            ADD CONSTRAINT fk_column_settings_user
            FOREIGN KEY (user_id) REFERENCES analytics.users(id) ON DELETE CASCADE
        """)

        # 6. UNIQUE Constraint hinzufügen (user_id, view_name, column_key)
        try:
            cur.execute("""
                ALTER TABLE analytics.user_column_settings
                ADD UNIQUE KEY unique_user_view_column (user_id, view_name, column_key)
            """)
        except Error:
            pass  # Constraint könnte bereits existieren

        conn.commit()
        print(f"  ✓ {updated_rows} Einträge migriert")

        cur.close()
        conn.close()

    except Error as e:
        print(f"  ✗ Fehler: {e}")
        sys.exit(1)


def main():
    """Hauptfunktion für die Migration."""
    print("\n" + "="*60)
    print("USER-TABELLEN MIGRATION")
    print("="*60)
    print("\nDieses Script:")
    print("1. Erstellt den ersten Admin-User")
    print("2. Fügt user_id Spalten zu bestehenden Tabellen hinzu")
    print("3. Migriert alle bestehenden Daten zum Admin-User")
    print("4. Setzt Foreign Keys und Constraints")
    print("\n⚠ WICHTIG: Erstellen Sie vorher ein Backup der Datenbank!")
    print("="*60)

    # Sicherheitsabfrage
    confirm = input("\nFortfahren? (ja/nein): ").strip().lower()
    if confirm != 'ja':
        print("Abgebrochen.")
        sys.exit(0)

    # 1. Ersten Admin erstellen
    admin_id = create_first_admin()

    # 2. Tabellen migrieren
    migrate_user_watchlist(admin_id)
    migrate_user_favorite_labels(admin_id)
    migrate_user_favorite_filter(admin_id)
    migrate_user_column_settings(admin_id)

    print("\n" + "="*60)
    print("✓ MIGRATION ERFOLGREICH ABGESCHLOSSEN!")
    print("="*60)
    print(f"\nAdmin-User ID: {admin_id}")
    print("Alle bestehenden Daten wurden dem Admin zugeordnet.")
    print("\nNächste Schritte:")
    print("1. Code-Änderungen durchführen (app.py, auth.py, Templates)")
    print("2. FLASK_SECRET_KEY in .env setzen")
    print("3. Website neu starten")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
