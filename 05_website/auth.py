#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Authentifizierungs-Modul für User-Management.
Implementiert User-Model für Flask-Login und Datenbank-Interaktionen.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import bcrypt
from flask_login import UserMixin
from db import get_connection


class User(UserMixin):
    """User-Model für Flask-Login."""

    def __init__(self, id, email, first_name, last_name, role, is_approved, is_active):
        self.id = id
        self.email = email
        self.first_name = first_name
        self.last_name = last_name
        self.role = role
        self.is_approved = is_approved
        self._is_active = is_active  # Interner Wert (wegen UserMixin Konflikt)

    @property
    def is_active(self):
        """Überschreibt UserMixin.is_active - Flask-Login verwendet diese Property."""
        return self._is_active

    def is_admin(self):
        """Prüft ob User Admin-Rechte hat."""
        return self.role == 'admin'

    def can_login(self):
        """Prüft ob User sich anmelden darf (freigegeben und aktiv)."""
        return self.is_approved and self._is_active

    @staticmethod
    def get_by_id(user_id):
        """Lädt User aus Datenbank anhand ID (für Flask-Login)."""
        conn = get_connection('analytics')
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        user_data = cur.fetchone()

        cur.close()
        conn.close()

        if user_data:
            return User(
                id=user_data['id'],
                email=user_data['email'],
                first_name=user_data['first_name'],
                last_name=user_data['last_name'],
                role=user_data['role'],
                is_approved=user_data['is_approved'],
                is_active=user_data['is_active']
            )
        return None

    @staticmethod
    def get_by_email(email):
        """Lädt User aus Datenbank anhand E-Mail (für Login)."""
        conn = get_connection('analytics')
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user_data = cur.fetchone()

        cur.close()
        conn.close()

        if user_data:
            user = User(
                id=user_data['id'],
                email=user_data['email'],
                first_name=user_data['first_name'],
                last_name=user_data['last_name'],
                role=user_data['role'],
                is_approved=user_data['is_approved'],
                is_active=user_data['is_active']
            )
            return user, user_data['password_hash']
        return None, None

    @staticmethod
    def create_user(email, password, first_name, last_name):
        """
        Erstellt neuen User (nicht freigegeben per Default).
        Initialisiert auch Default-Daten (Labels, Filter, Column Settings).
        """
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        conn = get_connection('analytics', autocommit=False)
        cur = conn.cursor()

        try:
            # User erstellen
            cur.execute("""
                INSERT INTO users
                (email, password_hash, first_name, last_name, role, is_approved)
                VALUES (%s, %s, %s, %s, 'user', FALSE)
            """, (email, password_hash, first_name, last_name))

            user_id = cur.lastrowid

            # Default-Daten initialisieren
            _initialize_user_data(cur, user_id)

            conn.commit()
            return user_id

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def verify_password(password, password_hash):
        """Prüft Passwort gegen Hash mit bcrypt."""
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8') if isinstance(password_hash, str) else password_hash)

    def update_last_login(self):
        """Aktualisiert last_login Timestamp."""
        conn = get_connection('analytics')
        cur = conn.cursor()

        cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (self.id,))

        conn.commit()
        cur.close()
        conn.close()


def _initialize_user_data(cur, user_id):
    """
    Initialisiert Default-Daten für neuen User:
    - Favoriten-Labels (1-9)
    - Favoriten-Filter (alle sichtbar)
    - Column Settings (kopiert vom ersten Admin)
    """
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

    # 3. Column Settings vom ersten Admin kopieren
    cur.execute("""
        SELECT MIN(id) FROM users WHERE role = 'admin'
    """)
    result = cur.fetchone()

    if result and result[0]:
        first_admin_id = result[0]

        # Alle Column Settings kopieren
        cur.execute("""
            INSERT INTO user_column_settings
                (user_id, view_name, source_table, column_key, display_name,
                 sort_order, is_visible, column_group, format_type)
            SELECT %s, view_name, source_table, column_key, display_name,
                   sort_order, is_visible, column_group, format_type
            FROM user_column_settings
            WHERE user_id = %s
        """, (user_id, first_admin_id))
