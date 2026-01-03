#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt die users Tabelle für Authentifizierung und Autorisierung.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from mysql.connector import Error
from db import get_connection


# =============================================================================
# Tabelle: users (Authentifizierung)
# =============================================================================
CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS analytics.users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL COMMENT 'bcrypt hash',
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    role ENUM('admin', 'user') DEFAULT 'user',
    is_approved BOOLEAN DEFAULT FALSE COMMENT 'Admin approval required',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Account enabled/disabled',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    last_login DATETIME NULL,

    KEY idx_email (email),
    KEY idx_role (role),
    KEY idx_approved (is_approved)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
COMMENT='User authentication and authorization';
"""


def create_auth_tables():
    """Erstellt die users Tabelle."""
    try:
        conn = get_connection('analytics')
        cur = conn.cursor()

        print("Erstelle users Tabelle...")
        cur.execute(CREATE_USERS_TABLE)
        print("✓ users Tabelle erstellt")

        conn.commit()
        cur.close()
        conn.close()

        print("\n✓ Auth-Tabellen erfolgreich erstellt!")

    except Error as e:
        print(f"✗ Fehler beim Erstellen der Auth-Tabellen: {e}")
        sys.exit(1)


if __name__ == "__main__":
    create_auth_tables()
