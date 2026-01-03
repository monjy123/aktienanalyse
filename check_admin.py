#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnose-Script: Prüft Admin-Accounts in der Datenbank
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

def check_users():
    """Zeigt alle User in der Datenbank an."""
    conn = get_connection('analytics')
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT id, email, first_name, last_name, role, is_approved, is_active, created_at FROM users ORDER BY id")
    users = cur.fetchall()

    cur.close()
    conn.close()

    if not users:
        print("❌ Keine User in der Datenbank gefunden!")
        return False

    print(f"\n✅ {len(users)} User gefunden:\n")
    print("-" * 120)
    print(f"{'ID':<5} {'Email':<30} {'Name':<25} {'Rolle':<10} {'Approved':<10} {'Aktiv':<8} {'Erstellt'}")
    print("-" * 120)

    for user in users:
        approved = "✅ Ja" if user['is_approved'] else "❌ Nein"
        active = "✅ Ja" if user['is_active'] else "❌ Nein"
        print(f"{user['id']:<5} {user['email']:<30} {user['first_name']} {user['last_name']:<20} {user['role']:<10} {approved:<10} {active:<8} {user['created_at']}")

    print("-" * 120)
    return True

if __name__ == "__main__":
    check_users()
