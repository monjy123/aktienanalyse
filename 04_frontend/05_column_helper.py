#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helper-Funktionen für die konfigurierbare Spaltenauswahl.

Stellt Funktionen bereit um:
- Konfigurierte Spalten abzurufen
- Dynamische SQL-Queries zu generieren
- Spalten-Reihenfolge zu ändern
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Optional
from mysql.connector import Error
from db import get_connection


def get_visible_columns(view_name: str = 'watchlist') -> list[dict]:
    """
    Gibt alle sichtbaren Spalten für eine View zurück.

    Args:
        view_name: 'watchlist' oder 'screener'

    Returns:
        Liste von Dictionaries mit column_key, source_table, display_name, etc.
    """
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT column_key, source_table, display_name,
                   sort_order, column_group, format_type
            FROM analytics.user_column_settings
            WHERE view_name = %s AND is_visible = TRUE
            ORDER BY sort_order
        """, (view_name,))

        return cur.fetchall()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_all_columns(view_name: str = 'watchlist') -> list[dict]:
    """
    Gibt ALLE Spalten für eine View zurück (auch ausgeblendete).

    Args:
        view_name: 'watchlist' oder 'screener'

    Returns:
        Liste von Dictionaries mit allen Spalten-Infos
    """
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT column_key, source_table, display_name,
                   sort_order, is_visible, column_group, format_type
            FROM analytics.user_column_settings
            WHERE view_name = %s
            ORDER BY column_group, sort_order
        """, (view_name,))

        return cur.fetchall()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def build_select_query(view_name: str = 'watchlist',
                       filter_isin: Optional[str] = None,
                       filter_favorite: Optional[int] = None,
                       filter_index: Optional[str] = None) -> tuple[str, list]:
    """
    Baut dynamisch eine SELECT-Query basierend auf den konfigurierten Spalten.

    Args:
        view_name: 'watchlist' oder 'screener'
        filter_isin: Optional - Filter auf bestimmte ISIN
        filter_favorite: Optional - Filter auf Favoriten-Status (1,2,3)
        filter_index: Optional - Filter auf Index (z.B. 'DAX', 'SP500')

    Returns:
        Tuple aus (SQL-Query, Parameter-Liste)
    """
    columns = get_visible_columns(view_name)

    if not columns:
        raise ValueError(f"Keine sichtbaren Spalten für View '{view_name}' konfiguriert")

    # Spalten nach Quelltabelle gruppieren
    select_parts = ['lm.isin']  # ISIN immer dabei
    needs_company_info = False

    for col in columns:
        if col['source_table'] == 'company_info':
            select_parts.append(f"ci.{col['column_key']}")
            needs_company_info = True
        else:
            select_parts.append(f"lm.{col['column_key']}")

    # Watchlist-Felder immer hinzufügen
    select_parts.extend(['uw.favorite', 'uw.notes'])

    # Query zusammenbauen
    select_clause = ",\n       ".join(select_parts)

    query = f"""
SELECT {select_clause}
FROM analytics.live_metrics lm
JOIN analytics.user_watchlist uw ON lm.isin = uw.isin
"""

    if needs_company_info:
        query += "LEFT JOIN analytics.company_info ci ON lm.isin = ci.isin\n"

    # WHERE-Bedingungen
    conditions = []
    params = []

    if filter_isin:
        conditions.append("lm.isin = %s")
        params.append(filter_isin)

    if filter_favorite is not None:
        conditions.append("uw.favorite = %s")
        params.append(filter_favorite)

    if filter_index:
        conditions.append("lm.stock_index = %s")
        params.append(filter_index)

    if conditions:
        query += "WHERE " + " AND ".join(conditions) + "\n"

    query += "ORDER BY lm.company_name"

    return query, params


def execute_view_query(view_name: str = 'watchlist', **filters) -> list[dict]:
    """
    Führt die View-Query aus und gibt die Ergebnisse zurück.

    Args:
        view_name: 'watchlist' oder 'screener'
        **filters: Optionale Filter (filter_isin, filter_favorite, filter_index)

    Returns:
        Liste von Dictionaries mit den Daten
    """
    conn = None
    cur = None

    try:
        query, params = build_select_query(view_name, **filters)

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(query, params)

        return cur.fetchall()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def set_column_visibility(view_name: str, column_key: str, visible: bool) -> bool:
    """
    Ändert die Sichtbarkeit einer Spalte.

    Args:
        view_name: 'watchlist' oder 'screener'
        column_key: Name der Spalte
        visible: True = anzeigen, False = ausblenden

    Returns:
        True bei Erfolg
    """
    conn = None
    cur = None

    try:
        conn = get_connection(autocommit=True)
        cur = conn.cursor()

        cur.execute("""
            UPDATE analytics.user_column_settings
            SET is_visible = %s
            WHERE view_name = %s AND column_key = %s
        """, (visible, view_name, column_key))

        return cur.rowcount > 0

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def set_column_order(view_name: str, column_key: str, new_order: int) -> bool:
    """
    Ändert die Reihenfolge einer Spalte.

    Args:
        view_name: 'watchlist' oder 'screener'
        column_key: Name der Spalte
        new_order: Neue Position (niedrig = weiter links)

    Returns:
        True bei Erfolg
    """
    conn = None
    cur = None

    try:
        conn = get_connection(autocommit=True)
        cur = conn.cursor()

        cur.execute("""
            UPDATE analytics.user_column_settings
            SET sort_order = %s
            WHERE view_name = %s AND column_key = %s
        """, (new_order, view_name, column_key))

        return cur.rowcount > 0

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_column_headers(view_name: str = 'watchlist') -> list[str]:
    """
    Gibt nur die Anzeigenamen der sichtbaren Spalten zurück.
    Nützlich für Tabellen-Header.

    Returns:
        Liste der Display-Namen in der richtigen Reihenfolge
    """
    columns = get_visible_columns(view_name)
    return ['ISIN'] + [col['display_name'] for col in columns] + ['Favorit', 'Notizen']


# =============================================================================
# Demo / Test
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("COLUMN HELPER - DEMO")
    print("=" * 60)

    # 1. Sichtbare Spalten anzeigen
    print("\n1. Sichtbare Spalten (Watchlist):")
    print("-" * 40)
    columns = get_visible_columns('watchlist')
    for col in columns:
        print(f"  {col['sort_order']:3d}. {col['display_name']:<20} ({col['source_table']}.{col['column_key']})")

    # 2. Column Headers
    print("\n2. Tabellen-Header:")
    print("-" * 40)
    headers = get_column_headers('watchlist')
    print("  " + " | ".join(headers))

    # 3. Generierte Query anzeigen
    print("\n3. Generierte SQL-Query:")
    print("-" * 40)
    query, params = build_select_query('watchlist')
    print(query)

    # 4. Query mit Filter
    print("\n4. Query mit Filter (favorite=1):")
    print("-" * 40)
    query, params = build_select_query('watchlist', filter_favorite=1)
    print(query)
    print(f"  Parameter: {params}")

    # 5. Alle Spalten (gruppiert)
    print("\n5. Alle verfügbaren Spalten:")
    print("-" * 40)
    all_cols = get_all_columns('watchlist')
    current_group = None
    for col in all_cols:
        if col['column_group'] != current_group:
            current_group = col['column_group']
            print(f"\n  [{current_group}]")
        status = "ON " if col['is_visible'] else "OFF"
        print(f"    [{status}] {col['display_name']:<20} ({col['column_key']})")
