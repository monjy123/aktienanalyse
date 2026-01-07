#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask Webserver für Aktien-Watchlist und Screener.

Starten mit: python app.py
Öffnen: http://localhost:5000
"""

import sys
import os
from pathlib import Path

# Parent-Ordner für db.py Import
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from functools import wraps
from db import get_connection
from auth import User

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

# Flask-Login Setup
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Bitte melden Sie sich an, um diese Seite zu sehen.'

@login_manager.user_loader
def load_user(user_id):
    return User.get_by_id(int(user_id))


def admin_required(f):
    """Decorator: Erfordert Admin-Rechte."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin():
            flash('Sie benötigen Admin-Rechte für diese Aktion.', 'error')
            return redirect(url_for('home'))
        return f(*args, **kwargs)
    return decorated_function


# =============================================================================
# Custom Jinja2 Filter: Deutsche Zahlenformatierung
# =============================================================================

def format_de(value, decimals=1):
    """Formatiert Zahlen im deutschen Format (Punkt als 1000er-Trenner, Komma als Dezimal)."""
    if value is None:
        return '-'
    try:
        # Zahl formatieren mit gewünschten Nachkommastellen
        formatted = f"{value:,.{decimals}f}"
        # Englisch -> Deutsch: Erst Komma zu Temp, dann Punkt zu Komma, dann Temp zu Punkt
        formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        return formatted
    except (ValueError, TypeError):
        return '-'


def format_de_percent(value, decimals=1):
    """Formatiert Prozent im deutschen Format."""
    if value is None:
        return '-'
    try:
        formatted = f"{value:,.{decimals}f}"
        formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        return f"{formatted}%"
    except (ValueError, TypeError):
        return '-'


def format_de_billions(value, decimals=1):
    """Formatiert Milliarden-Werte im deutschen Format."""
    if value is None:
        return '-'
    try:
        billions = value / 1_000_000_000
        formatted = f"{billions:,.{decimals}f}"
        formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        return formatted
    except (ValueError, TypeError):
        return '-'


# Filter registrieren
app.jinja_env.filters['de'] = format_de
app.jinja_env.filters['de_percent'] = format_de_percent
app.jinja_env.filters['de_billions'] = format_de_billions


# =============================================================================
# Helper: Spalten-Konfiguration
# =============================================================================

def get_column_config(view_name: str, user_id: int) -> list[dict]:
    """Holt alle Spalten-Konfigurationen für eine View und User."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT column_key, source_table, display_name,
               sort_order, is_visible, column_group, format_type
        FROM analytics.user_column_settings
        WHERE view_name = %s AND user_id = %s
        ORDER BY sort_order
    """, (view_name, user_id))

    columns = cur.fetchall()
    cur.close()
    conn.close()
    return columns


def get_visible_columns(view_name: str, user_id: int) -> list[dict]:
    """Holt nur sichtbare Spalten für eine View und User."""
    return [c for c in get_column_config(view_name, user_id) if c['is_visible']]


def build_dynamic_query(view_name: str, user_id: int, where_clause: str = "", order_by: str = "ci.company_name"):
    """Baut dynamisch eine SQL-Query basierend auf konfigurierten Spalten."""
    columns = get_visible_columns(view_name, user_id)

    # Immer ISIN dabei
    select_parts = ['ci.isin']
    needs_live_metrics = False

    for col in columns:
        if col['source_table'] == 'company_info':
            select_parts.append(f"ci.{col['column_key']}")
        else:
            select_parts.append(f"lm.{col['column_key']}")
            needs_live_metrics = True

    # Watchlist-Felder immer dabei
    select_parts.extend(['uw.favorite', 'uw.notes'])

    select_clause = ", ".join(select_parts)

    query = f"""
        SELECT {select_clause}
        FROM analytics.company_info ci
        LEFT JOIN analytics.user_watchlist uw ON (ci.isin = uw.isin AND uw.user_id = %s)
    """

    if needs_live_metrics:
        query += "LEFT JOIN analytics.live_metrics lm ON ci.isin = lm.isin\n"

    if where_clause:
        query += f"WHERE {where_clause}\n"

    query += f"ORDER BY {order_by}"

    return query, columns


# =============================================================================
# Routen
# =============================================================================

@app.route("/")
@login_required
def home():
    """Startseite - Weiterleitung zur Watchlist."""
    return render_template("home.html")


def get_visible_favorites(user_id: int) -> list[int]:
    """Holt die sichtbaren Favoriten-IDs aus dem Filter für einen User."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT favorite_id
        FROM analytics.user_favorite_filter
        WHERE user_id = %s AND is_visible = TRUE
        ORDER BY favorite_id
    """, (user_id,))
    visible = [row['favorite_id'] for row in cur.fetchall()]

    cur.close()
    conn.close()
    return visible if visible else [1, 2, 3, 4, 5, 6, 7, 8, 9]


def get_favorite_labels(user_id: int) -> dict:
    """Holt die Favoriten-Labels für einen User."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT favorite_id, label
        FROM analytics.user_favorite_labels
        WHERE user_id = %s
        ORDER BY favorite_id
    """, (user_id,))
    labels = {row['favorite_id']: row['label'] for row in cur.fetchall()}

    cur.close()
    conn.close()
    return labels


@app.route("/watchlist")
@login_required
def watchlist():
    """Watchlist-Seite mit Favoriten."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    user_id = current_user.id

    # Sichtbare Favoriten aus Filter holen
    visible_favorites = get_visible_favorites(user_id)
    placeholders = ','.join(['%s'] * len(visible_favorites))
    where_clause = f'uw.favorite IN ({placeholders})'

    # Dynamische Query basierend auf Spalten-Konfiguration
    query, columns = build_dynamic_query(
        view_name='watchlist',
        user_id=user_id,
        where_clause=where_clause,
        order_by='uw.favorite ASC, ci.company_name ASC'
    )

    # user_id ist bereits im JOIN eingebaut, daher als erstes Param übergeben
    cur.execute(query, [user_id] + visible_favorites)
    stocks = cur.fetchall()

    cur.close()
    conn.close()

    # Labels für die Legende
    favorite_labels = get_favorite_labels(user_id)

    return render_template("watchlist.html",
                           stocks=stocks,
                           columns=columns,
                           favorite_labels=favorite_labels,
                           visible_favorites=visible_favorites)


@app.route("/screener")
@login_required
def screener():
    """Aktien-Screener mit allen Aktien."""
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    user_id = current_user.id

    # Dynamische Query basierend auf Spalten-Konfiguration
    query, columns = build_dynamic_query(
        view_name='screener',
        user_id=user_id,
        order_by='ci.company_name ASC'
    )
    query += " LIMIT 100"

    cur.execute(query, [user_id])
    stocks = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("screener.html", stocks=stocks, columns=columns)


@app.route("/api/note", methods=["POST"])
@login_required
def update_note():
    """API: Notiz aktualisieren."""
    data = request.json
    isin = data.get("isin")
    notes = data.get("notes")

    if not isin:
        return jsonify({"error": "ISIN fehlt"}), 400

    user_id = current_user.id
    conn = get_connection()
    cur = conn.cursor()

    # INSERT ON DUPLICATE KEY UPDATE für User-spezifische Daten
    cur.execute("""
        INSERT INTO analytics.user_watchlist (user_id, isin, notes)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE notes = %s, updated_at = NOW()
    """, (user_id, isin, notes, notes))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"success": True})


@app.route("/api/favorite", methods=["POST"])
@login_required
def update_favorite():
    """API: Favoriten-Status aktualisieren."""
    data = request.json
    isin = data.get("isin")
    favorite = data.get("favorite", 0)

    if not isin:
        return jsonify({"error": "ISIN fehlt"}), 400

    user_id = current_user.id
    conn = get_connection()
    cur = conn.cursor()

    # INSERT ON DUPLICATE KEY UPDATE für User-spezifische Daten
    cur.execute("""
        INSERT INTO analytics.user_watchlist (user_id, isin, favorite)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE favorite = %s, updated_at = NOW()
    """, (user_id, isin, favorite, favorite))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"success": True})


@app.route("/api/favorite-settings")
@login_required
def get_favorite_settings():
    """API: Favoriten-Labels und Filter abrufen."""
    user_id = current_user.id
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # Labels abrufen
    cur.execute("""
        SELECT favorite_id, label
        FROM analytics.user_favorite_labels
        WHERE user_id = %s
        ORDER BY favorite_id
    """, (user_id,))
    labels = {row['favorite_id']: row['label'] for row in cur.fetchall()}

    # Filter abrufen
    cur.execute("""
        SELECT favorite_id, is_visible
        FROM analytics.user_favorite_filter
        WHERE user_id = %s
        ORDER BY favorite_id
    """, (user_id,))
    filters = {row['favorite_id']: bool(row['is_visible']) for row in cur.fetchall()}

    cur.close()
    conn.close()

    return jsonify({
        "labels": labels,
        "filters": filters
    })


@app.route("/api/favorite-settings", methods=["POST"])
@login_required
def update_favorite_settings():
    """API: Favoriten-Labels und Filter aktualisieren."""
    user_id = current_user.id
    data = request.json
    labels = data.get("labels", {})
    filters = data.get("filters", {})

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Labels aktualisieren
        for fav_id, label in labels.items():
            cur.execute("""
                UPDATE analytics.user_favorite_labels
                SET label = %s, updated_at = NOW()
                WHERE user_id = %s AND favorite_id = %s
            """, (label, user_id, int(fav_id)))

        # Filter aktualisieren
        for fav_id, is_visible in filters.items():
            cur.execute("""
                UPDATE analytics.user_favorite_filter
                SET is_visible = %s, updated_at = NOW()
                WHERE user_id = %s AND favorite_id = %s
            """, (is_visible, user_id, int(fav_id)))

        conn.commit()
        return jsonify({"success": True})

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        cur.close()
        conn.close()


@app.route("/api/filter-options")
@login_required
def get_filter_options():
    """API: Filter-Optionen für den Screener."""
    user_id = current_user.id
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # Kategorische Filter aus company_info
    categorical = {}
    for field in ['stock_index', 'sector', 'industry', 'country']:
        cur.execute(f"""
            SELECT DISTINCT {field} as value
            FROM analytics.company_info
            WHERE {field} IS NOT NULL AND {field} != ''
            ORDER BY {field}
        """)
        categorical[field] = [row['value'] for row in cur.fetchall()]

    # Numerische Filter aus live_metrics (alle Spalten außer isin, ticker, price_date)
    cur.execute("""
        SELECT column_key, display_name, column_group, format_type
        FROM analytics.user_column_settings
        WHERE view_name = 'screener'
          AND user_id = %s
          AND source_table = 'live_metrics'
          AND column_key NOT IN ('isin', 'ticker', 'price_date')
        ORDER BY sort_order
    """, (user_id,))
    numeric_columns = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        "categorical": categorical,
        "numeric": numeric_columns
    })


@app.route("/api/screener/filter", methods=["POST"])
@login_required
def filter_screener():
    """API: Gefilterte Screener-Daten."""
    user_id = current_user.id
    data = request.json or {}
    filters = data.get("filters", {})

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # WHERE-Klauseln aufbauen
    where_parts = []
    params = [user_id]  # Erstes Param ist user_id für JOIN

    # Suchfeld (ticker, isin, company_name)
    search = filters.get("search", "").strip()
    if search:
        where_parts.append("(ci.ticker LIKE %s OR ci.isin LIKE %s OR ci.company_name LIKE %s)")
        search_pattern = f"%{search}%"
        params.extend([search_pattern, search_pattern, search_pattern])

    # Kategorische Filter (company_info)
    for field in ['stock_index', 'sector', 'industry', 'country']:
        if field in filters and filters[field]:
            where_parts.append(f"ci.{field} = %s")
            params.append(filters[field])

    # Numerische Filter (live_metrics)
    numeric_filters = filters.get("numeric", [])
    for nf in numeric_filters:
        col = nf.get("column")
        op = nf.get("operator")
        val = nf.get("value")

        if not col or not op or val is None:
            continue

        # Operator validieren
        valid_ops = {"<": "<", ">": ">", "<=": "<=", ">=": ">=", "=": "="}
        if op not in valid_ops:
            continue

        # Spaltenname validieren (nur alphanumerisch und Unterstriche)
        if not col.replace('_', '').isalnum():
            continue

        where_parts.append(f"lm.{col} {valid_ops[op]} %s")
        params.append(float(val))

    where_clause = " AND ".join(where_parts) if where_parts else ""

    # Query aufbauen
    query, columns = build_dynamic_query(
        view_name='screener',
        user_id=user_id,
        where_clause=where_clause,
        order_by='ci.company_name ASC'
    )
    query += " LIMIT 500"

    cur.execute(query, params)
    stocks = cur.fetchall()

    cur.close()
    conn.close()

    # Daten formatieren für JSON
    result = []
    for stock in stocks:
        row = {}
        for key, value in stock.items():
            if value is None:
                row[key] = None
            elif isinstance(value, (int, float)):
                row[key] = value
            else:
                row[key] = str(value)
        result.append(row)

    return jsonify({
        "stocks": result,
        "count": len(result),
        "columns": columns
    })


@app.route("/api/columns/<view_name>")
@login_required
def get_columns(view_name):
    """API: Alle Spalten für eine View abrufen."""
    if view_name not in ('watchlist', 'screener'):
        return jsonify({"error": "Ungültige View"}), 400

    user_id = current_user.id
    columns = get_column_config(view_name, user_id)

    # Nach Gruppen sortieren für bessere Darstellung
    groups = {}
    for col in columns:
        group = col['column_group'] or 'Sonstige'
        if group not in groups:
            groups[group] = []
        groups[group].append(col)

    return jsonify({
        "columns": columns,
        "groups": groups
    })


@app.route("/api/stock/<isin>/details")
@login_required
def get_stock_details(isin):
    """
    API: Detaildaten für eine Aktie (Modal-Ansicht).

    Liefert:
    - Stammdaten (Name, Sector, etc.)
    - TTM PE Berechnung mit allen Komponenten
    - KGV-Verlauf der letzten 20 Jahre (FY + aktuelles TTM)
    - Income Statement der letzten 10 Jahre
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        # 1. Stammdaten aus company_info
        cur.execute("""
            SELECT isin, ticker, company_name, sector, industry, country, currency, fiscal_year_end
            FROM analytics.company_info
            WHERE isin = %s
        """, (isin,))
        company = cur.fetchone()

        if not company:
            return jsonify({"error": "Aktie nicht gefunden"}), 404

        # 2. Aktuelle Kennzahlen aus live_metrics (inkl. alle PE-, EV/EBIT-, CAGR- und Margen-Werte)
        cur.execute("""
            SELECT market_cap, price, price_date,
                   ttm_pe, fy_pe,
                   pe_avg_5y, pe_avg_10y, pe_avg_15y, pe_avg_20y, pe_avg_10y_2019,
                   pe_avg_5y_count, pe_avg_10y_count, pe_avg_15y_count, pe_avg_20y_count,
                   yf_ttm_pe, yf_forward_pe,
                   ttm_ev_ebit, fy_ev_ebit,
                   ev_ebit_avg_5y, ev_ebit_avg_10y, ev_ebit_avg_15y, ev_ebit_avg_20y, ev_ebit_avg_10y_2019,
                   ev_ebit_avg_5y_count, ev_ebit_avg_10y_count, ev_ebit_avg_15y_count, ev_ebit_avg_20y_count,
                   revenue_cagr_3y, revenue_cagr_5y, revenue_cagr_10y,
                   ebit_cagr_3y, ebit_cagr_5y, ebit_cagr_10y,
                   net_income_cagr_3y, net_income_cagr_5y, net_income_cagr_10y,
                   profit_margin, operating_margin,
                   profit_margin_avg_3y, profit_margin_avg_5y, profit_margin_avg_10y, profit_margin_avg_5y_2019,
                   operating_margin_avg_3y, operating_margin_avg_5y, operating_margin_avg_10y, operating_margin_avg_5y_2019,
                   yf_ttm_pe_vs_avg_5y, yf_ttm_pe_vs_avg_10y, yf_ttm_pe_vs_avg_15y, yf_ttm_pe_vs_avg_20y, yf_ttm_pe_vs_avg_10y_2019,
                   yf_fwd_pe_vs_avg_5y, yf_fwd_pe_vs_avg_10y, yf_fwd_pe_vs_avg_15y, yf_fwd_pe_vs_avg_20y, yf_fwd_pe_vs_avg_10y_2019,
                   ev_ebit_vs_avg_5y, ev_ebit_vs_avg_10y, ev_ebit_vs_avg_15y, ev_ebit_vs_avg_20y, ev_ebit_vs_avg_10y_2019
            FROM analytics.live_metrics
            WHERE isin = %s
        """, (isin,))
        live = cur.fetchone() or {}

        # 3. EV-Komponenten aus dem letzten FY-Eintrag holen
        cur.execute("""
            SELECT market_cap, net_debt, minority_interest
            FROM analytics.fmp_filtered_numbers
            WHERE isin = %s AND period = 'FY'
            ORDER BY date DESC
            LIMIT 1
        """, (isin,))
        ev_components = cur.fetchone() or {}

        # 4. TTM-Berechnung: Quartale ODER Halbjahre aus fmp_filtered_numbers
        cur.execute("""
            SELECT period, date, net_income, revenue, gross_profit, operating_income
            FROM analytics.fmp_filtered_numbers
            WHERE isin = %s AND period != 'FY'
              AND date >= DATE_SUB(CURDATE(), INTERVAL 15 MONTH)
            ORDER BY date DESC
        """, (isin,))
        all_periods = cur.fetchall()

        # Prüfe ob Quartals- oder Halbjahresberichterstatter
        period_types = set(p['period'] for p in all_periods)
        is_semiannual = period_types.issubset({'Q2', 'Q4', 'H1', 'H2'})

        # TTM-Werte berechnen
        ttm_net_income = None
        ttm_revenue = None
        ttm_gross_profit = None
        ttm_operating_income = None
        periods_for_display = []

        if is_semiannual and len(all_periods) >= 2:
            # Halbjahresberichterstatter: 2 Halbjahre
            periods_for_display = all_periods[:2]
        elif len(all_periods) >= 4:
            # Quartalsberichterstatter: 4 Quartale
            periods_for_display = all_periods[:4]

        if periods_for_display:
            # Net Income
            ni_values = [p['net_income'] for p in periods_for_display if p['net_income'] is not None]
            if len(ni_values) == len(periods_for_display):
                ttm_net_income = sum(ni_values)

            # Revenue
            rev_values = [p['revenue'] for p in periods_for_display if p['revenue'] is not None]
            if len(rev_values) == len(periods_for_display):
                ttm_revenue = sum(rev_values)

            # Gross Profit
            gp_values = [p['gross_profit'] for p in periods_for_display if p['gross_profit'] is not None]
            if len(gp_values) == len(periods_for_display):
                ttm_gross_profit = sum(gp_values)

            # Operating Income
            oi_values = [p['operating_income'] for p in periods_for_display if p['operating_income'] is not None]
            if len(oi_values) == len(periods_for_display):
                ttm_operating_income = sum(oi_values)

        # Perioden für Anzeige formatieren (ältestes zuerst)
        quarters_display = []
        quarters_ebit_display = []
        for q in reversed(periods_for_display):
            quarters_display.append({
                "period": q['period'],
                "date": q['date'].strftime('%Y-%m-%d') if q['date'] else None,
                "net_income": q['net_income']
            })
            quarters_ebit_display.append({
                "period": q['period'],
                "date": q['date'].strftime('%Y-%m-%d') if q['date'] else None,
                "operating_income": q['operating_income']
            })

        # 5. KGV-Verlauf: Letzte 20 FY-Jahre aus calcu_numbers
        cur.execute("""
            SELECT YEAR(date) as year, fy_pe
            FROM analytics.calcu_numbers
            WHERE isin = %s AND period = 'FY' AND fy_pe IS NOT NULL
            ORDER BY date DESC
            LIMIT 20
        """, (isin,))
        pe_history_raw = cur.fetchall()

        # In chronologischer Reihenfolge (ältestes zuerst)
        pe_history = []
        for row in reversed(pe_history_raw):
            pe_history.append({
                "year": row['year'],
                "pe": round(row['fy_pe'], 2) if row['fy_pe'] else None
            })

        # Aktuelles TTM PE ans Ende anhängen (falls vorhanden)
        current_ttm_pe = live.get('ttm_pe')

        # 6. EV/EBIT-Verlauf: Letzte 20 FY-Jahre aus calcu_numbers
        cur.execute("""
            SELECT YEAR(date) as year, fy_ev_ebit
            FROM analytics.calcu_numbers
            WHERE isin = %s AND period = 'FY' AND fy_ev_ebit IS NOT NULL
            ORDER BY date DESC
            LIMIT 20
        """, (isin,))
        ev_ebit_history_raw = cur.fetchall()

        # In chronologischer Reihenfolge (ältestes zuerst)
        ev_ebit_history = []
        for row in reversed(ev_ebit_history_raw):
            ev_ebit_history.append({
                "year": row['year'],
                "ev_ebit": round(row['fy_ev_ebit'], 2) if row['fy_ev_ebit'] else None
            })

        current_ttm_ev_ebit = live.get('ttm_ev_ebit')

        # 7. Income Statement: Letzte 10 FY-Jahre aus fmp_filtered_numbers
        cur.execute("""
            SELECT YEAR(date) as year, revenue, gross_profit, operating_income, net_income
            FROM analytics.fmp_filtered_numbers
            WHERE isin = %s AND period = 'FY'
            ORDER BY date DESC
            LIMIT 10
        """, (isin,))
        income_raw = cur.fetchall()

        # In chronologischer Reihenfolge (ältestes zuerst)
        income_statement = []
        for row in reversed(income_raw):
            income_statement.append({
                "year": row['year'],
                "revenue": row['revenue'],
                "gross_profit": row['gross_profit'],
                "operating_income": row['operating_income'],
                "net_income": row['net_income']
            })

        # Response zusammenbauen
        result = {
            "company": {
                "isin": company['isin'],
                "ticker": company['ticker'],
                "name": company['company_name'],
                "sector": company['sector'],
                "industry": company['industry'],
                "country": company['country'],
                "currency": company['currency']
            },
            "current": {
                "market_cap": live.get('market_cap'),
                "price": live.get('price'),
                "price_date": live.get('price_date').strftime('%Y-%m-%d') if live.get('price_date') else None,
                "ttm_pe": round(current_ttm_pe, 2) if current_ttm_pe else None
            },
            "pe_overview": {
                "ttm_pe": round(live.get('ttm_pe'), 2) if live.get('ttm_pe') else None,
                "fy_pe": round(live.get('fy_pe'), 2) if live.get('fy_pe') else None,
                "pe_avg_5y": round(live.get('pe_avg_5y'), 2) if live.get('pe_avg_5y') else None,
                "pe_avg_10y": round(live.get('pe_avg_10y'), 2) if live.get('pe_avg_10y') else None,
                "pe_avg_15y": round(live.get('pe_avg_15y'), 2) if live.get('pe_avg_15y') else None,
                "pe_avg_20y": round(live.get('pe_avg_20y'), 2) if live.get('pe_avg_20y') else None,
                "pe_avg_10y_2019": round(live.get('pe_avg_10y_2019'), 2) if live.get('pe_avg_10y_2019') else None,
                "pe_avg_5y_count": live.get('pe_avg_5y_count'),
                "pe_avg_10y_count": live.get('pe_avg_10y_count'),
                "pe_avg_15y_count": live.get('pe_avg_15y_count'),
                "pe_avg_20y_count": live.get('pe_avg_20y_count'),
                "yf_ttm_pe": round(live.get('yf_ttm_pe'), 2) if live.get('yf_ttm_pe') else None,
                "yf_forward_pe": round(live.get('yf_forward_pe'), 2) if live.get('yf_forward_pe') else None,
                # Abweichungen TTM-KGV vs. Durchschnitte
                "yf_ttm_pe_vs_avg_5y": round(live.get('yf_ttm_pe_vs_avg_5y'), 1) if live.get('yf_ttm_pe_vs_avg_5y') else None,
                "yf_ttm_pe_vs_avg_10y": round(live.get('yf_ttm_pe_vs_avg_10y'), 1) if live.get('yf_ttm_pe_vs_avg_10y') else None,
                "yf_ttm_pe_vs_avg_15y": round(live.get('yf_ttm_pe_vs_avg_15y'), 1) if live.get('yf_ttm_pe_vs_avg_15y') else None,
                "yf_ttm_pe_vs_avg_20y": round(live.get('yf_ttm_pe_vs_avg_20y'), 1) if live.get('yf_ttm_pe_vs_avg_20y') else None,
                "yf_ttm_pe_vs_avg_10y_2019": round(live.get('yf_ttm_pe_vs_avg_10y_2019'), 1) if live.get('yf_ttm_pe_vs_avg_10y_2019') else None,
                # Abweichungen Forward-KGV vs. Durchschnitte
                "yf_fwd_pe_vs_avg_5y": round(live.get('yf_fwd_pe_vs_avg_5y'), 1) if live.get('yf_fwd_pe_vs_avg_5y') else None,
                "yf_fwd_pe_vs_avg_10y": round(live.get('yf_fwd_pe_vs_avg_10y'), 1) if live.get('yf_fwd_pe_vs_avg_10y') else None,
                "yf_fwd_pe_vs_avg_15y": round(live.get('yf_fwd_pe_vs_avg_15y'), 1) if live.get('yf_fwd_pe_vs_avg_15y') else None,
                "yf_fwd_pe_vs_avg_20y": round(live.get('yf_fwd_pe_vs_avg_20y'), 1) if live.get('yf_fwd_pe_vs_avg_20y') else None,
                "yf_fwd_pe_vs_avg_10y_2019": round(live.get('yf_fwd_pe_vs_avg_10y_2019'), 1) if live.get('yf_fwd_pe_vs_avg_10y_2019') else None
            },
            "ev_ebit_overview": {
                "ttm_ev_ebit": round(live.get('ttm_ev_ebit'), 2) if live.get('ttm_ev_ebit') else None,
                "fy_ev_ebit": round(live.get('fy_ev_ebit'), 2) if live.get('fy_ev_ebit') else None,
                "ev_ebit_avg_5y": round(live.get('ev_ebit_avg_5y'), 2) if live.get('ev_ebit_avg_5y') else None,
                "ev_ebit_avg_10y": round(live.get('ev_ebit_avg_10y'), 2) if live.get('ev_ebit_avg_10y') else None,
                "ev_ebit_avg_15y": round(live.get('ev_ebit_avg_15y'), 2) if live.get('ev_ebit_avg_15y') else None,
                "ev_ebit_avg_20y": round(live.get('ev_ebit_avg_20y'), 2) if live.get('ev_ebit_avg_20y') else None,
                "ev_ebit_avg_10y_2019": round(live.get('ev_ebit_avg_10y_2019'), 2) if live.get('ev_ebit_avg_10y_2019') else None,
                "ev_ebit_avg_5y_count": live.get('ev_ebit_avg_5y_count'),
                "ev_ebit_avg_10y_count": live.get('ev_ebit_avg_10y_count'),
                "ev_ebit_avg_15y_count": live.get('ev_ebit_avg_15y_count'),
                "ev_ebit_avg_20y_count": live.get('ev_ebit_avg_20y_count'),
                # Abweichungen EV/EBIT vs. Durchschnitte
                "ev_ebit_vs_avg_5y": round(live.get('ev_ebit_vs_avg_5y'), 1) if live.get('ev_ebit_vs_avg_5y') else None,
                "ev_ebit_vs_avg_10y": round(live.get('ev_ebit_vs_avg_10y'), 1) if live.get('ev_ebit_vs_avg_10y') else None,
                "ev_ebit_vs_avg_15y": round(live.get('ev_ebit_vs_avg_15y'), 1) if live.get('ev_ebit_vs_avg_15y') else None,
                "ev_ebit_vs_avg_20y": round(live.get('ev_ebit_vs_avg_20y'), 1) if live.get('ev_ebit_vs_avg_20y') else None,
                "ev_ebit_vs_avg_10y_2019": round(live.get('ev_ebit_vs_avg_10y_2019'), 1) if live.get('ev_ebit_vs_avg_10y_2019') else None
            },
            "growth_overview": {
                "revenue_cagr_3y": round(live.get('revenue_cagr_3y'), 1) if live.get('revenue_cagr_3y') else None,
                "revenue_cagr_5y": round(live.get('revenue_cagr_5y'), 1) if live.get('revenue_cagr_5y') else None,
                "revenue_cagr_10y": round(live.get('revenue_cagr_10y'), 1) if live.get('revenue_cagr_10y') else None,
                "ebit_cagr_3y": round(live.get('ebit_cagr_3y'), 1) if live.get('ebit_cagr_3y') else None,
                "ebit_cagr_5y": round(live.get('ebit_cagr_5y'), 1) if live.get('ebit_cagr_5y') else None,
                "ebit_cagr_10y": round(live.get('ebit_cagr_10y'), 1) if live.get('ebit_cagr_10y') else None,
                "net_income_cagr_3y": round(live.get('net_income_cagr_3y'), 1) if live.get('net_income_cagr_3y') else None,
                "net_income_cagr_5y": round(live.get('net_income_cagr_5y'), 1) if live.get('net_income_cagr_5y') else None,
                "net_income_cagr_10y": round(live.get('net_income_cagr_10y'), 1) if live.get('net_income_cagr_10y') else None
            },
            "margins_overview": {
                "profit_margin": round(live.get('profit_margin'), 1) if live.get('profit_margin') else None,
                "operating_margin": round(live.get('operating_margin'), 1) if live.get('operating_margin') else None,
                "profit_margin_avg_3y": round(live.get('profit_margin_avg_3y'), 1) if live.get('profit_margin_avg_3y') else None,
                "profit_margin_avg_5y": round(live.get('profit_margin_avg_5y'), 1) if live.get('profit_margin_avg_5y') else None,
                "profit_margin_avg_10y": round(live.get('profit_margin_avg_10y'), 1) if live.get('profit_margin_avg_10y') else None,
                "profit_margin_avg_5y_2019": round(live.get('profit_margin_avg_5y_2019'), 1) if live.get('profit_margin_avg_5y_2019') else None,
                "operating_margin_avg_3y": round(live.get('operating_margin_avg_3y'), 1) if live.get('operating_margin_avg_3y') else None,
                "operating_margin_avg_5y": round(live.get('operating_margin_avg_5y'), 1) if live.get('operating_margin_avg_5y') else None,
                "operating_margin_avg_10y": round(live.get('operating_margin_avg_10y'), 1) if live.get('operating_margin_avg_10y') else None,
                "operating_margin_avg_5y_2019": round(live.get('operating_margin_avg_5y_2019'), 1) if live.get('operating_margin_avg_5y_2019') else None
            },
            "ttm_calculation": {
                "market_cap": live.get('market_cap'),
                "ttm_net_income": ttm_net_income,
                "quarters": quarters_display
            },
            "ev_calculation": {
                "market_cap": live.get('market_cap'),
                "net_debt": ev_components.get('net_debt'),
                "minority_interest": ev_components.get('minority_interest'),
                "ttm_ebit": ttm_operating_income,
                "quarters": quarters_ebit_display
            },
            "ttm_income_statement": {
                "revenue": ttm_revenue,
                "gross_profit": ttm_gross_profit,
                "operating_income": ttm_operating_income,
                "net_income": ttm_net_income
            },
            "pe_history": pe_history,
            "current_ttm_pe": round(current_ttm_pe, 2) if current_ttm_pe else None,
            "ev_ebit_history": ev_ebit_history,
            "current_ttm_ev_ebit": round(current_ttm_ev_ebit, 2) if current_ttm_ev_ebit else None,
            "income_statement": income_statement
        }

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cur.close()
        conn.close()


@app.route("/api/columns/<view_name>", methods=["POST"])
@login_required
def update_columns(view_name):
    """API: Spalten-Konfiguration aktualisieren."""
    if view_name not in ('watchlist', 'screener'):
        return jsonify({"error": "Ungültige View"}), 400

    user_id = current_user.id
    data = request.json
    columns = data.get("columns", [])

    if not columns:
        return jsonify({"error": "Keine Spalten angegeben"}), 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        for col in columns:
            cur.execute("""
                UPDATE analytics.user_column_settings
                SET is_visible = %s, sort_order = %s
                WHERE user_id = %s AND view_name = %s AND column_key = %s
            """, (col['is_visible'], col['sort_order'], user_id, view_name, col['column_key']))

        conn.commit()
        return jsonify({"success": True})

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        cur.close()
        conn.close()


@app.route("/api/stock/<isin>/info")
@login_required
def get_stock_info(isin):
    """
    API: Unternehmensinformationen für Modal.

    Liefert:
    - Stammdaten (Name, Ticker, Sector, Country, etc.)
    - Beschreibung (bereits auf Deutsch übersetzt)
    - Fiskaljahr-Ende
    """
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT isin, ticker, company_name, sector, industry, country,
                   currency, description, fiscal_year_end, stock_index
            FROM analytics.company_info
            WHERE isin = %s
        """, (isin,))
        info = cur.fetchone()

        if not info:
            return jsonify({"error": "Aktie nicht gefunden"}), 404

        return jsonify(info)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


# =============================================================================
# Authentifizierungs-Routen
# =============================================================================

@app.route("/login", methods=["GET", "POST"])
def login():
    """Login-Seite."""
    if current_user.is_authenticated:
        return redirect(url_for('home'))

    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")

        user, password_hash = User.get_by_email(email)

        if user and User.verify_password(password, password_hash):
            if not user.can_login():
                if not user.is_approved:
                    flash('Ihr Account wurde noch nicht freigeschaltet. Bitte warten Sie auf die Admin-Freigabe.', 'warning')
                else:
                    flash('Ihr Account wurde deaktiviert. Bitte kontaktieren Sie den Administrator.', 'error')
                return redirect(url_for('login'))

            login_user(user)
            user.update_last_login()

            next_page = request.args.get('next')
            return redirect(next_page or url_for('home'))
        else:
            flash('Ungültige E-Mail oder Passwort.', 'error')

    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    """Registrierungs-Seite."""
    if current_user.is_authenticated:
        return redirect(url_for('home'))

    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")
        password_confirm = request.form.get("password_confirm")
        first_name = request.form.get("first_name")
        last_name = request.form.get("last_name")

        # Validierung
        if not all([email, password, password_confirm, first_name, last_name]):
            flash('Bitte füllen Sie alle Felder aus.', 'error')
            return render_template("register.html")

        if password != password_confirm:
            flash('Passwörter stimmen nicht überein.', 'error')
            return render_template("register.html")

        if len(password) < 8:
            flash('Passwort muss mindestens 8 Zeichen lang sein.', 'error')
            return render_template("register.html")

        # E-Mail-Duplikat prüfen
        existing_user, _ = User.get_by_email(email)
        if existing_user:
            flash('Diese E-Mail ist bereits registriert.', 'error')
            return render_template("register.html")

        try:
            User.create_user(email, password, first_name, last_name)
            flash('Registrierung erfolgreich! Ihr Account wird vom Administrator geprüft.', 'success')
            return redirect(url_for('login'))
        except Exception as e:
            flash(f'Fehler bei der Registrierung: {str(e)}', 'error')
            return render_template("register.html")

    return render_template("register.html")


@app.route("/logout")
@login_required
def logout():
    """Logout."""
    logout_user()
    flash('Sie wurden erfolgreich abgemeldet.', 'success')
    return redirect(url_for('login'))


# =============================================================================
# Admin-Routen
# =============================================================================

@app.route("/admin/users")
@login_required
@admin_required
def admin_users():
    """Admin-Panel für Benutzerverwaltung."""
    conn = get_connection('analytics')
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT id, email, first_name, last_name, role, is_approved, is_active,
               created_at, last_login
        FROM users
        ORDER BY created_at DESC
    """)
    users = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("admin_users.html", users=users)


@app.route("/admin/users/<int:user_id>/approve", methods=["POST"])
@login_required
@admin_required
def admin_approve_user(user_id):
    """User freigeben."""
    conn = get_connection('analytics')
    cur = conn.cursor()

    cur.execute("UPDATE users SET is_approved = TRUE WHERE id = %s", (user_id,))
    conn.commit()

    cur.close()
    conn.close()

    flash('Benutzer wurde freigeschaltet.', 'success')
    return redirect(url_for('admin_users'))


@app.route("/admin/users/<int:user_id>/toggle-active", methods=["POST"])
@login_required
@admin_required
def admin_toggle_active(user_id):
    """User aktiv/inaktiv setzen."""
    conn = get_connection('analytics')
    cur = conn.cursor()

    cur.execute("UPDATE users SET is_active = NOT is_active WHERE id = %s", (user_id,))
    conn.commit()

    cur.close()
    conn.close()

    flash('Benutzer-Status wurde geändert.', 'success')
    return redirect(url_for('admin_users'))


@app.route("/admin/users/<int:user_id>/make-admin", methods=["POST"])
@login_required
@admin_required
def admin_make_admin(user_id):
    """User zum Admin machen."""
    conn = get_connection('analytics')
    cur = conn.cursor()

    cur.execute("UPDATE users SET role = 'admin' WHERE id = %s", (user_id,))
    conn.commit()

    cur.close()
    conn.close()

    flash('Benutzer wurde zum Administrator gemacht.', 'success')
    return redirect(url_for('admin_users'))


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("=" * 50)
    print("Starte Aktien-Webserver...")
    print("Öffne: http://localhost:5001")
    print("Beenden mit: Ctrl+C")
    print("=" * 50)
    app.run(debug=True, host="0.0.0.0", port=5001)
