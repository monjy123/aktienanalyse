#!/usr/bin/env python3
"""
Hybrid Earnings Fetcher
=======================
1. Holt heutige/gestrige Earnings aus earnings_calendar
2. Primär: yfinance für ~90% der Unternehmen
3. Fallback: Liste für manuelle Claude-Suche (die restlichen ~10%)
4. Speichert actual_value in analyst_estimates

Nutzung:
    python 03_fetch_actual_earnings.py              # Heute
    python 03_fetch_actual_earnings.py --yesterday  # Gestern (für US nachbörslich)
    python 03_fetch_actual_earnings.py --days 2     # Letzte 2 Tage
"""

import argparse
import sys
import subprocess
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal

# Füge Projektroot zum Pfad hinzu
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import yfinance as yf
from db import get_connection


def get_earnings_for_date_range(start_date, end_date):
    """Hole Earnings aus earnings_calendar für Datumsbereich."""
    conn = get_connection('analytics')
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT isin, period, release_date, event_type
        FROM earnings_calendar
        WHERE release_date BETWEEN %s AND %s
        AND event_type = 'earnings'
        ORDER BY release_date
    """, (start_date, end_date))

    results = cur.fetchall()
    conn.close()
    return results


def get_ticker_mapping(isins):
    """Hole yf_ticker für ISINs aus tickerlist."""
    if not isins:
        return {}

    conn = get_connection('ticker')
    cur = conn.cursor(dictionary=True)

    placeholders = ','.join(['%s'] * len(isins))
    cur.execute(f"""
        SELECT isin, yf_ticker, name
        FROM tickerlist
        WHERE isin IN ({placeholders})
    """, tuple(isins))

    mapping = {row['isin']: row for row in cur.fetchall()}
    conn.close()
    return mapping


def parse_period_to_quarter_end(period):
    """
    Konvertiere Period (z.B. 'Q4 2025') zu Quartalsende-Datum.

    Q1 -> 31.03, Q2 -> 30.06, Q3 -> 30.09, Q4 -> 31.12
    Für Fiscal Years die vom Kalenderjahr abweichen (z.B. Apple Q1 2026 = Okt-Dez 2025)
    """
    parts = period.split()
    if len(parts) != 2:
        return None

    quarter, year = parts[0], int(parts[1])

    quarter_ends = {
        'Q1': (3, 31),
        'Q2': (6, 30),
        'Q3': (9, 30),
        'Q4': (12, 31),
        'FY': (12, 31)
    }

    if quarter not in quarter_ends:
        return None

    month, day = quarter_ends[quarter]
    return datetime(year, month, day).date()


def fetch_yfinance_earnings(ticker_symbol):
    """Hole earnings_history von yfinance."""
    try:
        ticker = yf.Ticker(ticker_symbol)
        eh = ticker.earnings_history

        if eh is None or eh.empty:
            return None

        if 'epsActual' not in eh.columns:
            return None

        # Konvertiere zu Liste von Dicts
        results = []
        for quarter_date, row in eh.iterrows():
            results.append({
                'quarter_end': quarter_date.date() if hasattr(quarter_date, 'date') else quarter_date,
                'eps_actual': float(row['epsActual']) if row['epsActual'] is not None else None,
                'eps_estimate': float(row['epsEstimate']) if row['epsEstimate'] is not None else None,
                'surprise_pct': float(row['surprisePercent']) if row['surprisePercent'] is not None else None
            })

        return results

    except Exception as e:
        print(f"    yfinance Fehler für {ticker_symbol}: {e}")
        return None


def find_matching_quarter(yf_earnings, target_period):
    """
    Finde das passende Quartal in den yfinance-Daten.

    Problem: Fiscal Year kann vom Kalenderjahr abweichen.
    Lösung: Suche das nächstliegende Quartal zum target_period.
    """
    if not yf_earnings:
        return None

    target_date = parse_period_to_quarter_end(target_period)
    if not target_date:
        return None

    # Suche exaktes Match oder nächstes Quartal (±45 Tage Toleranz für Fiscal Year Unterschiede)
    best_match = None
    min_diff = timedelta(days=45)

    for entry in yf_earnings:
        if entry['quarter_end'] is None:
            continue

        diff = abs(entry['quarter_end'] - target_date)
        if diff <= min_diff:
            min_diff = diff
            best_match = entry

    return best_match


def upsert_actual_earnings(isin, period, eps_actual, quarter_end_date=None, source='yfinance'):
    """
    Insert oder Update actual_value in analyst_estimates.

    Verwendet INSERT ... ON DUPLICATE KEY UPDATE um:
    - Neue Einträge zu erstellen falls nicht vorhanden
    - Bestehende Einträge zu updaten falls vorhanden
    """
    conn = get_connection('analytics')
    cur = conn.cursor()

    # Bestimme period_type
    if period.startswith('FY'):
        period_type = 'fiscal_year'
    else:
        period_type = 'quarter'

    # INSERT mit ON DUPLICATE KEY UPDATE
    cur.execute("""
        INSERT INTO analyst_estimates
            (isin, period, period_type, period_end_date, metric, actual_value, source)
        VALUES
            (%s, %s, %s, %s, 'eps', %s, %s)
        ON DUPLICATE KEY UPDATE
            actual_value = COALESCE(VALUES(actual_value), actual_value),
            period_end_date = COALESCE(VALUES(period_end_date), period_end_date),
            source = CASE
                WHEN source IS NULL THEN VALUES(source)
                WHEN source NOT LIKE CONCAT('%%', VALUES(source), '%%')
                    THEN CONCAT(source, '+', VALUES(source))
                ELSE source
            END,
            updated_at = CURRENT_TIMESTAMP
    """, (isin, period, period_type, quarter_end_date, eps_actual, source))

    affected = cur.rowcount
    conn.commit()
    conn.close()

    # rowcount: 1 = inserted, 2 = updated
    return affected > 0


def check_existing_actual(isin, period):
    """Prüfe ob actual_value bereits existiert."""
    conn = get_connection('analytics')
    cur = conn.cursor()

    cur.execute("""
        SELECT actual_value
        FROM analyst_estimates
        WHERE isin = %s AND period = %s AND metric = 'eps'
    """, (isin, period))

    row = cur.fetchone()
    conn.close()

    if row is None:
        return 'NO_ENTRY'  # Kein Eintrag vorhanden
    if row[0] is None:
        return None  # Eintrag vorhanden, aber actual_value ist NULL
    return row[0]  # actual_value hat einen Wert


def run_claude_fallback(fallback_list, dry_run=False):
    """
    Ruft Claude CLI auf um fehlende Earnings per WebSearch zu finden.

    Args:
        fallback_list: Liste von (isin, name, period, ticker) Tuples
        dry_run: Wenn True, keine DB-Änderungen

    Returns:
        Dict mit gefundenen Earnings
    """
    if not fallback_list:
        return {}

    print("\n" + "=" * 60)
    print("🤖 STARTE CLAUDE FALLBACK-SUCHE")
    print("=" * 60)

    # Batch-Größe: 5 Unternehmen pro Suche (um Tokens zu sparen)
    batch_size = 5
    results = {}

    for i in range(0, len(fallback_list), batch_size):
        batch = fallback_list[i:i + batch_size]

        # Erstelle Prompt
        companies_str = ", ".join([f"{name} {period}" for _, name, period, _ in batch])

        prompt = f'''Suche die aktuellen Earnings-Ergebnisse für diese Unternehmen:
{companies_str}

WICHTIG: Gib die Ergebnisse NUR in diesem exakten Format aus (eine Zeile pro Unternehmen):
FIRMENNAME|PERIOD|EPS_WERT

Beispiel:
Apple Inc|Q1 2026|2.67
Roche|Q4 2025|4.52

Nur Unternehmen auflisten für die du EPS-Daten gefunden hast. Keine weiteren Erklärungen.'''

        print(f"\n🔍 Batch {i//batch_size + 1}: {len(batch)} Unternehmen...")

        try:
            # Claude CLI aufrufen (-p für non-interactive, --allowedTools für WebSearch)
            result = subprocess.run(
                ['claude', '-p', prompt, '--allowedTools', 'WebSearch,WebFetch'],
                capture_output=True,
                text=True,
                timeout=180,
                cwd=str(PROJECT_ROOT)
            )

            if result.returncode != 0:
                print(f"    ⚠ Claude Fehler: {result.stderr[:200]}")
                continue

            # Parse Antwort
            response = result.stdout.strip()
            print(f"    Claude Antwort:\n{response[:500]}")

            # Extrahiere EPS-Werte aus der Antwort
            for line in response.split('\n'):
                line = line.strip()
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 3:
                        company_name = parts[0].strip()
                        period = parts[1].strip()
                        eps_str = parts[2].strip()

                        # Parse EPS (kann Komma oder Punkt haben)
                        try:
                            eps = float(eps_str.replace(',', '.').replace('$', '').replace('€', ''))

                            # Finde passende ISIN aus batch
                            for isin, name, p, ticker in batch:
                                if (company_name.lower() in name.lower() or
                                    name.lower() in company_name.lower()) and period == p:
                                    results[isin] = {
                                        'period': period,
                                        'eps_actual': eps,
                                        'name': name
                                    }
                                    print(f"    ✅ {name}: EPS = {eps}")
                                    break
                        except ValueError:
                            continue

        except subprocess.TimeoutExpired:
            print(f"    ⚠ Timeout bei Claude-Aufruf")
        except FileNotFoundError:
            print(f"    ⚠ Claude CLI nicht gefunden - überspringe Fallback")
            return {}
        except Exception as e:
            print(f"    ⚠ Fehler: {e}")

    # Speichere gefundene Ergebnisse in DB
    if results and not dry_run:
        print(f"\n💾 Speichere {len(results)} Fallback-Ergebnisse...")
        for isin, data in results.items():
            success = upsert_actual_earnings(
                isin,
                data['period'],
                data['eps_actual'],
                source='claude_websearch'
            )
            if success:
                print(f"    ✅ {data['name']}: gespeichert")

    return results


def main():
    parser = argparse.ArgumentParser(description='Fetch actual earnings via yfinance')
    parser.add_argument('--yesterday', action='store_true', help='Fetch yesterday\'s earnings (for US after-hours)')
    parser.add_argument('--days', type=int, default=1, help='Number of days to look back')
    parser.add_argument('--dry-run', action='store_true', help='Don\'t write to database')
    parser.add_argument('--no-fallback', action='store_true', help='Skip Claude fallback search')
    args = parser.parse_args()

    # Datumsbereich bestimmen
    end_date = datetime.now().date()
    if args.yesterday:
        end_date = end_date - timedelta(days=1)
        start_date = end_date
    else:
        start_date = end_date - timedelta(days=args.days - 1)

    print(f"🔍 Suche Earnings von {start_date} bis {end_date}")
    print("=" * 60)

    # 1. Hole Earnings aus DB
    earnings = get_earnings_for_date_range(start_date, end_date)
    print(f"📅 {len(earnings)} Earnings-Termine gefunden\n")

    if not earnings:
        print("Keine Earnings für diesen Zeitraum.")
        return

    # 2. Hole Ticker-Mapping
    isins = list(set(e['isin'] for e in earnings))
    ticker_map = get_ticker_mapping(isins)
    print(f"🎯 {len(ticker_map)} Ticker-Mappings gefunden\n")

    # 3. Verarbeite jedes Unternehmen
    stats = {
        'inserted': [],
        'updated': [],
        'already_exists': [],
        'no_yf_data': [],
        'no_ticker': [],
        'no_match': []
    }

    for earning in earnings:
        isin = earning['isin']
        period = earning['period']

        # Ticker vorhanden?
        if isin not in ticker_map:
            stats['no_ticker'].append((isin, period))
            continue

        ticker_info = ticker_map[isin]
        yf_ticker = ticker_info['yf_ticker']
        name = ticker_info['name']

        print(f"📊 {name[:30]:30} | {period:10} | {yf_ticker}")

        # Bereits actual_value vorhanden?
        existing = check_existing_actual(isin, period)
        if existing not in (None, 'NO_ENTRY'):
            print(f"    ✓ Bereits vorhanden: {existing}")
            stats['already_exists'].append((isin, name, period, existing))
            continue

        is_new_entry = (existing == 'NO_ENTRY')

        # yfinance Daten holen
        yf_earnings = fetch_yfinance_earnings(yf_ticker)

        if not yf_earnings:
            print(f"    ❌ Keine yfinance Daten")
            stats['no_yf_data'].append((isin, name, period, yf_ticker))
            continue

        # Passendes Quartal finden
        match = find_matching_quarter(yf_earnings, period)

        if not match or match['eps_actual'] is None:
            print(f"    ❌ Kein passendes Quartal gefunden")
            stats['no_match'].append((isin, name, period, yf_ticker))
            continue

        # Upsert DB
        eps_actual = match['eps_actual']
        quarter_end = match['quarter_end']
        action = "INSERT" if is_new_entry else "UPDATE"
        print(f"    ✅ EPS Actual: {eps_actual:.4f} (Quartal: {quarter_end}) [{action}]")

        if not args.dry_run:
            success = upsert_actual_earnings(isin, period, eps_actual, quarter_end)
            if success:
                if is_new_entry:
                    stats['inserted'].append((isin, name, period, eps_actual))
                else:
                    stats['updated'].append((isin, name, period, eps_actual))
            else:
                print(f"    ⚠ Upsert fehlgeschlagen")
        else:
            if is_new_entry:
                stats['inserted'].append((isin, name, period, eps_actual))
            else:
                stats['updated'].append((isin, name, period, eps_actual))

    # 4. Zusammenfassung
    print("\n" + "=" * 60)
    print("📈 ZUSAMMENFASSUNG")
    print("=" * 60)
    print(f"🆕 Neu eingefügt:        {len(stats['inserted'])}")
    print(f"✅ Aktualisiert:         {len(stats['updated'])}")
    print(f"✓  Bereits vorhanden:    {len(stats['already_exists'])}")
    print(f"❌ Keine yfinance Daten: {len(stats['no_yf_data'])}")
    print(f"❌ Kein Match:           {len(stats['no_match'])}")
    print(f"⚠  Kein Ticker:          {len(stats['no_ticker'])}")

    # 5. Fallback-Liste für Claude
    # Nur Unternehmen ohne yfinance Daten - "Kein Match" sind meist noch nicht veröffentlicht
    fallback_list = stats['no_yf_data']

    if fallback_list:
        print("\n" + "=" * 60)
        print(f"🤖 {len(fallback_list)} UNTERNEHMEN OHNE YFINANCE-DATEN")
        print("=" * 60)

        for isin, name, period, ticker in fallback_list:
            print(f"  - {name} ({ticker}): {period}")

        # Speichere Fallback-Liste
        fallback_file = f"/home/monjy/aktienanalyse/06_scrapers/fallback_earnings_{end_date}.txt"
        with open(fallback_file, 'w') as f:
            f.write(f"# Fallback Earnings Search - {end_date}\n")
            f.write(f"# Diese Unternehmen brauchen Claude-Suche\n\n")
            for isin, name, period, ticker in fallback_list:
                f.write(f"{isin}|{name}|{period}|{ticker}\n")

        # Automatisch Claude-Fallback ausführen
        if not args.no_fallback:
            fallback_results = run_claude_fallback(fallback_list, args.dry_run)

            if fallback_results:
                stats['fallback_found'] = len(fallback_results)
                print(f"\n✅ Claude Fallback: {len(fallback_results)} Earnings gefunden")
            else:
                print(f"\n⚠ Claude Fallback: Keine zusätzlichen Earnings gefunden")
        else:
            print(f"\n📄 Fallback-Liste gespeichert: {fallback_file}")
            print("   (Fallback übersprungen mit --no-fallback)")

    # Info über "Kein Match" - diese sind meist noch nicht veröffentlicht
    if stats['no_match']:
        print("\n" + "-" * 60)
        print(f"ℹ️  {len(stats['no_match'])} Unternehmen mit 'Kein Match' - vermutlich noch nicht veröffentlicht")
        print("   → Morgen früh nochmal das Skript ausführen für US-Earnings")
        print("-" * 60)


if __name__ == '__main__':
    main()
