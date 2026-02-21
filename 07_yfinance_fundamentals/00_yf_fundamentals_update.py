#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ergaenzt analytics.historical_fundamentals mit neuen Perioden aus yfinance.

- Laedt GuV, Bilanz und Cashflow pro Unternehmen
- Erkennt Halbjahresreporter (leere quarterly_financials) → nur FY
- Duplikaterkennung ueber (year, period) statt exaktem Datum
- INSERT ... ON DUPLICATE KEY UPDATE mit COALESCE (kein Ueberschreiben mit NULL)
- Preis-Enrichment (avg_price, price, market_cap) fuer neue Zeilen
"""

import sys
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import pandas as pd
from mysql.connector import Error as MySQLError
from db import get_connection

# ============================================================
# Spalten-Mapping: yfinance Label → DB-Spalte
# Mehrere Varianten pro Feld fuer yfinance-Versionsunterschiede
# ============================================================

YF_INCOME_MAPPING = {
    # (yfinance label, db column)
    'Total Revenue': 'revenue',
    'Cost Of Revenue': 'cost_of_revenue',
    'Gross Profit': 'gross_profit',
    'Research And Development': 'research_and_development_expenses',
    'General And Administrative Expense': 'general_and_administrative_expenses',
    'Selling And Marketing Expense': 'selling_and_marketing_expenses',
    'Selling General And Administration': 'selling_general_and_administrative_expenses',
    'Other Operating Expenses': 'other_expenses',
    'Operating Expense': 'operating_expenses',
    'Total Expenses': 'cost_and_expenses',
    'Operating Income': 'operating_income',
    'Interest Income': 'interest_income',
    'Interest Expense': 'interest_expense',
    'EBITDA': 'ebitda',
    'Other Income Expense': 'total_other_income_expenses_net',
    'Pretax Income': 'income_before_tax',
    'Tax Provision': 'income_tax_expense',
    'Net Income': 'net_income',
    'Basic EPS': 'eps',
    'Diluted EPS': 'eps_diluted',
    'Basic Average Shares': 'weighted_average_shs_out',
    'Diluted Average Shares': 'weighted_average_shs_out_dil',
}

YF_BALANCE_MAPPING = {
    'Total Assets': 'total_assets',
    'Total Liabilities Net Minority Interest': 'total_liabilities',
    'Total Equity Gross Minority Interest': 'total_equity',
    'Stockholders Equity': 'total_stockholders_equity',
    'Cash And Cash Equivalents': 'cash_and_cash_equivalents',
    'Goodwill': 'goodwill',
    'Current Debt': 'short_term_debt',
    'Long Term Debt': 'long_term_debt',
    'Total Debt': 'total_debt',
    'Net Debt': 'net_debt',
    'Minority Interest': 'minority_interest',
}

YF_CASHFLOW_MAPPING = {
    'Net Income From Continuing Operations': 'net_income_cf',
    'Depreciation And Amortization': 'depreciation_and_amortization_cf',
    'Stock Based Compensation': 'stock_based_compensation',
    'Capital Expenditure': 'capital_expenditure',
    'Free Cash Flow': 'free_cash_flow',
}

# Alle DB-Spalten, die wir befuellen
DB_COLUMNS = [
    'ticker', 'isin', 'stock_index', 'company_name', 'date', 'period',
    # Balance Sheet
    'total_assets', 'total_liabilities', 'total_equity', 'total_stockholders_equity',
    'cash_and_cash_equivalents', 'goodwill', 'short_term_debt', 'long_term_debt',
    'total_debt', 'net_debt', 'minority_interest',
    # Income Statement
    'revenue', 'cost_of_revenue', 'gross_profit',
    'research_and_development_expenses', 'general_and_administrative_expenses',
    'selling_and_marketing_expenses', 'selling_general_and_administrative_expenses',
    'other_expenses', 'operating_expenses', 'cost_and_expenses', 'operating_income',
    'interest_income', 'interest_expense', 'ebitda', 'total_other_income_expenses_net',
    'income_before_tax', 'income_tax_expense', 'net_income',
    'eps', 'eps_diluted', 'weighted_average_shs_out', 'weighted_average_shs_out_dil',
    # Cash Flow
    'net_income_cf', 'depreciation_and_amortization_cf', 'stock_based_compensation',
    'capital_expenditure', 'free_cash_flow',
]

# Deutsche Monatsnamen → Nummer (fuer fiscal_year_end aus company_info)
MONTH_MAP = {
    'januar': 1, 'februar': 2, 'maerz': 3, 'märz': 3, 'april': 4,
    'mai': 5, 'juni': 6, 'juli': 7, 'august': 8, 'september': 9,
    'oktober': 10, 'november': 11, 'dezember': 12,
}


def parse_fy_end_month(fiscal_year_end_str):
    """Deutscher Monatsname → Monatsnummer (Default: 12 = Dezember)."""
    if not fiscal_year_end_str:
        return 12
    return MONTH_MAP.get(fiscal_year_end_str.lower().strip(), 12)


def determine_period(report_date, fy_end_month, is_annual):
    """Bestimmt Q1-Q4 oder FY basierend auf Datum und Geschaeftsjahresende."""
    if is_annual:
        return 'FY'
    fy_start_month = (fy_end_month % 12) + 1
    months_from_start = (report_date.month - fy_start_month) % 12
    quarter = (months_from_start // 3) + 1
    return f'Q{quarter}'


def get_existing_periods(cur, isin):
    """Laedt existierende (year, period) Kombinationen aus der DB.

    Returns:
        complete: set von (year, period) mit vollstaendigen Daten
        incomplete: dict von (year, period) → date fuer unvollstaendige Zeilen
    """
    cur.execute("""
        SELECT YEAR(date) AS yr, period, date,
               revenue, gross_profit, operating_income, net_income
        FROM analytics.historical_fundamentals
        WHERE isin = %s
    """, (isin,))
    complete = set()
    incomplete = {}
    for row in cur.fetchall():
        key = (row['yr'], row['period'])
        if (row['revenue'] is not None
                and row['gross_profit'] is not None
                and row['operating_income'] is not None
                and row['net_income'] is not None):
            complete.add(key)
        else:
            incomplete[key] = row['date']
    return complete, incomplete


def extract_values(df, mapping, date_col):
    """Extrahiert Werte aus einem yfinance DataFrame fuer ein bestimmtes Datum."""
    result = {}
    if df is None or df.empty:
        return result
    for yf_label, db_col in mapping.items():
        if yf_label in df.index:
            val = df.loc[yf_label, date_col] if date_col in df.columns else None
            if pd.notna(val):
                result[db_col] = float(val)
    return result


def build_insert_sql():
    """Baut das INSERT ... ON DUPLICATE KEY UPDATE SQL."""
    cols = ', '.join(DB_COLUMNS)
    placeholders = ', '.join(['%s'] * len(DB_COLUMNS))

    # COALESCE: yfinance-NULL ueberschreibt existierenden Wert NICHT
    update_parts = []
    skip_cols = {'ticker', 'isin', 'stock_index', 'date', 'period'}
    for col in DB_COLUMNS:
        if col in skip_cols:
            continue
        if col == 'company_name':
            update_parts.append(f'{col} = VALUES({col})')
        else:
            update_parts.append(f'{col} = COALESCE(VALUES({col}), {col})')
    update_parts.append('updated_at = NOW()')

    sql = f"""
        INSERT INTO analytics.historical_fundamentals ({cols})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {', '.join(update_parts)}
    """
    return sql


def process_ticker(ticker_obj, dates_income, dates_bs, dates_cf,
                   income_annual, income_quarterly,
                   bs_annual, bs_quarterly,
                   cf_annual, cf_quarterly,
                   company, fy_end_month, complete_periods, incomplete_periods, dry_run):
    """Verarbeitet ein Unternehmen und gibt neue + Update-Zeilen zurueck."""
    new_rows = []
    update_rows = []
    is_semiannual = income_quarterly is None or income_quarterly.empty

    # Annual + Quarterly verarbeiten
    datasets = [
        (True, income_annual, bs_annual, cf_annual),
    ]
    if not is_semiannual:
        datasets.append((False, income_quarterly, bs_quarterly, cf_quarterly))

    for is_annual, inc_df, bs_df, cf_df in datasets:
        # Alle Daten aus den DataFrames sammeln
        all_dates = set()
        if inc_df is not None and not inc_df.empty:
            all_dates.update(inc_df.columns)
        if bs_df is not None and not bs_df.empty:
            all_dates.update(bs_df.columns)
        if cf_df is not None and not cf_df.empty:
            all_dates.update(cf_df.columns)

        for date_col in sorted(all_dates):
            try:
                report_date = pd.Timestamp(date_col).date()
            except Exception:
                continue

            period = determine_period(report_date, fy_end_month, is_annual)
            year = report_date.year

            if (year, period) in complete_periods:
                continue

            # Werte extrahieren
            values = {}
            values.update(extract_values(inc_df, YF_INCOME_MAPPING, date_col))
            values.update(extract_values(bs_df, YF_BALANCE_MAPPING, date_col))
            values.update(extract_values(cf_df, YF_CASHFLOW_MAPPING, date_col))

            if not values:
                continue

            if (year, period) in incomplete_periods:
                # Unvollstaendige Periode → UPDATE mit DB-Datum
                update_rows.append({
                    'values': values,
                    'db_date': incomplete_periods[(year, period)],
                    'period': period,
                    'year': year,
                })
                del incomplete_periods[(year, period)]
            else:
                # Neue Periode → INSERT
                row = {col: None for col in DB_COLUMNS}
                row['ticker'] = company['yf_ticker']
                row['isin'] = company['isin']
                row['stock_index'] = company['stock_index']
                row['company_name'] = company['name']
                row['date'] = report_date
                row['period'] = period
                row.update(values)
                new_rows.append(row)
                complete_periods.add((year, period))

    return new_rows, update_rows, is_semiannual


def update_prices_for_isins(cur, conn, isins):
    """Aktualisiert avg_price, price und market_cap fuer bestimmte ISINs."""
    if not isins:
        return

    placeholders = ', '.join(['%s'] * len(isins))
    isin_list = list(isins)

    # avg_price
    cur.execute(f"""
        UPDATE analytics.historical_fundamentals ffn
        JOIN (
            SELECT isin, YEAR(date) AS year, AVG(close) AS avg_price
            FROM raw_data.yf_prices
            WHERE isin IN ({placeholders})
            GROUP BY isin, YEAR(date)
        ) yp ON yp.isin = ffn.isin AND yp.year = YEAR(ffn.date)
        SET ffn.avg_price = yp.avg_price
        WHERE ffn.isin IN ({placeholders})
          AND ffn.avg_price IS NULL
    """, isin_list + isin_list)

    # price (letzter Kurs des Jahres)
    cur.execute(f"""
        UPDATE analytics.historical_fundamentals ffn
        JOIN (
            SELECT isin, year, close AS price
            FROM (
                SELECT isin, YEAR(date) AS year, close,
                       ROW_NUMBER() OVER (PARTITION BY isin, YEAR(date) ORDER BY date DESC) AS rn
                FROM raw_data.yf_prices
                WHERE isin IN ({placeholders})
            ) ranked
            WHERE rn = 1
        ) yp ON yp.isin = ffn.isin AND yp.year = YEAR(ffn.date)
        SET ffn.price = yp.price
        WHERE ffn.isin IN ({placeholders})
          AND ffn.price IS NULL
    """, isin_list + isin_list)

    # market_cap
    cur.execute(f"""
        UPDATE analytics.historical_fundamentals
        SET market_cap = price * weighted_average_shs_out
        WHERE isin IN ({placeholders})
          AND price IS NOT NULL
          AND weighted_average_shs_out IS NOT NULL
          AND market_cap IS NULL
    """, isin_list)

    conn.commit()


def main():
    parser = argparse.ArgumentParser(description='yfinance Fundamentaldaten-Update')
    parser.add_argument('--index', type=str, help='Nur Unternehmen eines Index (z.B. DAX)')
    parser.add_argument('--isin', type=str, help='Nur ein Unternehmen (ISIN)')
    parser.add_argument('--dry-run', action='store_true', help='Nur anzeigen, was eingefuegt wuerde')
    parser.add_argument('--with-recalc', action='store_true', help='calcu_numbers + live_metrics danach neu berechnen')
    args = parser.parse_args()

    print("=" * 60)
    print("yfinance Fundamentaldaten-Update")
    print("=" * 60)

    try:
        conn = get_connection(autocommit=False)
        cur = conn.cursor(dictionary=True)
    except MySQLError as e:
        print(f"DB-Verbindung fehlgeschlagen: {e}")
        return

    # --- Schritt 1: Ticker laden ---
    print("\n[1] Lade Ticker...")
    query = """
        SELECT t.isin, t.yf_ticker, t.stock_index, t.name,
               ci.fiscal_year_end
        FROM tickerdb.tickerlist t
        LEFT JOIN analytics.company_info ci ON ci.isin = t.isin COLLATE utf8mb4_unicode_ci
        WHERE t.yf_ticker IS NOT NULL
    """
    params = []
    if args.index:
        query += " AND t.stock_index = %s"
        params.append(args.index)
    if args.isin:
        query += " AND t.isin = %s"
        params.append(args.isin)

    cur.execute(query, params)
    companies = cur.fetchall()
    print(f"    {len(companies)} Unternehmen geladen.")

    if not companies:
        print("Keine Unternehmen gefunden.")
        cur.close()
        conn.close()
        return

    # --- Schritt 2-6: Pro Unternehmen verarbeiten ---
    insert_sql = build_insert_sql()
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    semiannual_count = 0
    error_count = 0
    affected_isins = set()

    for i, company in enumerate(companies, 1):
        yf_ticker = company['yf_ticker']
        isin = company['isin']
        stock_index = company['stock_index']
        name = company['name'] or yf_ticker
        fy_end_month = parse_fy_end_month(company.get('fiscal_year_end'))

        print(f"\n[{i}/{len(companies)}] {name} ({yf_ticker}, FY-Ende: Monat {fy_end_month})")

        try:
            # yfinance-Daten abrufen
            ticker_obj = yf.Ticker(yf_ticker)
            income_annual = ticker_obj.financials
            income_quarterly = ticker_obj.quarterly_financials
            bs_annual = ticker_obj.balance_sheet
            bs_quarterly = ticker_obj.quarterly_balance_sheet
            cf_annual = ticker_obj.cashflow
            cf_quarterly = ticker_obj.quarterly_cashflow
        except Exception as e:
            print(f"    FEHLER beim Abrufen: {e}")
            error_count += 1
            time.sleep(1.5)
            continue

        # Existierende Perioden laden (complete + incomplete)
        complete_periods, incomplete_periods = get_existing_periods(cur, isin)

        # Verarbeiten
        new_rows, update_rows, is_semiannual = process_ticker(
            ticker_obj, None, None, None,
            income_annual, income_quarterly,
            bs_annual, bs_quarterly,
            cf_annual, cf_quarterly,
            company, fy_end_month, complete_periods, incomplete_periods, args.dry_run
        )

        if is_semiannual:
            semiannual_count += 1
            print(f"    Halbjahresreporter -> nur FY-Daten")

        if not new_rows and not update_rows:
            total_skipped += len(complete_periods)
            print(f"    Keine neuen Perioden (existierend: {len(complete_periods)}, unvollstaendig: 0)")
        else:
            # Neue Perioden
            for row in new_rows:
                print(f"    NEU: {row['date']} {row['period']}"
                      f" | Rev={row.get('revenue', '-')}"
                      f" | NI={row.get('net_income', '-')}"
                      f" | EPS={row.get('eps', '-')}")

            # Unvollstaendige Perioden nachfuellen
            for upd in update_rows:
                print(f"    UPDATE: {upd['db_date']} {upd['period']}"
                      f" | Rev={upd['values'].get('revenue', '-')}"
                      f" | NI={upd['values'].get('net_income', '-')}"
                      f" | EPS={upd['values'].get('eps', '-')}")

            if not args.dry_run:
                # INSERTs
                for row in new_rows:
                    values = tuple(row[col] for col in DB_COLUMNS)
                    try:
                        cur.execute(insert_sql, values)
                    except MySQLError as e:
                        print(f"    INSERT-Fehler: {e}")
                        continue

                # UPDATEs fuer unvollstaendige Perioden
                for upd in update_rows:
                    set_parts = []
                    params = []
                    for db_col, val in upd['values'].items():
                        set_parts.append(f'{db_col} = COALESCE(%s, {db_col})')
                        params.append(val)
                    set_parts.append('updated_at = NOW()')
                    params.extend([isin, stock_index, upd['db_date'], upd['period']])
                    cur.execute(f"""
                        UPDATE analytics.historical_fundamentals
                        SET {', '.join(set_parts)}
                        WHERE isin = %s AND stock_index = %s
                          AND date = %s AND period = %s
                    """, params)

                conn.commit()
                affected_isins.add(isin)

            total_inserted += len(new_rows)
            total_updated += len(update_rows)

        time.sleep(1.5)

    # --- Schritt 7: Preis-Enrichment ---
    if affected_isins and not args.dry_run:
        print(f"\n[Preis-Enrichment] Aktualisiere Preise fuer {len(affected_isins)} ISINs...")
        try:
            update_prices_for_isins(cur, conn, affected_isins)
            print("    Preise aktualisiert.")
        except MySQLError as e:
            print(f"    Preis-Update Fehler: {e}")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("ZUSAMMENFASSUNG")
    print("=" * 60)
    print(f"Unternehmen verarbeitet:  {len(companies)}")
    print(f"Halbjahresreporter:       {semiannual_count}")
    print(f"Neue Perioden eingefuegt: {total_inserted}" + (" (DRY-RUN)" if args.dry_run else ""))
    print(f"Unvollstaendige ergaenzt: {total_updated}" + (" (DRY-RUN)" if args.dry_run else ""))
    print(f"Fehler:                   {error_count}")
    if affected_isins:
        print(f"ISINs mit neuen Daten:    {len(affected_isins)}")

    # --- Schritt 8: Downstream-Pipeline ---
    if args.with_recalc and not args.dry_run and total_inserted > 0:
        print("\n[Recalc] Starte Downstream-Pipeline...")
        import subprocess
        base = Path(__file__).parent.parent

        scripts = [
            str(base / '03_analytics' / '05_fill_calcu_numbers.py'),
            str(base / '04_frontend' / '02_load_live_metrics.py'),
        ]
        for script in scripts:
            print(f"    Starte {Path(script).name}...")
            result = subprocess.run(
                [sys.executable, script],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                print(f"    FEHLER in {Path(script).name}: {result.stderr[:500]}")
            else:
                print(f"    {Path(script).name} erfolgreich.")

    cur.close()
    conn.close()
    print("\nFertig.")


if __name__ == "__main__":
    main()
