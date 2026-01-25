"""
finanzen.net Scraper für Earnings Calendar und Analyst Estimates

Workflow:
1. ISIN aus tickerlist laden
2. Slug via Suche finden (falls nicht in finanzen_name)
3. /termine/{slug} -> earnings_calendar
4. /schaetzungen/{slug} -> analyst_estimates
"""

import sys
import time
import re
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import unquote

sys.path.insert(0, str(Path(__file__).parent.parent))

from db import get_connection
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os

# =============================================================================
# KONFIGURATION
# =============================================================================

HEADLESS = True
SELENIUM_TIMEOUT = 15
REQUEST_DELAY = 1.5  # Sekunden zwischen Requests

# =============================================================================
# DRIVER SETUP
# =============================================================================

def create_driver(headless=True):
    """Erstellt einen Chrome-Driver"""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    driver_path = os.environ.get("CHROMEDRIVER_PATH")
    if not driver_path:
        for candidate in [
            "/usr/lib/chromium-browser/chromedriver",
            "/snap/bin/chromedriver",
            "/usr/bin/chromedriver",
        ]:
            if os.path.exists(candidate):
                driver_path = candidate
                break

    chrome_binary = os.environ.get("CHROME_BINARY")
    if not chrome_binary:
        for candidate in ["/usr/bin/chromium-browser", "/snap/bin/chromium", "/usr/bin/google-chrome"]:
            if os.path.exists(candidate):
                chrome_binary = candidate
                break
    if chrome_binary:
        chrome_options.binary_location = chrome_binary

    if driver_path:
        service = Service(driver_path)
    else:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    return webdriver.Chrome(service=service, options=chrome_options)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_german_number(text: str) -> Decimal | None:
    """Parst deutsche Zahlenformate (1.234,56 -> 1234.56)"""
    if not text or text == '-' or text == '—':
        return None
    try:
        # Entferne Währung und Whitespace
        cleaned = re.sub(r'[A-Za-z\s]', '', text)
        # Deutsche -> Englische Notation
        cleaned = cleaned.replace('.', '').replace(',', '.')
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def parse_german_date(text: str) -> str | None:
    """Parst deutsches Datum (12.02.2026 -> 2026-02-12)"""
    if not text:
        return None
    # Entferne (e)* Markierung
    text = re.sub(r'\s*\(e\)\*?\s*', '', text).strip()
    try:
        dt = datetime.strptime(text, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_currency(text: str) -> str | None:
    """Extrahiert Währung aus Text"""
    match = re.search(r'(EUR|USD|JPY|GBP|CHF|DKK|SEK|NOK)', text)
    return match.group(1) if match else None


def extract_period_info(info_text: str) -> tuple[str, str]:
    """
    Extrahiert Periode aus Info-Text.
    'Q1 2026' -> ('Q1 2026', 'quarter')
    'FY 2025' -> ('FY 2025', 'fiscal_year')
    """
    info_text = info_text.strip()

    # Quartale: Q1, Q2, Q3, Q4
    q_match = re.search(r'Q([1-4])\s*(\d{4})', info_text)
    if q_match:
        return f"Q{q_match.group(1)} {q_match.group(2)}", "quarter"

    # Geschäftsjahr: FY, GJ
    fy_match = re.search(r'(?:FY|GJ)\s*(\d{4})', info_text)
    if fy_match:
        return f"FY {fy_match.group(1)}", "fiscal_year"

    # Fallback: Jahr
    year_match = re.search(r'(\d{4})', info_text)
    if year_match:
        return f"FY {year_match.group(1)}", "fiscal_year"

    return info_text, "unknown"


# =============================================================================
# SLUG FINDER
# =============================================================================

def get_finanzen_slug(driver, isin: str) -> str | None:
    """Findet den finanzen.net Slug für eine ISIN"""
    search_url = f"https://www.finanzen.net/suchergebnis.asp?_search={isin}"

    driver.get(search_url)
    time.sleep(3)

    # Cookie-Banner
    try:
        cookie_btn = driver.find_element(By.XPATH, "//button[contains(text(),'Akzeptieren')]")
        cookie_btn.click()
        time.sleep(1)
    except:
        pass

    current_url = driver.current_url

    # Sonderfall: Redirect zu finanzen.ch
    if 'finanzen.ch' in current_url and 'countryredirect' in current_url:
        redirect_match = re.search(r'countryredirect=([^&]+)', current_url)
        if redirect_match:
            redirect_url = unquote(redirect_match.group(1))
            match = re.search(r'/aktien/([^/]+)-aktie', redirect_url)
            if match:
                return match.group(1)

    match = re.search(r'/aktien/([^/]+)-aktie', current_url)
    if match:
        slug = match.group(1)
        if '?' not in slug:
            return slug

    return None


# =============================================================================
# TERMINE SCRAPER
# =============================================================================

def scrape_termine(driver, slug: str, isin: str) -> list[dict]:
    """Scraped die Termine-Seite"""
    url = f"https://www.finanzen.net/termine/{slug}"
    driver.get(url)
    time.sleep(2)

    termine = []

    try:
        tables = driver.find_elements(By.TAG_NAME, "table")

        for table in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 4:
                    terminart = cells[0].text.strip()
                    eps_text = cells[1].text.strip()
                    info = cells[2].text.strip()
                    datum_text = cells[3].text.strip()

                    if not terminart or not datum_text:
                        continue

                    # Event-Type bestimmen
                    event_type = "earnings"
                    if "hauptversammlung" in terminart.lower():
                        event_type = "hauptversammlung"
                    elif "dividende" in terminart.lower():
                        event_type = "dividende"

                    # Periode extrahieren
                    period, _ = extract_period_info(info)

                    # Datum parsen
                    is_estimated = "(e)*" in datum_text or "(e)" in datum_text
                    release_date = parse_german_date(datum_text)

                    # EPS parsen
                    eps_value = parse_german_number(eps_text)
                    eps_currency = extract_currency(eps_text)

                    termine.append({
                        "isin": isin,
                        "period": period,
                        "release_date": release_date,
                        "event_type": event_type,
                        "eps_estimate": eps_value,
                        "eps_currency": eps_currency,
                        "is_estimated": is_estimated,
                    })

    except Exception as e:
        print(f"    Fehler beim Scrapen der Termine: {e}")

    return termine


# =============================================================================
# SCHÄTZUNGEN SCRAPER
# =============================================================================

# Mapping von deutschen Kennzahl-Namen zu standardisierten Metric-Namen
METRIC_MAPPING = {
    "umsatzerlöse": ("revenue", "millions"),
    "umsatz": ("revenue", "millions"),
    "dividende": ("dividend", "per_share"),
    "dividendenrendite": ("dividend_yield", "percent"),
    "gewinn/aktie": ("eps", "per_share"),
    "gewinn pro aktie": ("eps", "per_share"),
    "kgv": ("pe_ratio", "ratio"),
    "ebit": ("ebit", "millions"),
    "ebitda": ("ebitda", "millions"),
    "gewinn in mio": ("net_income", "millions"),
    "gewinn (vor steuern)": ("ebt", "millions"),
    "gewinn/aktie (reported)": ("eps_reported", "per_share"),
    "cashflow (operations)": ("cashflow_operations", "millions"),
    "cashflow (investing)": ("cashflow_investing", "millions"),
    "cashflow (financing)": ("cashflow_financing", "millions"),
    "cashflow/aktie": ("cashflow_per_share", "per_share"),
    "free cashflow": ("free_cashflow", "millions"),
    "buchwert/aktie": ("book_value_per_share", "per_share"),
    "buchwert": ("book_value_per_share", "per_share"),
    "nettofinanzverbindlichkeiten": ("net_debt", "millions"),
    "eigenkapital": ("equity", "millions"),
    "bilanzsumme": ("total_assets", "millions"),
}


def parse_period_from_header(header_text: str) -> tuple[str, str, str | None]:
    """
    Parst Periodeninfo aus Tabellen-Header.
    'letztes Quartal zum 31.12.2025' -> ('Q4 2025', 'quarter', '2025-12-31')
    'aktuelles Geschäftsjahr zum 30.9.2026' -> ('FY 2026', 'fiscal_year', '2026-09-30')
    '2026e' -> ('FY 2026', 'fiscal_year', None)
    """
    header_text = header_text.replace("\n", " ")

    # Format 1: Jahr mit 'e' (2026e, 2027e)
    year_match = re.search(r'(\d{4})e?$', header_text.strip())
    if year_match and "quartal" not in header_text.lower():
        year = year_match.group(1)
        return f"FY {year}", "fiscal_year", None

    # Format 2: Datum im Header
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', header_text)
    if date_match:
        day, month, year = date_match.groups()
        period_end = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        month_int = int(month)
        quarter_map = {3: 1, 6: 2, 9: 3, 12: 4, 1: 1, 2: 1, 4: 2, 5: 2, 7: 3, 8: 3, 10: 4, 11: 4}
        quarter = quarter_map.get(month_int, 4)

        if "geschäftsjahr" in header_text.lower():
            return f"FY {year}", "fiscal_year", period_end
        else:
            return f"Q{quarter} {year}", "quarter", period_end

    return "", "unknown", None


def identify_metric(label: str) -> tuple[str, str] | None:
    """Identifiziert die Metrik aus dem Zeilen-Label."""
    label_lower = label.lower().strip()

    for key, (metric, unit) in METRIC_MAPPING.items():
        if key in label_lower:
            return metric, unit

    return None


def scrape_schaetzungen(driver, slug: str, isin: str) -> list[dict]:
    """
    Scraped die Schätzungen-Seite komplett.

    Erfasst:
    - Quartalsschätzungen (EPS, Umsatz)
    - Geschäftsjahresschätzungen (alle Kennzahlen)
    - Historische Werte (Schätzung vs. Ist)
    """
    url = f"https://www.finanzen.net/schaetzungen/{slug}"
    driver.get(url)
    time.sleep(3)

    # Scroll to load all content
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

    estimates = []

    try:
        tables = driver.find_elements(By.TAG_NAME, "table")

        for table_idx, table in enumerate(tables):
            # innerHTML prüfen ob relevante Daten vorhanden
            html = table.get_attribute("innerHTML") or ""
            if not any(kw in html for kw in ["EUR", "USD", "JPY", "CHF", "Schätzung", "Analysten", "2026", "2027"]):
                continue

            rows = table.find_elements(By.TAG_NAME, "tr")
            if len(rows) < 2:
                continue

            # Headers analysieren (erste Zeile)
            first_row = rows[0].get_attribute("innerText") or ""
            headers = [h.strip() for h in first_row.split("\t")]

            # Perioden aus Headers extrahieren
            periods = []
            for h in headers[1:]:  # Erste Spalte ist Label
                if h:
                    period, period_type, period_end = parse_period_from_header(h)
                    periods.append((period, period_type, period_end, h))
                else:
                    periods.append((None, None, None, ""))

            if not periods or not any(p[0] for p in periods):
                continue

            # Tabellentyp erkennen
            # Typ 1: Quartalsschätzungen (Analysten/Schätzung/Vorjahr Zeilen)
            # Typ 2: Geschäftsjahre (Kennzahl pro Zeile)

            row_labels = []
            for row in rows[1:]:
                text = row.get_attribute("innerText") or ""
                parts = text.split("\t")
                if parts:
                    row_labels.append(parts[0].lower())

            is_quarterly_table = any("anzahl" in l and "analyst" in l for l in row_labels)
            is_annual_table = any(identify_metric(l) for l in row_labels)

            if is_quarterly_table:
                # Quartalsschätzungen verarbeiten
                estimates.extend(
                    _parse_quarterly_table(rows, periods, isin, table_idx)
                )

            elif is_annual_table:
                # Geschäftsjahresschätzungen verarbeiten
                estimates.extend(
                    _parse_annual_table(rows, periods, isin)
                )

    except Exception as e:
        print(f"    Fehler beim Scrapen der Schätzungen: {e}")
        import traceback
        traceback.print_exc()

    return estimates


def _parse_quarterly_table(rows, periods, isin: str, table_idx: int) -> list[dict]:
    """Parst Quartalsschätzungen (Tabellen 1-4)."""
    estimates = []

    num_analysts = {}
    estimate_values = {}
    prior_year_values = {}
    actual_values = {}
    currency = None

    # Metrik aus Tabellenindex ableiten (Tabelle 1,3 = EPS, Tabelle 2,4 = Revenue)
    # Besser: aus Kontext erkennen
    metric = "eps"  # Default

    for row in rows[1:]:
        text = row.get_attribute("innerText") or ""
        parts = [p.strip() for p in text.split("\t")]
        if len(parts) < 2:
            continue

        label = parts[0].lower()

        # Metrik aus Tabelle erkennen
        if "umsatz" in label:
            metric = "revenue"

        if "anzahl" in label and "analyst" in label:
            for i, val in enumerate(parts[1:]):
                if i < len(periods):
                    num_analysts[i] = parse_german_number(val)

        elif "mittlere schätzung" in label or (label == "mittlere schätzung"):
            for i, val in enumerate(parts[1:]):
                if i < len(periods):
                    estimate_values[i] = parse_german_number(val)
                    if not currency:
                        currency = extract_currency(val)

        elif "vorjahr" in label:
            for i, val in enumerate(parts[1:]):
                if i < len(periods):
                    prior_year_values[i] = parse_german_number(val)

        elif "tatsächlicher wert" in label or "tatsächlich" in label:
            for i, val in enumerate(parts[1:]):
                if i < len(periods):
                    actual_values[i] = parse_german_number(val)

    # Estimates erstellen
    for i, (period, period_type, period_end, _) in enumerate(periods):
        if not period:
            continue

        if i in estimate_values and estimate_values[i] is not None:
            estimates.append({
                "isin": isin,
                "period": period,
                "period_type": period_type,
                "period_end_date": period_end,
                "metric": metric,
                "estimate_value": estimate_values.get(i),
                "prior_year_value": prior_year_values.get(i),
                "actual_value": actual_values.get(i),
                "currency": currency,
                "unit": "per_share" if metric == "eps" else "millions",
                "num_analysts": int(num_analysts[i]) if num_analysts.get(i) else None,
                "release_date": None,
            })
        elif i in actual_values and actual_values[i] is not None:
            # Historische Werte ohne Schätzung
            estimates.append({
                "isin": isin,
                "period": period,
                "period_type": period_type,
                "period_end_date": period_end,
                "metric": metric,
                "estimate_value": estimate_values.get(i),
                "prior_year_value": None,
                "actual_value": actual_values.get(i),
                "currency": currency,
                "unit": "per_share" if metric == "eps" else "millions",
                "num_analysts": None,
                "release_date": None,
            })

    return estimates


def _parse_annual_table(rows, periods, isin: str) -> list[dict]:
    """Parst Geschäftsjahresschätzungen (Tabelle 5)."""
    estimates = []

    for row in rows[1:]:
        text = row.get_attribute("innerText") or ""
        parts = [p.strip() for p in text.split("\t")]
        if len(parts) < 2:
            continue

        label = parts[0]
        metric_info = identify_metric(label)

        if not metric_info:
            continue

        metric, unit = metric_info
        currency = None

        for i, val in enumerate(parts[1:]):
            if i >= len(periods):
                break

            period, period_type, period_end, _ = periods[i]
            if not period:
                continue

            value = parse_german_number(val)
            if value is None:
                continue

            if not currency:
                currency = extract_currency(val)
                # Fallback: EUR für europäische Aktien
                if not currency and ("EUR" not in val and "USD" not in val):
                    currency = "EUR"

            estimates.append({
                "isin": isin,
                "period": period,
                "period_type": period_type,
                "period_end_date": period_end,
                "metric": metric,
                "estimate_value": value,
                "prior_year_value": None,
                "actual_value": None,
                "currency": currency,
                "unit": unit,
                "num_analysts": None,
                "release_date": None,
            })

    return estimates


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def save_termine(con, termine: list[dict]):
    """Speichert Termine in die Datenbank"""
    if not termine:
        return 0

    cur = con.cursor()
    sql = """
        INSERT INTO earnings_calendar
        (isin, period, release_date, event_type, eps_estimate, eps_currency, is_estimated)
        VALUES (%(isin)s, %(period)s, %(release_date)s, %(event_type)s, %(eps_estimate)s, %(eps_currency)s, %(is_estimated)s)
        ON DUPLICATE KEY UPDATE
            release_date = VALUES(release_date),
            eps_estimate = VALUES(eps_estimate),
            eps_currency = VALUES(eps_currency),
            is_estimated = VALUES(is_estimated),
            updated_at = CURRENT_TIMESTAMP
    """

    count = 0
    for t in termine:
        try:
            cur.execute(sql, t)
            count += 1
        except Exception as e:
            print(f"    DB-Fehler (Termin): {e}")

    con.commit()
    cur.close()
    return count


def save_estimates(con, estimates: list[dict]):
    """Speichert Schätzungen in die Datenbank"""
    if not estimates:
        return 0

    cur = con.cursor()
    sql = """
        INSERT INTO analyst_estimates
        (isin, period, period_type, period_end_date, metric, estimate_value, prior_year_value, actual_value, currency, unit, num_analysts, release_date)
        VALUES (%(isin)s, %(period)s, %(period_type)s, %(period_end_date)s, %(metric)s, %(estimate_value)s, %(prior_year_value)s,
                %(actual_value)s, %(currency)s, %(unit)s, %(num_analysts)s, %(release_date)s)
        ON DUPLICATE KEY UPDATE
            estimate_value = COALESCE(VALUES(estimate_value), estimate_value),
            prior_year_value = COALESCE(VALUES(prior_year_value), prior_year_value),
            actual_value = COALESCE(VALUES(actual_value), actual_value),
            period_end_date = COALESCE(VALUES(period_end_date), period_end_date),
            currency = COALESCE(VALUES(currency), currency),
            num_analysts = COALESCE(VALUES(num_analysts), num_analysts),
            release_date = COALESCE(VALUES(release_date), release_date),
            updated_at = CURRENT_TIMESTAMP
    """

    count = 0
    for est in estimates:
        try:
            cur.execute(sql, est)
            count += 1
        except Exception as ex:
            print(f"    DB-Fehler (Estimate): {ex}")

    con.commit()
    cur.close()
    return count


def update_finanzen_slug(con_ticker, isin: str, slug: str):
    """Speichert den Slug in tickerlist.finanzen_name"""
    cur = con_ticker.cursor()
    cur.execute(
        "UPDATE tickerlist SET finanzen_name = %s WHERE isin = %s",
        (slug, isin)
    )
    con_ticker.commit()
    cur.close()


# =============================================================================
# MAIN
# =============================================================================

def process_isin(driver, con_analytics, con_ticker, isin: str, name: str, existing_slug: str | None) -> dict:
    """Verarbeitet eine einzelne ISIN"""
    result = {"isin": isin, "name": name, "slug": None, "termine": 0, "estimates": 0, "error": None}

    try:
        # Slug finden
        if existing_slug:
            slug = existing_slug
            print(f"  Slug aus DB: {slug}")
        else:
            slug = get_finanzen_slug(driver, isin)
            if slug:
                update_finanzen_slug(con_ticker, isin, slug)
                print(f"  Slug gefunden: {slug}")
            else:
                result["error"] = "Slug nicht gefunden"
                return result

        result["slug"] = slug
        time.sleep(REQUEST_DELAY)

        # Termine scrapen
        termine = scrape_termine(driver, slug, isin)
        result["termine"] = save_termine(con_analytics, termine)
        print(f"  Termine: {result['termine']}")
        time.sleep(REQUEST_DELAY)

        # Schätzungen scrapen
        estimates = scrape_schaetzungen(driver, slug, isin)
        result["estimates"] = save_estimates(con_analytics, estimates)
        print(f"  Schätzungen: {result['estimates']}")

    except Exception as e:
        result["error"] = str(e)
        print(f"  FEHLER: {e}")

    return result


def main(limit: int = None, index_filter: str = None):
    """
    Hauptfunktion

    Args:
        limit: Maximale Anzahl zu verarbeitender ISINs
        index_filter: Optional, nur bestimmten Index verarbeiten (z.B. 'DAX')
    """
    print("=" * 60)
    print("finanzen.net Scraper - Earnings & Estimates")
    print("=" * 60)

    driver = None
    con_ticker = None
    con_analytics = None

    try:
        # Verbindungen
        con_ticker = get_connection(db_name="ticker")
        con_analytics = get_connection(db_name="analytics")
        driver = create_driver(headless=HEADLESS)

        # ISINs laden
        cur = con_ticker.cursor()

        sql = "SELECT isin, name, finanzen_name FROM tickerlist"
        params = []

        if index_filter:
            sql += " WHERE stock_index = %s"
            params.append(index_filter)

        if limit:
            sql += f" LIMIT {limit}"

        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()

        print(f"\n{len(rows)} Aktien zu verarbeiten")
        if index_filter:
            print(f"Filter: {index_filter}")
        print()

        # Verarbeitung
        results = []
        for i, (isin, name, existing_slug) in enumerate(rows, 1):
            print(f"[{i}/{len(rows)}] {name} ({isin})")
            result = process_isin(driver, con_analytics, con_ticker, isin, name, existing_slug)
            results.append(result)
            time.sleep(REQUEST_DELAY)

        # Zusammenfassung
        print("\n" + "=" * 60)
        print("ZUSAMMENFASSUNG")
        print("=" * 60)

        successful = [r for r in results if r["slug"]]
        failed = [r for r in results if not r["slug"]]

        total_termine = sum(r["termine"] for r in results)
        total_estimates = sum(r["estimates"] for r in results)

        print(f"Erfolgreich: {len(successful)}/{len(results)}")
        print(f"Termine gespeichert: {total_termine}")
        print(f"Schätzungen gespeichert: {total_estimates}")

        if failed:
            print(f"\nFehlgeschlagen ({len(failed)}):")
            for r in failed[:10]:
                print(f"  {r['name']}: {r['error']}")

    except Exception as e:
        print(f"Fehler: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
        if con_ticker:
            con_ticker.close()
        if con_analytics:
            con_analytics.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="finanzen.net Scraper")
    parser.add_argument("--limit", type=int, help="Maximale Anzahl ISINs")
    parser.add_argument("--index", type=str, help="Nur bestimmten Index (z.B. DAX)")

    args = parser.parse_args()
    main(limit=args.limit, index_filter=args.index)
