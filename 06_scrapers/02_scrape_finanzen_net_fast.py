"""
finanzen.net Scraper - FAST VERSION (requests + BeautifulSoup)

~10x schneller als Selenium-Version durch:
- requests statt Selenium
- BeautifulSoup für HTML-Parsing
- Minimale Delays
- Fortschrittsanzeige
"""

import sys
import time
import re
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import unquote

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from bs4 import BeautifulSoup
from db import get_connection

# =============================================================================
# KONFIGURATION
# =============================================================================

REQUEST_DELAY = 0.3  # Sekunden zwischen Requests (höflich aber schnell)
REQUEST_TIMEOUT = 15

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
}

# Metriken-Mapping
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

# =============================================================================
# FORTSCHRITTSANZEIGE
# =============================================================================

class ProgressBar:
    def __init__(self, total: int, prefix: str = ""):
        self.total = total
        self.current = 0
        self.prefix = prefix
        self.start_time = time.time()
        self.last_print_len = 0

    def update(self, current: int = None, suffix: str = ""):
        if current is not None:
            self.current = current
        else:
            self.current += 1

        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        eta = (self.total - self.current) / rate if rate > 0 else 0

        pct = self.current / self.total * 100
        bar_len = 30
        filled = int(bar_len * self.current / self.total)
        bar = '█' * filled + '░' * (bar_len - filled)

        line = f"\r{self.prefix}[{bar}] {self.current}/{self.total} ({pct:.1f}%) | {rate:.1f}/s | ETA: {eta:.0f}s | {suffix}"

        # Clear previous line if shorter
        padding = max(0, self.last_print_len - len(line))
        print(line + " " * padding, end="", flush=True)
        self.last_print_len = len(line)

    def finish(self):
        elapsed = time.time() - self.start_time
        print(f"\n✓ Fertig in {elapsed:.1f}s ({self.current} Einträge)")


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_german_number(text: str) -> Decimal | None:
    if not text or text.strip() in ['-', '—', '']:
        return None
    try:
        cleaned = re.sub(r'[A-Za-z%\s]', '', text)
        cleaned = cleaned.replace('.', '').replace(',', '.')
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def parse_german_date(text: str) -> str | None:
    if not text:
        return None
    text = re.sub(r'\s*\(e\)\*?\s*', '', text).strip()
    try:
        dt = datetime.strptime(text, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_currency(text: str) -> str | None:
    match = re.search(r'(EUR|USD|JPY|GBP|CHF|DKK|SEK|NOK)', text)
    return match.group(1) if match else None


def extract_period_info(info_text: str) -> tuple[str, str]:
    info_text = info_text.strip()
    q_match = re.search(r'Q([1-4])\s*(\d{4})', info_text)
    if q_match:
        return f"Q{q_match.group(1)} {q_match.group(2)}", "quarter"
    fy_match = re.search(r'(?:FY|GJ)\s*(\d{4})', info_text)
    if fy_match:
        return f"FY {fy_match.group(1)}", "fiscal_year"
    year_match = re.search(r'(\d{4})', info_text)
    if year_match:
        return f"FY {year_match.group(1)}", "fiscal_year"
    return info_text, "unknown"


def identify_metric(label: str) -> tuple[str, str] | None:
    label_lower = label.lower().strip()
    for key, (metric, unit) in METRIC_MAPPING.items():
        if key in label_lower:
            return metric, unit
    return None


# =============================================================================
# SLUG FINDER (requests version)
# =============================================================================

def get_finanzen_slug(session: requests.Session, isin: str) -> str | None:
    """Findet den finanzen.net Slug für eine ISIN via Redirect."""
    search_url = f"https://www.finanzen.net/suchergebnis.asp?_search={isin}"

    try:
        r = session.get(search_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        final_url = r.url

        # Sonderfall: Redirect zu finanzen.ch
        if 'finanzen.ch' in final_url and 'countryredirect' in final_url:
            redirect_match = re.search(r'countryredirect=([^&]+)', final_url)
            if redirect_match:
                redirect_url = unquote(redirect_match.group(1))
                match = re.search(r'/aktien/([^/]+)-aktie', redirect_url)
                if match:
                    return match.group(1)

        match = re.search(r'/aktien/([^/]+)-aktie', final_url)
        if match:
            slug = match.group(1)
            if '?' not in slug:
                return slug

    except Exception as e:
        pass

    return None


# =============================================================================
# TERMINE SCRAPER (requests + BeautifulSoup)
# =============================================================================

def scrape_termine(session: requests.Session, slug: str, isin: str) -> list[dict]:
    """Scraped die Termine-Seite."""
    url = f"https://www.finanzen.net/termine/{slug}"

    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(r.text, 'html.parser')
    except Exception:
        return []

    termine = []

    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:
                terminart = cells[0].get_text(strip=True)
                eps_text = cells[1].get_text(strip=True)
                info = cells[2].get_text(strip=True)
                datum_text = cells[3].get_text(strip=True)

                if not terminart or not datum_text:
                    continue

                event_type = "earnings"
                if "hauptversammlung" in terminart.lower():
                    event_type = "hauptversammlung"
                elif "dividende" in terminart.lower():
                    event_type = "dividende"

                period, _ = extract_period_info(info)
                is_estimated = "(e)*" in datum_text or "(e)" in datum_text
                release_date = parse_german_date(datum_text)
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

    return termine


# =============================================================================
# SCHÄTZUNGEN SCRAPER (requests + BeautifulSoup)
# =============================================================================

def scrape_schaetzungen(session: requests.Session, slug: str, isin: str) -> list[dict]:
    """Scraped die Schätzungen-Seite komplett."""
    url = f"https://www.finanzen.net/schaetzungen/{slug}"

    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(r.text, 'html.parser')
    except Exception:
        return []

    estimates = []

    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue

        # Headers aus erster Zeile
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

        if not headers or not any(re.search(r'\d{4}', h) for h in headers):
            continue

        # Perioden aus Headers
        periods = []
        for h in headers[1:]:
            period, period_type, period_end = parse_period_from_header(h)
            periods.append((period, period_type, period_end))

        # Tabellentyp erkennen
        row_texts = [row.get_text(strip=True).lower() for row in rows[1:4]]
        is_quarterly = any("anzahl" in t and "analyst" in t for t in row_texts)
        is_annual = any(identify_metric(t) for t in row_texts)

        if is_quarterly:
            estimates.extend(_parse_quarterly_table_bs(rows, periods, isin))
        elif is_annual:
            estimates.extend(_parse_annual_table_bs(rows, periods, isin))

    return estimates


def parse_period_from_header(header_text: str) -> tuple[str, str, str | None]:
    header_text = header_text.replace("\n", " ")

    # Format: 2026e, 2027e
    year_match = re.search(r'(\d{4})e?$', header_text.strip())
    if year_match and "quartal" not in header_text.lower():
        year = year_match.group(1)
        return f"FY {year}", "fiscal_year", None

    # Format: Datum
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


def _parse_quarterly_table_bs(rows, periods, isin: str) -> list[dict]:
    """Parst Quartalsschätzungen mit BeautifulSoup."""
    estimates = []
    num_analysts = {}
    estimate_values = {}
    prior_year_values = {}
    actual_values = {}
    currency = None
    metric = "eps"

    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) < 2:
            continue

        label = cells[0].get_text(strip=True).lower()

        if "umsatz" in label:
            metric = "revenue"

        values = [c.get_text(strip=True) for c in cells[1:]]

        if "anzahl" in label and "analyst" in label:
            for i, val in enumerate(values):
                if i < len(periods):
                    num_analysts[i] = parse_german_number(val)

        elif "mittlere schätzung" in label:
            for i, val in enumerate(values):
                if i < len(periods):
                    estimate_values[i] = parse_german_number(val)
                    if not currency:
                        currency = extract_currency(val)

        elif "vorjahr" in label:
            for i, val in enumerate(values):
                if i < len(periods):
                    prior_year_values[i] = parse_german_number(val)

        elif "tatsächlicher wert" in label:
            for i, val in enumerate(values):
                if i < len(periods):
                    actual_values[i] = parse_german_number(val)

    for i, (period, period_type, period_end) in enumerate(periods):
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

    return estimates


def _parse_annual_table_bs(rows, periods, isin: str) -> list[dict]:
    """Parst Geschäftsjahresschätzungen mit BeautifulSoup."""
    estimates = []

    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) < 2:
            continue

        label = cells[0].get_text(strip=True)
        metric_info = identify_metric(label)
        if not metric_info:
            continue

        metric, unit = metric_info
        currency = None
        values = [c.get_text(strip=True) for c in cells[1:]]

        for i, val in enumerate(values):
            if i >= len(periods):
                break

            period, period_type, period_end = periods[i]
            if not period:
                continue

            value = parse_german_number(val)
            if value is None:
                continue

            if not currency:
                currency = extract_currency(val) or "EUR"

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

def save_termine(con, termine: list[dict]) -> int:
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
        except:
            pass
    con.commit()
    cur.close()
    return count


def save_estimates(con, estimates: list[dict]) -> int:
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
            updated_at = CURRENT_TIMESTAMP
    """
    count = 0
    for est in estimates:
        try:
            cur.execute(sql, est)
            count += 1
        except:
            pass
    con.commit()
    cur.close()
    return count


def update_finanzen_slug(con_ticker, isin: str, slug: str):
    cur = con_ticker.cursor()
    cur.execute("UPDATE tickerlist SET finanzen_name = %s WHERE isin = %s", (slug, isin))
    con_ticker.commit()
    cur.close()


# =============================================================================
# MAIN
# =============================================================================

def process_isin(session, con_analytics, con_ticker, isin: str, name: str, existing_slug: str | None) -> dict:
    """Verarbeitet eine einzelne ISIN."""
    result = {"isin": isin, "name": name, "slug": None, "termine": 0, "estimates": 0, "error": None}

    try:
        # Slug finden
        if existing_slug:
            slug = existing_slug
        else:
            slug = get_finanzen_slug(session, isin)
            if slug:
                update_finanzen_slug(con_ticker, isin, slug)

        if not slug:
            result["error"] = "Slug nicht gefunden"
            return result

        result["slug"] = slug
        time.sleep(REQUEST_DELAY)

        # Termine scrapen
        termine = scrape_termine(session, slug, isin)
        result["termine"] = save_termine(con_analytics, termine)
        time.sleep(REQUEST_DELAY)

        # Schätzungen scrapen
        estimates = scrape_schaetzungen(session, slug, isin)
        result["estimates"] = save_estimates(con_analytics, estimates)

    except Exception as e:
        result["error"] = str(e)

    return result


def main(limit: int = None, index_filter: str = None):
    """Hauptfunktion mit Fortschrittsanzeige."""
    print("=" * 60)
    print("finanzen.net Scraper - FAST VERSION")
    print("=" * 60)

    con_ticker = None
    con_analytics = None

    try:
        con_ticker = get_connection(db_name="ticker")
        con_analytics = get_connection(db_name="analytics")

        # Session für Connection-Pooling
        session = requests.Session()
        session.headers.update(HEADERS)

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

        total = len(rows)
        print(f"\n{total} Aktien zu verarbeiten", end="")
        if index_filter:
            print(f" (Filter: {index_filter})", end="")
        print("\n")

        # Fortschrittsanzeige
        progress = ProgressBar(total, prefix="Scraping: ")

        results = []
        total_termine = 0
        total_estimates = 0

        for i, (isin, name, existing_slug) in enumerate(rows):
            result = process_isin(session, con_analytics, con_ticker, isin, name, existing_slug)
            results.append(result)

            total_termine += result["termine"]
            total_estimates += result["estimates"]

            # Fortschritt aktualisieren
            status = f"{name[:20]:<20} | T:{result['termine']:>2} E:{result['estimates']:>3}"
            if result["error"]:
                status += f" | ✗"
            progress.update(i + 1, status)

            time.sleep(REQUEST_DELAY)

        progress.finish()

        # Zusammenfassung
        print("\n" + "=" * 60)
        print("ZUSAMMENFASSUNG")
        print("=" * 60)

        successful = [r for r in results if r["slug"]]
        failed = [r for r in results if not r["slug"]]

        print(f"Erfolgreich: {len(successful)}/{len(results)}")
        print(f"Termine gespeichert: {total_termine}")
        print(f"Schätzungen gespeichert: {total_estimates}")

        if failed:
            print(f"\nFehlgeschlagen ({len(failed)}):")
            for r in failed[:5]:
                print(f"  {r['name']}: {r['error']}")
            if len(failed) > 5:
                print(f"  ... und {len(failed)-5} weitere")

    except Exception as e:
        print(f"\nFehler: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if con_ticker:
            con_ticker.close()
        if con_analytics:
            con_analytics.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="finanzen.net Scraper (Fast Version)")
    parser.add_argument("--limit", type=int, help="Maximale Anzahl ISINs")
    parser.add_argument("--index", type=str, help="Nur bestimmten Index (z.B. DAX)")

    args = parser.parse_args()
    main(limit=args.limit, index_filter=args.index)
