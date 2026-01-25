"""
Testskript: finanzen.net Earnings Dates Scraper

Workflow:
1. ISIN-Suche auf finanzen.net -> Slug extrahieren
2. Termine-Seite abrufen -> Earnings Dates scrapen
"""

import sys
import time
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

# Konfiguration
HEADLESS = True
SELENIUM_TIMEOUT = 15

# Test-ISINs (bekannte deutsche Aktien)
TEST_ISINS_DAX = [
    ("DE0007236101", "Siemens"),
    ("DE000BASF111", "BASF"),
    ("DE0007164600", "SAP"),
    ("DE0008404005", "Allianz"),
    ("DE0007100000", "Mercedes-Benz"),
]

# Internationale Test-ISINs
TEST_ISINS_SP500 = [
    ("US67066G1040", "NVIDIA CORP"),
    ("US0378331005", "APPLE INC"),
    ("US5949181045", "MICROSOFT CORP"),
]

TEST_ISINS_NIKKEI = [
    ("JP3122400009", "ADVANTEST CORP"),
    ("JP3802300008", "FAST RETAILING LTD"),
    ("JP3436100006", "SOFTBANK GROUP CORP"),
]

TEST_ISINS_FTSE = [
    ("GB0009895292", "ASTRAZENECA PLC"),
    ("GB0005405286", "HSBC HOLDINGS PLC"),
    ("GB00BP6MXD84", "SHELL PLC"),
]

TEST_ISINS_STOXX = [
    ("NL0010273215", "ASML HOLDING NV"),
    ("CH0012032048", "ROCHE HOLDING PAR AG"),
    ("DK0062498333", "NOVO NORDISK"),
]

# Für den internationalen Test
TEST_ISINS_INTERNATIONAL = (
    TEST_ISINS_SP500 +
    TEST_ISINS_NIKKEI +
    TEST_ISINS_FTSE +
    TEST_ISINS_STOXX
)

# Standard-Test (nur DAX)
TEST_ISINS = TEST_ISINS_DAX


def create_driver(headless=True):
    """Erstellt einen Chrome-Driver (kopiert aus ishares_scraper)"""
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


def get_finanzen_slug_from_isin(driver, isin: str) -> str | None:
    """
    Sucht auf finanzen.net nach der ISIN und extrahiert den Slug.

    Die Suche leitet automatisch zur Aktien-Seite weiter, daher analysieren
    wir die finale URL nach dem Redirect.

    Returns: Slug (z.B. "siemens") oder None bei Fehler
    """
    search_url = f"https://www.finanzen.net/suchergebnis.asp?_search={isin}"
    print(f"  Suche: {search_url}")

    driver.get(search_url)
    time.sleep(3)  # Warten auf Redirect

    # Cookie-Banner akzeptieren falls vorhanden
    try:
        cookie_btn = driver.find_element(By.XPATH, "//button[contains(text(),'Akzeptieren') or contains(text(),'akzeptieren')]")
        cookie_btn.click()
        time.sleep(1)
    except:
        pass

    # Methode 1: Aktuelle URL nach Redirect analysieren
    # Die Suche leitet zu /aktien/{slug}-aktie weiter
    current_url = driver.current_url
    print(f"  Finale URL: {current_url}")

    # Sonderfall: Redirect zu finanzen.ch (Schweizer Aktien)
    # URL enthält countryredirect Parameter mit der richtigen finanzen.net URL
    if 'finanzen.ch' in current_url and 'countryredirect' in current_url:
        redirect_match = re.search(r'countryredirect=([^&]+)', current_url)
        if redirect_match:
            from urllib.parse import unquote
            redirect_url = unquote(redirect_match.group(1))
            match = re.search(r'/aktien/([^/]+)-aktie', redirect_url)
            if match:
                slug = match.group(1)
                print(f"  Gefunden (CH-Redirect): {slug}")
                return slug

    match = re.search(r'/aktien/([^/]+)-aktie', current_url)
    if match:
        slug = match.group(1)
        # Prüfen ob der Slug Query-Parameter enthält (Fehler)
        if '?' not in slug:
            print(f"  Gefunden (URL): {slug}")
            return slug

    # Methode 2: Canonical URL
    try:
        canonical = driver.find_element(By.XPATH, "//link[@rel='canonical']")
        href = canonical.get_attribute("href")
        match = re.search(r'/aktien/([^/]+)-aktie', href)
        if match:
            slug = match.group(1)
            print(f"  Gefunden (Canonical): {slug}")
            return slug
    except:
        pass

    # Methode 3: Link zu /termine/ auf der Seite suchen
    try:
        termine_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/termine/')]")
        for link in termine_links:
            href = link.get_attribute("href")
            # Nur Links die nicht zu einer Liste gehen
            if '/termine/' in href and not href.endswith('/termine/'):
                match = re.search(r'/termine/([^/\?"]+)', href)
                if match:
                    slug = match.group(1)
                    print(f"  Gefunden (Termine-Link): {slug}")
                    return slug
    except Exception as e:
        print(f"  Methode 3 fehlgeschlagen: {e}")

    print("  Slug nicht gefunden!")
    return None


def scrape_earnings_dates(driver, slug: str) -> list[dict]:
    """
    Scraped die Termine-Seite für den gegebenen Slug.

    Returns: Liste von Dicts mit Termininfos
    """
    url = f"https://www.finanzen.net/termine/{slug}"
    print(f"  Termine: {url}")

    driver.get(url)
    time.sleep(2)

    termine = []

    # Suche nach Tabellen mit Terminen
    try:
        # Zukünftige Termine
        tables = driver.find_elements(By.TAG_NAME, "table")

        for table in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 4:
                    terminart = cells[0].text.strip()
                    eps_schaetzung = cells[1].text.strip()
                    info = cells[2].text.strip()
                    datum = cells[3].text.strip()

                    if terminart and datum:
                        termine.append({
                            "terminart": terminart,
                            "eps_schaetzung": eps_schaetzung,
                            "info": info,
                            "datum": datum,
                        })
    except Exception as e:
        print(f"  Fehler beim Scrapen der Termine: {e}")

    return termine


def test_single_isin(driver, isin: str, expected_name: str):
    """Testet den Workflow für eine einzelne ISIN"""
    print(f"\n{'='*60}")
    print(f"Test: {expected_name} (ISIN: {isin})")
    print('='*60)

    # Schritt 1: Slug finden
    slug = get_finanzen_slug_from_isin(driver, isin)

    if not slug:
        print("  FEHLER: Slug konnte nicht gefunden werden")
        return None

    print(f"  Slug gefunden: {slug}")

    # Schritt 2: Termine scrapen
    termine = scrape_earnings_dates(driver, slug)

    print(f"  Termine gefunden: {len(termine)}")

    # Zeige die ersten 5 Termine
    for i, t in enumerate(termine[:5]):
        print(f"    {i+1}. {t['terminart']}: {t['datum']} ({t['info']}) - EPS: {t['eps_schaetzung']}")

    return {
        "isin": isin,
        "name": expected_name,
        "slug": slug,
        "termine": termine
    }


def main(test_set="dax"):
    """
    test_set: "dax" für deutsche Aktien, "international" für alle Indizes
    """
    if test_set == "international":
        test_isins = TEST_ISINS_INTERNATIONAL
        title = "finanzen.net Earnings Scraper - INTERNATIONAL Test"
    else:
        test_isins = TEST_ISINS_DAX
        title = "finanzen.net Earnings Scraper - DAX Test"

    print("=" * 60)
    print(title)
    print("=" * 60)

    driver = None
    results = []

    try:
        driver = create_driver(headless=HEADLESS)

        for isin, name in test_isins:
            result = test_single_isin(driver, isin, name)
            if result:
                results.append(result)
            time.sleep(1)  # Höfliche Pause zwischen Requests

        # Zusammenfassung
        print("\n" + "=" * 60)
        print("ZUSAMMENFASSUNG")
        print("=" * 60)

        successful = [r for r in results if r and r.get("slug")]
        failed = [r for r in results if r and not r.get("slug")]

        print(f"Erfolgreich: {len(successful)}/{len(test_isins)}")
        print()

        if successful:
            print("Gefunden:")
            for r in successful:
                print(f"  {r['name']}: {r['slug']} -> {len(r['termine'])} Termine")

        if failed:
            print("\nNicht gefunden:")
            for r in failed:
                print(f"  {r['name']} ({r['isin']})")

    except Exception as e:
        print(f"Fehler: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    import sys
    test_set = sys.argv[1] if len(sys.argv) > 1 else "dax"
    main(test_set)
