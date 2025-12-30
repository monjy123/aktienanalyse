import os
import time
import traceback
import requests
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ==========================
# KONFIGURATION
# ==========================

HEADLESS = True
SELENIUM_TIMEOUT = 15
PAGE_WAIT_SECONDS = 2

ETF_CONFIG = [
    {
        "code": "STOXX600",
        "index_name": "STOXX Europe 600",
        "main_url":
            "https://www.ishares.com/de/privatanleger/de/produkte/251931/"
            "ishares-stoxx-europe-600-ucits-etf-de-fund",
        "official_csv_url":
            "https://www.ishares.com/de/privatanleger/de/produkte/251931/"
            "ishares-stoxx-europe-600-ucits-etf-de-fund/"
            "1478358465952.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund",
    },
    {
        "code": "DAX",
        "index_name": "DAX",
        "main_url":
            "https://www.ishares.com/de/privatanleger/de/produkte/251464/"
            "ishares-dax-ucits-etf-de-fund",
        "official_csv_url": None,
    },
    {
        "code": "MDAX",
        "index_name": "MDAX",
        "main_url":
            "https://www.ishares.com/de/privatanleger/de/produkte/251845/"
            "ishares-mdax-ucits-etf-de-fund",
        "official_csv_url": None,
    },
    {
        "code": "SP500",
        "index_name": "S&P 500",
        "main_url":
            "https://www.ishares.com/de/privatanleger/de/produkte/253743/"
            "ishares-sp-500-b-ucits-etf-acc-fund",
        "official_csv_url": None,
    },
    {
        "code": "FTSE100",
        "index_name": "FTSE 100",
        "main_url":
            "https://www.ishares.com/de/privatanleger/de/produkte/251795/"
            "ishares-ftse-100-ucits-etf-inc-fund",
        "official_csv_url": None,
    },
    {
        "code": "NIKKEI225",
        "index_name": "Nikkei 225",
        "main_url":
            "https://www.ishares.com/de/privatanleger/de/produkte/251898/"
            "ishares-nikkei-225-ucits-etf-de-fund",
        "official_csv_url": None,
    },
]

# Helfer zum robusten Spaltensuchen
def find_col_generic(df, candidates, label):
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    raise ValueError(f"Spalte für {label} nicht gefunden. Gefunden: {df.columns}")

# ==========================
# HILFSFUNKTIONEN
# ==========================

def create_driver(headless=True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    # Bevorzugt den systemweiten Chromedriver (apt/snap), um falsche wdm-Binaries zu vermeiden.
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

    # Chromium-Binary für Snap/apt setzen, falls nötig.
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
        # Fallback: wdm zieht passenden Driver
        service = Service(ChromeDriverManager().install())

    return webdriver.Chrome(service=service, options=chrome_options)


def safe_sleep(sec):
    time.sleep(sec)


# ==========================
# 1) SCRAPING
# ==========================

def scrape_ishares_holdings(url: str, out_csv: str) -> int:
    print(f"==> Scraping {url}")

    driver = create_driver(headless=HEADLESS)
    wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
    rows_out = []

    try:
        driver.get(url)
        safe_sleep(2)

        # Cookies akzeptieren
        try:
            wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Alle Akzeptieren')]")
            )).click()
        except:
            pass

        # Privatanleger-Popup
        try:
            wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(text(),'Weiter')]")
            )).click()
        except:
            pass

        # Reiter öffnen
        try:
            wait.until(EC.element_to_be_clickable(
                (By.XPATH,
                 "//a[contains(text(),'Bestände') or contains(text(),'Holdings') or contains(text(),'Positionen')]")
            )).click()
            safe_sleep(PAGE_WAIT_SECONDS)
        except:
            print("⚠ Reiter nicht gefunden, lese Tabelle direkt…")

        # Seiten laden
        while True:
            try:
                rows = driver.find_elements(By.CSS_SELECTOR, "#allHoldingsTable tbody tr")
            except:
                break

            for r in rows:
                cols = r.find_elements(By.TAG_NAME, "td")
                if len(cols) < 9:
                    continue

                asset_class = cols[3].text.strip().lower()
                if asset_class != "aktien":
                    continue

                ticker = cols[0].text.strip()
                name = cols[1].text.strip()
                isin = cols[8].text.strip()

                rows_out.append([ticker, name, isin])

            # Weiter klicken?
            try:
                nxt = driver.find_element(By.ID, "allHoldingsTable_next")
                if "disabled" in nxt.get_attribute("class"):
                    break
                driver.execute_script("arguments[0].click();", nxt)
                safe_sleep(PAGE_WAIT_SECONDS)
            except:
                break

    finally:
        driver.quit()

    df = pd.DataFrame(rows_out, columns=["Emittententicker", "Name", "ISIN"])
    df.to_csv(out_csv, index=False)
    print(f"Scraping abgeschlossen: {len(df)} Zeilen → {out_csv}")
    return len(df)


# ==========================
# 2) OFFIZIELLE CSV
# ==========================

def download_official_csv(url: str, out_file: str) -> bool:
    print("==> Lade offizielle CSV…")
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            with open(out_file, "wb") as f:
                f.write(r.content)
            print(f"Download OK → {out_file}")
            return True
    except:
        pass
    print("❌ Download fehlgeschlagen")
    return False


# ==========================
# 3) ROBUSTER MERGE
# ==========================

def merge_official_and_scraped(official_file: str, scraped_file: str, out_file: str) -> int:
    print("==> Merge Dateien…")

    df_official = pd.read_csv(official_file, skiprows=2)
    df_scraped = pd.read_csv(scraped_file)

    # Scraped Spalten suchen (robust gegen andere Kopfzeilen)
    col_ticker = find_col_generic(df_scraped, ["Emittententicker", "Ticker"], "Ticker (scraped)")
    col_name = find_col_generic(df_scraped, ["Name"], "Name (scraped)")
    col_isin = find_col_generic(df_scraped, ["ISIN"], "ISIN (scraped)")

    merge_src = df_scraped[[col_ticker, col_name, col_isin]]
    merge_src.columns = ["ticker_scraped", "name_scraped", "isin_scraped"]

    # Offizielle Spalten suchen (können je nach Datei leicht abweichen)
    col_ticker_off = find_col_generic(df_official, ["Ticker", "Emittententicker"], "Ticker (official)")
    col_name_off = find_col_generic(df_official, ["Name"], "Name (official)")
    col_isin_off = find_col_generic(df_official, ["ISIN"], "ISIN (official)")

    df_official = df_official.rename(columns={
        col_ticker_off: "ticker_official",
        col_name_off: "name_official",
        col_isin_off: "isin_official"
    })

    merged = pd.merge(
        merge_src,
        df_official[["ticker_official", "name_official", "isin_official"]],
        left_on="isin_scraped",
        right_on="isin_official",
        how="left"
    )

    merged["ticker_final"] = merged["ticker_official"].combine_first(merged["ticker_scraped"])
    merged["name_final"] = merged["name_official"].combine_first(merged["name_scraped"])
    merged["isin_final"] = merged["isin_official"].combine_first(merged["isin_scraped"])

    merged_out = merged[["ticker_final", "name_final", "isin_final"]]
    merged_out.columns = ["ticker", "name", "isin"]

    merged_out.to_csv(out_file, index=False)
    print(f"Merge abgeschlossen: {len(merged_out)} Zeilen → {out_file}")
    return len(merged_out)


# ==========================
# 4) PIPELINE PRO ETF
# ==========================

def process_etf(cfg):
    code = cfg["code"]
    main_url = cfg["main_url"]
    official_csv_url = cfg.get("official_csv_url")

    scraped = f"{code}_scraped.csv"
    cleaned = f"{code}_clean.csv"
    merged = f"{code}_merged.csv"

    print(f"\n==== ETF {code} ({cfg['index_name']}) ====")

    # 1) Scrape
    scrape_ishares_holdings(main_url, scraped)

    # 2) Falls offizielle CSV existiert, downloaden und mergen
    if official_csv_url:
        if download_official_csv(official_csv_url, f"{code}_official.csv"):
            try:
                merge_official_and_scraped(f"{code}_official.csv", scraped, merged)
            except Exception as e:
                print(f"⚠ Merge fehlgeschlagen ({e}), verwende Scrape-Daten.")
                merged = scraped
        else:
            print("⚠ Offizielle CSV fehlgeschlagen, verwende nur Scrape-Daten.")
            merged = scraped
    else:
        merged = scraped

    # 3) Bereinigen (Trimmen, Deduplizieren)
    df = pd.read_csv(merged)
    if not {"ticker", "name", "isin"}.issubset(set(df.columns)):
        rename_map = {
            find_col_generic(df, ["ticker", "Emittententicker"], "Ticker (final)"): "ticker",
            find_col_generic(df, ["name"], "Name (final)"): "name",
            find_col_generic(df, ["isin"], "ISIN (final)"): "isin",
        }
        df = df.rename(columns=rename_map)

    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["isin"] = df["isin"].astype(str).str.strip()

    df = df.drop_duplicates(subset=["isin"])
    df.to_csv(cleaned, index=False)
    print(f"Bereinigt & gespeichert: {len(df)} Zeilen → {cleaned}")


# ==========================
# 5) MAIN
# ==========================

def main():
    try:
        for cfg in ETF_CONFIG:
            process_etf(cfg)
    except Exception as e:
        print("❌ Fehler:", e)
        traceback.print_exc()


if __name__ == "__main__":
    main()
