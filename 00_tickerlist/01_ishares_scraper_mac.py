import os
import time
import traceback
import requests
import pandas as pd

from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

    cols_lower = [c.lower() for c in df_scraped.columns]

    def find_col(name):
        if name.lower() in cols_lower:
            return df_scraped.columns[cols_lower.index(name.lower())]
        raise ValueError(f"Spalte '{name}' nicht gefunden. Gefunden: {df_scraped.columns}")

    col_ticker = find_col("Emittententicker")
    col_name = find_col("Name")
    col_isin = find_col("ISIN")

    merge_src = df_scraped[[col_ticker, col_name, col_isin]]

    df_merged = df_official.merge(
        merge_src,
        how="left",
        left_on=[col_ticker, col_name],
        right_on=[col_ticker, col_name]
    )

    df_merged.to_csv(out_file, index=False)
    print(f"Merge fertig → {out_file}")
    return len(df_merged)


# ==========================
# 4) CLEAN
# ==========================

def clean_and_filter(in_file: str, out_file: str) -> int:
    print("==> Bereinige Daten…")

    df = pd.read_csv(in_file, dtype=str)

    if "Anlageklasse" in df.columns:
        df = df[df["Anlageklasse"] == "Aktien"]

    cols = [c for c in df.columns if c in ["Emittententicker", "Name", "Börse", "ISIN"]]
    df = df[cols] if cols else df

    df.to_csv(out_file, index=False)
    print(f"Bereinigung abgeschlossen → {out_file} ({len(df)} Zeilen)")
    return len(df)


# ==========================
# MAIN PIPELINE
# ==========================

def process_etf(cfg):
    code = cfg["code"]
    index_name = cfg["index_name"]
    main_url = cfg["main_url"]

    print(f"\n==== ETF {code} ({index_name}) ====")

    scraped = f"{code}_scraped.csv"
    official = f"{code}_official.csv"
    merged = f"{code}_merged.csv"
    clean = f"{code}_clean.csv"

    scrape_ishares_holdings(main_url, scraped)

    if cfg["official_csv_url"]:
        if download_official_csv(cfg["official_csv_url"], official):
            merge_official_and_scraped(official, scraped, merged)
            base = merged
        else:
            base = scraped
    else:
        base = scraped

    clean_and_filter(base, clean)


def main():
    for cfg in ETF_CONFIG:
        process_etf(cfg)
    print("\n==> Alle ETFs verarbeitet.")


if __name__ == "__main__":
    main()
