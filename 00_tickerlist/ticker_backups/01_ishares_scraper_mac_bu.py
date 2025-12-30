"""
Multi-ETF Version deines funktionierenden EXSA-Skripts.

Pipeline pro ETF:
1) Scraping (Selenium) der iShares Holdings (nur Aktien) -> {code}_scraped.csv
2) Optional: Download der offiziellen ETF-Holdings-CSV -> {code}_official.csv
3) Optional: Merge (offizielle CSV + Scraped ISIN) -> {code}_merged.csv
4) Bereinigung -> {code}_clean.csv
5) Optionaler Import in MySQL (tick_table) mit ON DUPLICATE KEY UPDATE

Hinweis:
- WebDriver Manager sorgt für passenden ChromeDriver zur installierten Chrome-Version.
- DB-Zugangsdaten möglichst via Umgebungsvariablen setzen.
"""

import os
import time
import sys
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

import mysql.connector
from mysql.connector import Error as MySQLError

# ==========================
# KONFIGURATION
# ==========================

# Selenium / Headless
HEADLESS = True
SELENIUM_TIMEOUT = 15
PAGE_WAIT_SECONDS = 2

# MySQL Konfiguration (gern per ENV überschreiben)
DB_HOST = os.getenv("MYDB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("MYDB_PORT", "3306"))
DB_USER = os.getenv("MYDB_USER", "monjy")
DB_PASSWORD = os.getenv("MYDB_PASSWORD", "Emst4558!!")
DB_NAME = os.getenv("MYDB_NAME", "livedb")

DO_DB_IMPORT = True  # globaler Schalter

# ETF-Konfigurationen:
# code: kurzer Name fürs Dateipräfix
# index_name: landet in tick_table.indizes
# main_url: iShares Produktseite
# official_csv_url: optional; wenn None -> kein Download/Merge
ETF_CONFIG = [
    {
        "code": "STOXX600",
        "index_name": "STOXX Europe 600",
        "main_url": (
            "https://www.ishares.com/de/privatanleger/de/produkte/251931/"
            "ishares-stoxx-europe-600-ucits-etf-de-fund"
        ),
        "official_csv_url": (
            "https://www.ishares.com/de/privatanleger/de/produkte/251931/"
            "ishares-stoxx-europe-600-ucits-etf-de-fund/"
            "1478358465952.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund"
        ),
    },
    {
        "code": "DAX",
        "index_name": "DAX",
        "main_url": (
            "https://www.ishares.com/de/privatanleger/de/produkte/251464/"
            "ishares-dax-ucits-etf-de-fund"
        ),
        "official_csv_url": None,  # wenn du später einen Holdings-CSV-Link findest, hier eintragen
    },
    {
        "code": "MDAX",
        "index_name": "MDAX",
        "main_url": (
            "https://www.ishares.com/de/privatanleger/de/produkte/251845/"
            "ishares-mdax-ucits-etf-de-fund"
        ),
        "official_csv_url": None,
    },
    {
        "code": "SP500",
        "index_name": "S&P 500",
        "main_url": (
            "https://www.ishares.com/de/privatanleger/de/produkte/253743/"
            "ishares-sp-500-b-ucits-etf-acc-fund"
        ),
        "official_csv_url": None,
    },
    {
        "code": "FTSE100",
        "index_name": "FTSE 100",
        "main_url": (
            "https://www.ishares.com/de/privatanleger/de/produkte/251795/"
            "ishares-ftse-100-ucits-etf-inc-fund"
        ),
        "official_csv_url": None,
    },
    {
        "code": "NIKKEI225",
        "index_name": "Nikkei 225",
        "main_url": (
            "https://www.ishares.com/de/privatanleger/de/produkte/251898/"
            "ishares-nikkei-225-ucits-etf-de-fund"
        ),
        "official_csv_url": None,
    },
]


# ==========================
# Hilfsfunktionen
# ==========================

def create_driver(headless=True):
    """Erstellt einen Chromedriver via webdriver-manager."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def safe_sleep(sec):
    time.sleep(sec)


# ==========================
# 1) Scraping (Selenium)
# ==========================
def scrape_ishares_holdings(url: str, out_csv: str) -> int:
    print(f"==> Starte Selenium-Scraping für {url}")
    driver = create_driver(headless=HEADLESS)
    wait = WebDriverWait(driver, SELENIUM_TIMEOUT)
    all_rows = []

    try:
        driver.get(url)
        safe_sleep(2)

        # Cookies akzeptieren
        try:
            wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Alle Akzeptieren')]")
            )).click()
            print("Cookies akzeptiert")
        except Exception:
            print("Kein Cookie-Popup gefunden oder Klick fehlgeschlagen")

        # Privatanleger-Popup
        try:
            wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(text(),'Weiter')]")
            )).click()
            print("Privatanleger-Popup bestätigt")
        except Exception:
            print("Kein 'Privatanleger'-Popup gefunden oder Klick fehlgeschlagen")

        # Reiter Bestände/Positionen/Holdings
        try:
            tab = wait.until(EC.element_to_be_clickable(
                (By.XPATH,
                 "//a[contains(text(),'Bestände') or contains(text(),'Holdings') or contains(text(),'Positionen')]")
            ))
            tab.click()
            safe_sleep(PAGE_WAIT_SECONDS)
            print("Reiter 'Bestände/Holdings/Positionen' geöffnet")
        except Exception:
            print("WARNUNG: Reiter 'Bestände/Holdings/Positionen' nicht gefunden - versuche direkt Tabelle zu lesen")

        # Seiten durchlaufen
        while True:
            try:
                rows = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "#allHoldingsTable tbody tr")
                ))
            except Exception:
                print("Keine Tabelleneinträge gefunden (oder Timeout). Breche ab.")
                break

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 9:
                    continue
                try:
                    asset_class = cols[3].text.strip()
                except Exception:
                    asset_class = ""
                if asset_class.lower() != "aktien":
                    continue
                ticker = cols[0].text.strip()
                name = cols[1].text.strip()
                isin = cols[8].text.strip() if len(cols) > 8 else ""
                all_rows.append([ticker, name, isin])

            # Next-Button klicken
            try:
                next_btn = driver.find_element(By.ID, "allHoldingsTable_next")
                if "disabled" in next_btn.get_attribute("class"):
                    break
                driver.execute_script("arguments[0].click();", next_btn)
                safe_sleep(PAGE_WAIT_SECONDS)
            except Exception:
                break

    except Exception as e:
        print("Fehler während des Scraping:", e)
        traceback.print_exc()
    finally:
        driver.quit()

    df = pd.DataFrame(all_rows, columns=["Emittententicker", "Name", "ISIN"])
    df.to_csv(out_csv, index=False)
    print(f"Scraping abgeschlossen. {len(df)} Zeilen gespeichert -> {out_csv}")
    return len(df)


# ==========================
# 2) Offizielle CSV downloaden (optional)
# ==========================
def download_official_csv(csv_url: str, output_filename: str) -> bool:
    print("==> Starte Download der offiziellen Holdings-CSV...")
    try:
        resp = requests.get(csv_url, timeout=20)
    except Exception as e:
        print("Netzwerkfehler beim Download:", e)
        return False

    if resp.status_code == 200 and resp.content:
        try:
            with open(output_filename, "wb") as f:
                f.write(resp.content)
            print(f"Download erfolgreich -> {output_filename}")
            return True
        except Exception as e:
            print("Fehler beim Schreiben der Datei:", e)
            return False
    else:
        print(f"Download fehlgeschlagen. HTTP Status: {resp.status_code}")
        return False


# ==========================
# 3) Merge offizielles CSV + Scraped
# ==========================
def merge_official_and_scraped(official_file: str, scraped_file: str, out_file: str) -> int:
    print("==> Merge offizielle CSV mit gescrapten ISINs...")
    # wie bei EXSA: erste 2 Zeilen sind Datum + Leerzeile
    df_official = pd.read_csv(official_file, skiprows=2)
    df_scraped = pd.read_csv(scraped_file)

    df_merged = df_official.merge(
        df_scraped[["Emittententicker", "Name", "ISIN"]],
        on=["Emittententicker", "Name"],
        how="left"
    )

    cols = list(df_merged.columns)
    if "ISIN" in cols:
        cols.insert(cols.index("Name") + 1, cols.pop(cols.index("ISIN")))
        df_merged = df_merged[cols]

    df_merged.to_csv(out_file, index=False)
    print(f"Merge fertig -> {out_file} ({len(df_merged)} Zeilen)")
    return len(df_merged)


# ==========================
# 4) Bereinigung
# ==========================
def clean_and_filter(in_file: str, out_file: str) -> int:
    print("==> Bereinige Daten und filtere 'Aktien'...")
    df = pd.read_csv(in_file, dtype=str)

    if "Anlageklasse" not in df.columns:
        print("WARNUNG: Spalte 'Anlageklasse' nicht vorhanden. Ganze Datei wird weiterverarbeitet.")
    else:
        df = df[df["Anlageklasse"] == "Aktien"]

    # Relevante Spalten
    wanted = []
    for c in ["Emittententicker", "Name", "Börse", "ISIN"]:
        if c in df.columns:
            wanted.append(c)
    if not wanted:
        raise ValueError("Keine der erwarteten Spalten vorhanden in der Datei.")

    df = df[wanted].copy()

    if "Emittententicker" in df.columns:
        df["Emittententicker"] = df["Emittententicker"].astype(str).str.replace(" ", "-", regex=False)
        df["Emittententicker"] = df["Emittententicker"].str.replace(".", "", regex=False)

    df = df.where(pd.notnull(df), None)

    df.to_csv(out_file, index=False)
    print(f"Bereinigung abgeschlossen -> {out_file} ({len(df)} Zeilen)")
    return len(df)


# ==========================
# 5) Import in MySQL
# ==========================
def import_into_mysql(csv_file: str, index_name: str):
    print(f"==> Starte Import in MySQL für Index '{index_name}'...")
    df = pd.read_csv(csv_file, dtype=str)

    required_cols = ["Emittententicker", "Name", "Börse", "ISIN"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Spalte fehlt für DB-Import: {c}")

    try:
        con = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=False
        )
    except MySQLError as e:
        print("Fehler: Verbindung zur Datenbank fehlgeschlagen:", e)
        return

    cur = con.cursor()

    insert_stmt = """
    INSERT INTO tick_table (default_ticker, name, stock_exchange, ISIN, indizes)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        default_ticker = VALUES(default_ticker),
        name = VALUES(name),
        stock_exchange = VALUES(stock_exchange),
        indizes = VALUES(indizes),
        updated_at = CURRENT_TIMESTAMP
    """

    inserted = 0
    try:
        for _, row in df.iterrows():
            vals = (
                row["Emittententicker"],
                row["Name"],
                row["Börse"],
                row.get("ISIN", None),
                index_name
            )
            cur.execute(insert_stmt, vals)
            inserted += 1
        con.commit()
        print(f"DB-Import abgeschlossen: {inserted} Zeilen verarbeitet.")
    except MySQLError as e:
        con.rollback()
        print("DB-Fehler während Import, Rollback ausgeführt:", e)
    finally:
        cur.close()
        con.close()


# ==========================
# MAIN PRO ETF
# ==========================
def process_etf(cfg):
    code = cfg["code"]
    index_name = cfg["index_name"]
    main_url = cfg["main_url"]
    csv_url = cfg.get("official_csv_url")

    print("\n===============================")
    print(f"ETF: {code} ({index_name})")
    print("===============================")

    scraped_file = f"{code}_scraped.csv"
    official_file = f"{code}_official.csv"
    merged_file = f"{code}_merged.csv"
    clean_file = f"{code}_clean.csv"

    # 1) Scraping
    scraped_count = scrape_ishares_holdings(main_url, scraped_file)
    if scraped_count == 0:
        print("WARNUNG: Scraping ergab 0 Einträge – ggf. Seite geändert.")

    # 2+3) Falls offizielle CSV vorhanden: Download + Merge
    if csv_url:
        ok = download_official_csv(csv_url, official_file)
        if not ok:
            print("FEHLER: Offizielle CSV konnte nicht geladen werden, fahre mit Scraped weiter.")
            base_for_clean = scraped_file
        else:
            try:
                merge_official_and_scraped(official_file, scraped_file, merged_file)
                base_for_clean = merged_file
            except Exception as e:
                print("FEHLER beim Merge, nutze Scraped-Datei als Basis:", e)
                traceback.print_exc()
                base_for_clean = scraped_file
    else:
        print("Keine offizielle CSV-URL konfiguriert – nutze Scraped-Datei als Basis.")
        base_for_clean = scraped_file

    # 4) Bereinigung
    try:
        clean_and_filter(base_for_clean, clean_file)
    except Exception as e:
        print("FEHLER bei Clean/Bereinigung:", e)
        traceback.print_exc()
        return

    # 5) Optional DB-Import
    if DO_DB_IMPORT:
        try:
            import_into_mysql(clean_file, index_name)
        except Exception as e:
            print("FEHLER beim DB-Import:", e)
            traceback.print_exc()
    else:
        print("DB-Import deaktiviert (DO_DB_IMPORT=False).")


def main():
    try:
        for cfg in ETF_CONFIG:
            process_etf(cfg)
        print("\n==> Alle ETFs verarbeitet.")
    except KeyboardInterrupt:
        print("Abbruch durch Benutzer.")
    except Exception as e:
        print("Unbehandelter Fehler:", e)
        traceback.print_exc()


if __name__ == "__main__":
    main()
