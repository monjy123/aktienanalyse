# Aktienanalyse - Setup-Anleitung

## Voraussetzungen
- Ubuntu 22.04+ Server
- Root-/sudo-Zugang
- FMP API Key (financialmodelingprep.com)

## 1. Automatisches Setup

```bash
# Repository klonen
git clone <repo-url> ~/aktienanalyse
cd ~/aktienanalyse

# Setup-Skript ausfuehren (installiert Python, MySQL, Chromium, erstellt venv + DBs)
chmod +x setup_ubuntu.sh
./setup_ubuntu.sh
```

Das Skript installiert:
- Python 3, pip, venv
- MySQL Server + Client
- Chromium + Chromedriver (fuer Selenium-Scraper)
- Python-Packages aus `requirements.txt`
- Erstellt MySQL-User `aktien_user` und die Datenbanken `tickerdb`, `raw_data`, `analytics`
- Erstellt `.env` mit den DB-Credentials

## 2. .env konfigurieren

Nach dem Setup die `.env`-Datei anpassen:
```bash
nano .env
```

Benoetigte Variablen (siehe `.env.example`):
- `DB_PASSWORD` - MySQL-Passwort (wird vom Setup-Skript gesetzt)
- `FMP_API_KEY` - API-Key von financialmodelingprep.com
- `FLASK_SECRET_KEY` - Zufaelliger Key fuer Flask-Sessions (generieren mit `python3 -c "import secrets; print(secrets.token_hex(32))"`)

## 3. Datenbank-Schemas importieren

Falls die Schemas nicht ueber die Pipeline erstellt werden, koennen sie aus den Dumps importiert werden:
```bash
mysql -u aktien_user -p raw_data < 00_setup/schema_raw_data.sql
mysql -u aktien_user -p tickerdb < 00_setup/schema_tickerdb.sql
mysql -u aktien_user -p analytics < 00_setup/schema_analytics.sql
```

## 4. FMP-Rohdaten importieren

Die FMP-Rohdaten (`raw_data`-Datenbank) muessen vom bestehenden Server exportiert und importiert werden:
```bash
# Auf dem bestehenden Server:
mysqldump -u aktien_user -p raw_data > raw_data_dump.sql

# Auf dem neuen Server:
mysql -u aktien_user -p raw_data < raw_data_dump.sql
```

## 5. Pipeline ausfuehren

```bash
source venv/bin/activate

# Komplette Pipeline (Schritt 0-4)
./run_pipeline.sh

# Oder einzelne Schritte:
./run_pipeline.sh 0   # Ticker-Datenbank aufbauen
./run_pipeline.sh 1   # FMP Fundamentaldaten laden
./run_pipeline.sh 2   # Preishistorie laden
./run_pipeline.sh 3   # Analytics berechnen
./run_pipeline.sh 4   # Frontend-Daten laden
```

## 6. Cronjobs einrichten

Die aktiven Cronjobs befinden sich in `00_setup/crontab.txt`. Importieren mit:
```bash
crontab 00_setup/crontab.txt
```

Ueberblick der Cronjobs:
| Zeitplan | Skript | Beschreibung |
|----------|--------|--------------|
| Mo-Sa 02:00 | `run_daily_update.sh` | Taegliches Daten-Update |
| Sa 02:30, So 02:00 | `run_weekend_scraping.sh` | finanzen.net Estimates |
| Taeglich 02:45 | `run_yf_fundamentals.sh` | yfinance Fundamentaldaten |
| Taeglich 05:30 | `restart_website.sh` | Website Neustart |
| Mo-Fr 09:00 | `run_earnings_fetch.sh` | EU/Asien Earnings |
| Di-Sa 08:00 | `run_earnings_fetch.sh --yesterday` | US Earnings |

## 7. Website starten

```bash
cd 05_website
python app.py
# Erreichbar unter http://SERVER_IP:5001
```

## Ordnerstruktur

```
aktienanalyse/
├── 00_setup/              # Setup-Dateien (Schema-Dumps, Crontab)
├── 00_tickerlist/          # Ticker-Datenbank aufbauen
├── 01_load_fundamentals/   # FMP API Daten laden
├── 02_history/             # Preishistorie (Yahoo Finance)
├── 02_filter_fmp_data/     # FMP-Daten filtern
├── 03_analytics/           # Kennzahlen berechnen
├── 03_create_analytics_schema/  # Analytics-Schema erstellen
├── 04_frontend/            # Frontend-Daten aufbereiten
├── 04_populate_analytics/  # Analytics befuellen
├── 05_website/             # Flask-Webapplikation
├── 06_scrapers/            # finanzen.net Scraper
├── 07_yfinance_fundamentals/  # yfinance Updates
├── db.py                   # Zentrale DB-Verbindung
├── .env                    # Credentials (nicht im Git!)
├── requirements.txt        # Python-Dependencies
├── setup_ubuntu.sh         # Automatisches Server-Setup
├── run_pipeline.sh         # Daten-Pipeline
└── *.sh                    # Cron-Skripte
```
