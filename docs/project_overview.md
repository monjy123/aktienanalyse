# Aktienanalyse-System – Projektübersicht

Kurzbeschreibung: lokales Aktien-Datenprojekt mit Fokus auf saubere Datenbeschaffung (Ticker, Kurse, Fundamentaldaten), Konsolidierung/Anreicherung (Analytics) und später UI (Watchlist/Screener/Detailansicht). Zielumgebung ist weiterhin ein Ubuntu-Server (z. B. Hetzner) mit MySQL als zentrales Backend.

## Aktueller Stand
- Tickerbasis: `tickerdb.tickerlist` aus iShares-Scrapes (DAX, MDAX, STOXX600, S&P500, FTSE100, NIKKEI225) inkl. Mapping zu Yahoo und FMP.
- Kurse: `raw_data.yf_prices` via yfinance.
- Fundamentals & Zusatzdaten (FMP): `raw_data.fmp_financial_statements`, `fmp_historical_market_cap`, Revenue-Segmente, Sector PE/Performance, Treasury Rates, Economic Indicators.
- Analytics-Layer: `analytics.fmp_filtered_numbers` (aus FMP + Kursen), `analytics.calcu_numbers` (berechnete Kennzahlen). Legacy-Pfad für EODHD (`analytics.eodhd_filtered_numbers`) existiert noch.
- Frontend: noch nicht implementiert; Watchlist/Screener als nächster Meilenstein.

## Projektziele
1) Watchlist/Dashboard für Holdings & eng beobachtete Werte (Kurse, Kennzahlen, Favoriten, Notizen).  
2) Professioneller Aktienscreener über die gepflegten Indizes mit Filter auf eigene Kennzahlen.  
3) Kennzahlen-Engine (Value/Quality/Growth-Scores, historische Multiples, CAGR, Risiko- und Bewertungsmodelle).  
4) API + Frontend (FastAPI, React/Next.js) für Web-Nutzung und spätere Mobil-Optimierung.

## Datenquellen & Aktualisierung
- FMP (Fundamentals, Market Cap, Segmente, Sektor-Daten, Makro): periodisch, primäre Fundamentaldatenquelle.  
- yfinance (Historische Kurse): täglich/regelmäßig.  
- iShares-Scrapes für Indexzusammensetzung (Ticker/ISIN-Mapping).  
- Legacy: EODHD-Daten sind noch im Codepfad, aber nicht mehr primär.

## Pipelines / Ordnerstruktur
- `00_tickerlist`: Tickerlisten anlegen/aktualisieren (`create_table`, iShares-Scraper, CSV-Import, Yahoo-/EODHD-Ticker-Fill).  
- `01_load_fundamentals`: FMP-Loader für Financial Statements, Historical Market Cap, Revenue Segmente, Sector PE/Performance, Treasury Rates, Economic Indicators.  
- `02_history`: Kurs-Tabelle `raw_data.yf_prices` anlegen und per yfinance befüllen.  
- `03_analytics`: FMP-Daten nach `analytics.fmp_filtered_numbers` mappen (inkl. Kurs/Market Cap), Kennzahlen nach `analytics.calcu_numbers` berechnen, Legacy-Pivot aus EODHD.  
- `04_frontend`: Platzhalter für künftige UI/Assets.  
- `db.py`: zentrale DB-Verbindung (Environment-gestützt).

## Architektur / Betrieb
- MySQL als Kern-DB (Schemas: `tickerdb`, `raw_data`, `analytics`).  
- Python-ETLs (Batch/Threaded) laufen lokal; Ziel ist automatischer Betrieb (systemd/cron) auf Ubuntu-Server.  
- API/Frontend sind geplant, noch nicht implementiert.

## Roadmap (kurzfristig)
- Watchlist-/Screener-Schema und Docs aktuell halten (inkl. Favoriten/Notizen).  
- Stabilen ETL-Zyklus für FMP + yfinance etablieren (inkl. Logging/Monitoring).  
- API-Schnittstellen für Watchlist/Screener entwerfen.  
- Frontend-Prototyp Watchlist (Spaltenwahl, Favoriten, Notizen-Popup) anschieben.
