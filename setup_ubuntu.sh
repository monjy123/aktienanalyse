#!/bin/bash
# =============================================================================
# Ubuntu Server Setup Script - Aktien-Projekt
# =============================================================================
# Dieses Skript richtet das Projekt auf einem frischen Ubuntu Server ein.
#
# Ausfuehrung:
#   chmod +x setup_ubuntu.sh
#   ./setup_ubuntu.sh
# =============================================================================

set -e  # Bei Fehler abbrechen

echo "=============================================="
echo "  AKTIEN-PROJEKT - Ubuntu Setup"
echo "=============================================="
echo ""

# Farben fuer Output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# -----------------------------------------------------------------------------
# 1. System-Updates
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[1/6] System-Updates...${NC}"
sudo apt update && sudo apt upgrade -y

# -----------------------------------------------------------------------------
# 2. System-Dependencies installieren
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[2/6] System-Dependencies installieren...${NC}"

# Python 3 + pip + venv
sudo apt install -y python3 python3-pip python3-venv

# MySQL Server + Client
sudo apt install -y mysql-server mysql-client libmysqlclient-dev

# Chromium fuer Selenium Scraper
sudo apt install -y chromium-browser chromium-chromedriver

# Zusaetzliche Tools
sudo apt install -y git curl wget

echo -e "${GREEN}   System-Dependencies installiert.${NC}"

# -----------------------------------------------------------------------------
# 3. MySQL konfigurieren
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[3/6] MySQL konfigurieren...${NC}"

# MySQL Secure Installation Hinweis
echo ""
echo -e "${YELLOW}WICHTIG: MySQL Root-Passwort setzen${NC}"
echo "Fuehre nach dem Setup aus: sudo mysql_secure_installation"
echo ""

# MySQL User und Datenbanken erstellen
echo "Erstelle MySQL User und Datenbanken..."
echo "Bitte gib dein gewuenschtes MySQL-Passwort ein:"
read -s MYSQL_PASSWORD

sudo mysql << EOF
-- User erstellen (falls nicht vorhanden)
CREATE USER IF NOT EXISTS 'aktien_user'@'localhost' IDENTIFIED BY '${MYSQL_PASSWORD}';

-- Datenbanken erstellen
CREATE DATABASE IF NOT EXISTS tickerdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS raw_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS analytics CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Rechte vergeben
GRANT ALL PRIVILEGES ON tickerdb.* TO 'aktien_user'@'localhost';
GRANT ALL PRIVILEGES ON raw_data.* TO 'aktien_user'@'localhost';
GRANT ALL PRIVILEGES ON analytics.* TO 'aktien_user'@'localhost';

FLUSH PRIVILEGES;
EOF

echo -e "${GREEN}   MySQL konfiguriert.${NC}"

# -----------------------------------------------------------------------------
# 4. Python Virtual Environment erstellen
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[4/6] Python Virtual Environment erstellen...${NC}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# venv erstellen falls nicht vorhanden
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "   Virtual Environment erstellt."
else
    echo "   Virtual Environment existiert bereits."
fi

# venv aktivieren und Packages installieren
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo -e "${GREEN}   Python Packages installiert.${NC}"

# -----------------------------------------------------------------------------
# 5. .env Datei erstellen
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[5/6] .env Datei erstellen...${NC}"

if [ ! -f ".env" ]; then
    cat > .env << EOF
# Datenbank-Konfiguration
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=aktien_user
DB_PASSWORD=${MYSQL_PASSWORD}

# Datenbank-Namen
DB_NAME_RAW=raw_data
DB_NAME_TICKER=tickerdb
DB_NAME_ANALYTICS=analytics

# API Keys
FMP_API_KEY=DEIN_FMP_API_KEY_HIER

# Optional: Chromium Pfade (normalerweise automatisch erkannt)
# CHROMEDRIVER_PATH=/usr/lib/chromium-browser/chromedriver
# CHROME_BINARY=/usr/bin/chromium-browser
EOF
    echo -e "${GREEN}   .env erstellt. Bitte FMP_API_KEY eintragen!${NC}"
else
    echo "   .env existiert bereits."
fi

# -----------------------------------------------------------------------------
# 6. Verzeichnisstruktur pruefen
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[6/6] Verzeichnisstruktur pruefen...${NC}"

FOLDERS=("00_tickerlist" "01_load_fundamentals" "02_history" "03_analytics" "04_frontend" "05_website")

echo "   Gefundene Ordner:"
for folder in "${FOLDERS[@]}"; do
    if [ -d "$folder" ]; then
        count=$(find "$folder" -maxdepth 1 -name "*.py" | wc -l)
        echo -e "   ${GREEN}[OK]${NC} $folder ($count Python-Skripte)"
    else
        echo -e "   ${RED}[FEHLT]${NC} $folder"
    fi
done

# -----------------------------------------------------------------------------
# Abschluss
# -----------------------------------------------------------------------------
echo ""
echo "=============================================="
echo -e "${GREEN}  SETUP ABGESCHLOSSEN${NC}"
echo "=============================================="
echo ""
echo "Naechste Schritte:"
echo ""
echo "1. FMP API Key in .env eintragen:"
echo "   nano .env"
echo ""
echo "2. Virtual Environment aktivieren:"
echo "   source venv/bin/activate"
echo ""
echo "3. Skripte in dieser Reihenfolge ausfuehren:"
echo ""
echo "   # Schritt 0: Ticker-Datenbank aufbauen"
echo "   cd 00_tickerlist"
echo "   python 00_create_table_tickerlist.py"
echo "   python 01_ishares_scrap_ubuntu.py        # <-- Ubuntu-Version!"
echo "   python 02_add_to_tickerlist_from_csv.py"
echo "   python 03_update_yf_ticker.py"
echo "   python 04_fill_eodhd_ticker.py"
echo "   cd .."
echo ""
echo "   # Schritt 1: Fundamentaldaten laden"
echo "   cd 01_load_fundamentals"
echo "   python 00_fmp_financial_loader.py"
echo "   python 01_fmp_market_cap_loader.py"
echo "   python 02_fmp_revenue_segments_loader.py"
echo "   python 03_fmp_sector_pe_loader.py"
echo "   python 04_fmp_sector_performance_loader.py"
echo "   python 05_fmp_treasury_rates_loader.py"
echo "   python 06_fmp_economic_indicators_loader.py"
echo "   cd .."
echo ""
echo "   # Schritt 2: Preishistorie laden"
echo "   cd 02_history"
echo "   python 00_create_history_table.py"
echo "   python 01_yf_history_all.py"
echo "   cd .."
echo ""
echo "   # Schritt 3: Daten aufbereiten"
echo "   cd 03_analytics"
echo "   python 00_create_table_eodtofiltered.py"
echo "   python 01_eoddata_to_filtered.py"
echo "   python 02_create_table_fmpfiltered.py"
echo "   python 03_fmp_to_filtered.py"
echo "   python 04_create_table_calcu_numbers.py"
echo "   python 05_fill_calcu_numbers.py"
echo "   cd .."
echo ""
echo "   # Schritt 4: Frontend-Daten laden"
echo "   cd 04_frontend"
echo "   python 00_create_frontend_tables.py"
echo "   python 01_load_company_info.py"
echo "   python 02_load_live_metrics.py"
echo "   python 03_init_watchlist.py"
echo "   python 04_create_column_settings.py"
echo "   cd .."
echo ""
echo "   # Schritt 5: Webserver starten"
echo "   cd 05_website"
echo "   python app.py"
echo ""
echo "4. Webserver erreichbar unter:"
echo "   http://SERVER_IP:5001"
echo ""
echo "=============================================="
