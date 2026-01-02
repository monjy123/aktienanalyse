#!/bin/bash
# =============================================================================
# Daily Update Script - Aktualisiert yfinance Daten und Berechnungen
# =============================================================================
# Täglich auszuführen für:
# - Kursdaten von yfinance (Schritt 2)
# - Live Metrics von yfinance (04_frontend/02_load_live_metrics.py)
# - Berechnungen auf Basis der Daten (Schritt 3)
# - Frontend-Tabellen aktualisieren (Schritt 4, aber OHNE company_info)
#
# NICHT enthalten (weil FMP API):
# - Schritt 1: FMP Fundamentaldaten (API-Key abgelaufen)
# - 01_load_company_info.py (nur bei Bedarf manuell ausführen)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Logdatei mit Datum
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily_update_$(date +%Y%m%d_%H%M%S).log"

# Farben
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Logging-Funktion
log() {
    echo -e "$1" | tee -a "$LOG_FILE"
}

# Virtual Environment aktivieren
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    log "${RED}FEHLER: Virtual Environment nicht gefunden.${NC}"
    exit 1
fi

log "=============================================="
log "  DAILY UPDATE - YFINANCE DATEN"
log "=============================================="
log "Start: $(date)"
log "Logfile: $LOG_FILE"
log ""

# =============================================================================
# SCHRITT 1: Kursdaten aktualisieren (yfinance History)
# =============================================================================
log "${YELLOW}>>> SCHRITT 1: Kursdaten laden (yfinance)${NC}"
cd "$SCRIPT_DIR/02_history"
python 01_yf_history_all.py 2>&1 | tee -a "$LOG_FILE"
cd "$SCRIPT_DIR"

# =============================================================================
# SCHRITT 2: Analytics - Berechnungen durchführen
# =============================================================================
log ""
log "${YELLOW}>>> SCHRITT 2: Daten aufbereiten (Analytics)${NC}"
cd "$SCRIPT_DIR/03_analytics"

python 01_eoddata_to_filtered.py 2>&1 | tee -a "$LOG_FILE"
python 03_fmp_to_filtered.py 2>&1 | tee -a "$LOG_FILE"
python 05_fill_calcu_numbers.py 2>&1 | tee -a "$LOG_FILE"

cd "$SCRIPT_DIR"

# =============================================================================
# SCHRITT 3: Frontend - Live Metrics aktualisieren (yfinance API)
# =============================================================================
log ""
log "${YELLOW}>>> SCHRITT 3: Live Metrics laden (yfinance API)${NC}"
cd "$SCRIPT_DIR/04_frontend"

# Nur live_metrics aktualisieren, NICHT company_info (Stammdaten ändern sich selten)
python 02_load_live_metrics.py 2>&1 | tee -a "$LOG_FILE"

cd "$SCRIPT_DIR"

# =============================================================================
# Abschluss
# =============================================================================
log ""
log "=============================================="
log "${GREEN}  DAILY UPDATE ABGESCHLOSSEN${NC}"
log "=============================================="
log "Ende: $(date)"
log ""

# Bereinige alte Logfiles (älter als 30 Tage)
find "$LOG_DIR" -name "daily_update_*.log" -mtime +30 -delete 2>/dev/null || true

exit 0
