#!/bin/bash
# =============================================================================
# Weekend Scraping Script - finanzen.net Earnings & Estimates
# =============================================================================
# Läuft am Wochenende (Sa/So) wenn Börsen geschlossen sind
# Scraped alle Indizes rotierend:
#   Samstag: DAX, MDAX, FTSE 100, Nikkei 225
#   Sonntag: STOXX600, S&P 500
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Logdatei
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/weekend_scraping_$(date +%Y%m%d_%H%M%S).log"

# Logging-Funktion
log() {
    echo -e "$1" | tee -a "$LOG_FILE"
}

# Virtual Environment aktivieren
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    log "FEHLER: Virtual Environment nicht gefunden."
    exit 1
fi

log "=============================================="
log "  WEEKEND SCRAPING - finanzen.net"
log "=============================================="
log "Start: $(date)"
log "Logfile: $LOG_FILE"
log ""

# Tag der Woche (0=Sonntag, 6=Samstag)
DAY_OF_WEEK=$(date +%w)

cd "$SCRIPT_DIR/06_scrapers"

# FAST VERSION verwenden (requests statt Selenium, ~100x schneller)
SCRAPER="02_scrape_finanzen_net_fast.py"

if [ "$DAY_OF_WEEK" -eq 6 ]; then
    # SAMSTAG: Kleinere Indizes (~415 Aktien, ~17 Min)
    log ">>> SAMSTAG: DAX, MDAX, FTSE 100, Nikkei 225"
    log ""

    log "--- DAX ---"
    python $SCRAPER --index "DAX" 2>&1 | tee -a "$LOG_FILE"

    log ""
    log "--- MDAX ---"
    python $SCRAPER --index "MDAX" 2>&1 | tee -a "$LOG_FILE"

    log ""
    log "--- FTSE 100 ---"
    python $SCRAPER --index "FTSE 100" 2>&1 | tee -a "$LOG_FILE"

    log ""
    log "--- Nikkei 225 ---"
    python $SCRAPER --index "Nikkei 225" 2>&1 | tee -a "$LOG_FILE"

elif [ "$DAY_OF_WEEK" -eq 0 ]; then
    # SONNTAG: Große Indizes (~1106 Aktien, ~45 Min)
    log ">>> SONNTAG: STOXX600, S&P 500"
    log ""

    log "--- STOXX600 ---"
    python $SCRAPER --index "STOXX600" 2>&1 | tee -a "$LOG_FILE"

    log ""
    log "--- S&P 500 ---"
    python $SCRAPER --index "S&P 500" 2>&1 | tee -a "$LOG_FILE"

else
    log "Kein Wochenende - Skript wird übersprungen."
fi

cd "$SCRIPT_DIR"

log ""
log "=============================================="
log "  WEEKEND SCRAPING ABGESCHLOSSEN"
log "=============================================="
log "Ende: $(date)"

# Bereinige alte Logfiles (älter als 30 Tage)
find "$LOG_DIR" -name "weekend_scraping_*.log" -mtime +30 -delete 2>/dev/null || true

exit 0
