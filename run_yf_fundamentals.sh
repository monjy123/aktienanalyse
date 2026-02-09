#!/bin/bash
# =============================================================================
# yfinance Fundamentaldaten-Update
# =============================================================================
# Ergaenzt neue Quartals-/Jahreszahlen aus yfinance in fmp_filtered_numbers.
# Laufzeit: ~1.5-2h fuer alle ~1350 Aktien.
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Logdatei
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/yf_fundamentals_$(date +%Y%m%d_%H%M%S).log"

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
log "  YFINANCE FUNDAMENTALDATEN-UPDATE"
log "=============================================="
log "Start: $(date)"
log "Logfile: $LOG_FILE"
log ""

cd "$SCRIPT_DIR/07_yfinance_fundamentals"
python 00_yf_fundamentals_update.py --with-recalc 2>&1 | tee -a "$LOG_FILE"
cd "$SCRIPT_DIR"

log ""
log "=============================================="
log "  YFINANCE FUNDAMENTALS ABGESCHLOSSEN"
log "=============================================="
log "Ende: $(date)"

# Bereinige alte Logfiles (aelter als 30 Tage)
find "$LOG_DIR" -name "yf_fundamentals_*.log" -mtime +30 -delete 2>/dev/null || true

exit 0
