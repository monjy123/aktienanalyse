#!/bin/bash
# =============================================================================
# Website Restart Script
# =============================================================================
# Stoppt die laufende Website und startet sie neu
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBSITE_DIR="$SCRIPT_DIR/05_website"
LOG_FILE="$SCRIPT_DIR/logs/website.log"

# Farben
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}>>> Website wird neu gestartet...${NC}"

# Stoppe laufende Website-Prozesse
pkill -f "python.*app.py" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "  → Website-Prozess gestoppt"
    sleep 2
else
    echo "  → Kein laufender Website-Prozess gefunden"
fi

# Starte Website neu
cd "$WEBSITE_DIR"

# Aktiviere Virtual Environment
if [ -f "../venv/bin/activate" ]; then
    source ../venv/bin/activate
fi

# Starte Website im Hintergrund
nohup python app.py > "$LOG_FILE" 2>&1 &
WEBSITE_PID=$!

echo -e "${GREEN}  → Website gestartet (PID: $WEBSITE_PID)${NC}"
echo "  → Logfile: $LOG_FILE"

# Kurz warten und prüfen ob Prozess noch läuft
sleep 3
if ps -p $WEBSITE_PID > /dev/null; then
    echo -e "${GREEN}✓ Website läuft erfolgreich${NC}"
    exit 0
else
    echo -e "${RED}✗ Website konnte nicht gestartet werden${NC}"
    echo "  Prüfe Logfile: $LOG_FILE"
    exit 1
fi
