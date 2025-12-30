#!/bin/bash
# =============================================================================
# Pipeline Runner - Fuehrt alle Skripte in der richtigen Reihenfolge aus
# =============================================================================
# Ausfuehrung:
#   chmod +x run_pipeline.sh
#   ./run_pipeline.sh [step]
#
# Optionen:
#   ./run_pipeline.sh        # Alles ausfuehren
#   ./run_pipeline.sh 0      # Nur Schritt 0 (Ticker)
#   ./run_pipeline.sh 1      # Nur Schritt 1 (Fundamentals)
#   ./run_pipeline.sh 2      # Nur Schritt 2 (History)
#   ./run_pipeline.sh 3      # Nur Schritt 3 (Analytics)
#   ./run_pipeline.sh 4      # Nur Schritt 4 (Frontend)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Virtual Environment aktivieren
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "FEHLER: Virtual Environment nicht gefunden. Fuehre zuerst setup_ubuntu.sh aus."
    exit 1
fi

# Farben
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

run_step() {
    local step=$1
    local dir=$2
    local scripts=("${@:3}")

    echo ""
    echo -e "${YELLOW}=============================================="
    echo "  SCHRITT $step: $dir"
    echo "==============================================${NC}"

    cd "$SCRIPT_DIR/$dir"

    for script in "${scripts[@]}"; do
        if [ -f "$script" ]; then
            echo -e "${GREEN}>>> $script${NC}"
            python "$script"
            echo ""
        else
            echo -e "${RED}WARNUNG: $script nicht gefunden${NC}"
        fi
    done

    cd "$SCRIPT_DIR"
}

# Welchen Schritt ausfuehren?
STEP=${1:-all}

echo "=============================================="
echo "  AKTIEN-PROJEKT PIPELINE"
echo "=============================================="
echo "Startzeit: $(date)"

# SCHRITT 0: Ticker-Datenbank
if [ "$STEP" = "all" ] || [ "$STEP" = "0" ]; then
    run_step "0" "00_tickerlist" \
        "00_create_table_tickerlist.py" \
        "01_ishares_scrap_ubuntu.py" \
        "02_add_to_tickerlist_from_csv.py" \
        "03_update_yf_ticker.py" \
        "04_fill_eodhd_ticker.py"
fi

# SCHRITT 1: Fundamentaldaten (FMP API)
if [ "$STEP" = "all" ] || [ "$STEP" = "1" ]; then
    run_step "1" "01_load_fundamentals" \
        "00_fmp_financial_loader.py" \
        "01_fmp_market_cap_loader.py" \
        "02_fmp_revenue_segments_loader.py" \
        "03_fmp_sector_pe_loader.py" \
        "04_fmp_sector_performance_loader.py" \
        "05_fmp_treasury_rates_loader.py" \
        "06_fmp_economic_indicators_loader.py"
fi

# SCHRITT 2: Preishistorie (Yahoo Finance)
if [ "$STEP" = "all" ] || [ "$STEP" = "2" ]; then
    run_step "2" "02_history" \
        "00_create_history_table.py" \
        "01_yf_history_all.py"
fi

# SCHRITT 3: Daten aufbereiten (Analytics)
if [ "$STEP" = "all" ] || [ "$STEP" = "3" ]; then
    run_step "3" "03_analytics" \
        "00_create_table_eodtofiltered.py" \
        "01_eoddata_to_filtered.py" \
        "02_create_table_fmpfiltered.py" \
        "03_fmp_to_filtered.py" \
        "04_create_table_calcu_numbers.py" \
        "04a_alter_table_add_margins.py" \
        "05_fill_calcu_numbers.py"
fi

# SCHRITT 4: Frontend-Daten
if [ "$STEP" = "all" ] || [ "$STEP" = "4" ]; then
    run_step "4" "04_frontend" \
        "00_create_frontend_tables.py" \
        "00a_alter_live_metrics_add_margins.py" \
        "00b_alter_live_metrics_add_pe_diffs.py" \
        "01_load_company_info.py" \
        "02_load_live_metrics.py" \
        "03_init_watchlist.py" \
        "04_create_column_settings.py" \
        "04a_add_pe_diff_columns.py"
fi

echo ""
echo "=============================================="
echo -e "${GREEN}  PIPELINE ABGESCHLOSSEN${NC}"
echo "=============================================="
echo "Endzeit: $(date)"
echo ""
echo "Webserver starten mit:"
echo "  cd 05_website && python app.py"
