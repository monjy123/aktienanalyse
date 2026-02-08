#!/bin/bash
# =============================================================================
# Earnings Actual Fetcher Wrapper
# =============================================================================

cd /home/monjy/aktienanalyse

# Argumente durchreichen (z.B. --yesterday)
./venv/bin/python 06_scrapers/03_fetch_actual_earnings.py "$@" >> logs/earnings_fetch.log 2>&1
