#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Frage FMP API direkt für Yara ab
"""
import os
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load .env
load_dotenv(Path(__file__).parent / ".env")
FMP_API_KEY = os.getenv("FMP_API_KEY")

ticker = "YAR.OL"

print(f"=== FMP API - Income Statement für {ticker} ===\n")

# Income Statement Annual
url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?apikey={FMP_API_KEY}&limit=5"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    for item in data:
        print(f"\nDate: {item.get('date')}")
        print(f"  Period: {item.get('period')}")
        print(f"  Currency: {item.get('reportedCurrency', 'N/A')}")
        print(f"  Revenue: {item.get('revenue'):,.0f}" if item.get('revenue') else "  Revenue: None")
        print(f"  Net Income: {item.get('netIncome'):,.0f}" if item.get('netIncome') else "  Net Income: None")
        print(f"  Operating Income: {item.get('operatingIncome'):,.0f}" if item.get('operatingIncome') else "  Operating Income: None")
        print(f"  EPS: {item.get('eps')}")
        print(f"  Shares Outstanding: {item.get('weightedAverageShsOut'):,.0f}" if item.get('weightedAverageShsOut') else "  Shares: None")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
