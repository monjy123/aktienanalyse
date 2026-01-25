#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zeige Tabellenstruktur
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_connection

conn = get_connection()
cur = conn.cursor()

# Show columns from fmp_financial_statements
cur.execute("SHOW COLUMNS FROM raw_data.fmp_financial_statements")
print("=== Columns in fmp_financial_statements ===")
for row in cur.fetchall():
    print(f"{row[0]} - {row[1]}")

cur.close()
conn.close()
