import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import json
import time
import requests
from difflib import SequenceMatcher
from db import get_connection
from mysql.connector import Error as MySQLError

# ============================================================
#   CONFIG
# ============================================================

DB_SCHEMA = "TICKER"
TABLE_NAME = "tickerlist"
SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"

OVERRIDE_FILE = "manual_overrides.json"
with open(OVERRIDE_FILE, "r") as f:
    MANUAL_OVERRIDES = json.load(f)

ISIN_SUFFIX_MAP = {
    "DE": ".DE", "FR": ".PA", "NL": ".AS", "BE": ".BR", "AT": ".VI",
    "CH": ".SW", "IT": ".MI", "ES": ".MC", "SE": ".ST", "DK": ".CO",
    "FI": ".HE", "PT": ".LS", "PL": ".WA", "CZ": ".PR", "HU": ".BD",
    "GB": ".L", "NO": ".OL", "LU": ".AS", "IE": ".L", "US": "", "JP": ".T"
}

EU_SUFFIXES = [
    ".AS", ".PA", ".DE", ".BR", ".MC", ".MI", ".L",
    ".SW", ".CO", ".ST", ".HE", ".VI", ".OL"
]

BLACKLIST_SUFFIXES = {".MX", ".SG", ".KQ", ".KS", ".TW", ".TWO", ".XC"}

# ============================================================
#   HELFER
# ============================================================

def yahoo_search(query):
    if not query:
        return []
    try:
        r = requests.get(
            SEARCH_URL,
            params={"q": query, "quotesCount": 100},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        if r.status_code != 200:
            return []
        return r.json().get("quotes", [])
    except Exception:
        return []

def similarity(a, b):
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# ============================================================
#   BEST CANDIDATE
# ============================================================

def best_candidate(results, preferred_suffix, name):
    candidates = []

    for r in results:
        sym = r.get("symbol", "")
        yname = r.get("shortname") or r.get("longname") or ""
        if not yname:
            continue

        if any(sym.endswith(bad) for bad in BLACKLIST_SUFFIXES):
            continue

        score = similarity(name, yname)
        candidates.append((score, sym, yname))

    if not candidates:
        return None

    candidates.sort(reverse=True, key=lambda x: x[0])

    if preferred_suffix:
        filt = [c for c in candidates if c[1].endswith(preferred_suffix)]
        if filt:
            return filt[0][1]

    filt = [c for c in candidates if any(c[1].endswith(s) for s in EU_SUFFIXES)]
    if filt:
        return filt[0][1]

    adr = [c[1] for c in candidates if "." not in c[1]]
    if adr:
        return adr[0]

    return None

# ============================================================
#   TICKER FINDEN
# ============================================================

def find_yahoo_ticker(name, isin, exchange):
    preferred_suffix = ISIN_SUFFIX_MAP.get(isin[:2].upper(), None)

    r = yahoo_search(isin)
    t = best_candidate(r, preferred_suffix, name)
    if t:
        return t

    r = yahoo_search(name)
    t = best_candidate(r, preferred_suffix, name)
    if t:
        return t

    return None

# ============================================================
#   DB UPDATE
# ============================================================

def update_yf_tickers():

    print("🔗 Verbinde DB...")
    try:
        con = get_connection(db_name=DB_SCHEMA, autocommit=False)
    except MySQLError as e:
        print("❌ DB Fehler:", e)
        return

    cur = con.cursor(dictionary=True)
    cur.execute(f"SELECT id, name, exchange, isin FROM {TABLE_NAME}")
    rows = cur.fetchall()

    update_sql = """
        UPDATE tickerlist
        SET yf_ticker = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """

    not_found = []
    print(f"🔍 Verarbeite {len(rows)} Einträge...\n")

    for i, r in enumerate(rows, start=1):
        rid = r["id"]
        name = r["name"]
        exchange = r["exchange"] or ""
        isin = r["isin"]

        print(f"[{i}/{len(rows)}] {name} ({exchange}) – {isin}")

        if isin in MANUAL_OVERRIDES:
            override = MANUAL_OVERRIDES[isin]
            if override:
                print(f"  ✔ Override → {override}")
                cur.execute(update_sql, (override, rid))
            else:
                print("  → ❌ Override: kein Ticker")
                not_found.append((rid, name, exchange, isin))
            continue

        ticker = find_yahoo_ticker(name, isin, exchange)

        if not ticker:
            print("  → ❌ Nicht gefunden")
            not_found.append((rid, name, exchange, isin))
            continue

        print(f"  ✔ {ticker}")
        cur.execute(update_sql, (ticker, rid))
        time.sleep(0.15)

    con.commit()
    cur.close()
    con.close()

    print("\n🎉 Fertig.")

    print("\n--------------------------------------------------")
    print("❗ NICHT GEFUNDENE TICKER")
    print("--------------------------------------------------")

    for nf in not_found:
        print(f"ID {nf[0]} – {nf[1]} – {nf[2]} – ISIN {nf[3]}")

    print("--------------------------------------------------")


if __name__ == "__main__":
    update_yf_tickers()
