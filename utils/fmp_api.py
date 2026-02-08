# -*- coding: utf-8 -*-
"""
Zentralisierte FMP API Request Funktionen.

Wird verwendet von allen FMP-Loadern in 01_load_fundamentals/.
"""

import os
import time
import logging
import threading

import requests
from dotenv import load_dotenv

# Thread-lokaler Storage für Connection Pooling
thread_local = threading.local()

# Logger (wird vom aufrufenden Modul konfiguriert)
logger = logging.getLogger(__name__)

# .env laden (falls noch nicht geschehen)
load_dotenv()

FMP_BASE_URL = "https://financialmodelingprep.com"


def get_fmp_api_key():
    """Lädt FMP API Key aus .env."""
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        raise ValueError("FMP_API_KEY nicht in .env gefunden")
    return api_key


def get_session():
    """Thread-lokale requests Session für Connection Pooling."""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def api_request(endpoint, params=None, max_retries=3, rate_limit=0.1, use_session=True):
    """
    Zentralisierte FMP API Request Funktion mit Retry-Logik.

    Args:
        endpoint: API Endpoint (z.B. "/stable/search-isin")
        params: Query Parameter (apikey wird automatisch hinzugefügt)
        max_retries: Maximale Anzahl Versuche (default: 3)
        rate_limit: Pause zwischen Requests in Sekunden (default: 0.1)
        use_session: Thread-lokale Session verwenden (default: True)

    Returns:
        JSON Response oder None bei Fehler
    """
    if params is None:
        params = {}
    params["apikey"] = get_fmp_api_key()

    url = f"{FMP_BASE_URL}{endpoint}"

    # Session oder direkter Request
    requester = get_session() if use_session else requests

    for attempt in range(max_retries):
        try:
            if rate_limit > 0:
                time.sleep(rate_limit)
            response = requester.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(f"API Fehler (Versuch {attempt+1}): {e}. Warte {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"API Fehler nach {max_retries} Versuchen: {e}")
                return None
    return None
