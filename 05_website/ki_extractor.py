"""KI-Extraktor: Liest Geschäftsberichte/Pressemitteilungen, extrahiert Fundamentaldaten.

Architektur:
- download_pdf()        — PDF holen
- extract_text()        — Text mit Seitennummern
- find_relevant_pages() — Filter auf Income/Bilanz/Cashflow-Seiten
- extract_via_claude()  — claude -p mit JSON-Schema + Pro-Abo-Auth
- normalize_units()     — millions/thousands/absolute auf Roh-€-Werte mappen
- validate()            — Plausibility (Σ Quartale ≈ FY, EPS-Range, Currency)
- discover_pdfs()       — IR-Seite scrapen (HTML-Parsing) → PDF-Kandidaten

Backend nutzt das modul aus app.py via from ki_extractor import ...
"""

import json
import os
import re
import shutil
import subprocess
from io import BytesIO
from urllib.parse import urljoin

import requests
from pypdf import PdfReader


def _find_claude_binary():
    """Findet claude-CLI auch wenn PATH nicht gesetzt ist (z.B. unter Flask)."""
    found = shutil.which('claude')
    if found:
        return found
    candidates = [
        os.path.expanduser('~/.local/bin/claude'),
        '/usr/local/bin/claude',
        '/opt/claude/claude',
    ]
    for c in candidates:
        if os.path.isfile(c) or os.path.islink(c):
            return c
    raise RuntimeError(
        "claude CLI nicht gefunden. Bitte installieren oder PATH setzen "
        "(npm i -g @anthropic-ai/claude-code)."
    )


CLAUDE_BIN = None  # lazy resolved beim ersten Aufruf


# =============================================================================
# JSON-Schema für Claude-Output
# =============================================================================

EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "period_label": {"type": "string", "description": "FY 2025, Q4 2025, etc."},
        "fiscal_year_end_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
        "currency": {"type": "string", "description": "ISO 4217 (EUR, USD, ...)"},
        "unit": {"type": "string", "enum": ["absolute", "thousands", "millions", "billions"]},
        "values": {
            "type": "object",
            "properties": {
                "revenue": {"type": ["number", "null"]},
                "cost_of_revenue": {"type": ["number", "null"]},
                "gross_profit": {"type": ["number", "null"]},
                "operating_income": {"type": ["number", "null"]},
                "ebitda": {"type": ["number", "null"]},
                "income_before_tax": {"type": ["number", "null"]},
                "income_tax_expense": {"type": ["number", "null"]},
                "net_income": {"type": ["number", "null"]},
                "eps": {"type": ["number", "null"]},
                "eps_diluted": {"type": ["number", "null"]},
                "weighted_average_shs_out": {"type": ["number", "null"]},
                "weighted_average_shs_out_dil": {"type": ["number", "null"]},
                "free_cash_flow": {"type": ["number", "null"]},
                "capital_expenditure": {"type": ["number", "null"]},
                "depreciation_and_amortization_cf": {"type": ["number", "null"]},
                "stock_based_compensation": {"type": ["number", "null"]},
                "total_assets": {"type": ["number", "null"]},
                "total_equity": {"type": ["number", "null"]},
                "cash_and_cash_equivalents": {"type": ["number", "null"]},
                "total_debt": {"type": ["number", "null"]},
                "net_debt": {"type": ["number", "null"]},
                "shares_outstanding_eop": {"type": ["number", "null"], "description": "Anzahl ausstehender Aktien zum Periodenende (point-in-time, NICHT weighted average)"}
            }
        },
        "report_metadata": {
            "type": "object",
            "description": "Berichts-Metadaten (alle Felder optional, nur was im Text steht)",
            "properties": {
                "publication_date": {"type": ["string", "null"], "description": "Veröffentlichungsdatum dieses Berichts/dieser PR, YYYY-MM-DD"},
                "period_type": {"type": ["string", "null"], "enum": ["FY", "Q1", "Q2", "Q3", "Q4", "H1", "H2", "9M", None], "description": "Welche Periode der Bericht beschreibt — exakt einer der Werte"},
                "period_year": {"type": ["number", "null"], "description": "Geschäftsjahr der berichteten Periode (z.B. 2025)"},
                "next_release_date": {"type": ["string", "null"], "description": "Datum der nächsten Veröffentlichung (falls im Bericht erwähnt), YYYY-MM-DD"},
                "next_release_period_label": {"type": ["string", "null"], "description": "z.B. 'Q1 2026' — Periode der nächsten Veröffentlichung"}
            }
        },
        "quotes": {
            "type": "object",
            "description": "Pro extrahierter Metrik UND pro Metadaten-Feld: {value_in_doc, page, quote}"
        },
        "warnings": {"type": "array", "items": {"type": "string"}}
    },
    "required": ["period_label", "currency", "unit", "values"]
}


# Welche Felder sind per_share (nicht skalieren)?
PER_SHARE_FIELDS = {"eps", "eps_diluted"}

# Welche Felder zählen Stück und werden NICHT in EUR/USD skaliert (aber ggf. millions/thousands)?
SHARE_COUNT_FIELDS = {"weighted_average_shs_out", "weighted_average_shs_out_dil", "shares_outstanding_eop"}


# =============================================================================
# PDF-Handling
# =============================================================================

def download_pdf(url, timeout=60):
    """Lädt PDF, gibt bytes zurück. Wirft bei Nicht-PDF-Inhalt."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) aktienanalyse/1.0',
        'Accept': 'application/pdf,*/*'
    }
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    if not r.content[:5].startswith(b'%PDF-'):
        ct = r.headers.get('Content-Type', '?')
        raise ValueError(f"Antwort ist keine PDF-Datei (Content-Type={ct}, erste Bytes: {r.content[:20]!r})")
    return r.content


def extract_text(pdf_bytes):
    """Extrahiert Seiten-Text. Gibt list[(page_num_1based, text)] zurück."""
    reader = PdfReader(BytesIO(pdf_bytes))
    pages = []
    for i, page in enumerate(reader.pages, 1):
        try:
            text = page.extract_text() or ''
        except Exception:
            text = ''
        pages.append((i, text))
    return pages


# Hochpriorisierte Tabellen-Header (Score x3) — meist auf den eigentlichen Konzernabschluss-Seiten
HIGH_VALUE_KEYWORDS = [
    'Konzernbilanz', 'Konzern-Bilanz', 'Konzern-Gewinn- und Verlustrechnung',
    'Konzern-Gesamtergebnisrechnung', 'Konzern-Kapitalflussrechnung',
    'Consolidated Balance Sheet', 'Consolidated Income Statement',
    'Consolidated Statement of Income', 'Consolidated Statements of Operations',
    'Consolidated Cash Flow', 'Consolidated Statements of Cash Flows',
]

# Mittlere Priorität (Score x2) — Übersichts/Eckdaten-Seiten
MEDIUM_VALUE_KEYWORDS = [
    'Konzernzahlen im Überblick', 'Eckdaten', 'Wesentliche Kennzahlen',
    'Key figures', 'Headline figures', 'Financial highlights',
    'Konzernumsatz', 'Total revenues',
]

# Allgemeine Tabellen-Indikatoren (Score x1)
LOW_VALUE_KEYWORDS = [
    'Umsatzerlöse', 'Ergebnis je Aktie', 'Ergebnis nach Steuern',
    'Earnings per share', 'Net income', 'Cash flow',
    'Gewinn- und Verlustrechnung', 'Kapitalflussrechnung',
]


def find_relevant_pages(pages, max_pages=20):
    """Filter auf Seiten mit Finanzkennzahlen.

    Heuristik: pro Seite Score aus Keyword-Treffern bilden, Top-Seiten + Folgeseite
    (Tabellen-Continuation) auswählen. Vermeidet Inhaltsverzeichnis-Treffer.
    """
    if len(pages) <= max_pages:
        # Pressemitteilung / Kurzbericht — nimm alles
        return pages

    scores = {}
    for page_num, text in pages:
        text_lower = text.lower()
        score = 0
        score += 3 * sum(1 for kw in HIGH_VALUE_KEYWORDS if kw.lower() in text_lower)
        score += 2 * sum(1 for kw in MEDIUM_VALUE_KEYWORDS if kw.lower() in text_lower)
        score += 1 * sum(1 for kw in LOW_VALUE_KEYWORDS if kw.lower() in text_lower)
        # Bonus: Seite enthält viele Zahlen (Tabellen-Indikator)
        digit_chars = sum(c.isdigit() for c in text)
        if len(text) > 100 and digit_chars / max(len(text), 1) > 0.05:
            score += 2
        if score > 0:
            scores[page_num] = score

    if not scores:
        return pages[:max_pages]

    # Nimm Top-Seiten + Folgeseite für Tabellen-Continuation
    top_pages = sorted(scores.keys(), key=lambda p: (-scores[p], p))
    selected = set()
    for p in top_pages:
        selected.add(p)
        selected.add(p + 1)
        if len(selected) >= max_pages:
            break

    selected = sorted({p for p in selected if 1 <= p <= len(pages)})[:max_pages]
    return [(p, t) for p, t in pages if p in selected]


# =============================================================================
# Claude-Aufruf
# =============================================================================

PROMPT_TEMPLATE = """Du bist ein präziser Finanz-Extraktor. Extrahiere Konzern-Kennzahlen aus dem unten stehenden Bericht-Auszug für:

UNTERNEHMEN: {company} (ISIN {isin})
GEWÜNSCHTE PERIODE: {period_label}

REGELN — strikt einhalten:
1. Nimm NUR Werte die EXPLIZIT im Text stehen. NICHT raten, NICHT berechnen, NICHT halluzinieren.
2. Wenn ein Wert nicht klar im Text steht oder nicht zur gewünschten Periode gehört → setze null.
3. Wenn der Text mehrere Geschäftsjahre zeigt: nur die GEWÜNSCHTE Periode extrahieren.
4. Einheiten MÜSSEN korrekt erfasst werden:
   - "5.566,2 Mio. EUR" → unit="millions", value=5566.2
   - "5.566.200 TEUR" → unit="thousands", value=5566200
   - "5,57 Mrd. EUR" → unit="billions", value=5.57
   - Plain "5566200000" → unit="absolute", value=5566200000
   WICHTIG: Eine konsistente unit pro Bericht (alle Werte in derselben Skalierung).
5. EPS und EPS_diluted sind IMMER per_share — nicht skalieren, exakter Roh-Wert ("8,77 EUR" → eps=8.77).
6. Aktienzahl (weighted_average_shs_out): in der Einheit des Berichts (oft millions oder thousands).
7. Pro extrahiertem Wert füge in `quotes` einen Eintrag hinzu:
     "metric_name": {{"value_in_doc": "5.566,2", "page": 12, "quote": "Konzernumsatz 2025: 5.566,2 Mio. EUR (Vj. 5.293,5)"}}
8. In `warnings` Plausibilitäts-Auffälligkeiten notieren (z.B. "EBIT > Revenue", "Periode unklar").
9. Wenn der gesamte Auszug die gewünschte Periode NICHT abdeckt: alle values=null, warnings=["Periode {period_label} nicht im Auszug gefunden"].
10. Zusätzlich `report_metadata` ausfüllen (alles optional, nur was klar im Text steht):
    - `publication_date`: Veröffentlichungsdatum DIESES Berichts/dieser PR (oft Cover, Header oder "Stuttgart, 15. Februar 2026") als YYYY-MM-DD
    - `period_type`: exakt einer aus FY/Q1/Q2/Q3/Q4/H1/H2/9M — welche Periode beschreibt der Bericht (vergleiche mit gewünschter Periode!)
    - `period_year`: Geschäftsjahr der berichteten Periode (z.B. 2025)
    - `next_release_date`: Datum der nächsten Veröffentlichung falls im Bericht steht (z.B. "Veröffentlichung Q1-Bericht: 5. Mai 2026") als YYYY-MM-DD
    - `next_release_period_label`: z.B. "Q1 2026" — passende Periode zum next_release_date
    - Für jedes ausgefüllte Metadatenfeld füge ebenfalls einen Eintrag in `quotes` hinzu (key=Feldname), damit der Reviewer die Stelle prüfen kann.
11. `shares_outstanding_eop` (Anzahl ausstehender Aktien zum Periodenende, point-in-time): NICHT verwechseln mit `weighted_average_shs_out` (gewichteter Durchschnitt für EPS-Berechnung). Stichworte: "outstanding shares", "im Umlauf befindliche Aktien", "shares issued and outstanding".

BERICHT-AUSZUG:

{pdf_text}

Antworte NUR mit dem strukturierten Output gemäß Schema."""


def extract_via_claude(company, isin, period_label, relevant_pages,
                       model='claude-haiku-4-5', timeout=180,
                       max_budget_usd=0.30, max_chars=80000):
    """Ruft `claude -p` mit JSON-Schema auf. Returns dict gemäß EXTRACTION_SCHEMA."""

    pdf_text = '\n\n'.join(f'═══ Seite {p} ═══\n{t}' for p, t in relevant_pages)
    if len(pdf_text) > max_chars:
        pdf_text = pdf_text[:max_chars] + '\n[...gekürzt...]'

    prompt = PROMPT_TEMPLATE.format(
        company=company, isin=isin, period_label=period_label,
        pdf_text=pdf_text
    )

    schema_json = json.dumps(EXTRACTION_SCHEMA)

    global CLAUDE_BIN
    if CLAUDE_BIN is None:
        CLAUDE_BIN = _find_claude_binary()

    cmd = [
        CLAUDE_BIN, '-p',
        '--model', model,
        '--output-format', 'json',
        '--json-schema', schema_json,
        '--max-budget-usd', str(max_budget_usd),
        '--exclude-dynamic-system-prompt-sections',
        prompt
    ]

    # Stelle sicher dass HOME gesetzt ist (für claude OAuth-Token)
    env = os.environ.copy()
    if 'HOME' not in env:
        env['HOME'] = os.path.expanduser('~')

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"claude -p failed (rc={proc.returncode}): stderr={proc.stderr[:500]} stdout={proc.stdout[:500]}")

    try:
        wrapper = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Konnte claude-Output nicht parsen: {e}\nOutput: {proc.stdout[:500]}")

    if wrapper.get('is_error'):
        raise RuntimeError(f"claude returned error: {wrapper.get('result')}")

    structured = wrapper.get('structured_output')
    if not structured:
        raise RuntimeError(f"Kein structured_output in claude-Antwort. Result-Text: {wrapper.get('result', '')[:500]}")

    cost = wrapper.get('total_cost_usd', 0)
    structured['_meta'] = {
        'cost_usd': cost,
        'duration_ms': wrapper.get('duration_ms'),
        'model': model,
        'session_id': wrapper.get('session_id')
    }
    return structured


# =============================================================================
# Unit-Normalisierung: Roh-Werte aus claude → DB-Werte (absolut)
# =============================================================================

UNIT_FACTORS = {
    'absolute': 1,
    'thousands': 1_000,
    'millions': 1_000_000,
    'billions': 1_000_000_000
}


def normalize_units(extracted):
    """Skaliert Werte gemäß `unit` auf absolute Beträge.

    EPS bleibt per-share. Aktienzahl wird ebenfalls skaliert.
    Wenn operating_income (EBIT) fehlt aber EBITDA + D&A vorhanden sind,
    wird EBIT als EBITDA - D&A berechnet (häufig bei IFRS-Berichten, die
    EBIT nicht separat ausweisen).
    """
    unit = extracted.get('unit', 'absolute')
    factor = UNIT_FACTORS.get(unit, 1)
    values = extracted.get('values', {}) or {}

    normalized = {}
    for k, v in values.items():
        if v is None:
            normalized[k] = None
            continue
        if k in PER_SHARE_FIELDS:
            normalized[k] = float(v)
        else:
            normalized[k] = float(v) * factor

    # Fallback: EBIT aus EBITDA - D&A berechnen, falls nicht explizit ausgewiesen
    if normalized.get('operating_income') is None:
        ebitda = normalized.get('ebitda')
        da = normalized.get('depreciation_and_amortization_cf')
        if ebitda is not None and da is not None:
            normalized['operating_income'] = ebitda - da
            normalized['_ebit_derived'] = True

    return normalized


# =============================================================================
# Plausibility-Validator
# =============================================================================

def validate(normalized, period_label):
    """Returns Liste von Warnings. Leer = alles plausibel."""
    warnings = []
    rev = normalized.get('revenue')
    ebit = normalized.get('operating_income')
    ebitda = normalized.get('ebitda')
    ni = normalized.get('net_income')
    eps = normalized.get('eps')

    # Negative Revenue ist verdächtig
    if rev is not None and rev < 0:
        warnings.append(f'revenue={rev} ist negativ — verdächtig')

    # EBIT > Revenue ist sehr unwahrscheinlich
    if rev and ebit and ebit > rev:
        warnings.append(f'operating_income ({ebit:.0f}) > revenue ({rev:.0f}) — verdächtig')
    if rev and ebitda and ebitda > rev:
        warnings.append(f'ebitda ({ebitda:.0f}) > revenue ({rev:.0f}) — verdächtig')

    # EBITDA sollte >= EBIT sein
    if ebit and ebitda and ebitda < ebit:
        warnings.append(f'ebitda ({ebitda:.0f}) < operating_income ({ebit:.0f}) — verdächtig (D&A müsste positiv sein)')

    # Net Income > Revenue ist möglich (One-offs) aber selten
    if rev and ni and abs(ni) > rev * 0.8:
        warnings.append(f'net_income ({ni:.0f}) > 80% von revenue ({rev:.0f}) — bitte prüfen')

    # EPS in plausibler Range (±200)
    if eps is not None and abs(eps) > 500:
        warnings.append(f'eps={eps} außerhalb plausibler Range (±500)')

    # FY/Quartals-Längen plausibel?
    if period_label.startswith('FY') and rev is not None and rev < 10_000_000:
        warnings.append(f'FY-revenue {rev:.0f} unter 10 Mio — Skalierungs-Check erforderlich (unit korrekt?)')
    if period_label.startswith('Q') and rev is not None and rev > 200_000_000_000:
        warnings.append(f'Quartal-revenue {rev:.0f} über 200 Mrd — Skalierungs-Check erforderlich')

    return warnings


# =============================================================================
# Discovery: PDFs auf IR-Seite finden
# =============================================================================

PDF_LINK_RE = re.compile(r'href=[\'"]([^\'"]+\.pdf[^\'"]*)[\'"]', re.IGNORECASE)
LINK_TEXT_RE = re.compile(
    r'<a[^>]+href=[\'"]([^\'"]+\.pdf[^\'"]*)[\'"][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL
)

# Nicht-aussagekräftige Linktexte — fallback auf Dateinamen
GENERIC_LINK_TEXTS = {
    'download', 'pdf', 'herunterladen', 'mehr', 'more', 'view', 'open',
    'click here', 'hier klicken', 'link', 'see more'
}


def _filename_from_url(url):
    """Extrahiert sauberen Dateinamen aus URL — auch aus tokenisierten URLs (Symrise etc.)."""
    last = url.rsplit('/', 1)[-1].split('?', 1)[0].split('#', 1)[0]
    if last.lower().endswith('.pdf'):
        return last
    return last or url


def _is_generic_text(text):
    """True wenn Linktext nicht aussagekräftig ist (z.B. nur 'Download' oder 'pdf (275 KB)')."""
    t = text.strip().lower()
    if not t or len(t) < 4:
        return True
    if t in GENERIC_LINK_TEXTS:
        return True
    # "pdf (275 KB)", "pdf 3 MB" etc.
    if re.match(r'^pdf\s*[\(\d\s\.,kmgb]+\)?$', t, re.I):
        return True
    return False


def _humanize_filename(filename):
    """260304-Symrise-Press-Release-Financial-Results-2025.pdf → 'Press Release Financial Results 2025'."""
    name = re.sub(r'\.pdf$', '', filename, flags=re.I)
    # Lead-Datum (YYMMDD oder YYYY-MM-DD) entfernen
    name = re.sub(r'^[\d]{6,8}[-_]', '', name)
    name = re.sub(r'^[\d]{4}[-_][\d]{2}[-_][\d]{2}[-_]', '', name)
    # Trenner zu Spaces
    name = name.replace('_', ' ').replace('-', ' ')
    # Mehrfach-Spaces zusammen
    name = re.sub(r'\s+', ' ', name).strip()
    return name


# =============================================================================
# Period-Inferenz: PDF-Name/Linktext → mögliche Perioden
# =============================================================================

# Monat → typisches Berichts-Quartal des fiskalen Vorlaufs (Earnings Call vom April
# berichtet typischerweise Q1; vom August → H1; vom November → 9M/Q3; vom März → Q4/FY).
# Nur als Fallback genutzt, wenn weder URL noch Linktext explizite Perioden enthalten.
_MONTH_TO_REPORTED_PERIOD = {
    'january':  ('Q4', -1),  'jan': ('Q4', -1),
    'february': ('Q4', -1),  'feb': ('Q4', -1),
    'march':    ('FY', -1),  'mar': ('FY', -1),  'märz': ('FY', -1),
    'april':    ('Q1',  0),  'apr': ('Q1',  0),
    'may':      ('Q1',  0),  'mai': ('Q1',  0),
    'june':     ('Q1',  0),  'jun': ('Q1',  0),
    'july':     ('Q2',  0),  'jul': ('Q2',  0),  'juli': ('Q2',  0),
    'august':   ('Q2',  0),  'aug': ('Q2',  0),
    'september':('Q3',  0),  'sep': ('Q3',  0),  'sept': ('Q3',  0),
    'october':  ('Q3',  0),  'oct': ('Q3',  0),  'okt': ('Q3',  0),
    'november': ('Q3',  0),  'nov': ('Q3',  0),
    'december': ('Q4',  0),  'dec': ('Q4',  0),  'dez': ('Q4',  0),
}


def infer_periods(url, link_text):
    """Liest aus URL + Linktext alle ableitbaren Perioden ab.

    Returns:
        list[str] — z.B. ["FY 2025", "Q1 2026"]. Mehrere Perioden möglich
        (Earnings Calls decken oft FY-Vorjahr + Q1-Aktuell ab). Leer wenn nichts erkennbar.

    Heuristik (mit absteigender Konfidenz):
      1. Explizite Q-Patterns: Q1 2026, Q1_2026, Q1-2026, Q12026, 1Q26
      2. FY-Patterns: FY2025, FY 25, GB 2025
      3. Halbjahr/9M: H1 2025, HY1 2025, 9M 2024, Halbjahr 2025, Halbjahres
      4. Datums-Heuristik (Earnings Call vom 30. April 2026 → Q1 2026)
    """
    # Underscores zu Spaces normalisieren, damit \b an Token-Grenzen wieder greift.
    # (Python-regex zählt _ als Word-Char, daher matchen \b-Patterns sonst nicht
    #  in Filenamen wie Fielmann_Earnings_Call_FY2025_Q1-2026.pdf.)
    raw = f"{_filename_from_url(url)} {link_text or ''}"
    haystack = raw.replace('_', ' ')
    found = []
    seen = set()

    def add(period):
        if period not in seen:
            seen.add(period)
            found.append(period)

    # 1) Quartals-Patterns: Q1 2026, Q1-2026, Q1.2026, Q12026
    #    Auch reverse: 2026 Q1
    for m in re.finditer(r'\b(Q[1-4])[\s.-]*((?:20)?\d{2})\b', haystack, re.I):
        q = m.group(1).upper()
        y = _normalize_year(m.group(2))
        if y:
            add(f"{q} {y}")
    for m in re.finditer(r'\b((?:20)?\d{2})[\s.-]*(Q[1-4])\b', haystack, re.I):
        y = _normalize_year(m.group(1))
        q = m.group(2).upper()
        if y:
            add(f"{q} {y}")
    # 1b) Reverse-kompakt: 1Q26, 1Q2026
    for m in re.finditer(r'\b([1-4])Q[\s.-]*((?:20)?\d{2})\b', haystack, re.I):
        q = f"Q{m.group(1)}"
        y = _normalize_year(m.group(2))
        if y:
            add(f"{q} {y}")

    # 2) FY-Patterns: FY2025, FY 2025, FY-2025, FY25
    for m in re.finditer(r'\bFY[\s.-]*((?:20)?\d{2})\b', haystack, re.I):
        y = _normalize_year(m.group(1))
        if y:
            add(f"FY {y}")
    # "Annual Report 2025", "Geschäftsbericht 2025"
    for m in re.finditer(r'\b(?:Annual\s*Report|Geschäftsbericht|Jahresbericht|Annual)[\s.-]*((?:20)\d{2})\b', haystack, re.I):
        y = _normalize_year(m.group(1))
        if y:
            add(f"FY {y}")
    # "Preliminary Figures 2025", "Vorläufige Zahlen 2025" → typisch FY-Vorab
    for m in re.finditer(r'\b(?:Preliminary\s*Figures|Vorläufige\s*Zahlen)[\s.-]*((?:20)\d{2})\b', haystack, re.I):
        y = _normalize_year(m.group(1))
        if y:
            add(f"FY {y}")

    # 3) Halbjahr / 9M
    for m in re.finditer(r'\b(?:H1|HY1|H\.?1\.?|Halbjahr(?:es)?)[\s.-]*((?:20)?\d{2})\b', haystack, re.I):
        y = _normalize_year(m.group(1))
        if y:
            add(f"H1 {y}")
    for m in re.finditer(r'\b(?:H2|HY2|H\.?2\.?)[\s.-]*((?:20)?\d{2})\b', haystack, re.I):
        y = _normalize_year(m.group(1))
        if y:
            add(f"H2 {y}")
    for m in re.finditer(r'\b9\s*M[\s.-]*((?:20)?\d{2})\b', haystack, re.I):
        y = _normalize_year(m.group(1))
        if y:
            # 9M = Q3-Cumulative — als Q3 markieren (Earnings-Modal denkt in Quartalen)
            add(f"Q3 {y}")

    # 4) Datums-Heuristik (nur wenn noch nichts gefunden):
    #    a) Numerisch: "31.03.2026" / "31-03-2026" / "2026-03-31" → März 2026 → Q1 2026
    #    b) Monatsname: "April 30, 2026" → Q1 2026
    if not found:
        # 4a) DD.MM.YYYY oder DD-MM-YYYY
        for m in re.finditer(r'\b(\d{1,2})[.\-/](\d{1,2})[.\-/]((?:20)\d{2})\b', haystack):
            month = int(m.group(2))
            year = int(m.group(3))
            mapping = _MONTH_NUM_TO_REPORTED_PERIOD.get(month)
            if mapping:
                period, year_offset = mapping
                add(f"{period} {year + year_offset}")
                break

        # 4b) YYYY-MM-DD
        if not found:
            for m in re.finditer(r'\b((?:20)\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})\b', haystack):
                year = int(m.group(1))
                month = int(m.group(2))
                mapping = _MONTH_NUM_TO_REPORTED_PERIOD.get(month)
                if mapping:
                    period, year_offset = mapping
                    add(f"{period} {year + year_offset}")
                    break

        # 4c) Monatsname: "Earnings Call from April 30, 2026" → Q1 2026
        if not found:
            date_match = re.search(
                r'\b(January|February|March|April|May|June|July|August|September|October|November|December|'
                r'Januar|Februar|März|Mai|Juni|Juli|August|September|Oktober|November|Dezember|'
                r'Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Okt|Nov|Dec|Dez|Mär)'
                r'(?:\.?\s+\d{1,2})?(?:[,\s]+|\s)+((?:20)\d{2})\b',
                haystack, re.I
            )
            if date_match:
                month_key = date_match.group(1).lower().rstrip('.')
                year = int(date_match.group(2))
                mapping = _MONTH_TO_REPORTED_PERIOD.get(month_key)
                if mapping:
                    period, year_offset = mapping
                    add(f"{period} {year + year_offset}")

    return found


# Numerische Variante derselben Mapping-Tabelle
_MONTH_NUM_TO_REPORTED_PERIOD = {
    1: ('Q4', -1), 2: ('Q4', -1),
    3: ('FY', -1),  # März → typisch FY-Vorjahr (Geschäftsbericht-Veröffentlichung)
    4: ('Q1', 0), 5: ('Q1', 0), 6: ('Q1', 0),
    7: ('Q2', 0), 8: ('Q2', 0),
    9: ('Q3', 0), 10: ('Q3', 0), 11: ('Q3', 0),
    12: ('Q4', 0),
}

# Pfad-Schlüsselwörter für Berichte mit Finanzzahlen — werden bei
# max_pdfs-Überschreitung bevorzugt aufgenommen, damit Marketing-Material
# (Capital Markets Day, NYC-Roadshow, Strategy Update) Berichten weicht.
_REPORT_PATH_KEYWORDS = (
    'quartal', 'quarterly', 'interim', 'zwischenbericht',
    'halbjahres', 'halbjahr', 'semiannual', 'half-year', 'half_year',
    'geschaeftsbericht', 'jahresbericht', 'annual-report', 'annual_report', 'annualreport',
    'quartalsmitteilung', 'quartalsbericht',
    'berichte', 'reports', 'finanzbericht', 'financial-report',
)


def _path_priority_score(url):
    """Höher = wichtiger. Reports/Quartale > Earnings Calls > Marketing."""
    u = url.lower()
    score = 0
    for kw in _REPORT_PATH_KEYWORDS:
        if kw in u:
            score += 10
    if 'earnings_call' in u or 'earnings-call' in u or 'earningscall' in u:
        score += 5
    return score


def _normalize_year(y):
    """Normalisiert 2-stellige (25 → 2025) und 4-stellige Jahre. None bei Müll."""
    s = str(y).strip()
    if len(s) == 2 and s.isdigit():
        n = int(s)
        return 2000 + n if n < 80 else 1900 + n
    if len(s) == 4 and s.isdigit():
        n = int(s)
        if 1990 <= n <= 2100:
            return n
    return None


# =============================================================================
# Auto-Discovery der IR-URL via Claude + WebSearch
# =============================================================================

IR_URL_SCHEMA = {
    "type": "object",
    "properties": {
        "ir_urls": {
            "type": "array",
            "description": "Eine oder mehrere IR-Unterseiten mit Finanzberichten als PDF. "
                           "Bei Firmen mit getrennten Seiten je 1x annual + 1x quarterly; "
                           "bei Tab-/Mixed-Seiten reicht 1x mixed.",
            "items": {
                "type": "object",
                "properties": {
                    "url":   {"type": "string"},
                    "kind":  {"type": "string", "enum": ["annual", "quarterly", "mixed"]},
                    "label": {"type": ["string", "null"], "description": "Kurzer Titel der Seite"}
                },
                "required": ["url", "kind"]
            }
        },
        "page_title": {"type": ["string", "null"]},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "reasoning": {"type": "string"}
    },
    "required": ["ir_urls", "confidence", "reasoning"]
}

IR_URL_PROMPT = """Finde die offizielle(n) Investor-Relations-Seite(n) mit Finanzberichten/Geschäftsberichten als PDF für:

UNTERNEHMEN: {company_name}
ISIN: {isin}

Anforderungen:
- Es muss die SPEZIFISCHE Unterseite sein die PDF-Berichte (Geschäftsbericht, Quartalsberichte, Pressemitteilungen) auflistet, NICHT die allgemeine IR-Übersicht.
- Viele IR-Sites trennen JAHRESBERICHTE und QUARTALSBERICHTE auf separate Unterseiten.
  Wenn das hier der Fall ist, gib BEIDE URLs zurück:
    · eine mit kind="annual"     (Geschäftsberichte / Annual Reports)
    · eine mit kind="quarterly"  (Quartalsmitteilungen / Halbjahres- und Zwischenberichte)
  Wenn die IR-Seite alles auf EINER Seite zeigt (z.B. Tab-Container, Mixed-Listing),
  reicht EINE URL mit kind="mixed".
- Bevorzuge die deutsche Sprachversion wenn das Unternehmen in DE/AT/CH ist, sonst englisch.
- Beispiele richtige URLs:
  · https://www.krones.com/de/unternehmen/investor-relations/finanzberichte.php  (mixed)
  · https://www.symrise.com/investors/financial-results/                          (mixed)
  · https://investors.servicenow.com/financial-reports                            (mixed)
  · …/investor-relations/jahresberichte/    +    …/investor-relations/quartalsmitteilungen/   (annual+quarterly)
- Beispiele FALSCHE URLs (zu generisch):
  · https://www.example.com/investors/  (nur Übersicht)
  · https://www.example.com/  (Homepage)

Verwende WebSearch und ggf. WebFetch.

Wenn du dir nicht sicher bist welche URL(s) die richtige(n) sind (z.B. Seite nicht
erreichbar, mehrere widersprüchliche Kandidaten), setze ir_urls=[] und confidence="low"
und beschreibe das Problem in reasoning. NIEMALS halluzinieren — lieber leere Liste.

Antworte gemäß Schema."""


def find_ir_url_via_claude(company_name, isin, model='claude-haiku-4-5',
                           timeout=180, max_budget_usd=0.20):
    """Claude sucht via WebSearch die offizielle(n) IR-Seite(n). Returns dict mit:
      ir_urls   — list[{url, kind, label?}], kind ∈ {annual, quarterly, mixed}
      ir_url    — Backward-compat: erste URL der Liste oder None
      confidence, reasoning, page_title, _meta
    """
    global CLAUDE_BIN
    if CLAUDE_BIN is None:
        CLAUDE_BIN = _find_claude_binary()

    prompt = IR_URL_PROMPT.format(company_name=company_name, isin=isin)

    cmd = [
        CLAUDE_BIN, '-p',
        '--model', model,
        '--output-format', 'json',
        '--json-schema', json.dumps(IR_URL_SCHEMA),
        '--allowedTools', 'WebSearch,WebFetch',
        '--max-budget-usd', str(max_budget_usd),
        '--exclude-dynamic-system-prompt-sections',
        prompt
    ]
    env = os.environ.copy()
    if 'HOME' not in env:
        env['HOME'] = os.path.expanduser('~')

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"claude IR-search failed (rc={proc.returncode}): stderr={proc.stderr[:500]}")
    try:
        wrapper = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Konnte claude-Output nicht parsen: {e}")
    if wrapper.get('is_error'):
        raise RuntimeError(f"claude returned error: {wrapper.get('result')}")

    structured = wrapper.get('structured_output') or {}

    # Normalisieren + Backward-compat: ir_url := erste URL der Liste
    raw_list = structured.get('ir_urls') or []
    cleaned = []
    seen = set()
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        u = (item.get('url') or '').strip()
        if not u or u in seen:
            continue
        if not u.startswith(('http://', 'https://')):
            continue
        seen.add(u)
        kind = item.get('kind') or 'mixed'
        if kind not in ('annual', 'quarterly', 'mixed'):
            kind = 'mixed'
        cleaned.append({'url': u, 'kind': kind, 'label': item.get('label')})
    structured['ir_urls'] = cleaned
    structured['ir_url'] = cleaned[0]['url'] if cleaned else None

    structured['_meta'] = {
        'cost_usd': wrapper.get('total_cost_usd', 0),
        'duration_ms': wrapper.get('duration_ms')
    }
    return structured


def _normalize_ir_input(ir_url):
    """Akzeptiert string (ggf. newline-separiert), list[str], oder list[{url, kind?}].
    Returns list[(url, kind)] mit kind ∈ {annual, quarterly, mixed}; default 'mixed'.
    """
    out = []
    seen = set()

    def add(u, kind=None):
        u = (u or '').strip()
        if not u or u in seen:
            return
        if not u.startswith(('http://', 'https://')):
            return
        seen.add(u)
        out.append((u, kind or 'mixed'))

    if not ir_url:
        return out
    if isinstance(ir_url, str):
        # Newline ODER Komma als Separator (DB-Spalte ist VARCHAR — wir nutzen \n)
        for part in re.split(r'[\n\r,]+', ir_url):
            add(part)
    elif isinstance(ir_url, (list, tuple)):
        for item in ir_url:
            if isinstance(item, str):
                add(item)
            elif isinstance(item, dict):
                add(item.get('url'), item.get('kind'))
    return out


def _scrape_pdfs_from_url(url, kind, timeout=20):
    """Lädt eine IR-Seite, extrahiert PDF-Kandidaten. Returns list[dict] (ohne Limit-Logik)."""
    headers = {'User-Agent': 'Mozilla/5.0 aktienanalyse/1.0'}
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    html = r.text

    page_seen = set()
    candidates = []
    for match in LINK_TEXT_RE.finditer(html):
        href = match.group(1).strip()
        link_text = re.sub(r'<[^>]+>', ' ', match.group(2))
        link_text = re.sub(r'\s+', ' ', link_text).strip()[:200]
        full_url = urljoin(url, href)
        if full_url in page_seen:
            continue
        page_seen.add(full_url)

        filename = _filename_from_url(full_url)
        title = _humanize_filename(filename) if _is_generic_text(link_text) else link_text
        periods = infer_periods(full_url, link_text)
        candidates.append({
            'url': full_url,
            'title': title or filename,
            'filename': filename,
            'periods': periods,
            'source_kind': kind,
            'source_ir_url': url,
        })

    if not candidates:
        for href in PDF_LINK_RE.findall(html):
            full_url = urljoin(url, href)
            if full_url in page_seen:
                continue
            page_seen.add(full_url)
            filename = _filename_from_url(full_url)
            candidates.append({
                'url': full_url,
                'title': _humanize_filename(filename),
                'filename': filename,
                'periods': infer_periods(full_url, ''),
                'source_kind': kind,
                'source_ir_url': url,
            })
    return candidates


def discover_pdfs(ir_url, max_pdfs=60, timeout=20):
    """Holt IR-Seite(n), extrahiert PDF-Links + sprechenden Titel + erkannte Perioden.

    `ir_url` darf sein:
      - string (eine URL, oder mehrere durch \\n/Komma getrennt)
      - list[str] oder list[{url, kind}]

    Mehrere URLs werden parallel-sequenziell gescraped und über alle Quellen dedupliziert.
    Sammelt ZUERST alle PDFs (kein Frühabbruch bei max_pdfs), damit auch tiefer im
    DOM stehende Quartalsberichte gefunden werden — Tab-basierte IR-Seiten (Fielmann,
    o.ä.) listen alle Tab-Inhalte im selben HTML, oft über 30 PDFs gesammelt.
    Bei Limit-Überschreitung wird nach Pfad-Score priorisiert (Berichte vor Marketing).

    Returns list[{url, title, filename, periods, source_kind, source_ir_url}].
    """
    sources = _normalize_ir_input(ir_url)
    if not sources:
        return []

    seen_urls = set()
    all_candidates = []
    for src_url, kind in sources:
        try:
            page_candidates = _scrape_pdfs_from_url(src_url, kind, timeout=timeout)
        except Exception:
            # Eine kaputte Sub-Seite (z.B. 404 auf der Quartalsseite) darf die andere
            # nicht killen — überspringen, weiter mit den restlichen URLs.
            if len(sources) == 1:
                raise
            continue

        for c in page_candidates:
            if c['url'] in seen_urls:
                continue
            seen_urls.add(c['url'])
            c['_dom_index'] = len(all_candidates)
            all_candidates.append(c)

    # Wenn Limit nicht überschritten: in Sammel-Reihenfolge zurückgeben (neueste oben).
    if len(all_candidates) <= max_pdfs:
        for c in all_candidates:
            c.pop('_dom_index', None)
        return all_candidates

    # Limit überschritten → nach Score absteigend sortieren, Sammel-Reihenfolge als Tiebreak.
    def sort_key(c):
        score = _path_priority_score(c['url'])
        if c['periods']:
            score += 3
        return (-score, c['_dom_index'])

    all_candidates.sort(key=sort_key)
    selected = all_candidates[:max_pdfs]
    selected.sort(key=lambda c: c['_dom_index'])
    for c in selected:
        c.pop('_dom_index', None)
    return selected


# =============================================================================
# Komplette Pipeline (Convenience)
# =============================================================================

def extract_period_from_pdf_url(pdf_url, isin, company_name, period_label,
                                model='claude-haiku-4-5'):
    """Ende-zu-Ende: PDF-URL → DB-konforme Werte + Quotes + Warnings."""
    pdf_bytes = download_pdf(pdf_url)
    pages = extract_text(pdf_bytes)
    relevant = find_relevant_pages(pages)
    extracted = extract_via_claude(
        company=company_name, isin=isin,
        period_label=period_label, relevant_pages=relevant,
        model=model
    )
    normalized = normalize_units(extracted)
    extra_warnings = validate(normalized, period_label)
    all_warnings = list(extracted.get('warnings') or []) + extra_warnings

    return {
        'values': normalized,
        'quotes': extracted.get('quotes') or {},
        'warnings': all_warnings,
        'currency': extracted.get('currency'),
        'unit_in_doc': extracted.get('unit'),
        'period_label': extracted.get('period_label'),
        'fiscal_year_end_date': extracted.get('fiscal_year_end_date'),
        'report_metadata': extracted.get('report_metadata') or {},
        'meta': extracted.get('_meta', {}),
        'source_url': pdf_url,
        'pages_analyzed': [p for p, _ in relevant]
    }
