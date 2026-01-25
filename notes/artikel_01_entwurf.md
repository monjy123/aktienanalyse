# Das KGV von Siemens ist 18,71. Oder 25,04. Oder 27,31.

*Entwurf v1 – Stand: 17. Januar 2026*

---

Je nachdem, wo du schaust.

Ich habe heute Morgen eine einfache Frage gestellt: **Was ist das aktuelle KGV von Siemens?**

- **finanzen.net** sagt: 18,71
- **onvista** sagt: 25,04
- **Yahoo Finance** sagt: 27,31

Drei seriöse Quellen. Drei völlig unterschiedliche Zahlen. Abweichung: fast 50%.

Und das Beste: Keine dieser Seiten erklärt, wie sie auf ihre Zahl kommt.

---

## Das Problem: Die Black Box

Das KGV (Kurs-Gewinn-Verhältnis) ist die meistgenutzte Kennzahl der Welt. Jeder Anleger kennt sie. Jeder Screener zeigt sie.

Aber kaum jemand fragt: **Welches KGV eigentlich?**

- **TTM** (Trailing Twelve Months) – die letzten 4 Quartale?
- **FY** (Fiscal Year) – das letzte Geschäftsjahr?
- **Forward** – Schätzungen für das nächste Jahr?

Und selbst wenn du weißt, dass es TTM ist: **Welche Datenquelle?** Bloomberg? Reuters? Eigene Berechnung? Wann wurde der Gewinn zuletzt aktualisiert? Ist der Kurs von heute oder von letzter Woche?

Bei den meisten Screenern: Keine Ahnung. Black Box.

---

## Warum das ein echtes Problem ist

Stell dir vor, du vergleichst zwei Aktien:

- Aktie A: KGV 15
- Aktie B: KGV 20

Aktie A sieht günstiger aus, richtig?

Aber was, wenn Quelle A das Forward-KGV zeigt und Quelle B das TTM-KGV? Dann vergleichst du Äpfel mit Birnen – und triffst möglicherweise eine falsche Entscheidung.

Oder noch schlimmer: Du schaust dir historische KGV-Durchschnitte an. Manche Screener zeigen die. Aber **filtern sie Ausreißer?**

Ein einziges Jahr mit einem KGV von 200 (weil der Gewinn fast auf Null gefallen ist) zerstört jeden Durchschnitt. Und plötzlich sieht eine Aktie mit KGV 25 "günstig" aus, weil der 10-Jahres-Durchschnitt bei 45 liegt – verzerrt durch ein einziges Krisenjahr.

---

## Die Lösung: Bau dir deine eigenen Kennzahlen

Vor zwei Jahren habe ich angefangen, mir eigene Analyse-Tools zu bauen. Nicht weil ich Programmierer bin – bin ich nicht. Sondern weil ich **wissen wollte, was hinter den Zahlen steckt**.

Mit KI-Tools wie Claude oder ChatGPT kann heute jeder programmieren. Auch ohne Vorkenntnisse. Du beschreibst, was du willst – und die KI schreibt den Code.

Was ich mir gebaut habe:

- **Eigene KGV-Berechnung**: Ich weiß exakt, ob es TTM oder FY ist, welche Datenquelle, wann aktualisiert
- **Historische Durchschnitte**: 5, 10, 15, 20 Jahre – mit Outlier-Filterung
- **EV/EBIT statt nur KGV**: Für den Vergleich einer Aktie mit sich selbst die deutlich bessere Kennzahl
- **Margen, Wachstum, Verschuldung**: Alles historisch vergleichbar
- **DCF-Modelle**: Um Annahmen durchzuspielen

Das Tool deckt inzwischen über 2.000 Aktien ab: DAX, MDAX, STOXX 600, S&P 500, FTSE 100, Nikkei 225.

---

## Ein Beispiel: Was mein Tool zeigt

Wenn ich Siemens in meinem Tool aufrufe, sehe ich:

- **TTM-KGV**: [Wert] – berechnet aus den letzten 4 Quartalen
- **FY-KGV**: [Wert] – basierend auf dem letzten Geschäftsjahr
- **10-Jahres-Durchschnitt**: [Wert] – mit Outlier-Filterung
- **Abweichung vom Durchschnitt**: [X]% über/unter dem historischen Mittel

Ich weiß bei jeder Zahl, woher sie kommt. Keine Black Box.

---

## Warum ich das hier schreibe

Ich bin seit 20 Jahren im Wealth Management einer großen Privatbank. Ich schaue mir täglich Aktien an – für Kunden mit siebenstelligen Depots.

Und ich war frustriert. Frustriert davon, dass selbst teure Profi-Tools manchmal intransparent sind. Frustriert davon, dass Standard-Screener mir Zahlen zeigen, ohne zu erklären, was dahinter steckt.

Also habe ich angefangen, mir eigene Tools zu bauen. Mit KI. Ohne Programmierkenntnisse.

Hier schreibe ich darüber:
- Wie ich Kennzahlen berechne und warum
- Welche Fallstricke es gibt
- Konkrete Aktienanalysen mit meinem Tool

**Wenn du wissen willst, was hinter den Zahlen steckt – abonniere den Newsletter.**

---

*AI-Fundie*

---

## TODO / Offene Punkte

- [ ] Beispiel-Unternehmen: Siemens behalten oder besseres Beispiel finden?
- [ ] Aktuelle Zahlen von finanzen.net, onvista, Yahoo Finance einfügen
- [ ] Screenshot vom eigenen Tool einfügen
- [ ] Konkrete Werte aus dem Tool für das Beispiel-Unternehmen
- [ ] Tonalität prüfen: Zu locker? Zu sachlich?
- [ ] Länge prüfen: Zu lang für Substack?
