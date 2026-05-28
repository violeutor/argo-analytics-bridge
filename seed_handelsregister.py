"""
seed_handelsregister.py — Argo Analytics · Shadow DB Seed
==========================================================
Generiert eine kuratierte DE-GmbH-Liste via handelsregister.ai API.
Output: seed_data/de_gmbh_curated.txt (ein Unternehmensname pro Zeile)

Nutzung:
    export HANDELSREGISTER_API_KEY=your_key_here
    python seed_handelsregister.py [--dry-run] [--out PATH] [--limit N]

Idempotent: merged mit bestehender .txt, nie überschreibend.
Credits: 1 Credit pro API-Call. Default ~200 Calls = ~200 Credits.
         Schätzung: 500 Free Credits reichen für Vollständig-Run mit --limit 10.

API-Doku: GET /api/v1/search-organizations?q={keyword}&limit={n}&skip={offset}
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("requests fehlt. Installieren: pip install requests")

# ── Konfiguration ─────────────────────────────────────────────────────────────

API_BASE      = "https://handelsregister.ai/api/v1"
PAGE_SIZE     = 10          # Max per API-Call (lt. Doku)
SLEEP_BETWEEN = 0.4         # Sekunden zwischen Calls (Rate-Limit-Schutz)
DEFAULT_OUT   = Path(__file__).parent / "seed_data" / "de_gmbh_curated.txt"

# Rechtsformen die als "GmbH-artig" gelten (client-side Filter)
GMBH_FORMS = frozenset({
    "GmbH",
    "GmbH & Co. KG",
    "GmbH & Co. KGaA",
    "UG (haftungsbeschränkt)",
    "UG (haftungsbeschränkt) & Co. KG",
})

# ── Taxonomy-Keywords ─────────────────────────────────────────────────────────
# Abgeleitet aus taxonomy.py v1.0 — spezifische Begriffe die im
# Unternehmensgegenstand von PE/VC-relevanten DE-Targets auftauchen.
# Bewusst keine generischen Begriffe ("Software", "Energie") — zu viel Rauschen.
#
# Format: (keyword, max_pages, sektor_label)
# max_pages steuert Credit-Verbrauch pro Keyword (1 page = 1 Credit).

KEYWORDS: list[tuple[str, int, str]] = [
    # ── Energy & Power ──────────────────────────────────────────────────────
    ("Photovoltaik",                    3, "Energy & Power"),
    ("Windenergie",                     2, "Energy & Power"),
    ("Batteriespeicher",                3, "Energy & Power"),
    ("Elektrolyse",                     2, "Energy & Power"),
    ("Wasserstoff",                     3, "Energy & Power"),
    ("Stromspeicher",                   2, "Energy & Power"),
    ("Geothermie",                      2, "Energy & Power"),
    ("Energiemanagement",               2, "Energy & Power"),
    ("Netzintegration",                 1, "Energy & Power"),
    ("Wärmepumpe",                      2, "Built Environment"),

    # ── Carbon & Climate ────────────────────────────────────────────────────
    ("CO2-Abscheidung",                 2, "Carbon & Climate"),
    ("Kohlenstoffabscheidung",          1, "Carbon & Climate"),
    ("Klimatechnologie",                2, "Carbon & Climate"),
    ("Emissionshandel",                 1, "Carbon & Climate"),
    ("CO2-Zertifikate",                 1, "Carbon & Climate"),
    ("Klimarisikoanalyse",              1, "Carbon & Climate"),
    ("Nachhaltigkeitsberatung",         2, "Carbon & Climate"),

    # ── Mobility & Transport ────────────────────────────────────────────────
    ("Elektromobilität",                3, "Mobility & Transport"),
    ("Ladeinfrastruktur",               2, "Mobility & Transport"),
    ("autonomes Fahren",                2, "Mobility & Transport"),
    ("Sustainable Aviation Fuel",       1, "Mobility & Transport"),
    ("Schienenfahrzeug",                2, "Mobility & Transport"),
    ("Logistikoptimierung",             2, "Mobility & Transport"),
    ("Letzte Meile",                    1, "Mobility & Transport"),

    # ── Industrial & Manufacturing ──────────────────────────────────────────
    ("Industrieautomation",             3, "Industrial & Manufacturing"),
    ("Robotik",                         3, "Industrial & Manufacturing"),
    ("Fertigungsautomatisierung",       2, "Industrial & Manufacturing"),
    ("Predictive Maintenance",          2, "Industrial & Manufacturing"),
    ("Prozessoptimierung",              2, "Industrial & Manufacturing"),
    ("Abfallverwertung",                2, "Industrial & Manufacturing"),
    ("Kreislaufwirtschaft",             2, "Industrial & Manufacturing"),

    # ── Materials & Chemicals ───────────────────────────────────────────────
    ("Halbleiter",                      2, "Materials & Chemicals"),
    ("Spezialmaterialien",              1, "Materials & Chemicals"),
    ("grüne Chemie",                    1, "Materials & Chemicals"),
    ("Batterierecycling",               2, "Materials & Chemicals"),
    ("Verbundwerkstoffe",               1, "Materials & Chemicals"),

    # ── Agriculture & Food ──────────────────────────────────────────────────
    ("Präzisionslandwirtschaft",        2, "Agriculture & Food"),
    ("Agrardigitalisierung",            2, "Agriculture & Food"),
    ("alternative Proteine",            1, "Agriculture & Food"),
    ("Vertical Farming",                1, "Agriculture & Food"),
    ("Pflanzenschutz Biotechnologie",   1, "Agriculture & Food"),
    ("Bodenanalyse",                    1, "Agriculture & Food"),

    # ── Built Environment ───────────────────────────────────────────────────
    ("Gebäudeautomation",               2, "Built Environment"),
    ("Smart Building",                  2, "Built Environment"),
    ("Bautechnologie",                  2, "Built Environment"),
    ("Wasserinfrastruktur",             1, "Built Environment"),
    ("Facility Management Software",    1, "Built Environment"),

    # ── Life Sciences & Health ──────────────────────────────────────────────
    ("Biotechnologie",                  3, "Life Sciences & Health"),
    ("Medizintechnik",                  3, "Life Sciences & Health"),
    ("Diagnostik",                      2, "Life Sciences & Health"),
    ("Genomik",                         1, "Life Sciences & Health"),
    ("digitale Gesundheit",             2, "Life Sciences & Health"),
    ("Wirkstoffforschung",              1, "Life Sciences & Health"),

    # ── Digital Infrastructure ──────────────────────────────────────────────
    ("künstliche Intelligenz",          3, "Digital Infrastructure"),
    ("Machine Learning",                2, "Digital Infrastructure"),
    ("Cybersicherheit",                 2, "Digital Infrastructure"),
    ("Datenbankmanagement",             1, "Digital Infrastructure"),
    ("Edge Computing",                  1, "Digital Infrastructure"),
    ("Cloud-Infrastruktur",             1, "Digital Infrastructure"),

    # ── PE-Software Signals (aus Session 30 _PE_SOFTWARE_SIGNALS) ───────────
    ("ERP-Software",                    2, "Digital Infrastructure"),
    ("Systemhaus",                      2, "Digital Infrastructure"),
    ("Softwareentwicklung",             3, "Digital Infrastructure"),
    ("IT-Dienstleistungen",             2, "Digital Infrastructure"),
    ("Unternehmensberatung IT",         1, "Digital Infrastructure"),

    # ── Financial Services ──────────────────────────────────────────────────
    ("Finanztechnologie",               2, "Financial Services"),
    ("Versicherungstechnologie",        1, "Financial Services"),
    ("Vermögensverwaltung Software",    1, "Financial Services"),

    # ── Water & Circular Economy ────────────────────────────────────────────
    ("Wasseraufbereitung",              2, "Water & Circular Economy"),
    ("Abfallmanagement",                2, "Water & Circular Economy"),
    ("Recycling Technologie",           2, "Water & Circular Economy"),
    ("Kunststoffalternativen",          1, "Water & Circular Economy"),

    # ── Mining & Resources ──────────────────────────────────────────────────
    ("kritische Rohstoffe",             1, "Mining & Resources"),
    ("Bergbautechnologie",              1, "Mining & Resources"),

    # ── Space & Defense ─────────────────────────────────────────────────────
    ("Raumfahrttechnologie",            1, "Space & Defense"),
    ("unbemannte Luftfahrzeuge",        1, "Space & Defense"),
    ("Drohnen",                         2, "Space & Defense"),
]

# Credit-Schätzung
ESTIMATED_CREDITS = sum(pages for _, pages, _ in KEYWORDS)


# ── API-Client ────────────────────────────────────────────────────────────────

class HandelsregisterClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })

    def search(self, keyword: str, limit: int = PAGE_SIZE, skip: int = 0) -> list[dict]:
        """Ruft /search-organizations auf. Gibt leere Liste bei Fehler zurück."""
        params = {"q": keyword, "limit": limit, "skip": skip}
        try:
            r = self.session.get(
                f"{API_BASE}/search-organizations",
                params=params,
                timeout=15,
            )
            if r.status_code == 429:
                print(f"  ⚠️  Rate-Limit (429) — 5s warten …")
                time.sleep(5)
                return self.search(keyword, limit, skip)  # einmal retry
            if r.status_code == 401:
                sys.exit("❌  API-Key ungültig (401). Prüfe HANDELSREGISTER_API_KEY.")
            r.raise_for_status()
            data = r.json()
            # API gibt je nach Version list oder {"results": [...]} zurück
            if isinstance(data, list):
                return data
            return data.get("results") or data.get("organizations") or []
        except requests.RequestException as e:
            print(f"  ⚠️  Request-Fehler für '{keyword}' skip={skip}: {e}")
            return []


# ── GmbH-Filter ───────────────────────────────────────────────────────────────

def is_gmbh_like(entry: dict) -> bool:
    """
    Prüft ob Eintrag eine GmbH-artige Rechtsform hat.
    Felder: 'legal_form', 'rechtsform', 'type' (je nach API-Version).
    Fallback: Namens-Check (weniger präzise, aber sicherer als alles zu akzeptieren).
    """
    for field in ("legal_form", "rechtsform", "company_type", "type"):
        form = (entry.get(field) or "").strip()
        if form in GMBH_FORMS:
            return True
        # Partial match für Varianten wie "GmbH & Co. KGaA i.L."
        if form and "GmbH" in form:
            return True

    # Namens-Fallback wenn kein Rechtsform-Feld vorhanden
    name = (entry.get("name") or entry.get("company_name") or "").strip()
    return "GmbH" in name or "UG (haftungsbeschränkt)" in name


def extract_name(entry: dict) -> str | None:
    """Extrahiert den Unternehmensnamen aus einem API-Eintrag."""
    for field in ("name", "company_name", "firma", "unternehmensname"):
        name = (entry.get(field) or "").strip()
        if name:
            return name
    return None


# ── Core ──────────────────────────────────────────────────────────────────────

def fetch_keyword(
    client: HandelsregisterClient,
    keyword: str,
    max_pages: int,
    dry_run: bool,
) -> list[str]:
    """Fetcht alle Seiten für ein Keyword, filtert GmbH, gibt Namen zurück."""
    results: list[str] = []

    for page in range(max_pages):
        skip = page * PAGE_SIZE
        if dry_run:
            print(f"    [DRY-RUN] GET /search-organizations?q={keyword!r}&skip={skip}")
            continue

        time.sleep(SLEEP_BETWEEN)
        entries = client.search(keyword, limit=PAGE_SIZE, skip=skip)

        if not entries:
            break  # Keine weiteren Ergebnisse

        for entry in entries:
            if is_gmbh_like(entry):
                name = extract_name(entry)
                if name:
                    results.append(name)

        # Wenn weniger Ergebnisse als PAGE_SIZE → letzte Seite
        if len(entries) < PAGE_SIZE:
            break

    return results


def run(api_key: str, out_path: Path, dry_run: bool, page_limit: int | None) -> None:
    client = HandelsregisterClient(api_key)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Bestehende Namen laden (für idempotentes Merge)
    existing: set[str] = set()
    if out_path.exists():
        existing = {
            line.strip()
            for line in out_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
        print(f"📂  Bestehende Liste: {len(existing)} Einträge in {out_path}")
    else:
        print(f"📂  Neue Liste wird angelegt: {out_path}")

    all_new: list[str] = []
    total_calls = 0
    sectors_stats: dict[str, int] = {}

    estimated = ESTIMATED_CREDITS if not page_limit else sum(
        min(pages, page_limit) for _, pages, _ in KEYWORDS
    )
    print(f"\n🔑  Geschätzte Credits: {estimated}")
    print(f"📋  Keywords: {len(KEYWORDS)}\n")

    for keyword, max_pages, sector in KEYWORDS:
        effective_pages = min(max_pages, page_limit) if page_limit else max_pages
        print(f"  🔍  {keyword!r:<40} [{sector}] — {effective_pages} page(s)")

        names = fetch_keyword(client, keyword, effective_pages, dry_run)
        total_calls += effective_pages if not dry_run else 0

        new_names = [n for n in names if n not in existing and n not in all_new]
        all_new.extend(new_names)
        sectors_stats[sector] = sectors_stats.get(sector, 0) + len(new_names)

        if new_names:
            print(f"     ✅  {len(new_names)} neue GmbHs gefunden")
        else:
            print(f"     —   Keine neuen")

    # Ergebnis schreiben (idempotent: append-only)
    if all_new and not dry_run:
        with out_path.open("a", encoding="utf-8") as f:
            for name in all_new:
                f.write(name + "\n")

    # Summary
    print(f"\n{'─'*60}")
    print(f"✅  Fertig.")
    print(f"   Bestehend:  {len(existing)}")
    print(f"   Neu hinzu:  {len(all_new)}")
    print(f"   Total:      {len(existing) + len(all_new)}")
    print(f"   API-Calls:  {total_calls}")

    if all_new:
        print(f"\n   Neu nach Sektor:")
        for sector, count in sorted(sectors_stats.items(), key=lambda x: -x[1]):
            if count:
                print(f"   {'':4}{sector:<40} +{count}")

    if dry_run:
        print(f"\n   ⚠️  DRY-RUN — keine Datei geschrieben, keine Credits verbraucht.")
    else:
        print(f"\n   📄  Output: {out_path}")
        print(f"   → Committen: git add {out_path.relative_to(Path.cwd())} && git commit -m 'data: shadow seed DE GmbH update'")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Argo Shadow DB Seed — handelsregister.ai"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Kein echter API-Call, kein Credit-Verbrauch.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output-Datei (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max Seiten pro Keyword — für Credit-sparende Test-Runs (z.B. --limit 1).",
    )
    args = parser.parse_args()

    api_key = os.environ.get("HANDELSREGISTER_API_KEY", "").strip()
    if not api_key:
        sys.exit(
            "❌  HANDELSREGISTER_API_KEY nicht gesetzt.\n"
            "    export HANDELSREGISTER_API_KEY=your_key_here"
        )

    if args.dry_run:
        print("🧪  DRY-RUN Modus — kein API-Call, kein Credit-Verbrauch\n")

    run(
        api_key=api_key,
        out_path=args.out,
        dry_run=args.dry_run,
        page_limit=args.limit,
    )


if __name__ == "__main__":
    main()
