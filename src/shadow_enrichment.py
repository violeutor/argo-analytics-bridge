"""
Shadow Enrichment Worker — BA-Bridge
======================================
Füllt shadow_companies proaktiv mit Bundesanzeiger-Daten für deutsche private Companies.

Zwei Jobs (via APScheduler in main.py):
  1. seed_shadow_queue()    — täglich 03:30 UTC: neue DE GmbH-Companies aus Wikidata
  2. enrich_one_shadow()    — alle 2.5h: 1 pending Company via BA anreichern (≈10/Tag)

Seed-Quelle:
  Wikidata SPARQL → DE Companies mit Rechtsform GmbH / GmbH & Co. KG.
  Filtert: bereits in Supabase + bereits in shadow_companies.
  Kein API-Key nötig. 1 Request pro Seed-Run.
  Zweck: Companies die der User noch NIE gesucht hat — proaktiver Pre-fetch.

Prio-Score via Wikipedia Pageviews API (DE):
  ≥50k views/Monat  → 100  (Würth, Aldi, Bosch etc.)
  ≥10k              → 70
  ≥1k               → 40
  <1k / kein Artikel → 10  (FIFO)

Rate-Limiting:
  Max 1 Company alle 2.5h → kein CAPTCHA-Risiko bei BA-Scraping.
  Kein Parallelisieren — sequentiell.
"""
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta

import httpx
from sqlalchemy.orm import Session

from src.models_shadow import ShadowCompany

logger = logging.getLogger(__name__)

# Nach MAX_RETRIES erfolglosen BA-Versuchen → status='exhausted' (nicht mehr retried)
MAX_RETRIES = 5

_WIKI_PAGEVIEWS_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/de.wikipedia/all-access/all-agents/{title}/monthly/{start}/{end}"
)
_WIKI_HEADERS = {"User-Agent": "ArgoAnalytics/1.0 (info@argo-analytics.io)"}

# ── Wikipedia Kategorie-Seed ─────────────────────────────────────────────────
# Ersetzt Wikidata SPARQL + OpenCorporates.
# Quelle: DE Wikipedia Unternehmenskategorien → taxonomy-aligned + PE-Software-Expansion.
# Vorteile: kein API-Key, kein Storage, kein Rate-Limit-Problem, hohe Datenqualität.

_WIKI_API_URL = "https://de.wikipedia.org/w/api.php"

# (Kategoriename, gmbh_only)
# gmbh_only=True  → nur Artikel mit "GmbH" oder "GmbH & Co" im Titel
# gmbh_only=False → alle Artikel (für Softwaresektor: breiter, PE-relevant)
_WIKI_CATEGORIES: list[tuple[str, bool]] = [
    # ── Carbon & Climate ──────────────────────────────────────────────────
    ("Solarunternehmen_(Deutschland)",                   True),
    ("Windkraftunternehmen_(Deutschland)",               True),
    ("Energieunternehmen_(Deutschland)",                 True),
    # ── Industrial Tech ───────────────────────────────────────────────────
    ("Maschinenbauunternehmen_(Deutschland)",            True),
    ("Elektronikunternehmen_(Deutschland)",              True),
    ("Automobilindustrie_(Deutschland)",                 True),
    # ── Life Sciences ─────────────────────────────────────────────────────
    ("Biotechnologieunternehmen_(Deutschland)",          True),
    ("Medizintechnikunternehmen_(Deutschland)",          True),
    ("Pharmaunternehmen_(Deutschland)",                  True),
    ("Chemieunternehmen_(Deutschland)",                  True),
    # ── Software / IT — PE-relevant, bewusst breiter ──────────────────────
    # Ziel: ältere DE Softwareunternehmen (ERP, Systemhäuser, Nische) als PE-Target
    ("Softwareunternehmen_(Deutschland)",                False),
    ("Informationstechnologieunternehmen_(Deutschland)", False),
]

# Signalwörter für PE-relevante Altsoftware im Unternehmensnamen.
# Companies mit diesen Begriffen werden auch ohne GmbH-Filter im Namen aufgenommen —
# viele Systemhäuser und ERP-Anbieter heißen z.B. "ABC Datentechnik GmbH".
_PE_SOFTWARE_SIGNALS: frozenset[str] = frozenset({
    "software", "systeme", "systemhaus", "datentechnik", "datenverarbeitung",
    "edv", "informationstechnik", "informationssysteme", "it-systeme",
    "softwareentwicklung", "anwendungsentwicklung", "betriebssoftware",
    "warenwirtschaft", "erp", "crm",
})

_GMBH_SIGNALS: frozenset[str] = frozenset({"gmbh", "gmbh & co"})


def _is_gmbh_name(name: str) -> bool:
    """True wenn Name auf GmbH oder GmbH & Co. KG hindeutet."""
    lower = name.lower()
    return any(sig in lower for sig in _GMBH_SIGNALS)


def _is_pe_software_candidate(name: str) -> bool:
    """True wenn Name PE-relevante Softwaresignale enthält."""
    lower = name.lower()
    return any(sig in lower for sig in _PE_SOFTWARE_SIGNALS)


def _fetch_wikipedia_category_companies(
    category: str,
    gmbh_only: bool = True,
    limit: int = 500,
) -> list[str]:
    """
    Holt Unternehmensnamen aus einer DE-Wikipedia-Kategorie via MediaWiki API.

    Filtert:
      - gmbh_only=True: nur Artikel mit GmbH/GmbH & Co im Titel
      - gmbh_only=False: alle + PE-Software-Signal-Filter als Erweiterung
      - Unterkategorien (ns != 0) werden übersprungen
      - Duplikate (case-insensitiv) werden dedupliziert

    Rate-Limit: 1 Request pro Seite, 0.3s Sleep — Wikimedia-konform.
    """
    headers = {
        "User-Agent": "ArgoAnalytics/1.0 (info@argo-analytics.io)",
        "Accept":     "application/json",
    }
    seen:   set[str]  = set()
    result: list[str] = []
    cmcontinue: str | None = None

    with httpx.Client(timeout=20, headers=headers) as client:
        while len(result) < limit:
            params: dict = {
                "action":  "query",
                "list":    "categorymembers",
                "cmtitle": f"Kategorie:{category}",
                "cmlimit": 500,
                "cmnamespace": 0,   # nur Artikel, keine Unterkategorien
                "format":  "json",
            }
            if cmcontinue:
                params["cmcontinue"] = cmcontinue

            try:
                resp = client.get(_WIKI_API_URL, params=params)
            except Exception as e:
                logger.warning("_fetch_wikipedia_category_companies '%s' failed: %s", category, e)
                break

            if resp.status_code == 429:
                logger.warning("_fetch_wikipedia_category_companies '%s': 429 — abgebrochen", category)
                break
            if resp.status_code != 200:
                logger.warning(
                    "_fetch_wikipedia_category_companies '%s': HTTP %s", category, resp.status_code
                )
                break

            data = resp.json()
            members = data.get("query", {}).get("categorymembers", [])

            for member in members:
                name = member.get("title", "").strip()
                if not name or name.lower() in seen:
                    continue

                # GmbH-Filter
                if gmbh_only and not _is_gmbh_name(name):
                    # Ausnahme: PE-Software-Signal → trotzdem aufnehmen
                    if not _is_pe_software_candidate(name):
                        continue

                seen.add(name.lower())
                result.append(name)

            # Pagination
            cmcontinue = data.get("continue", {}).get("cmcontinue")
            if not cmcontinue or not members:
                break

            time.sleep(0.3)   # Wikimedia Rate-Limit

    logger.info(
        "_fetch_wikipedia_category_companies: Kategorie '%s' → %d Companies (gmbh_only=%s)",
        category, len(result), gmbh_only,
    )
    return result[:limit]


def _fetch_all_wiki_category_companies() -> list[str]:
    """
    Iteriert alle _WIKI_CATEGORIES und sammelt deduplizierte Unternehmensnamen.
    Reihenfolge: Taxonomy-Kategorien zuerst, PE-Software zuletzt (breiter).
    """
    seen:   set[str]  = set()
    result: list[str] = []

    for category, gmbh_only in _WIKI_CATEGORIES:
        names = _fetch_wikipedia_category_companies(category, gmbh_only=gmbh_only)
        for name in names:
            if name.lower() not in seen:
                seen.add(name.lower())
                result.append(name)
        time.sleep(1.0)   # zwischen Kategorien: Wikimedia schonen

    logger.info(
        "_fetch_all_wiki_category_companies: %d Companies aus %d Kategorien",
        len(result), len(_WIKI_CATEGORIES),
    )
    return result



# ── Supabase-Abruf (Ausschluss-Filter) ───────────────────────────────────────

def _fetch_supabase_company_names() -> set[str]:
    """
    Holt alle Company-Namen aus Supabase (für Ausschluss im Seed).
    Companies die bereits in Supabase sind, brauchen kein Shadow Pre-fetch.
    """
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("ARGO_SUPABASE_URL", "")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
        or os.getenv("ARGO_SUPABASE_KEY", "")
    )
    if not supabase_url or not supabase_key:
        logger.warning("_fetch_supabase_company_names: keine Supabase-Credentials")
        return set()
    try:
        from supabase import create_client
        sb     = create_client(supabase_url, supabase_key)
        # Limit 9999 — PostgREST-Default ist 1000, würde bei >1000 Companies
        # den Exclusion-Filter still truncaten → Shadow-Duplikate
        result = sb.table("companies").select("name").limit(9999).execute()
        return {r["name"].lower() for r in (result.data or []) if r.get("name")}
    except Exception as e:
        logger.warning("_fetch_supabase_company_names failed: %s", e)
        return set()


# ── Prio-Score ────────────────────────────────────────────────────────────────

def _get_prio_score(company_name: str) -> float:
    """Wikipedia Pageviews (DE) → Prio-Score. Letzten 2 Monate."""
    title = company_name.replace(" ", "_")
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(days=60)).strftime("%Y%m01")
    end   = now.strftime("%Y%m01")
    url   = _WIKI_PAGEVIEWS_URL.format(title=title, start=start, end=end)

    try:
        with httpx.Client(timeout=8) as client:
            resp = client.get(url, headers=_WIKI_HEADERS)
        if resp.status_code == 404:
            return 10.0
        if resp.status_code != 200:
            return 10.0
        items = resp.json().get("items", [])
        if not items:
            return 10.0
        avg = sum(i.get("views", 0) for i in items) / len(items)
        if avg >= 50_000: return 100.0
        if avg >= 10_000: return 70.0
        if avg >=  1_000: return 40.0
        return 20.0
    except Exception as e:
        logger.debug("_get_prio_score failed für '%s': %s", company_name, e)
        return 10.0


# ── Startup Cleanup ───────────────────────────────────────────────────────────

def reset_stale_running(db: Session) -> int:
    """
    Setzt alle Companies im Status 'running' auf 'pending' zurück.

    Wird beim BA-Bridge Startup aufgerufen — 'running' beim Start bedeutet
    zwingend ein Crash-Relikt aus dem vorherigen Prozess (Render Redeploy,
    OOM, etc.). Da enrich_one_shadow() sequentiell läuft und max. ein paar
    Minuten dauert, gibt es keinen legitimen 'running'-Eintrag beim Startup.

    retry_count wird NICHT zurückgesetzt — vorherige Versuche zählen weiter.
    """
    stale = (
        db.query(ShadowCompany)
        .filter(ShadowCompany.enrichment_status == "running")
        .all()
    )
    for sc in stale:
        sc.enrichment_status = "pending"
    if stale:
        db.commit()
        logger.info(
            "reset_stale_running: %d stale 'running' → 'pending' zurückgesetzt",
            len(stale),
        )
    return len(stale)


# ── Seed Job ──────────────────────────────────────────────────────────────────

def seed_shadow_queue(db: Session) -> int:
    """
    Befüllt Shadow-Queue mit deutschen GmbH-Companies die noch NICHT in Supabase sind.

    Quelle:  Wikipedia DE Unternehmenskategorien — taxonomy-aligned + PE-Software-Expansion.
             Kategorien: Climate, Industrial, Life Sciences, Software/IT.
             PE-Software: breiter Scope (ohne GmbH-Pflicht) für ältere DE Softwareunternehmen.

    Filter:  Bereits in Supabase → überspringen (Rolling Refresh reicht)
             Bereits in shadow_companies → überspringen
    Prio:    Wikipedia DE Pageviews (Würth/Aldi zuerst, Nischenplayer hinten)

    Täglich 03:30 UTC + beim Startup wenn Queue < 10.
    """
    # Alle Wikipedia-Kategorien durchlaufen (taxonomy-aligned + PE-Software)
    combined = _fetch_all_wiki_category_companies()
    logger.info("seed_shadow_queue: Wikipedia-Kategorien → %d Companies", len(combined))

    if not combined:
        logger.warning("seed_shadow_queue: keine Companies aus Wikipedia-Kategorien — Seed abgebrochen")
        return 0

    supabase_existing = _fetch_supabase_company_names()
    shadow_existing: set[str] = {
        row[0].lower()
        for row in db.query(ShadowCompany.name).all()
    }

    new_names = [
        n for n in combined
        if n.lower() not in supabase_existing
        and n.lower() not in shadow_existing
    ]

    logger.info(
        "seed_shadow_queue: %d kombiniert → %d neu "
        "(Supabase: %d, Shadow: %d bereits vorhanden)",
        len(combined), len(new_names),
        len(supabase_existing), len(shadow_existing),
    )

    added = 0
    for name in new_names:
        prio = _get_prio_score(name)
        time.sleep(0.1)   # Wikimedia Rate-Limit — kein Burst
        db.add(ShadowCompany(
            name=name,
            prio_score=prio,
            enrichment_status="pending",
            source="bundesanzeiger",
        ))
        added += 1

    if added:
        db.commit()

    logger.info("seed_shadow_queue: %d neue Companies in Queue aufgenommen", added)
    return added


# ── Enrichment Job ────────────────────────────────────────────────────────────

def enrich_one_shadow(db: Session) -> bool:
    """
    Enrichert genau 1 pending Shadow-Company mit BA-Daten.
    Sortierung: prio_score DESC → created_at ASC (älteste bei Gleichstand).

    Flow:
      fetch_and_store(name) → parse_pending() → Daten aus ba_financials/ba_persons lesen
      → in shadow_companies schreiben → status='done'

    Wird alle 2.5h aufgerufen → ≈10 Companies/Tag, kein CAPTCHA-Risiko.
    Returns True wenn erfolgreich angereichert, False wenn Queue leer oder Fehler.
    """
    from src.models import BAFinancial, BAPerson
    from src.ba_fetcher import fetch_and_store

    sc: ShadowCompany | None = (
        db.query(ShadowCompany)
        .filter(ShadowCompany.enrichment_status == "pending")
        .order_by(ShadowCompany.prio_score.desc(), ShadowCompany.created_at.asc())
        .first()
    )

    if not sc:
        logger.debug("enrich_one_shadow: Queue leer")
        return False

    logger.info(
        "enrich_one_shadow: '%s' (prio=%.0f)",
        sc.name, sc.prio_score,
    )
    sc.enrichment_status = "running"
    db.commit()

    try:
        # Rückgabewert capturen — [] bei CAPTCHA oder "nicht in BA"
        reports = fetch_and_store(sc.name, db)

        # Company-spezifisches Parsing — nicht alle pending Reports global abarbeiten
        from src.models import BAReport as _BAReport
        from src.ba_parser import parse_report as _parse_report
        pending_for_company = (
            db.query(_BAReport)
            .filter_by(company_name=sc.name, parse_status="pending")
            .limit(5)
            .all()
        )
        for r in pending_for_company:
            _parse_report(r, db)

        fin: BAFinancial | None = (
            db.query(BAFinancial)
            .filter(BAFinancial.company_name == sc.name)
            .order_by(BAFinancial.fiscal_year.desc().nullslast())
            .first()
        )

        # BA-11 CAPTCHA-Propagation:
        # fetch_and_store gibt [] bei CAPTCHA UND bei "nicht in BA".
        # Wenn kein Report gefetcht + kein BAFinancial vorhanden → zurück auf pending.
        # Shadow Company wird beim nächsten 2.5h-Run automatisch retried.
        if not reports and not fin:
            sc.retry_count = (sc.retry_count or 0) + 1
            if sc.retry_count >= MAX_RETRIES:
                sc.enrichment_status = "exhausted"
                logger.info(
                    "enrich_one_shadow: '%s' — exhausted nach %d Versuchen "
                    "(kein BA-Treffer / CAPTCHA)",
                    sc.name, sc.retry_count,
                )
            else:
                sc.enrichment_status = "pending"
                logger.info(
                    "enrich_one_shadow: '%s' — kein BA-Treffer → pending "
                    "(Versuch %d/%d, nächster Run in ~2.5h)",
                    sc.name, sc.retry_count, MAX_RETRIES,
                )
            db.commit()
            return False

        if fin:
            sc.ba_id               = str(fin.report_id)
            sc.revenue_eur_mn      = fin.revenue_eur_mn
            sc.ebitda_eur_mn       = fin.ebitda_eur_mn
            sc.ebit_eur_mn         = fin.ebit_eur_mn
            sc.net_income_eur_mn   = fin.net_income_eur_mn
            sc.equity_eur_mn       = fin.equity_eur_mn
            sc.total_assets_eur_mn = fin.total_assets_eur_mn
            sc.headcount           = fin.headcount
            sc.fiscal_year         = fin.fiscal_year

        persons: list[BAPerson] = (
            db.query(BAPerson)
            .filter(BAPerson.company_name == sc.name)
            .all()
        )
        sc.shareholders = [
            {"name": p.name, "share_pct": p.share_pct, "is_company": p.is_company}
            for p in persons if p.role == "shareholder"
        ]
        sc.managing_directors = [
            {"name": p.name, "role": p.role}
            for p in persons if p.role in ("executive", "supervisory_board")
        ]

        sc.enrichment_status = "done"
        sc.enriched_at       = datetime.now(timezone.utc)
        db.commit()

        logger.info(
            "enrich_one_shadow OK: '%s' — FY%s rev=%.1fM shareholders=%d",
            sc.name, sc.fiscal_year,
            sc.revenue_eur_mn or 0.0,
            len(sc.shareholders or []),
        )
        return True

    except Exception as e:
        sc.retry_count = (sc.retry_count or 0) + 1
        if sc.retry_count >= MAX_RETRIES:
            sc.enrichment_status = "exhausted"
            logger.warning(
                "enrich_one_shadow: '%s' — exhausted nach %d Versuchen (Exception: %s)",
                sc.name, sc.retry_count, e,
            )
        else:
            sc.enrichment_status = "pending"
            logger.warning(
                "enrich_one_shadow FAILED '%s' (Versuch %d/%d): %s",
                sc.name, sc.retry_count, MAX_RETRIES, e,
            )
        db.commit()
        return False
