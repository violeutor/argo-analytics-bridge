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

# Wikidata SPARQL — DE GmbH-Companies
_WIKIDATA_URL   = "https://query.wikidata.org/sparql"
_WIKIDATA_QUERY = """
SELECT DISTINCT ?companyLabel WHERE {
  VALUES ?legalForm { wd:Q460377 wd:Q1915791 }
  ?company wdt:P1454 ?legalForm ;
           wdt:P17   wd:Q183 .
  ?company wdt:P1375 [] .
  FILTER NOT EXISTS { ?company wdt:P576 [] }
  FILTER NOT EXISTS { ?company wdt:P31 wd:Q13406463 }
  ?company rdfs:label ?companyLabel .
  FILTER(LANG(?companyLabel) = "de")
}
ORDER BY ?companyLabel
LIMIT 2000
"""

# OpenCorporates — DE GmbH aktiv, kostenlose API (kein Key nötig)
_OPENCORP_URL = "https://api.opencorporates.com/v0.4/companies/search"
_OPENCORP_PARAMS_BASE = {
    "jurisdiction_code": "de",
    "company_type":      "Gesellschaft mit beschraenkter Haftung",
    "current_status":    "Active",
    "per_page":          100,
}


# ── Wikidata SPARQL Abruf ─────────────────────────────────────────────────────

def _fetch_wikidata_de_gmbh_companies(limit: int = 500) -> list[str]:
    """
    Holt DE-GmbH-Unternehmen aus Wikidata via SPARQL.

    Rechtsform: GmbH (Q460377) + GmbH & Co. KG (Q1915791)
    Land:       Deutschland (Q183)
    Filter:     aufgelöste Companies (P576) werden ausgeschlossen
    Kein API-Key nötig. 1 HTTP-Request pro Seed-Run.
    """
    headers = {
        "User-Agent": "ArgoAnalytics/1.0 (info@argo-analytics.io)",
        "Accept":     "application/sparql-results+json",
    }
    params  = {"query": _WIKIDATA_QUERY, "format": "json"}
    backoff = [65, 120, 300]   # 429: 65s → 120s → 300s

    for attempt, wait in enumerate([0] + backoff):
        if wait:
            logger.info(
                "_fetch_wikidata_de_gmbh_companies: 429 — warte %ds (Attempt %d/%d)",
                wait, attempt, len(backoff) + 1,
            )
            time.sleep(wait)
        try:
            with httpx.Client(timeout=60, headers=headers) as client:
                resp = client.get(_WIKIDATA_URL, params=params)

            if resp.status_code == 429:
                if attempt < len(backoff):
                    continue   # nächster Backoff-Schritt
                logger.warning(
                    "_fetch_wikidata_de_gmbh_companies: 429 nach %d Versuchen — abgebrochen",
                    len(backoff) + 1,
                )
                return []

            if resp.status_code != 200:
                logger.warning(
                    "_fetch_wikidata_de_gmbh_companies HTTP %s", resp.status_code
                )
                return []

            bindings = resp.json().get("results", {}).get("bindings", [])
            seen:   set[str]  = set()
            unique: list[str] = []
            for b in bindings:
                label = (b.get("companyLabel") or {}).get("value", "").strip()
                if label and label.lower() not in seen:
                    seen.add(label.lower())
                    unique.append(label)

            logger.info(
                "_fetch_wikidata_de_gmbh_companies: %d eindeutige GmbH-Companies "
                "aus Wikidata", len(unique),
            )
            return unique[:limit]

        except Exception as e:
            logger.warning("_fetch_wikidata_de_gmbh_companies failed: %s", e)
            return []

    return []



# ── OpenCorporates Abruf ──────────────────────────────────────────────────────

def _fetch_opencorporates_de_gmbh_companies(pages: int = 5) -> list[str]:
    """
    Holt aktive DE GmbH-Companies aus OpenCorporates (kostenlose API, kein Key).
    pages x 100 Companies pro Aufruf — default 5 Seiten = 500 Companies.
    Dedupliziert. Bei Rate-Limit (429) oder Fehler: leere Liste.
    """
    headers = {"User-Agent": "ArgoAnalytics/1.0 (info@argo-analytics.io)"}
    seen:   set[str]  = set()
    result: list[str] = []

    with httpx.Client(timeout=15, headers=headers) as client:
        for page in range(1, pages + 1):
            try:
                params = {**_OPENCORP_PARAMS_BASE, "page": page}
                resp   = client.get(_OPENCORP_URL, params=params)

                if resp.status_code == 429:
                    logger.warning(
                        "_fetch_opencorporates_de_gmbh_companies: 429 auf Seite %d — abgebrochen",
                        page,
                    )
                    break

                if resp.status_code != 200:
                    logger.warning(
                        "_fetch_opencorporates_de_gmbh_companies HTTP %s auf Seite %d",
                        resp.status_code, page,
                    )
                    break

                companies = (
                    resp.json()
                    .get("results", {})
                    .get("companies", [])
                )
                if not companies:
                    break

                for item in companies:
                    name = (
                        (item.get("company") or {})
                        .get("name", "")
                        .strip()
                    )
                    if name and name.lower() not in seen:
                        seen.add(name.lower())
                        result.append(name)

                time.sleep(0.5)   # OpenCorporates Rate-Limit

            except Exception as e:
                logger.warning(
                    "_fetch_opencorporates_de_gmbh_companies failed (Seite %d): %s",
                    page, e,
                )
                break

    logger.info(
        "_fetch_opencorporates_de_gmbh_companies: %d Companies abgerufen",
        len(result),
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

    Quellen:
      1. Wikidata SPARQL — DE GmbH + GmbH & Co. KG (geschärft: HR-Nummer-Filter,
         Listen-Artikel-Ausschluss)
      2. OpenCorporates — aktive DE GmbHs (kostenlose API, 5 Seiten = 500 Companies)
    Beide Quellen werden zusammengeführt und dedupliziert.

    Filter:  Bereits in Supabase → überspringen (Rolling Refresh reicht)
             Bereits in shadow_companies → überspringen
    Prio:    Wikipedia DE Pageviews (Würth/Aldi zuerst, Nischenplayer hinten)

    Täglich 03:30 UTC + beim Startup wenn Queue < 10.
    """
    # Quelle 1: Wikidata (geschärfter Query mit HR-Nummer-Filter)
    wikidata_names = _fetch_wikidata_de_gmbh_companies(limit=500)
    logger.info("seed_shadow_queue: Wikidata → %d Companies", len(wikidata_names))

    # Quelle 2: OpenCorporates (aktive DE GmbHs)
    opencorp_names = _fetch_opencorporates_de_gmbh_companies(pages=5)
    logger.info("seed_shadow_queue: OpenCorporates → %d Companies", len(opencorp_names))

    # Zusammenführen + deduplizieren (Wikidata hat Prio bei Namens-Konflikt)
    seen: set[str] = set()
    combined: list[str] = []
    for name in wikidata_names + opencorp_names:
        if name.lower() not in seen:
            seen.add(name.lower())
            combined.append(name)

    if not combined:
        logger.warning("seed_shadow_queue: beide Quellen leer — Seed abgebrochen")
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
        "seed_shadow_queue: %d kombiniert (WD=%d OC=%d) → %d neu "
        "(Supabase: %d, Shadow: %d bereits vorhanden)",
        len(combined), len(wikidata_names), len(opencorp_names), len(new_names),
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
