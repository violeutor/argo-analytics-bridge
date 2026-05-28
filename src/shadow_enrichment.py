"""
Shadow Enrichment Worker — BA-Bridge
======================================
Füllt shadow_companies proaktiv mit Bundesanzeiger-Daten für deutsche private Companies.

Zwei Jobs (via APScheduler in main.py):
  1. seed_shadow_queue()    — täglich 03:30 UTC: neue DE GmbH-Companies aus seed_data/de_gmbh_curated.txt
  2. enrich_one_shadow()    — alle 2.5h: 1 pending Company via BA anreichern (≈10/Tag)

Seed-Quelle:
  seed_data/de_gmbh_curated.txt — generiert via seed_handelsregister.py (handelsregister.ai API).
  Taxonomy-aligned, aktive GmbHs, dedupliziert. Einmalig lokal generiert + ins Repo committed.
  Filtert: bereits in Supabase + bereits in shadow_companies.

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
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

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

# Pfad zur kuratierten GmbH-Liste (generiert via seed_handelsregister.py)
_SEED_FILE = Path(__file__).parent.parent / "seed_data" / "de_gmbh_curated.txt"



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

    Quelle:  seed_data/de_gmbh_curated.txt — generiert via seed_handelsregister.py.
             Taxonomy-aligned (14 Sektoren), aktive GmbHs, dedupliziert.
             Datei einmalig lokal generieren + ins Repo committen. Kein Cron nötig.

    Filter:  Bereits in Supabase → überspringen (Rolling Refresh reicht)
             Bereits in shadow_companies → überspringen
    Prio:    Wikipedia DE Pageviews (Würth/Aldi zuerst, Nischenplayer hinten)

    Täglich 03:30 UTC + beim Startup wenn Queue < 10.
    """
    if not _SEED_FILE.exists():
        logger.warning(
            "seed_shadow_queue: Seed-Datei nicht gefunden: %s — "
            "seed_handelsregister.py lokal ausführen und Ergebnis committen.",
            _SEED_FILE,
        )
        return 0

    raw = _SEED_FILE.read_text(encoding="utf-8").splitlines()
    combined = [line.strip() for line in raw if line.strip()]
    logger.info("seed_shadow_queue: Seed-Datei → %d Companies", len(combined))

    if not combined:
        logger.warning("seed_shadow_queue: Seed-Datei leer — Seed abgebrochen")
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
        "seed_shadow_queue: %d in Datei → %d neu "
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
