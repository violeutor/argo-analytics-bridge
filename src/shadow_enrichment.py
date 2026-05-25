"""
Shadow Enrichment Worker — BA-Bridge
======================================
Füllt shadow_companies proaktiv mit Bundesanzeiger-Daten für deutsche private Companies.

Zwei Jobs (via APScheduler in main.py):
  1. seed_shadow_queue()    — täglich 03:30 UTC: neue DE Companies aus Wikipedia-Kategorie
  2. enrich_one_shadow()    — alle 2.5h: 1 pending Company via BA anreichern (≈10/Tag)

Seed-Quelle:
  Wikipedia DE Kategorie "Unternehmen_(Deutschland)" → paginiert, bis 500 Artikel.
  Filtert: bereits in Supabase + bereits in shadow_companies.
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

_WIKI_PAGEVIEWS_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/de.wikipedia/all-access/all-agents/{title}/monthly/{start}/{end}"
)
_WIKI_CATEGORY_URL = "https://de.wikipedia.org/w/api.php"
_WIKI_HEADERS = {"User-Agent": "ArgoAnalytics/1.0 (info@argo-analytics.io)"}

# Klammerzusätze die aus Artikeltiteln entfernt werden
_DISAMBIG_RE = re.compile(
    r"\s*\((Unternehmen|Konzern|Firma|Gruppe|Deutschland|GmbH|AG|SE|KG)\)\s*$",
    re.IGNORECASE,
)


# ── Wikipedia Kategorie-Abruf ─────────────────────────────────────────────────

def _fetch_wikipedia_de_companies(limit: int = 500) -> list[str]:
    """
    Holt Artikel-Titel aus Wikipedia DE Kategorie 'Unternehmen_(Deutschland)'.
    Paginiert automatisch. Bereinigt Klammerzusätze aus Titeln.
    Gibt bereinigte, eindeutige Company-Namen zurück.
    """
    names: list[str] = []
    params: dict = {
        "action":  "query",
        "list":    "categorymembers",
        "cmtitle": "Kategorie:Unternehmen_(Deutschland)",
        "cmlimit": min(limit, 500),
        "cmtype":  "page",
        "format":  "json",
    }

    try:
        with httpx.Client(timeout=15, headers=_WIKI_HEADERS) as client:
            while len(names) < limit:
                resp = client.get(_WIKI_CATEGORY_URL, params=params)
                if resp.status_code != 200:
                    logger.warning(
                        "_fetch_wikipedia_de_companies HTTP %s", resp.status_code
                    )
                    break

                data    = resp.json()
                members = data.get("query", {}).get("categorymembers", [])

                for m in members:
                    title = m.get("title", "").strip()
                    if ":" in title:
                        continue
                    clean = _DISAMBIG_RE.sub("", title).strip()
                    if clean:
                        names.append(clean)

                if "continue" in data:
                    params["cmcontinue"] = data["continue"]["cmcontinue"]
                else:
                    break

    except Exception as e:
        logger.warning("_fetch_wikipedia_de_companies failed: %s", e)

    seen: set[str] = set()
    unique: list[str] = []
    for n in names:
        if n.lower() not in seen:
            seen.add(n.lower())
            unique.append(n)

    logger.info(
        "_fetch_wikipedia_de_companies: %d eindeutige Companies aus Wikipedia DE", len(unique)
    )
    return unique


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
        result = sb.table("companies").select("name").execute()
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


# ── Seed Job ──────────────────────────────────────────────────────────────────

def seed_shadow_queue(db: Session) -> int:
    """
    Befüllt Shadow-Queue mit deutschen Companies die noch NICHT in Supabase sind.

    Quelle:  Wikipedia DE Kategorie 'Unternehmen_(Deutschland)' (bis 500 Artikel)
    Filter:  Bereits in Supabase → überspringen (Rolling Refresh reicht)
             Bereits in shadow_companies → überspringen
    Prio:    Wikipedia DE Pageviews (Würth/Aldi zuerst, Nischenplayer hinten)

    Täglich 03:30 UTC + beim Startup wenn Queue leer.
    """
    wiki_names = _fetch_wikipedia_de_companies(limit=500)
    if not wiki_names:
        logger.warning("seed_shadow_queue: Wikipedia-Abruf leer — Seed abgebrochen")
        return 0

    supabase_existing = _fetch_supabase_company_names()
    shadow_existing: set[str] = {
        row[0].lower()
        for row in db.query(ShadowCompany.name).all()
    }

    new_names = [
        n for n in wiki_names
        if n.lower() not in supabase_existing
        and n.lower() not in shadow_existing
    ]

    logger.info(
        "seed_shadow_queue: %d Wikipedia-Namen → %d neu "
        "(Supabase: %d, Shadow: %d bereits vorhanden)",
        len(wiki_names), len(new_names),
        len(supabase_existing), len(shadow_existing),
    )

    added = 0
    for name in new_names:
        prio = _get_prio_score(name)
        db.add(ShadowCompany(
            name=name,
            prio_score=prio,
            enrichment_status="pending",
            source="bundesanzeiger",
        ))
        added += 1
        time.sleep(0.05)

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
    from src.ba_parser import parse_pending

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
        fetch_and_store(sc.name, db)
        parse_pending(db, limit=5)

        fin: BAFinancial | None = (
            db.query(BAFinancial)
            .filter(BAFinancial.company_name == sc.name)
            .order_by(BAFinancial.fiscal_year.desc().nullslast())
            .first()
        )

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
        logger.warning("enrich_one_shadow FAILED für '%s': %s", sc.name, e)
        sc.enrichment_status = "error"
        db.commit()
        return False
