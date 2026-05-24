"""
Shadow Enrichment Worker — BA-Bridge
======================================
Füllt shadow_companies proaktiv mit Bundesanzeiger-Daten.

Zwei Jobs (via APScheduler in main.py):
  1. seed_shadow_queue()    — täglich nach BA-Cron: neue Companies aus ba_reports in Queue
  2. enrich_one_shadow()    — alle 2.5h: 1 pending Company anreichern (≈10/Tag)

Prio-Score via Wikipedia Pageviews API (DE):
  ≥50k views/Monat  → 100  (Siemens, Bosch etc.)
  ≥10k              → 70
  ≥1k               → 40
  Kein Wikipedia    → 10   (FIFO nach BA-Eintrags-Reihenfolge)

Rate-Limiting:
  Max 1 Company alle 2.5h → kein CAPTCHA-Risiko bei BA-Scraping.
  Kein Parallelisieren — sequentiell mit 5s Pause zwischen Sub-Requests.
"""
import logging
from datetime import datetime, timezone, timedelta

import httpx
from sqlalchemy.orm import Session

from src.models_shadow import ShadowCompany

logger = logging.getLogger(__name__)

_WIKI_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/de.wikipedia/all-access/all-agents/{title}/monthly/{start}/{end}"
)
_WIKI_HEADERS = {"User-Agent": "ArgoAnalytics/1.0 (info@argo-analytics.io)"}


# ── Prio-Score ────────────────────────────────────────────────────────────────

def _get_prio_score(company_name: str) -> float:
    """
    Wikipedia Pageviews (DE) → Prio-Score.
    Schauts letzten 2 Monate an → robuster gegen Ausreißer.
    Kein Key nötig — öffentliche Wikimedia API.
    """
    title = company_name.replace(" ", "_")
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(days=60)).strftime("%Y%m01")
    end   = now.strftime("%Y%m01")
    url   = _WIKI_URL.format(title=title, start=start, end=end)

    try:
        with httpx.Client(timeout=8) as client:
            resp = client.get(url, headers=_WIKI_HEADERS)

        if resp.status_code == 404:
            return 10.0   # Kein Wikipedia-Artikel → FIFO-Prio
        if resp.status_code != 200:
            logger.debug("Wikipedia Pageviews HTTP %s für '%s'", resp.status_code, company_name)
            return 10.0

        items = resp.json().get("items", [])
        if not items:
            return 10.0

        avg = sum(i.get("views", 0) for i in items) / len(items)
        if avg >= 50_000:  return 100.0
        if avg >= 10_000:  return 70.0
        if avg >= 1_000:   return 40.0
        return 20.0   # Wikipedia vorhanden, aber wenig Traffic

    except Exception as e:
        logger.debug("_get_prio_score failed für '%s': %s", company_name, e)
        return 10.0


# ── Seed Job ──────────────────────────────────────────────────────────────────

def seed_shadow_queue(db: Session) -> int:
    """
    Scannt ba_reports nach Company-Namen die noch nicht in shadow_companies sind.
    Neu gefundene Companies werden mit Wikipedia-Prio-Score in Queue aufgenommen.

    Wird täglich nach _cron_enrich_all() aufgerufen.
    Gibt Anzahl neuer Queue-Einträge zurück.
    """
    from src.models import BAReport
    from sqlalchemy import distinct

    # Alle bekannten BA-Company-Namen
    ba_names: list[str] = [
        row[0]
        for row in db.query(distinct(BAReport.company_name)).all()
        if row[0]
    ]

    # Bereits in Shadow-Queue
    existing: set[str] = {
        row[0]
        for row in db.query(ShadowCompany.name).all()
    }

    new_names = [n for n in ba_names if n not in existing]
    logger.info("seed_shadow_queue: %d neue Companies aus ba_reports", len(new_names))

    added = 0
    for name in new_names:
        prio = _get_prio_score(name)
        sc = ShadowCompany(
            name=name,
            prio_score=prio,
            enrichment_status="pending",
            source="bundesanzeiger",
        )
        db.add(sc)
        added += 1

    if added:
        db.commit()

    logger.info("seed_shadow_queue: %d Companies in Queue aufgenommen", added)
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

    # Nächste pending Company nach Prio
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
        "enrich_one_shadow: '%s' (prio=%.0f, queue_pos=pending)",
        sc.name, sc.prio_score,
    )
    sc.enrichment_status = "running"
    db.commit()

    try:
        # BA fetchen + parsen (sequentiell, kein Burst)
        fetch_and_store(sc.name, db)
        parse_pending(db, limit=5)

        # Neueste Finanzdaten (höchstes fiscal_year)
        fin: BAFinancial | None = (
            db.query(BAFinancial)
            .filter(BAFinancial.company_name == sc.name)
            .order_by(BAFinancial.fiscal_year.desc().nullslast())
            .first()
        )

        if fin:
            sc.ba_id             = str(fin.report_id)
            sc.revenue_eur_mn    = fin.revenue_eur_mn
            sc.ebitda_eur_mn     = fin.ebitda_eur_mn
            sc.ebit_eur_mn       = fin.ebit_eur_mn
            sc.net_income_eur_mn = fin.net_income_eur_mn
            sc.equity_eur_mn     = fin.equity_eur_mn
            sc.total_assets_eur_mn = fin.total_assets_eur_mn
            sc.headcount         = fin.headcount
            sc.fiscal_year       = fin.fiscal_year

        # Gesellschafter + Geschäftsführer
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
            "enrich_one_shadow OK: '%s' — FY%s rev=%.1fM shareholders=%d directors=%d",
            sc.name,
            sc.fiscal_year,
            sc.revenue_eur_mn or 0.0,
            len(sc.shareholders or []),
            len(sc.managing_directors or []),
        )
        return True

    except Exception as e:
        logger.warning("enrich_one_shadow FAILED für '%s': %s", sc.name, e)
        sc.enrichment_status = "error"
        db.commit()
        return False
