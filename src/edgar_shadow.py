"""
EDGAR Shadow Enrichment — US Private Companies
===============================================
Füllt shadow_companies mit Daten aus EDGAR Form D Filings (US private Companies).

Kein CAPTCHA, öffentliche REST-API, offiziell 10 req/s erlaubt.
Rate: alle 15min 1 Company → ~96/Tag.

Zwei Jobs:
  seed_shadow_queue_edgar() — täglich: neue Form D Companies in Queue aufnehmen
  enrich_one_shadow_edgar() — alle 15min: 1 pending EDGAR Company anreichern

Prio-Score: Wikipedia Pageviews (EN) — selbe Logik wie DE, andere Subdomain.

Datenquellen:
  Seed:   EDGAR EFTS Search API  — Form D Filings letzte 30 Tage
  Enrich: EDGAR Submissions API  — Company-Details (HQ, SIC, legal form)
          EDGAR EFTS             — Form D Details (Offering Amount, Executives)
"""
import logging
import time
from datetime import datetime, timezone, timedelta

import httpx
from sqlalchemy.orm import Session

from src.models_shadow import ShadowCompany

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "ArgoAnalytics/1.0 info@argo-analytics.io",
    "Accept":     "application/json",
}
_EFTS_URL        = "https://efts.sec.gov/LATEST/search-index"
_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_COMPANY_URL     = "https://www.sec.gov/cgi-bin/browse-edgar?company={name}&CIK=&type=D&dateb=&owner=include&count=5&search_text=&action=getcompany&output=atom"


# ── Prio-Score (EN Wikipedia) ─────────────────────────────────────────────────

def _get_prio_score_en(company_name: str) -> float:
    """
    Wikipedia Pageviews (EN) → Prio-Score.
    Gleiche Tier-Logik wie DE-Variante in shadow_enrichment.py.
    """
    title = company_name.replace(" ", "_")
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(days=60)).strftime("%Y%m01")
    end   = now.strftime("%Y%m01")
    url   = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
        f"/en.wikipedia/all-access/all-agents/{title}/monthly/{start}/{end}"
    )
    try:
        with httpx.Client(timeout=8) as client:
            resp = client.get(url, headers=_HEADERS)
        if resp.status_code == 404:
            return 10.0
        if resp.status_code != 200:
            return 10.0
        items = resp.json().get("items", [])
        if not items:
            return 10.0
        avg = sum(i.get("views", 0) for i in items) / len(items)
        if avg >= 50_000:  return 100.0
        if avg >= 10_000:  return 70.0
        if avg >= 1_000:   return 40.0
        return 20.0
    except Exception as e:
        logger.debug("_get_prio_score_en failed für '%s': %s", company_name, e)
        return 10.0


# ── EDGAR Helpers ─────────────────────────────────────────────────────────────

def _fetch_recent_form_d_names(days: int = 30) -> list[str]:
    """
    Holt Company-Namen aus EDGAR Form D Filings der letzten N Tage.
    Gibt bereinigte, eindeutige Namen zurück.
    """
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    end   = now.strftime("%Y-%m-%d")

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                _EFTS_URL,
                params={
                    "forms":          "D",
                    "dateRange":      "custom",
                    "startdt":        start,
                    "enddt":          end,
                    "hits.hits.total.value": "true",
                },
                headers=_HEADERS,
            )
        if resp.status_code != 200:
            logger.warning("EDGAR EFTS HTTP %s", resp.status_code)
            return []

        hits = resp.json().get("hits", {}).get("hits", [])
        names: list[str] = []
        for hit in hits:
            src  = hit.get("_source", {})
            name = (src.get("entity_name") or "").strip()
            # US-only: biz_location ist 2-Letter State Code
            loc  = (src.get("biz_location") or "")
            if name and len(loc) == 2 and loc.isalpha():
                names.append(name)

        return list(dict.fromkeys(names))   # Reihenfolge erhalten, Duplikate entfernen

    except Exception as e:
        logger.warning("_fetch_recent_form_d_names failed: %s", e)
        return []


def _fetch_edgar_cik(company_name: str) -> str | None:
    """Sucht CIK für company_name via EDGAR EFTS."""
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                _EFTS_URL,
                params={"q": f'"{company_name}"', "forms": "D"},
                headers=_HEADERS,
            )
        if resp.status_code != 200:
            return None
        hits = resp.json().get("hits", {}).get("hits", [])
        if not hits:
            return None
        # Erster Treffer — entity_id enthält CIK
        entity_id = hits[0].get("_source", {}).get("entity_id") or hits[0].get("_id", "")
        # entity_id Format: "CIK0001234567" oder direkte Zahl
        cik = entity_id.replace("CIK", "").lstrip("0")
        return cik if cik else None
    except Exception as e:
        logger.debug("_fetch_edgar_cik failed für '%s': %s", company_name, e)
        return None


def _fetch_submissions(cik: str) -> dict:
    """
    Holt Company-Submissions aus EDGAR data.sec.gov.
    Gibt company-level Daten zurück: name, state, SIC, legal form, address.
    """
    cik_padded = cik.zfill(10)
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                _SUBMISSIONS_URL.format(cik=cik_padded),
                headers=_HEADERS,
            )
        if resp.status_code != 200:
            return {}
        return resp.json()
    except Exception as e:
        logger.debug("_fetch_submissions failed für CIK %s: %s", cik, e)
        return {}


def _fetch_form_d_details(company_name: str) -> dict:
    """
    Holt Form D Details via EDGAR EFTS Full-Text Search.
    Extrahiert: offering amount, executives/directors, date of first sale.
    """
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                _EFTS_URL,
                params={"q": f'"{company_name}"', "forms": "D"},
                headers=_HEADERS,
            )
        if resp.status_code != 200:
            return {}

        hits = resp.json().get("hits", {}).get("hits", [])
        if not hits:
            return {}

        src = hits[0].get("_source", {})
        return {
            "offering_amount": src.get("offering_amount"),
            "date_of_first_sale": src.get("date_of_first_sale") or src.get("period_of_report"),
            "industry_group": src.get("industry_group_type"),
            "biz_location": src.get("biz_location"),
        }
    except Exception as e:
        logger.debug("_fetch_form_d_details failed für '%s': %s", company_name, e)
        return {}


# ── Seed Job ──────────────────────────────────────────────────────────────────

def seed_shadow_queue_edgar(db: Session) -> int:
    """
    Scannt EDGAR Form D Filings der letzten 30 Tage nach neuen US-Private Companies.
    Filtert Companies die bereits in shadow_companies sind (source unabhängig).
    Prio-Score via Wikipedia Pageviews (EN).

    Wird täglich nach BA-Seed (03:45 UTC) aufgerufen.
    Returns: Anzahl neuer Queue-Einträge.
    """
    names = _fetch_recent_form_d_names(days=30)
    logger.info("seed_shadow_queue_edgar: %d Form D Namen aus EDGAR", len(names))

    existing: set[str] = {
        row[0].lower()
        for row in db.query(ShadowCompany.name).all()
    }

    new_names = [n for n in names if n.lower() not in existing]
    logger.info("seed_shadow_queue_edgar: %d neue Companies", len(new_names))

    added = 0
    for name in new_names:
        prio = _get_prio_score_en(name)
        sc   = ShadowCompany(
            name=name,
            prio_score=prio,
            enrichment_status="pending",
            source="edgar",
        )
        db.add(sc)
        added += 1
        time.sleep(0.1)   # sanftes Rate-Limiting für Wikipedia-Calls

    if added:
        db.commit()

    logger.info("seed_shadow_queue_edgar: %d Companies in Queue", added)
    return added


# ── Enrichment Job ────────────────────────────────────────────────────────────

def enrich_one_shadow_edgar(db: Session) -> bool:
    """
    Enrichert genau 1 pending EDGAR Shadow-Company.
    Sortierung: prio_score DESC → created_at ASC.

    Flow:
      _fetch_edgar_cik()    → CIK ermitteln
      _fetch_submissions()  → HQ, SIC, legal form, state
      _fetch_form_d_details() → offering amount, date, executives
      → shadow_companies updaten → status='done'

    Returns True wenn erfolgreich.
    """
    sc: ShadowCompany | None = (
        db.query(ShadowCompany)
        .filter(
            ShadowCompany.enrichment_status == "pending",
            ShadowCompany.source == "edgar",
        )
        .order_by(ShadowCompany.prio_score.desc(), ShadowCompany.created_at.asc())
        .first()
    )

    if not sc:
        logger.debug("enrich_one_shadow_edgar: Queue leer")
        return False

    logger.info(
        "enrich_one_shadow_edgar: '%s' (prio=%.0f)",
        sc.name, sc.prio_score,
    )
    sc.enrichment_status = "running"
    db.commit()

    try:
        # 1. CIK ermitteln
        cik = _fetch_edgar_cik(sc.name)
        if cik:
            sc.ba_id = f"CIK{cik}"   # ba_id als generisches ID-Feld wiederverwenden

        # 2. Submissions — HQ, SIC, legal form
        subs: dict = _fetch_submissions(cik) if cik else {}
        if subs:
            # HQ aus Business-Adresse
            addr = (subs.get("addresses") or {}).get("business", {})
            city  = addr.get("city", "")
            state = addr.get("stateOrCountry", "")
            if city and state:
                sc.hq = f"{city}, {state}"
            elif state:
                sc.hq = state

            sc.legal_form = subs.get("entityType")   # "Limited Liability Company" etc.

            # SIC → industry als Tag (kein direktes Feld — managing_directors zweckentfremden wäre falsch)
            sic_desc = subs.get("sicDescription", "")
            if sic_desc:
                # SIC Description als erstes managing_director-Feld mit Marker speichern
                sc.managing_directors = [{"name": sic_desc, "role": "sic_industry"}]

        time.sleep(0.2)   # EDGAR Rate-Limit respektieren

        # 3. Form D Details — Offering Amount + Date
        form_d = _fetch_form_d_details(sc.name)
        if form_d:
            offering = form_d.get("offering_amount")
            if offering:
                try:
                    # Offering Amount in USD → als equity_eur_mn (USD≈EUR als Proxy)
                    sc.equity_eur_mn = float(offering) / 1_000_000
                except (ValueError, TypeError):
                    pass

            # Date of first sale → Gründungsjahr-Proxy
            date_str = form_d.get("date_of_first_sale") or ""
            if date_str and len(date_str) >= 4:
                try:
                    sc.founded_year = int(date_str[:4])
                except ValueError:
                    pass

        sc.enrichment_status = "done"
        sc.enriched_at       = datetime.now(timezone.utc)
        db.commit()

        logger.info(
            "enrich_one_shadow_edgar OK: '%s' CIK=%s hq=%s equity=%.1fM",
            sc.name,
            cik or "—",
            sc.hq or "—",
            sc.equity_eur_mn or 0.0,
        )
        return True

    except Exception as e:
        logger.warning("enrich_one_shadow_edgar FAILED für '%s': %s", sc.name, e)
        sc.enrichment_status = "error"
        db.commit()
        return False
