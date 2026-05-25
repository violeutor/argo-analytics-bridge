"""
Shadow Routes — BA-Bridge
===========================
Endpunkte für Shadow-Company Lookup + Queue-Status.

GET /shadow/company/{name}   — Fuzzy-Lookup, gibt Shadow-Daten zurück
GET /shadow/queue/stats      — Queue-Übersicht (intern/debug)

Wird vom Argo Backend bei One-Click aufgerufen (vor Blank-Entry-Anlage).
"""
import logging

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.database import SessionLocal
from src.models_shadow import ShadowCompany

router = APIRouter(prefix="/shadow", tags=["shadow"])
logger = logging.getLogger(__name__)


def _get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/company/{name}")
def lookup_shadow_company(name: str, db: Session = Depends(_get_db)):
    """
    Fuzzy-Lookup in shadow_companies nach Company-Name.
    Priorisiert status='done' wenn mehrere Treffer.

    Returns:
      status='not_found'  → kein Treffer
      status='pending'    → in Queue aber noch nicht angereichert
      status='done'       → fertig angereichert, Daten vorhanden
    """
    name_q = name.lower().replace("-", " ").replace("_", " ")

    # Exakter Match zuerst, dann contains
    sc: ShadowCompany | None = (
        db.query(ShadowCompany)
        .filter(
            func.lower(ShadowCompany.name).contains(name_q)
        )
        # Exakter Match zuerst, dann done vor pending, dann prio_score
        .order_by(
            (func.lower(ShadowCompany.name) == name_q).desc(),
            (ShadowCompany.enrichment_status == "done").desc(),
            ShadowCompany.prio_score.desc(),
        )
        .first()
    )

    if not sc:
        return {"status": "not_found", "name": name}

    return {
        "status":              sc.enrichment_status,
        "name":                sc.name,
        "ba_id":               sc.ba_id,
        "handelsregister_nr":  sc.handelsregister_nr,
        "legal_form":          sc.legal_form,
        "hq":                  sc.hq,
        "founded_year":        sc.founded_year,
        "headcount":           sc.headcount,
        "fiscal_year":         sc.fiscal_year,
        "revenue_eur_mn":      sc.revenue_eur_mn,
        "ebitda_eur_mn":       sc.ebitda_eur_mn,
        "ebit_eur_mn":         sc.ebit_eur_mn,
        "net_income_eur_mn":   sc.net_income_eur_mn,
        "equity_eur_mn":       sc.equity_eur_mn,
        "total_assets_eur_mn": sc.total_assets_eur_mn,
        "shareholders":        sc.shareholders or [],
        "managing_directors":  sc.managing_directors or [],
        "enriched_at":         sc.enriched_at.isoformat() if sc.enriched_at else None,
        "prio_score":          sc.prio_score,
    }


@router.get("/queue/stats")
def shadow_queue_stats(db: Session = Depends(_get_db)):
    """Queue-Status — intern/debug. Zeigt Verteilung der enrichment_status."""
    stats = (
        db.query(ShadowCompany.enrichment_status, func.count(ShadowCompany.id))
        .group_by(ShadowCompany.enrichment_status)
        .all()
    )
    breakdown = {status: count for status, count in stats}
    return {
        "total":     sum(breakdown.values()),
        "breakdown": breakdown,
    }
