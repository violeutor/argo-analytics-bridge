"""
Route: GET /ba/company/{name}
==============================
Gibt gecachtes JSON aus Shadow-DB zurück.
Intern only — X-API-Key Header required (wenn bridge_api_key gesetzt).

Wenn kein Cache vorhanden:
  → Fetch + Parse on-demand triggern
  → 202 Accepted zurückgeben (polling)

Wenn Cache vorhanden:
  → 200 mit strukturierten Daten
"""
import logging
from fastapi import APIRouter, Depends, Header, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from src.config import settings
from src.database import get_db
from src.models import BAReport, BAFinancial, BAPerson

logger = logging.getLogger(__name__)
router = APIRouter()

# In-Flight-Set — verhindert parallele Fetches für denselben Namen
# Kein CAPTCHA-Risiko durch Concurrency
_fetching: set[str] = set()


def _check_api_key(x_api_key: str = Header(default="")) -> None:
    """API-Key Guard — nur wenn bridge_api_key konfiguriert."""
    if settings.bridge_api_key and x_api_key != settings.bridge_api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _build_response(company_name: str, db: Session) -> dict | None:
    """
    Baut Response-Dict aus Shadow-DB auf.
    Returns None wenn keine Daten vorhanden.
    """
    # Aktuellste Finanzkennzahlen (neuestes fiscal_year)
    fin = (
        db.query(BAFinancial)
        .filter_by(company_name=company_name)
        .order_by(BAFinancial.fiscal_year.desc())
        .first()
    )

    # Alle Personen (Shareholders + Executives)
    persons = (
        db.query(BAPerson)
        .filter_by(company_name=company_name)
        .all()
    )

    if not fin and not persons:
        return None

    shareholders = [
        {
            "name":       p.name,
            "share_pct":  p.share_pct,
            "is_company": p.is_company,
        }
        for p in persons if p.role == "shareholder"
    ]
    executives = [
        {"name": p.name, "role": p.role}
        for p in persons if p.role in ("executive", "supervisory_board")
    ]

    return {
        "company_name": company_name,
        "financials": {
            "fiscal_year":          fin.fiscal_year         if fin else None,
            "revenue_eur_mn":       fin.revenue_eur_mn      if fin else None,
            "ebitda_eur_mn":        fin.ebitda_eur_mn       if fin else None,
            "ebit_eur_mn":          fin.ebit_eur_mn         if fin else None,
            "net_income_eur_mn":    fin.net_income_eur_mn   if fin else None,
            "equity_eur_mn":        fin.equity_eur_mn       if fin else None,
            "total_assets_eur_mn":  fin.total_assets_eur_mn if fin else None,
            "headcount":            fin.headcount           if fin else None,
            "confidence":           fin.confidence          if fin else None,
        } if fin else None,
        "shareholders": shareholders,
        "executives":   executives,
        "cached": True,
    }


def _fetch_and_parse_bg(company_name: str) -> None:
    """Background Task: Fetch + Parse für company_name.
    In-Flight-Guard verhindert parallele Fetches für denselben Namen.
    """
    # Guard bereits im Route-Handler gesetzt — hier nur noch ausführen
    from src.database import SessionLocal
    from src.ba_fetcher import fetch_and_store
    from src.ba_parser import parse_pending

    db = SessionLocal()
    try:
        fetch_and_store(company_name, db)
        parse_pending(db, limit=10)
    except Exception as e:
        logger.error("BG fetch+parse failed für '%s': %s", company_name, e)
    finally:
        _fetching.discard(company_name)
        db.close()


def _push_kpi_to_argo(company_name: str, db: Session) -> int:
    """
    Schreibt alle ba_financials Zeitreihen-Rows für company_name
    in Argo Supabase kpi_timeseries.
    Wird vom Cron nach parse_pending aufgerufen.
    Returns: Anzahl geschriebener Rows.
    """
    if not settings.argo_backend_url or not settings.argo_api_key:
        logger.debug("_push_kpi_to_argo: argo_backend_url/api_key nicht konfiguriert — übersprungen")
        return 0

    fins = (
        db.query(BAFinancial)
        .filter_by(company_name=company_name)
        .order_by(BAFinancial.fiscal_year.asc())
        .all()
    )
    if not fins:
        return 0

    # Metriken die wir pushen — metric_name → BAFinancial-Attribut
    METRIC_MAP = {
        "revenue_eur_mn":       "revenue_eur_mn",
        "ebitda_eur_mn":        "ebitda_eur_mn",
        "ebit_eur_mn":          "ebit_eur_mn",
        "net_income_eur_mn":    "net_income_eur_mn",
        "equity_eur_mn":        "equity_eur_mn",
        "total_assets_eur_mn":  "total_assets_eur_mn",
        "headcount":            "headcount",
    }

    rows = []
    for fin in fins:
        if not fin.fiscal_year:
            continue
        for metric, attr in METRIC_MAP.items():
            val = getattr(fin, attr, None)
            if val is None:
                continue
            rows.append({
                "metric":      metric,
                "fiscal_year": fin.fiscal_year,
                "value":       float(val),
                "currency":    "EUR" if "eur" in metric else None,
                "source":      "ba_bridge",
                "confidence":  fin.confidence or "medium",
            })

    if not rows:
        return 0

    try:
        import httpx
        with httpx.Client(timeout=15) as client:
            resp = client.post(
                f"{settings.argo_backend_url}/api/v1/company/{company_name}/kpi-timeseries",
                headers={"X-API-Key": settings.argo_api_key},
                json={"rows": rows},
            )
        if resp.status_code == 200:
            written = resp.json().get("written", 0)
            logger.info("_push_kpi_to_argo '%s': %d Rows geschrieben", company_name, written)
            return written
        else:
            logger.warning("_push_kpi_to_argo '%s': HTTP %s", company_name, resp.status_code)
    except Exception as e:
        logger.warning("_push_kpi_to_argo failed für '%s': %s", company_name, e)

    return 0


@router.get("/ba/company/{company_name}/history")
def get_company_history(
    company_name: str,
    db: Session = Depends(get_db),
    _: None = Depends(_check_api_key),
):
    """
    GET /ba/company/{company_name}/history
    Gibt alle verfügbaren Jahresabschlüsse als Zeitreihe zurück.
    Sortiert aufsteigend nach fiscal_year.
    """
    fins = (
        db.query(BAFinancial)
        .filter_by(company_name=company_name)
        .order_by(BAFinancial.fiscal_year.asc())
        .all()
    )
    if not fins:
        return {"company_name": company_name, "history": [], "years": 0}

    history = [
        {
            "fiscal_year":         fin.fiscal_year,
            "revenue_eur_mn":      fin.revenue_eur_mn,
            "ebitda_eur_mn":       fin.ebitda_eur_mn,
            "ebit_eur_mn":         fin.ebit_eur_mn,
            "net_income_eur_mn":   fin.net_income_eur_mn,
            "equity_eur_mn":       fin.equity_eur_mn,
            "total_assets_eur_mn": fin.total_assets_eur_mn,
            "headcount":           fin.headcount,
            "confidence":          fin.confidence,
        }
        for fin in fins
    ]

    return {
        "company_name": company_name,
        "history":      history,
        "years":        len(history),
    }


@router.get("/ba/company/{company_name}")
def get_company(
    company_name: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _: None = Depends(_check_api_key),
):
    """
    GET /ba/company/{company_name}

    200 → Daten aus Cache (financials + shareholders + executives)
    202 → Kein Cache, Fetch+Parse wurde getriggert — bitte in 30s wiederholen
    404 → Fetch durchgeführt, aber keine Daten im Bundesanzeiger gefunden
    """
    # 1. Cache-Check
    data = _build_response(company_name, db)
    if data:
        return data

    # 2. Prüfen ob bereits ein pending/done Report existiert
    existing = (
        db.query(BAReport)
        .filter_by(company_name=company_name)
        .first()
    )

    if existing and existing.parse_status == "done":
        # Reports da aber keine Finanzdaten extrahierbar (z.B. nur Lagebericht)
        return {
            "company_name": company_name,
            "financials":   None,
            "shareholders": [],
            "executives":   [],
            "cached":       True,
            "note":         "Keine strukturierten Finanzdaten im Bundesanzeiger verfügbar.",
        }

    # 3. Noch nicht im Cache → Background Fetch triggern
    # Guard synchron im Route-Handler setzen — verhindert Race Condition
    # wenn zwei Requests gleichzeitig ankommen bevor BG-Task startet
    if company_name in _fetching:
        logger.info("BG fetch bereits in Progress für '%s' — 202 ohne neuen Task", company_name)
        return {
            "company_name": company_name,
            "status":       "fetching",
            "message":      "Daten werden bereits abgerufen. Bitte in 30–60 Sekunden erneut anfragen.",
            "cached":       False,
        }, 202

    _fetching.add(company_name)
    background_tasks.add_task(_fetch_and_parse_bg, company_name)
    logger.info("BG fetch getriggert für '%s'", company_name)

    return {
        "company_name": company_name,
        "status":       "fetching",
        "message":      "Daten werden abgerufen. Bitte in 30–60 Sekunden erneut anfragen.",
        "cached":       False,
    }, 202
