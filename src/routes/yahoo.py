"""
YH-05 · yahoo.py
Pfad: argo-analytics-bridge/src/routes/yahoo.py

BA-Bridge REST Endpoint — Beta-Cache Abfrage.
Intern only (X-API-Key Guard via Middleware).

Endpoints:
    GET /yahoo/ticker/{ticker}
        → beta_cache Eintrag für einen Ticker
        → 200 OK (Cache-Hit) | 404 Not Found

    GET /yahoo/ticker/{ticker}/damodaran?category={argo_category}
        → Branchen-Beta aus damodaran_beta für eine Argo-Kategorie
        → 200 OK | 404 Not Found

    GET /yahoo/ownership/{ticker}
        → Institutionelle Anteilseigner via yfinance (EN-08 Fallback)
        → 200 OK (leere holders-Liste wenn keine Daten)

Entfernt (Session 22):
    GET /yahoo/fundamentals/{ticker}
        → Verschoben in Argo Backend (company_detail._fetch_yf_fundamentals).
           Bridge-Hop war unnötiger Overhead — kein Caching-Benefit für on-demand Fundamentals.
"""

import logging
from datetime import timezone

import yfinance as yf

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.database import get_db
from src.models import BetaCache, DamodaranBeta

log = logging.getLogger(__name__)

router = APIRouter(prefix="/yahoo", tags=["yahoo"])

# In-Flight-Guard (BETA-REVIEW-01): verhindert dass mehrere Cold-Loads denselben
# yfinance-Download parallel starten. Prozess-weites Set — analog zum _fetching-Set
# der alten company.py (BA). Kein Lock nötig: GIL macht set.add/discard atomar genug
# für diesen Zweck (Doppel-Trigger-Schutz, kein harter Mutex-Bedarf).
_enriching: set[str] = set()


class BetaEnrichRequest(BaseModel):
    """
    Payload für POST /yahoo/ticker/enrich.

    Das Backend schickt den FERTIG aufgelösten yf-Ticker — die Bridge leitet
    nichts ab (kein OpenFIGI-Wissen in der Bridge). exchange nur für Benchmark.
    """
    yf_ticker:  str            # z.B. "BAYN.DE" — Speicher-Key, as-is verwendet
    exchange:   str = ""       # z.B. "Frankfurt" — nur für Benchmark-Auswahl
    raw_ticker: str = ""       # z.B. "BAYN" — nur fürs Logging


# ---------------------------------------------------------------------------
# Response-Schemas (inline, kein separates schemas.py nötig)
# ---------------------------------------------------------------------------

def _beta_cache_to_dict(entry: BetaCache) -> dict:
    calculated_at = entry.calculated_at
    if calculated_at and calculated_at.tzinfo is None:
        # Naive datetime → UTC annehmen
        calculated_at = calculated_at.replace(tzinfo=timezone.utc)

    return {
        "ticker":                entry.ticker,
        "exchange":              entry.exchange,
        "beta_1y":               float(entry.beta_1y)        if entry.beta_1y        is not None else None,
        "beta_3y":               float(entry.beta_3y)        if entry.beta_3y        is not None else None,
        "volatility_30d":        float(entry.volatility_30d) if entry.volatility_30d is not None else None,
        "benchmark_ticker":      entry.benchmark_ticker,
        "benchmark_is_fallback": entry.benchmark_is_fallback,
        "trading_days_1y":       entry.trading_days_1y,
        "trading_days_3y":       entry.trading_days_3y,
        "data_quality":          entry.data_quality,
        "calculated_at":         calculated_at.isoformat() if calculated_at else None,
        "source":                entry.source,
    }


def _damodaran_to_dict(entry: DamodaranBeta) -> dict:
    return {
        "sector":          entry.sector,
        "argo_category":   entry.argo_category,
        "unlevered_beta":  float(entry.unlevered_beta),
        "levered_beta":    float(entry.levered_beta)  if entry.levered_beta is not None else None,
        "d_e_ratio":       float(entry.d_e_ratio)     if entry.d_e_ratio    is not None else None,
        "updated_year":    entry.updated_year,
        "source_url":      entry.source_url,
    }


# ---------------------------------------------------------------------------
# POST /yahoo/ticker/enrich
# Ad-hoc Beta-Enrichment (BETA-REVIEW-01)
# WICHTIG: muss VOR GET /ticker/{ticker} stehen — sonst fängt der Pfad-Parameter
#          {ticker} das Wort "enrich" ab (FastAPI matcht in Definitionsreihenfolge).
# ---------------------------------------------------------------------------

@router.post("/ticker/enrich", status_code=202)
def trigger_beta_enrich(
    payload: BetaEnrichRequest,
    background_tasks: BackgroundTasks,
):
    """
    Ad-hoc Beta-Enrichment für einen frisch aufgelösten Ticker.

    Wird vom Argo-Backend beim Cold-Load eines listed Company aufgerufen, sobald
    der Ticker via OpenFIGI feststeht und beta_cache noch leer ist. Startet den
    teuren yfinance-Pfad (~5-6s) als BackgroundTask in der Bridge → Backend wird
    NICHT blockiert (Fire-and-Forget). Frontend pollt GET /yahoo/ticker/{yf_ticker}.

    Returns 202 Accepted sofort. In-Flight-Guard verhindert Doppel-Downloads bei
    parallelen Cold-Loads desselben Tickers.
    """
    yf_ticker = payload.yf_ticker.strip().upper()
    if not yf_ticker:
        raise HTTPException(status_code=422, detail="yf_ticker darf nicht leer sein.")

    if yf_ticker in _enriching:
        log.info("[POST /yahoo/ticker/enrich] '%s' bereits in-flight — 202 ohne neuen Task", yf_ticker)
        return {"status": "already_running", "yf_ticker": yf_ticker}

    def _run_enrich():
        from src.price_fetcher import enrich_one_adhoc
        try:
            enrich_one_adhoc(
                yf_ticker=yf_ticker,
                exchange=payload.exchange,
                raw_ticker=payload.raw_ticker,
            )
        except Exception as e:
            log.error("[enrich BG] '%s' failed: %s", yf_ticker, e)
        finally:
            _enriching.discard(yf_ticker)

    _enriching.add(yf_ticker)
    background_tasks.add_task(_run_enrich)
    log.info("[POST /yahoo/ticker/enrich] '%s' getriggert (exchange=%s)", yf_ticker, payload.exchange or "?")
    return {"status": "triggered", "yf_ticker": yf_ticker}


# ---------------------------------------------------------------------------
# GET /yahoo/ticker/{ticker}
# ---------------------------------------------------------------------------

@router.get("/ticker/{ticker}")
def get_beta_cache(ticker: str, db: Session = Depends(get_db)):
    """
    Gibt gecachte Beta-Kennzahlen für einen Ticker zurück.

    Response-Felder:
        ticker, exchange
        beta_1y, beta_3y, volatility_30d
        benchmark_ticker, benchmark_is_fallback   ← für Frontend-Tooltip
        trading_days_1y, trading_days_3y
        data_quality                              ← 'full' | 'partial'
        calculated_at                             ← ISO 8601 UTC
        source                                    ← 'yfinance'

    HTTP-Status:
        200 — Cache-Hit
        404 — Ticker nicht in beta_cache (noch nicht berechnet)
    """
    ticker = ticker.strip().upper()
    entry  = db.query(BetaCache).filter_by(ticker=ticker).first()

    if not entry:
        log.info(f"[GET /yahoo/ticker/{ticker}] 404 — nicht in beta_cache.")
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker}' nicht in beta_cache. Cron läuft täglich ~22:00 UTC.",
        )

    log.info(f"[GET /yahoo/ticker/{ticker}] 200 — beta_1y={entry.beta_1y}")
    return _beta_cache_to_dict(entry)


# ---------------------------------------------------------------------------
# GET /yahoo/ticker/{ticker}/damodaran?category={argo_category}
# ---------------------------------------------------------------------------

@router.get("/ticker/{ticker}/damodaran")
def get_damodaran_beta(
    ticker: str,
    category: str,
    db: Session = Depends(get_db),
):
    """
    Gibt Branchen-Beta aus damodaran_beta für eine Argo-Kategorie zurück.
    Wird von Argo für Private Companies verwendet (kein Börsenkurs).

    Query-Parameter:
        category — Argo-Kategorie (z.B. 'Geothermal / EGS')

    HTTP-Status:
        200 — Treffer
        404 — Kategorie nicht in damodaran_beta gemappt
    """
    # Suche über argo_category (enthält kommagetrennte Argo-Kategorien)
    # ILIKE für case-insensitive Substring-Match
    entry = (
        db.query(DamodaranBeta)
        .filter(DamodaranBeta.argo_category.ilike(f"%{category}%"))
        .first()
    )

    if not entry:
        log.info(f"[GET /yahoo/damodaran] 404 — Kategorie '{category}' kein Mapping.")
        raise HTTPException(
            status_code=404,
            detail=(
                f"Keine Damodaran-Beta für Argo-Kategorie '{category}'. "
                f"Mapping in src/damodaran_importer.py ergänzen."
            ),
        )

    log.info(
        f"[GET /yahoo/damodaran] 200 — "
        f"sector={entry.sector} unlevered_beta={entry.unlevered_beta}"
    )
    return _damodaran_to_dict(entry)



# ---------------------------------------------------------------------------
# GET /yahoo/ownership/{ticker}
# EN-08 · Institutional Holders via yfinance — Fallback für listed Companies
# Wird vom Argo Backend aufgerufen wenn BaFin/EDGAR keinen Treffer liefern.
# ---------------------------------------------------------------------------

@router.get("/ownership/{ticker}")
def get_ownership(ticker: str):
    """
    EN-08: Gibt institutionelle Anteilseigner für einen Ticker via yfinance zurück.
    Fallback-Quelle für listed Companies (nach BaFin DE + EDGAR SC 13G/13D).

    Response-Felder:
        ticker          — normalisierter Ticker
        holders         — Liste mit name, share_pct, shares, value_usd, date_reported, type
        source          — "yfinance_institutional_holders"
        holder_count    — Anzahl Einträge

    HTTP-Status:
        200 — immer (leere holders-Liste wenn keine Daten verfügbar)
        422 — Ticker leer
    """
    ticker = ticker.strip().upper()
    if not ticker:
        raise HTTPException(status_code=422, detail="Ticker darf nicht leer sein.")

    holders: list[dict] = []
    try:
        yf_ticker   = yf.Ticker(ticker)
        inst        = yf_ticker.institutional_holders   # DataFrame oder None

        if inst is not None and not inst.empty:
            for _, row in inst.iterrows():
                name = str(row.get("Holder") or "").strip()
                if not name:
                    continue
                pct_raw = row.get("% Out")
                try:
                    share_pct = round(float(pct_raw) * 100, 2) if pct_raw is not None else None
                except (ValueError, TypeError):
                    share_pct = None

                shares_raw = row.get("Shares")
                try:
                    shares = int(shares_raw) if shares_raw is not None else None
                except (ValueError, TypeError):
                    shares = None

                value_raw = row.get("Value")
                try:
                    value_usd = int(value_raw) if value_raw is not None else None
                except (ValueError, TypeError):
                    value_usd = None

                date_raw = row.get("Date Reported")
                date_str = str(date_raw)[:10] if date_raw is not None else None

                holders.append({
                    "name":          name,
                    "share_pct":     share_pct,
                    "shares":        shares,
                    "value_usd":     value_usd,
                    "date_reported": date_str,
                    "type":          "institutional",
                    "role":          "shareholder",
                })

        log.info(
            "[GET /yahoo/ownership/%s] OK — %d institutional holders",
            ticker, len(holders),
        )
    except Exception as e:
        log.warning("[GET /yahoo/ownership/%s] yfinance error: %s", ticker, e)

    return {
        "ticker":       ticker,
        "holders":      holders,
        "holder_count": len(holders),
        "source":       "yfinance_institutional_holders",
    }
