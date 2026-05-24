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
"""

import logging
from datetime import timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.database import get_db
from src.models import BetaCache, DamodaranBeta

log = logging.getLogger(__name__)

router = APIRouter(prefix="/yahoo", tags=["yahoo"])


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
# GET /yahoo/fundamentals/{ticker}
# YH-08 · Fundamentals via yfinance — Revenue, EBITDA, Margen, EV etc.
# Wird von Argo Backend als Ersatz für quoteSummary genutzt (Free-Tier-kompatibel).
# ---------------------------------------------------------------------------

import yfinance as yf  # bereits in requirements.txt der Bridge


@router.get("/fundamentals/{ticker}")
def get_fundamentals(ticker: str):
    """
    Liefert Fundamentaldaten für einen Ticker via yfinance.
    Kein DB-Cache — on-demand, Timeout 10s.

    Response-Felder (alle optional / None wenn nicht verfügbar):
        ticker, pe_ratio, week_52_high, week_52_low
        revenue_bn, ebitda_bn, debt_ebitda
        gross_margin_pct, operating_margin_pct, profit_margin_pct
        revenue_growth_pct, earnings_growth_pct
        free_cashflow_bn, operating_cashflow_bn
        enterprise_value_bn, ev_revenue, ev_ebitda

    HTTP-Status:
        200 — immer (leere Felder = None, kein Hard-Fail)
        422 — Ticker leer
    """
    ticker = ticker.strip().upper()
    if not ticker:
        raise HTTPException(status_code=422, detail="Ticker darf nicht leer sein.")

    out: dict = {"ticker": ticker}
    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info or {}

        def _pct(v) -> float | None:
            return round(v * 100, 2) if v is not None else None

        def _bn(v) -> float | None:
            return round(v / 1e9, 3) if v else None

        out["pe_ratio"]             = info.get("trailingPE")
        out["week_52_high"]         = info.get("fiftyTwoWeekHigh")
        out["week_52_low"]          = info.get("fiftyTwoWeekLow")
        out["revenue_bn"]           = _bn(info.get("totalRevenue"))
        out["ebitda_bn"]            = _bn(info.get("ebitda"))
        out["gross_margin_pct"]     = _pct(info.get("grossMargins"))
        out["operating_margin_pct"] = _pct(info.get("operatingMargins"))
        out["profit_margin_pct"]    = _pct(info.get("profitMargins"))
        out["revenue_growth_pct"]   = _pct(info.get("revenueGrowth"))
        out["earnings_growth_pct"]  = _pct(info.get("earningsGrowth"))
        out["free_cashflow_bn"]     = _bn(info.get("freeCashflow"))
        out["operating_cashflow_bn"]= _bn(info.get("operatingCashflow"))

        ev = info.get("enterpriseValue")
        out["enterprise_value_bn"]  = _bn(ev)

        rev = info.get("totalRevenue")
        ebitda = info.get("ebitda")
        debt = info.get("totalDebt")
        if ebitda and debt:
            out["debt_ebitda"] = round((debt / 1e9) / (ebitda / 1e9), 2)
        if ev and rev:
            out["ev_revenue"] = round(ev / rev, 1)
        if ev and ebitda:
            out["ev_ebitda"]  = round(ev / ebitda, 1)

        log.info(
            "[GET /yahoo/fundamentals/%s] OK — rev=%.1fBn margin=%.1f%%",
            ticker,
            out.get("revenue_bn") or 0,
            out.get("gross_margin_pct") or 0,
        )
    except Exception as e:
        log.warning("[GET /yahoo/fundamentals/%s] yfinance error: %s", ticker, e)

    return out


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
