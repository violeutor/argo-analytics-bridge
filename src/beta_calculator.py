"""
YH-03 · beta_calculator.py
Pfad: argo-analytics-bridge/src/beta_calculator.py

BA-Bridge — Beta + Volatilitäts-Berechnung

Berechnet aus yfinance-Rohdaten (OHLCV DataFrames):
    - Beta 1Y  = Cov(r_stock, r_market) / Var(r_market) über 252 Handelstage
    - Beta 3Y  = Cov(r_stock, r_market) / Var(r_market) über 756 Handelstage
    - Volatilität 30d = std(daily_returns_30d) × sqrt(252) annualisiert

Kein price_history in Phase 1 — Rohdaten kommen direkt aus yfinance,
nur das berechnete Ergebnis landet in beta_cache.

Aufgerufen von src/price_fetcher.py · compute_beta_metrics().
"""

import logging
import math

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Mindest-Datenpunkte für eine valide Beta-Berechnung
MIN_DAYS_1Y = 60    # Untergrenze — weniger → None
MIN_DAYS_3Y = 200   # Untergrenze für 3Y-Beta


def _daily_returns(df: pd.DataFrame) -> pd.Series:
    """
    Berechnet tägliche Log-Returns aus Adj. Close.
    Log-Returns: r_t = ln(P_t / P_t-1)
    Robuster als einfache Returns bei Splits/Dividenden.
    """
    close = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
    # yfinance multi-level columns workaround
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close = close.dropna()
    return np.log(close / close.shift(1)).dropna()


def _align_returns(
    stock_returns: pd.Series,
    benchmark_returns: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """
    Aligned beide Return-Serien auf gemeinsame Handelstage.
    Wichtig: Stock und Benchmark haben unterschiedliche Börsenfeiertage.
    """
    aligned = pd.DataFrame({
        "stock":     stock_returns,
        "benchmark": benchmark_returns,
    }).dropna()
    return aligned["stock"], aligned["benchmark"]


def _compute_beta(
    stock_r: pd.Series,
    benchmark_r: pd.Series,
) -> float | None:
    """
    β = Cov(r_stock, r_market) / Var(r_market)
    Gibt None zurück wenn Var(r_market) ≈ 0 (degenerierter Fall).
    """
    var_market = benchmark_r.var()
    if var_market < 1e-10:
        log.warning("Var(r_market) ≈ 0 — Beta nicht berechenbar.")
        return None
    cov  = np.cov(stock_r, benchmark_r, ddof=1)[0][1]
    beta = cov / var_market
    # Plausibilitäts-Check: Beta außerhalb [-5, 10] ist ein Datenproblem
    if not (-5.0 <= beta <= 10.0):
        log.warning(f"Beta={beta:.3f} außerhalb plausiblem Bereich — verwerfen.")
        return None
    return round(float(beta), 4)


def _compute_volatility_30d(stock_returns: pd.Series) -> float | None:
    """
    Annualisierte Volatilität der letzten 30 Handelstage.
    vol = std(r_30d) × sqrt(252)
    """
    recent = stock_returns.tail(30)
    if len(recent) < 15:
        return None
    vol = recent.std(ddof=1) * math.sqrt(252)
    return round(float(vol), 4)


def compute_beta_metrics(
    stock_data: pd.DataFrame,
    benchmark_data: pd.DataFrame,
    ticker: str = "",
) -> dict | None:
    """
    Haupt-Funktion — aufgerufen von src/price_fetcher.py.

    Returns dict:
        beta_1y           float
        beta_3y           float | None   (None bei jungem Listing < 3J)
        volatility_30d    float | None
        trading_days_1y   int
        trading_days_3y   int | None

    Returns None wenn Mindest-Datenpunkte nicht erreicht.
    """
    stock_r     = _daily_returns(stock_data)
    benchmark_r = _daily_returns(benchmark_data)

    if stock_r.empty or benchmark_r.empty:
        log.warning(f"[{ticker}] Leere Return-Serie — übersprungen.")
        return None

    # --- 1Y Beta (252 Handelstage) ---
    stock_1y     = stock_r.tail(252)
    benchmark_1y = benchmark_r.tail(252)
    s1, b1       = _align_returns(stock_1y, benchmark_1y)

    if len(s1) < MIN_DAYS_1Y:
        log.warning(
            f"[{ticker}] Nur {len(s1)} aligned Tage (1Y) — "
            f"Minimum {MIN_DAYS_1Y} — übersprungen."
        )
        return None

    beta_1y = _compute_beta(s1, b1)
    if beta_1y is None:
        return None

    # --- 3Y Beta (756 Handelstage) ---
    beta_3y         = None
    trading_days_3y = None
    s3, b3          = _align_returns(stock_r.tail(756), benchmark_r.tail(756))

    if len(s3) >= MIN_DAYS_3Y:
        beta_3y         = _compute_beta(s3, b3)
        trading_days_3y = len(s3)
    else:
        log.info(
            f"[{ticker}] Nur {len(s3)} aligned Tage (3Y) — "
            f"beta_3y=None (junges Listing)."
        )

    # --- Volatilität 30d ---
    volatility_30d = _compute_volatility_30d(stock_r)

    result = {
        "beta_1y":         beta_1y,
        "beta_3y":         beta_3y,
        "volatility_30d":  volatility_30d,
        "trading_days_1y": len(s1),
        "trading_days_3y": trading_days_3y,
    }

    log.debug(
        f"[{ticker}] beta_1y={beta_1y} beta_3y={beta_3y} "
        f"vol_30d={volatility_30d} days_1y={len(s1)} days_3y={trading_days_3y}"
    )

    return result
