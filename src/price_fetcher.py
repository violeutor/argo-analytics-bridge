"""
YH-02 · price_fetcher.py
Pfad: argo-analytics-bridge/src/price_fetcher.py

BA-Bridge — Yahoo History Cron

Täglich ~22:00 UTC (nach Börsenschluss US).
Holt für jeden is_listed Ticker aus Argo-Supabase die letzten 3 Jahre
Kursdaten via yfinance, berechnet Beta + Volatilität direkt (kein
price_history in Phase 1), schreibt Ergebnis in beta_cache.

Aufruf:
    python -m src.price_fetcher              # alle Ticker
    python -m src.price_fetcher LNZA H2O    # einzelne Ticker (Debug)
"""

import sys
import time
import logging
from datetime import datetime, timezone

import yfinance as yf

from src.database import SessionLocal
from src.models import BetaCache
from src.beta_calculator import compute_beta_metrics
from src.config import settings

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Benchmark-Mapping (Exchange → Index-Ticker)
# Quelle: yfinance — alle Indizes zuverlässig verfügbar.
# ---------------------------------------------------------------------------
BENCHMARK_MAP: dict[str, str] = {
    # USA
    "NYSE":       "^GSPC",       # S&P 500
    "Nasdaq":     "^GSPC",       # S&P 500
    # Deutschland
    "Frankfurt":  "^GDAXI",      # DAX 40
    # UK
    "London":     "^FTSE",       # FTSE 100
    # Frankreich
    "Euronext":   "^FCHI",       # CAC 40
    # Italien
    "Milan":      "FTSEMIB.MI",  # FTSE MIB
    # Schweiz
    "Swiss":      "^SSMI",       # SMI
    # Niederlande
    "Amsterdam":  "AEX",         # AEX
    # Schweden
    "Stockholm":  "^OMX",        # OMX Stockholm 30
}
BENCHMARK_FALLBACK = "^GSPC"

# Pause zwischen Ticker-Requests (yfinance Rate-Limit)
RATE_LIMIT_SEC = 2.0

# Mindest-Handelstage für "full" Qualität (1Y)
MIN_TRADING_DAYS_FULL = 200


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def resolve_benchmark(exchange: str | None) -> tuple[str, bool]:
    """
    Gibt (benchmark_ticker, is_fallback) zurück.
    is_fallback=True wenn kein lokaler Index → S&P 500.
    """
    if exchange and exchange in BENCHMARK_MAP:
        return BENCHMARK_MAP[exchange], False
    return BENCHMARK_FALLBACK, True


def fetch_tickers_from_argo() -> list[dict]:
    """
    Liest alle is_listed Companies mit Ticker + Exchange aus Argo-Backend.
    """
    import httpx

    try:
        resp = httpx.get(
            f"{settings.argo_backend_url}/api/v1/companies",
            headers={"X-API-Key": settings.argo_api_key},
            timeout=15,
        )
        resp.raise_for_status()
        companies = resp.json()
    except Exception as e:
        log.error(f"Argo-Companies fetch fehlgeschlagen: {e}")
        return []

    result = []
    for c in companies:
        if c.get("is_listed") and c.get("ticker"):
            result.append({
                "ticker":   c["ticker"].strip().upper(),
                "exchange": c.get("exchange") or "",
            })

    log.info(f"{len(result)} listed Ticker aus Argo geladen.")
    return result


def fetch_price_data(ticker: str, period: str = "3y"):
    """
    Holt OHLCV-Daten via yfinance für den angegebenen Zeitraum.
    Gibt None zurück wenn keine Daten verfügbar.
    """
    try:
        data = yf.download(
            ticker,
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
        if data.empty:
            log.warning(f"[{ticker}] Keine yfinance-Daten für period={period}.")
            return None
        return data
    except Exception as e:
        log.error(f"[{ticker}] yfinance Download-Fehler: {e}")
        return None


# ---------------------------------------------------------------------------
# Haupt-Pipeline
# ---------------------------------------------------------------------------

def process_ticker(ticker: str, exchange: str, session) -> bool:
    """
    Verarbeitet einen einzelnen Ticker:
    1. Benchmark bestimmen
    2. Stock + Benchmark Kursdaten holen
    3. Beta + Volatilität berechnen (via beta_calculator)
    4. beta_cache upsert

    Gibt True zurück bei Erfolg.
    """
    benchmark_ticker, is_fallback = resolve_benchmark(exchange)

    log.info(
        f"[{ticker}] Exchange={exchange or '?'} → "
        f"Benchmark={benchmark_ticker}"
        f"{' (fallback)' if is_fallback else ''}"
    )

    # Kursdaten holen
    stock_data = fetch_price_data(ticker, period="3y")
    if stock_data is None:
        return False

    time.sleep(RATE_LIMIT_SEC)

    benchmark_data = fetch_price_data(benchmark_ticker, period="3y")
    if benchmark_data is None:
        log.warning(f"[{ticker}] Benchmark {benchmark_ticker} nicht verfügbar.")
        return False

    time.sleep(RATE_LIMIT_SEC)

    # Beta + Volatilität berechnen
    metrics = compute_beta_metrics(
        stock_data=stock_data,
        benchmark_data=benchmark_data,
        ticker=ticker,
    )
    if metrics is None:
        log.warning(f"[{ticker}] Berechnung fehlgeschlagen — übersprungen.")
        return False

    # Datenqualität bestimmen
    trading_days_1y = metrics["trading_days_1y"]
    data_quality = "full" if trading_days_1y >= MIN_TRADING_DAYS_FULL else "partial"

    if data_quality == "partial":
        log.warning(
            f"[{ticker}] Nur {trading_days_1y} Handelstage (1Y) — data_quality=partial."
        )

    # beta_cache upsert
    try:
        existing = session.query(BetaCache).filter_by(ticker=ticker).first()

        if existing:
            existing.exchange              = exchange
            existing.beta_1y               = metrics["beta_1y"]
            existing.beta_3y               = metrics.get("beta_3y")
            existing.volatility_30d        = metrics["volatility_30d"]
            existing.benchmark_ticker      = benchmark_ticker
            existing.benchmark_is_fallback = is_fallback
            existing.trading_days_1y       = trading_days_1y
            existing.trading_days_3y       = metrics.get("trading_days_3y")
            existing.data_quality          = data_quality
            existing.calculated_at         = datetime.now(timezone.utc)
            existing.source                = "yfinance"
        else:
            entry = BetaCache(
                ticker                = ticker,
                exchange              = exchange,
                beta_1y               = metrics["beta_1y"],
                beta_3y               = metrics.get("beta_3y"),
                volatility_30d        = metrics["volatility_30d"],
                benchmark_ticker      = benchmark_ticker,
                benchmark_is_fallback = is_fallback,
                trading_days_1y       = trading_days_1y,
                trading_days_3y       = metrics.get("trading_days_3y"),
                data_quality          = data_quality,
                calculated_at         = datetime.now(timezone.utc),
                source                = "yfinance",
            )
            session.add(entry)

        session.commit()
        log.info(
            f"[{ticker}] ✓  beta_1y={metrics['beta_1y']:.3f} "
            f"beta_3y={metrics.get('beta_3y', '—')} "
            f"vol_30d={metrics['volatility_30d']:.3f} "
            f"quality={data_quality}"
        )
        return True

    except Exception as e:
        session.rollback()
        log.error(f"[{ticker}] DB-Fehler beim Upsert: {e}")
        return False


def run(tickers_override: list[str] | None = None) -> None:
    """
    Haupt-Einstiegspunkt.
    tickers_override: wenn gesetzt, nur diese Ticker verarbeiten (Debug).
    """
    log.info("=== YH price_fetcher · Start ===")
    session = SessionLocal()

    try:
        if tickers_override:
            all_tickers = fetch_tickers_from_argo()
            ticker_map  = {t["ticker"]: t["exchange"] for t in all_tickers}
            targets = [
                {"ticker": t.upper(), "exchange": ticker_map.get(t.upper(), "")}
                for t in tickers_override
            ]
        else:
            targets = fetch_tickers_from_argo()

        if not targets:
            log.warning("Keine Ticker zu verarbeiten.")
            return

        success = 0
        failed  = 0

        for entry in targets:
            ok = process_ticker(
                ticker=entry["ticker"],
                exchange=entry["exchange"],
                session=session,
            )
            if ok:
                success += 1
            else:
                failed += 1
            time.sleep(RATE_LIMIT_SEC)

        log.info(
            f"=== YH price_fetcher · Fertig · "
            f"{success} OK · {failed} fehlgeschlagen ==="
        )

    finally:
        session.close()


# ---------------------------------------------------------------------------
# CLI-Einstieg
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    override = sys.argv[1:] if len(sys.argv) > 1 else None
    run(tickers_override=override)
