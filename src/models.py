"""
Bridge Models — BA-Bridge
==========================
YH-Tabellen (Yahoo History):
  beta_cache      — Gecachte Beta-Kennzahlen je Ticker (YH-01/YH-03)
  damodaran_beta  — Branchen-Beta für Private Companies, NYU Damodaran (YH-01/YH-04)

BA-Tabellen (ba_reports, ba_financials, ba_persons) entfernt — S47.
Bundesanzeiger seit 2022 strukturell leer (Daten im Unternehmensregister).
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Text, Float, Integer, Boolean, Numeric,
    DateTime, SmallInteger,
)
from src.database import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── beta_cache ────────────────────────────────────────────────────────────────

class BetaCache(Base):
    """
    Gecachte Beta-Kennzahlen je börsennotiertem Ticker.
    Befüllt durch src/beta_calculator.py via src/price_fetcher.py (täglich ~22:00 UTC).
    Kein price_history in Phase 1 — nur Ergebnis-Cache.
    """
    __tablename__ = "beta_cache"

    id                      = Column(Integer, primary_key=True, autoincrement=True)
    ticker                  = Column(String(20), nullable=False, unique=True, index=True)
    exchange                = Column(String(50))                    # 'NYSE', 'Nasdaq', 'Frankfurt', …

    # Beta
    beta_1y                 = Column(Numeric(8, 4))                 # Beta über 252 Handelstage
    beta_3y                 = Column(Numeric(8, 4))                 # Beta über 756 Handelstage (None wenn < 3J)

    # Volatilität annualisiert: std(daily_returns_30d) × sqrt(252)
    volatility_30d          = Column(Numeric(8, 4))

    # Benchmark
    benchmark_ticker        = Column(String(20), nullable=False)    # '^GSPC', '^GDAXI', …
    benchmark_is_fallback   = Column(Boolean, nullable=False, default=False)
    # True  → kein lokaler Index verfügbar → S&P 500 als Fallback
    # False → lokaler Benchmark (DAX, FTSE, …)

    # Datenqualität
    trading_days_1y         = Column(Integer)                       # tatsächlich verfügbare Handelstage (1Y)
    trading_days_3y         = Column(Integer)                       # tatsächlich verfügbare Handelstage (3Y)
    data_quality            = Column(String(10), default="full")
    # full    = >= 200 Handelstage (1Y) — Beta aussagekräftig
    # partial = < 200 Handelstage     — junges Listing, Beta mit Vorsicht

    # Metadaten
    calculated_at           = Column(DateTime, default=_now)
    source                  = Column(String(20), nullable=False, default="yfinance")


# ── damodaran_beta ────────────────────────────────────────────────────────────

class DamodaranBeta(Base):
    """
    Branchen-Beta für Private Companies (kein Börsenkurs verfügbar).
    Quelle: NYU Damodaran (kostenlos, jährlich aktualisiert).
    Befüllt durch src/damodaran_importer.py (1× jährlich, Januar).
    Mapping: Argo-Kategorie → Damodaran-Sektor in damodaran_importer.py.
    Industriestandard bei VC/PE/M&A für Private-Company-Bewertung.
    """
    __tablename__ = "damodaran_beta"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    sector          = Column(String(255), nullable=False, unique=True)  # Damodaran-Sektorname (Original)
    argo_category   = Column(String(255), index=True)                   # Argo-Kategorie-Mapping

    # Beta-Werte (aus Damodaran-Excel, Blatt "betas")
    unlevered_beta  = Column(Numeric(8, 4), nullable=False)             # Asset Beta (ohne Leverage)
    levered_beta    = Column(Numeric(8, 4))                             # Equity Beta (mit Branchen-D/E)
    d_e_ratio       = Column(Numeric(8, 4))                             # Ø D/E-Ratio der Branche

    # Metadaten
    updated_year    = Column(SmallInteger, nullable=False)              # Jahr der Damodaran-Publikation
    source_url      = Column(Text)                                      # URL zur Damodaran-Excel
    imported_at     = Column(DateTime, default=_now)
