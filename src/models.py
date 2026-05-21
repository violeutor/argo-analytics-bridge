"""
Shadow-DB Models — BA-Bridge
=============================
BA-Tabellen:
  ba_reports    — Rohtexte aus bundesAPI (ein Eintrag pro Jahresabschluss-Dokument)
  ba_financials — Claude-NER-Output: strukturierte Finanzkennzahlen
  ba_persons    — Claude-NER-Output: Gesellschafter + Geschäftsführer

YH-Tabellen (Yahoo History):
  beta_cache      — Gecachte Beta-Kennzahlen je Ticker (YH-01/YH-03)
  damodaran_beta  — Branchen-Beta für Private Companies, NYU Damodaran (YH-01/YH-04)
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Text, Float, Integer, Boolean, Numeric,
    DateTime, ForeignKey, UniqueConstraint, SmallInteger,
)
from sqlalchemy.orm import relationship
from src.database import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── ba_reports ────────────────────────────────────────────────────────────────

class BAReport(Base):
    __tablename__ = "ba_reports"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    company_name  = Column(String(255), nullable=False, index=True)
    document_date = Column(String(20))          # "2023" oder "2023-12-31"
    document_type = Column(String(100))         # "Jahresabschluss", "Lagebericht", …
    raw_text      = Column(Text)                # Volltext aus bundesAPI
    source_id     = Column(String(100))         # interne ID aus bundesAPI falls vorhanden
    fetched_at    = Column(DateTime, default=_now)
    parse_status  = Column(String(20), default="pending")  # pending | done | error

    financials    = relationship("BAFinancial", back_populates="report", cascade="all, delete-orphan")
    persons       = relationship("BAPerson",    back_populates="report", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("company_name", "document_date", "document_type", name="uq_report"),
    )


# ── ba_financials ─────────────────────────────────────────────────────────────

class BAFinancial(Base):
    __tablename__ = "ba_financials"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    report_id       = Column(Integer, ForeignKey("ba_reports.id", ondelete="CASCADE"), nullable=False)
    company_name    = Column(String(255), nullable=False, index=True)
    fiscal_year     = Column(Integer)
    revenue_eur_mn  = Column(Float)             # Umsatz in EUR Mio
    ebitda_eur_mn   = Column(Float)             # EBITDA in EUR Mio (wenn vorhanden)
    ebit_eur_mn     = Column(Float)             # EBIT
    net_income_eur_mn = Column(Float)           # Jahresüberschuss/-fehlbetrag
    equity_eur_mn   = Column(Float)             # Eigenkapital
    total_assets_eur_mn = Column(Float)         # Bilanzsumme
    headcount       = Column(Integer)           # Mitarbeiteranzahl
    confidence      = Column(String(10))        # high | medium | low
    parsed_at       = Column(DateTime, default=_now)

    report          = relationship("BAReport", back_populates="financials")

    __table_args__ = (
        UniqueConstraint("company_name", "fiscal_year", name="uq_financial"),
    )


# ── ba_persons ────────────────────────────────────────────────────────────────

class BAPerson(Base):
    __tablename__ = "ba_persons"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    report_id     = Column(Integer, ForeignKey("ba_reports.id", ondelete="CASCADE"), nullable=False)
    company_name  = Column(String(255), nullable=False, index=True)
    name          = Column(String(255), nullable=False)
    role          = Column(String(50))          # shareholder | executive | supervisory_board
    share_pct     = Column(Float)               # Anteil in % — null wenn nicht öffentlich
    is_company    = Column(Boolean, default=False)  # True wenn juristische Person
    parsed_at     = Column(DateTime, default=_now)

    report        = relationship("BAReport", back_populates="persons")

    __table_args__ = (
        UniqueConstraint("company_name", "name", "role", name="uq_person"),
    )


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
