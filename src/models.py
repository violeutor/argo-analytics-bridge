"""
Shadow-DB Models — BA-Bridge
=============================
Drei Tabellen:
  ba_reports    — Rohtexte aus bundesAPI (ein Eintrag pro Jahresabschluss-Dokument)
  ba_financials — Claude-NER-Output: strukturierte Finanzkennzahlen
  ba_persons    — Claude-NER-Output: Gesellschafter + Geschäftsführer
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Text, Float, Integer, Boolean,
    DateTime, ForeignKey, UniqueConstraint,
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
