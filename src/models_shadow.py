"""
Shadow Company Model — BA-Bridge
==================================
SQLAlchemy-Modell für shadow_companies Tabelle.
Lebt in der BA-Bridge Postgres — vollständig getrennt von Supabase.

Zweck: proaktives BA-Enrichment bevor ein User die Company sucht.
       Bei One-Click → Sofort-Promote in Supabase statt Blank-Entry + Warten.
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, JSON, DateTime

from src.database import Base


class ShadowCompany(Base):
    __tablename__ = "shadow_companies"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    name                = Column(String, nullable=False, unique=True, index=True)
    ba_id               = Column(String, nullable=True)   # report_id aus ba_reports
    handelsregister_nr  = Column(String, nullable=True)
    source              = Column(String, default="bundesanzeiger")

    # ── BA Nutzdaten (aus ba_financials + ba_persons) ─────────────────────────
    legal_form          = Column(String, nullable=True)
    hq                  = Column(String, nullable=True)
    founded_year        = Column(Integer, nullable=True)
    headcount           = Column(Integer, nullable=True)
    fiscal_year         = Column(Integer, nullable=True)   # Jahresabschluss-Jahr
    revenue_eur_mn      = Column(Float, nullable=True)
    ebitda_eur_mn       = Column(Float, nullable=True)
    ebit_eur_mn         = Column(Float, nullable=True)
    net_income_eur_mn   = Column(Float, nullable=True)
    equity_eur_mn       = Column(Float, nullable=True)
    total_assets_eur_mn = Column(Float, nullable=True)
    shareholders        = Column(JSON, nullable=True)      # [{name, share_pct, is_company}]
    managing_directors  = Column(JSON, nullable=True)      # [{name, role}]

    # ── Queue-Steuerung ───────────────────────────────────────────────────────
    prio_score          = Column(Float, default=10.0, index=True)
    # pending | running | done | error
    enrichment_status   = Column(String, default="pending", index=True)
    enriched_at         = Column(DateTime(timezone=True), nullable=True)
    promoted_at         = Column(DateTime(timezone=True), nullable=True)  # gesetzt wenn in Supabase übernommen
    created_at          = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
