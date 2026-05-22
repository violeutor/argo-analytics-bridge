"""
BA Parser — Claude NER → strukturiertes JSON → Shadow-DB
==========================================================
Nimmt Rohtexte aus ba_reports und extrahiert via Claude:
  - Finanzkennzahlen → ba_financials
  - Gesellschafter + Geschäftsführer → ba_persons

Wird aufgerufen:
  - Durch Cron nach fetch_and_store (Batch)
  - On-demand durch /ba/company/{name} wenn Cache leer
"""
import json
import logging
import re

import httpx
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.config import settings
from src.models import BAReport, BAFinancial, BAPerson

logger = logging.getLogger(__name__)

_CLAUDE_URL = "https://api.anthropic.com/v1/messages"
_HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": settings.anthropic_api_key,
    "anthropic-version": "2023-06-01",
}

_PROMPT_TEMPLATE = """Du bist ein Finanzanalyst. Extrahiere strukturierte Daten aus diesem Bundesanzeiger-Jahresabschluss.

Unternehmen: {company_name}
Dokument: {doc_type} ({doc_date})

Text (gekürzt auf 6000 Zeichen):
{text}

Gib NUR valides JSON zurück — keine Präambel, keine Markdown-Backticks.

{{
  "fiscal_year": <Jahreszahl als Integer oder null>,
  "revenue_eur_mn": <Umsatz in EUR Mio als Float oder null>,
  "ebitda_eur_mn": <EBITDA in EUR Mio als Float oder null>,
  "ebit_eur_mn": <EBIT in EUR Mio als Float oder null>,
  "net_income_eur_mn": <Jahresüberschuss/Jahresfehlbetrag in EUR Mio als Float oder null>,
  "equity_eur_mn": <Eigenkapital in EUR Mio als Float oder null>,
  "total_assets_eur_mn": <Bilanzsumme in EUR Mio als Float oder null>,
  "headcount": <Mitarbeiteranzahl als Integer oder null>,
  "confidence": "high|medium|low",
  "shareholders": [
    {{"name": "<Name>", "share_pct": <Float oder null>, "is_company": <true|false>}}
  ],
  "executives": [
    {{"name": "<Name>", "role": "executive|supervisory_board"}}
  ]
}}

Regeln:
- Alle Beträge in EUR Mio (nicht Tsd EUR, nicht EUR) — umrechnen wenn nötig
- Jahresfehlbetrag als negativen Wert
- Fehlende Werte als null — nie erfinden
- confidence: high wenn Jahresabschluss klar lesbar, medium bei Kurzform, low bei Lageberichten
"""


def _call_claude(prompt: str) -> dict | None:
    """Synchroner Claude-Call — gibt geparsten JSON-Dict zurück oder None."""
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                _CLAUDE_URL,
                headers=_HEADERS,
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 1000,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        if resp.status_code != 200:
            logger.warning("Claude NER HTTP %s: %s", resp.status_code, resp.text[:200])
            return None

        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"```(?:json)?|```", "", raw).strip()
        return json.loads(raw)

    except json.JSONDecodeError as e:
        logger.warning("Claude NER JSON-Parse-Fehler: %s", e)
        return None
    except Exception as e:
        logger.warning("Claude NER failed: %s", e)
        return None


def parse_report(report: BAReport, db: Session) -> bool:
    """
    Parst einen BAReport via Claude NER.
    Persistiert Ergebnisse in ba_financials + ba_persons.
    Setzt report.parse_status = done | error.

    Returns: True wenn erfolgreich.
    """
    if not report.raw_text or len(report.raw_text) < 50:
        report.parse_status = "error"
        db.commit()
        return False

    prompt = _PROMPT_TEMPLATE.format(
        company_name=report.company_name,
        doc_type=report.document_type or "Jahresabschluss",
        doc_date=report.document_date or "unbekannt",
        text=report.raw_text[:6000],
    )

    result = _call_claude(prompt)
    if not result:
        report.parse_status = "error"
        db.commit()
        return False

    # ── Finanzkennzahlen speichern ────────────────────────────────────────────
    fiscal_year = result.get("fiscal_year")
    financial_fields = {
        "revenue_eur_mn", "ebitda_eur_mn", "ebit_eur_mn",
        "net_income_eur_mn", "equity_eur_mn", "total_assets_eur_mn", "headcount",
    }
    has_financials = any(result.get(f) is not None for f in financial_fields)

    if has_financials:
        try:
            fin = BAFinancial(
                report_id=report.id,
                company_name=report.company_name,
                fiscal_year=fiscal_year,
                revenue_eur_mn=result.get("revenue_eur_mn"),
                ebitda_eur_mn=result.get("ebitda_eur_mn"),
                ebit_eur_mn=result.get("ebit_eur_mn"),
                net_income_eur_mn=result.get("net_income_eur_mn"),
                equity_eur_mn=result.get("equity_eur_mn"),
                total_assets_eur_mn=result.get("total_assets_eur_mn"),
                headcount=result.get("headcount"),
                confidence=result.get("confidence", "medium"),
            )
            db.add(fin)
            db.flush()
        except IntegrityError:
            db.rollback()
            logger.debug(
                "BAFinancial Duplikat übersprungen: %s FY%s",
                report.company_name, fiscal_year,
            )

    # ── Gesellschafter + Geschäftsführer speichern ────────────────────────────
    for sh in result.get("shareholders", []):
        name = (sh.get("name") or "").strip()
        if not name:
            continue
        try:
            person = BAPerson(
                report_id=report.id,
                company_name=report.company_name,
                name=name,
                role="shareholder",
                share_pct=sh.get("share_pct"),
                is_company=bool(sh.get("is_company", False)),
            )
            db.add(person)
            db.flush()
        except IntegrityError:
            db.rollback()
            logger.debug("BAPerson Duplikat übersprungen: %s / %s", report.company_name, name)

    for ex in result.get("executives", []):
        name = (ex.get("name") or "").strip()
        if not name:
            continue
        try:
            person = BAPerson(
                report_id=report.id,
                company_name=report.company_name,
                name=name,
                role=ex.get("role", "executive"),
                share_pct=None,
                is_company=False,
            )
            db.add(person)
            db.flush()
        except IntegrityError:
            db.rollback()
            logger.debug("BAPerson Exec Duplikat: %s / %s", report.company_name, name)

    # BA-09: extraction_confidence für Logging ableiten (kein DB-Feld)
    guv_count = sum(1 for f in (
        result.get("revenue_eur_mn"), result.get("ebitda_eur_mn"),
        result.get("ebit_eur_mn"), result.get("net_income_eur_mn"),
    ) if f is not None)
    balance_count = sum(1 for f in (
        result.get("equity_eur_mn"), result.get("total_assets_eur_mn"),
    ) if f is not None)
    if guv_count >= 3:       log_confidence = "full"
    elif guv_count >= 1:     log_confidence = "partial"
    elif balance_count >= 1: log_confidence = "balance_only"
    else:                    log_confidence = "not_found"

    report.parse_status = "done"
    db.commit()
    logger.info(
        "parse_report OK: %s — FY%s — extraction_confidence=%s",
        report.company_name, fiscal_year, log_confidence,
    )
    return True


def parse_pending(db: Session, limit: int = 20) -> int:
    """
    Batch-Parser: parst bis zu `limit` pending Reports.
    Wird vom Cron aufgerufen nach fetch_and_store.
    Returns: Anzahl erfolgreich geparster Reports.
    """
    from src.ba_fetcher import get_pending_reports
    pending = get_pending_reports(db)[:limit]
    success = 0
    for report in pending:
        if parse_report(report, db):
            success += 1
    logger.info("parse_pending: %d/%d erfolgreich", success, len(pending))
    return success
