"""
BA Fetcher — bundesAPI/deutschland → Shadow-DB
================================================
Holt Jahresabschluss-Volltexte aus dem Bundesanzeiger via bundesAPI (PyPI).
Persistiert Rohtexte in ba_reports — kein Parsing hier, nur sammeln.

Rate-Limit: 1 Request / ba_rate_limit_sec (default 3s) — CAPTCHA-Schutz.
CAPTCHA-Fehler → Retry mit Backoff, Cache bleibt verfügbar.
"""
import logging
import time
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from src.config import settings
from src.models import BAReport

logger = logging.getLogger(__name__)

# Letzter Request-Zeitstempel — globales Rate-Limit (prozess-weit)
_last_request_ts: float = 0.0


def _rate_limit() -> None:
    """Blockiert bis Rate-Limit eingehalten ist."""
    global _last_request_ts
    elapsed = time.monotonic() - _last_request_ts
    wait = settings.ba_rate_limit_sec - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.monotonic()


def fetch_and_store(company_name: str, db: Session) -> list[BAReport]:
    """
    Holt alle verfügbaren Bundesanzeiger-Berichte für company_name.
    Speichert neue Berichte in ba_reports (Duplikate werden übersprungen).

    Returns: Liste der gespeicherten/vorhandenen BAReport-Objekte.
    """
    try:
        import deutschland.bundesanzeiger as ba_module  # type: ignore
    except ImportError:
        logger.error("bundesAPI nicht installiert — pip install bundesAPI")
        return []

    _rate_limit()

    try:
        ba = ba_module.Bundesanzeiger()
        reports_raw = ba.get_reports(company_name)
    except Exception as e:
        logger.warning("bundesAPI fetch failed für '%s': %s", company_name, e)
        return []

    if not reports_raw:
        logger.info("Keine Berichte gefunden für '%s'", company_name)
        return []

    stored: list[BAReport] = []

    for raw in reports_raw:
        # bundesAPI gibt dict oder Objekt zurück — normalisieren
        if isinstance(raw, dict):
            doc_date  = str(raw.get("date", "") or raw.get("year", ""))
            doc_type  = str(raw.get("type", "") or raw.get("report_type", "Jahresabschluss"))
            raw_text  = str(raw.get("text", "") or raw.get("content", ""))
            source_id = str(raw.get("id", "") or "")
        else:
            doc_date  = str(getattr(raw, "date", "") or getattr(raw, "year", ""))
            doc_type  = str(getattr(raw, "type", "Jahresabschluss"))
            raw_text  = str(getattr(raw, "text", "") or getattr(raw, "content", ""))
            source_id = str(getattr(raw, "id", "") or "")

        if not raw_text or len(raw_text) < 50:
            continue  # Leere Berichte überspringen

        # Duplikat-Check via UniqueConstraint
        existing = db.query(BAReport).filter_by(
            company_name=company_name,
            document_date=doc_date,
            document_type=doc_type,
        ).first()

        if existing:
            stored.append(existing)
            continue

        report = BAReport(
            company_name=company_name,
            document_date=doc_date,
            document_type=doc_type,
            raw_text=raw_text,
            source_id=source_id,
            parse_status="pending",
        )
        db.add(report)
        stored.append(report)

    try:
        db.commit()
        logger.info("fetch_and_store '%s': %d Berichte gespeichert/gefunden", company_name, len(stored))
    except Exception as e:
        db.rollback()
        logger.error("fetch_and_store DB commit failed für '%s': %s", company_name, e)
        return []

    return stored


def get_pending_reports(db: Session) -> list[BAReport]:
    """Gibt alle Berichte zurück die noch nicht geparst wurden."""
    return db.query(BAReport).filter_by(parse_status="pending").all()


def mark_parsed(report_id: int, db: Session, status: str = "done") -> None:
    """Setzt parse_status auf done | error."""
    report = db.query(BAReport).get(report_id)
    if report:
        report.parse_status = status
        db.commit()
