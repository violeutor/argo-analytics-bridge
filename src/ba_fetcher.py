"""
BA Fetcher — bundesAPI/deutschland → Shadow-DB
================================================
Holt Jahresabschluss-Volltexte aus dem Bundesanzeiger via bundesAPI (PyPI).
Persistiert Rohtexte in ba_reports — kein Parsing hier, nur sammeln.

Rate-Limit: 1 Request / ba_rate_limit_sec (default 3s) — CAPTCHA-Schutz.
BA-11: CAPTCHA-Detection + Exponential Backoff (30s → 60s → 120s, Max 3×).
       Bei persistentem CAPTCHA: Abbruch + Log 'captcha_blocked'.
       Shadow Company bleibt 'pending' → automatischer Retry beim nächsten Cron-Run.
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

# BA-11: CAPTCHA-Erkennungs-Keywords in Exception-Messages + Response-Text
_CAPTCHA_KEYWORDS = (
    "captcha", "robot", "blocked", "access denied",
    "too many requests", "429", "rate limit", "gesperrt",
    "zugriff verweigert", "bitte bestätigen",
)

# BA-11: Exponential Backoff Delays (Sekunden) — 3 Versuche
_BACKOFF_DELAYS = [30, 60, 120]


def _rate_limit() -> None:
    """Blockiert bis Rate-Limit eingehalten ist."""
    global _last_request_ts
    elapsed = time.monotonic() - _last_request_ts
    wait = settings.ba_rate_limit_sec - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.monotonic()


# Fallback-Suffixe für Bundesanzeiger-Namenssuche
_LEGAL_SUFFIXES = ["AG", "GmbH", "GmbH & Co. KG", "GmbH & Co. KGaA", "SE", "KGaA"]


def _candidate_names(company_name: str) -> list[str]:
    """
    Gibt Suchkandidaten zurück: zuerst den Originalnamen, dann mit
    gängigen Rechtssuffixen — falls der Kurzname nicht im Bundesanzeiger steht.
    Bereits enthaltene Suffixe werden nicht doppelt angehängt.
    """
    candidates = [company_name]
    name_upper = company_name.upper()
    for suffix in _LEGAL_SUFFIXES:
        if suffix.upper() not in name_upper:
            candidates.append(f"{company_name} {suffix}")
    return candidates


def _is_captcha_signal(exc: Exception | None = None, text: str = "") -> bool:
    """
    BA-11: Erkennt CAPTCHA-Signal in Exception-Message oder Response-Text.
    Prüft beide Quellen — bundesAPI kann CAPTCHA als Exception oder als
    leeren/fehlerhaften Response zurückgeben.
    """
    combined = (str(exc) + " " + text).lower()
    return any(kw in combined for kw in _CAPTCHA_KEYWORDS)


def _fetch_with_backoff(ba, candidate: str) -> tuple[list | None, bool]:
    """
    BA-11: Fetcht Reports für einen Kandidaten mit exponentiellem Backoff.

    Returns:
      (result, captcha_blocked)
        result=None          → Fehler oder kein Treffer
        captcha_blocked=True → CAPTCHA nach max. Retries — Abbruch
        captcha_blocked=False → normaler Fehler oder Treffer
    """
    max_retries = len(_BACKOFF_DELAYS)

    for attempt in range(max_retries):
        _rate_limit()
        try:
            result = ba.get_reports(candidate)
            return result, False

        except Exception as e:
            if _is_captcha_signal(exc=e):
                if attempt < max_retries - 1:
                    wait = _BACKOFF_DELAYS[attempt]
                    logger.warning(
                        "BA-11 CAPTCHA detected für '%s' (Attempt %d/%d) — "
                        "Backoff %ds",
                        candidate, attempt + 1, max_retries, wait,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "BA-11 CAPTCHA persistent für '%s' nach %d Versuchen — "
                        "captcha_blocked, Shadow Company bleibt pending",
                        candidate, max_retries,
                    )
                    return None, True
            else:
                # Normaler Fehler (Timeout, Netzwerk, BA down) — kein Retry
                logger.warning(
                    "bundesanzeiger fetch failed für '%s': %s", candidate, e
                )
                return None, False

    return None, False  # sollte nicht erreicht werden


def fetch_and_store(company_name: str, db: Session) -> list[BAReport]:
    """
    Holt alle verfügbaren Bundesanzeiger-Berichte für company_name.
    Versucht bei 0 Treffern automatisch Fallback-Namen (AG, GmbH, etc.).
    Speichert neue Berichte in ba_reports (Duplikate werden übersprungen).

    Returns: Liste der gespeicherten/vorhandenen BAReport-Objekte.
    """
    try:
        import deutschland.bundesanzeiger as ba_module  # type: ignore
    except ImportError:
        logger.error("bundesAPI nicht installiert — pip install bundesAPI")
        return []

    ba = ba_module.Bundesanzeiger()
    reports_raw = None
    matched_name = company_name
    captcha_blocked = False

    for candidate in _candidate_names(company_name):
        result, captcha_blocked = _fetch_with_backoff(ba, candidate)
        if captcha_blocked:
            # CAPTCHA persistent — alle weiteren Kandidaten überspringen
            break
        if result:
            reports_raw = result
            matched_name = candidate
            if candidate != company_name:
                logger.info("Namens-Fallback für '%s' -> '%s'", company_name, candidate)
            break

    if captcha_blocked:
        logger.error(
            "fetch_and_store '%s': captcha_blocked — "
            "Shadow Company bleibt pending, Retry beim nächsten Cron-Run (2.5h)",
            company_name,
        )
        return []

    if not reports_raw:
        logger.info("Keine Berichte gefunden für '%s' (inkl. Fallbacks)", company_name)
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
            company_name=matched_name,
            document_date=doc_date,
            document_type=doc_type,
        ).first()

        if existing:
            stored.append(existing)
            continue

        report = BAReport(
            company_name=matched_name,
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
        logger.info("fetch_and_store '%s' (matched: '%s'): %d Berichte gespeichert/gefunden", company_name, matched_name, len(stored))
    except Exception as e:
        db.rollback()
        logger.error("fetch_and_store DB commit failed für '%s': %s", company_name, e)
        return []

    return stored


def get_pending_reports(db: Session) -> list[BAReport]:
    """Gibt alle Berichte zurück die noch nicht geparst wurden."""
    return db.query(BAReport).filter_by(parse_status="pending").all()


def mark_parsed(
    report_id: int,
    db: Session,
    status: str = "done",
    extraction_confidence: str | None = None,
) -> None:
    """
    Setzt parse_status auf done | error.
    BA-09: extraction_confidence optional mitsetzen:
      'full'         — GuV vollständig (Revenue + EBITDA/EBIT + Net Income)
      'partial'      — Teilfelder fehlen (nur Bilanz oder nur ein GuV-Feld)
      'balance_only' — Nur Bilanz vorhanden (§267 HGB kleine KapGes)
      'not_found'    — Kein strukturiertes Zahlenmaterial extrahierbar
    """
    report = db.query(BAReport).get(report_id)
    if report:
        report.parse_status = status
        if extraction_confidence is not None:
            report.extraction_confidence = extraction_confidence
        db.commit()
