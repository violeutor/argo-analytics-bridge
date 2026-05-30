"""
Shadow Enrichment Worker — BA-Bridge
======================================
Füllt shadow_companies proaktiv mit Bundesanzeiger-Daten für deutsche private Companies.

Zwei Jobs (via APScheduler in main.py):
  1. seed_shadow_queue()    — täglich 03:30 UTC: neue DE GmbH-Companies aus seed_data/de_gmbh_curated.txt
  2. enrich_one_shadow()    — alle 2.5h: 1 pending Company via BA anreichern (≈10/Tag)

Seed-Quelle:
  seed_data/de_gmbh_curated.txt — generiert via seed_handelsregister.py (handelsregister.ai API).
  Taxonomy-aligned, aktive GmbHs, dedupliziert. Einmalig lokal generiert + ins Repo committed.
  Filtert: bereits in Supabase + bereits in shadow_companies.

Prio-Score:
  Fix 10.0 für alle GmbHs — FIFO-Reihenfolge aus der Seed-Datei reicht aus.

Rate-Limiting:
  Max 1 Company alle 2.5h → kein CAPTCHA-Risiko bei BA-Scraping.
  Kein Parallelisieren — sequentiell.
"""
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from src.models_shadow import ShadowCompany

logger = logging.getLogger(__name__)

# Nach MAX_RETRIES CAPTCHA-Blocks → status='exhausted' (nicht mehr retried)
# Nur relevant bei echtem CAPTCHA — No-Match wird direkt exhausted (kein Retry).
MAX_RETRIES = 2

# Pfad zur kuratierten GmbH-Liste (generiert via seed_handelsregister.py)
_SEED_FILE = Path(__file__).parent.parent / "seed_data" / "de_gmbh_curated.txt"



# ── Supabase-Abruf (Ausschluss-Filter) ───────────────────────────────────────

def _fetch_supabase_company_names() -> set[str]:
    """
    Holt alle Company-Namen aus Supabase (für Ausschluss im Seed).
    Companies die bereits in Supabase sind, brauchen kein Shadow Pre-fetch.
    """
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("ARGO_SUPABASE_URL", "")
    supabase_key = (
        os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
        or os.getenv("ARGO_SUPABASE_KEY", "")
    )
    if not supabase_url or not supabase_key:
        logger.warning("_fetch_supabase_company_names: keine Supabase-Credentials")
        return set()
    try:
        from supabase import create_client
        sb     = create_client(supabase_url, supabase_key)
        # Limit 9999 — PostgREST-Default ist 1000, würde bei >1000 Companies
        # den Exclusion-Filter still truncaten → Shadow-Duplikate
        result = sb.table("companies").select("name").limit(9999).execute()
        return {r["name"].lower() for r in (result.data or []) if r.get("name")}
    except Exception as e:
        logger.warning("_fetch_supabase_company_names failed: %s", e)
        return set()


# ── Startup Cleanup ───────────────────────────────────────────────────────────

def reset_stale_running(db: Session) -> int:
    """
    Setzt alle Companies im Status 'running' auf 'pending' zurück.

    Wird beim BA-Bridge Startup aufgerufen — 'running' beim Start bedeutet
    zwingend ein Crash-Relikt aus dem vorherigen Prozess (Render Redeploy,
    OOM, etc.). Da enrich_one_shadow() sequentiell läuft und max. ein paar
    Minuten dauert, gibt es keinen legitimen 'running'-Eintrag beim Startup.

    retry_count wird NICHT zurückgesetzt — vorherige Versuche zählen weiter.
    """
    stale = (
        db.query(ShadowCompany)
        .filter(ShadowCompany.enrichment_status == "running")
        .all()
    )
    for sc in stale:
        sc.enrichment_status = "pending"
    if stale:
        db.commit()
        logger.info(
            "reset_stale_running: %d stale 'running' → 'pending' zurückgesetzt",
            len(stale),
        )
    return len(stale)


# ── Seed Job ──────────────────────────────────────────────────────────────────

def seed_shadow_queue(db: Session) -> int:
    """
    Befüllt Shadow-Queue mit deutschen GmbH-Companies die noch NICHT in Supabase sind.

    Quelle:  seed_data/de_gmbh_curated.txt — generiert via seed_handelsregister.py.
             Taxonomy-aligned (14 Sektoren), aktive GmbHs, dedupliziert.
             Datei einmalig lokal generieren + ins Repo committen. Kein Cron nötig.

    Filter:  Bereits in Supabase → überspringen (Rolling Refresh reicht)
             Bereits in shadow_companies → überspringen
    Prio:    Fix 10.0 — FIFO aus Seed-Datei reicht, kein Wikipedia-Overhead.

    Täglich 03:30 UTC + beim Startup wenn Queue < 10.
    """
    if not _SEED_FILE.exists():
        logger.warning(
            "seed_shadow_queue: Seed-Datei nicht gefunden: %s — "
            "seed_handelsregister.py lokal ausführen und Ergebnis committen.",
            _SEED_FILE,
        )
        return 0

    raw = _SEED_FILE.read_text(encoding="utf-8").splitlines()
    combined = [line.strip() for line in raw if line.strip()]
    logger.info("seed_shadow_queue: Seed-Datei → %d Companies", len(combined))

    if not combined:
        logger.warning("seed_shadow_queue: Seed-Datei leer — Seed abgebrochen")
        return 0

    supabase_existing = _fetch_supabase_company_names()
    shadow_existing: set[str] = {
        row[0].lower()
        for row in db.query(ShadowCompany.name).all()
    }

    # Deduplizieren innerhalb der Datei (verhindert UniqueViolation bei doppelten Einträgen)
    seen: set[str] = set()
    new_names = []
    for n in combined:
        key = n.lower()
        if key not in supabase_existing and key not in shadow_existing and key not in seen:
            seen.add(key)
            new_names.append(n)

    logger.info(
        "seed_shadow_queue: %d in Datei → %d neu "
        "(Supabase: %d, Shadow: %d bereits vorhanden)",
        len(combined), len(new_names),
        len(supabase_existing), len(shadow_existing),
    )

    added = 0
    for name in new_names:
        prio = 10.0   # Wikipedia-Pageviews für DE GmbHs nicht sinnvoll — FIFO reicht
        db.add(ShadowCompany(
            name=name,
            prio_score=prio,
            enrichment_status="pending",
            source="bundesanzeiger",
        ))
        added += 1

    if added:
        db.commit()

    logger.info("seed_shadow_queue: %d neue Companies in Queue aufgenommen", added)
    return added


# ── Enrichment Job ────────────────────────────────────────────────────────────

def enrich_one_shadow(db: Session) -> bool:
    """
    Enrichert genau 1 pending Shadow-Company mit BA-Daten.
    Sortierung: prio_score DESC → created_at ASC (älteste bei Gleichstand).

    Flow:
      fetch_and_store(name) → parse_pending() → Daten aus ba_financials/ba_persons lesen
      → in shadow_companies schreiben → status='done'

    Wird alle 2.5h aufgerufen → ≈10 Companies/Tag, kein CAPTCHA-Risiko.
    Returns True wenn erfolgreich angereichert, False wenn Queue leer oder Fehler.
    """
    from src.models import BAFinancial, BAPerson
    from src.ba_fetcher import fetch_and_store

    sc: ShadowCompany | None = (
        db.query(ShadowCompany)
        .filter(ShadowCompany.enrichment_status == "pending")
        .order_by(ShadowCompany.prio_score.desc(), ShadowCompany.created_at.asc())
        .first()
    )

    if not sc:
        logger.debug("enrich_one_shadow: Queue leer")
        return False

    logger.info(
        "enrich_one_shadow: '%s' (prio=%.0f)",
        sc.name, sc.prio_score,
    )
    sc.enrichment_status = "running"
    db.commit()

    try:
        # Rückgabewert capturen:
        # reports          → gespeicherte BAReport-Objekte
        # captcha_blocked  → CAPTCHA nach max. Retries → Retry sinnvoll
        # matched_but_empty → BA hat Company gefunden, aber 0 Berichte → direkt exhausted
        reports, captcha_blocked, matched_but_empty = fetch_and_store(sc.name, db)

        # Company-spezifisches Parsing — nicht alle pending Reports global abarbeiten
        from src.models import BAReport as _BAReport
        from src.ba_parser import parse_report as _parse_report
        pending_for_company = (
            db.query(_BAReport)
            .filter_by(company_name=sc.name, parse_status="pending")
            .limit(5)
            .all()
        )
        for r in pending_for_company:
            _parse_report(r, db)

        fin: BAFinancial | None = (
            db.query(BAFinancial)
            .filter(BAFinancial.company_name == sc.name)
            .order_by(BAFinancial.fiscal_year.desc().nullslast())
            .first()
        )

        # BA-11 Differenzierung:
        # matched_but_empty → BA kennt die Company, keine Berichte → keine Publikationspflicht erfüllt
        #                     Kein Retry — direkt exhausted
        # captcha_blocked   → Infrastruktur-Problem → Retry (bis MAX_RETRIES)
        # reports leer ohne beides → Name nicht in BA gefunden → direkt exhausted
        if not reports and not fin:
            if matched_but_empty:
                sc.enrichment_status = "exhausted"
                logger.info(
                    "enrich_one_shadow: '%s' — BA-Treffer aber 0 Berichte "
                    "(keine Publikationspflicht) → direkt exhausted",
                    sc.name,
                )
            elif captcha_blocked:
                sc.retry_count = (sc.retry_count or 0) + 1
                if sc.retry_count >= MAX_RETRIES:
                    sc.enrichment_status = "exhausted"
                    logger.info(
                        "enrich_one_shadow: '%s' — CAPTCHA exhausted nach %d Versuchen",
                        sc.name, sc.retry_count,
                    )
                else:
                    sc.enrichment_status = "pending"
                    logger.info(
                        "enrich_one_shadow: '%s' — CAPTCHA → pending "
                        "(Versuch %d/%d, nächster Run in ~2.5h)",
                        sc.name, sc.retry_count, MAX_RETRIES,
                    )
            else:
                # Name nicht in BA gefunden (inkl. Fallbacks) → kein Retry
                sc.enrichment_status = "exhausted"
                logger.info(
                    "enrich_one_shadow: '%s' — nicht in BA gefunden → direkt exhausted",
                    sc.name,
                )
            db.commit()
            return False

        if fin:
            sc.ba_id               = str(fin.report_id)
            sc.revenue_eur_mn      = fin.revenue_eur_mn
            sc.ebitda_eur_mn       = fin.ebitda_eur_mn
            sc.ebit_eur_mn         = fin.ebit_eur_mn
            sc.net_income_eur_mn   = fin.net_income_eur_mn
            sc.equity_eur_mn       = fin.equity_eur_mn
            sc.total_assets_eur_mn = fin.total_assets_eur_mn
            sc.headcount           = fin.headcount
            sc.fiscal_year         = fin.fiscal_year

        persons: list[BAPerson] = (
            db.query(BAPerson)
            .filter(BAPerson.company_name == sc.name)
            .all()
        )
        sc.shareholders = [
            {"name": p.name, "share_pct": p.share_pct, "is_company": p.is_company}
            for p in persons if p.role == "shareholder"
        ]
        sc.managing_directors = [
            {"name": p.name, "role": p.role}
            for p in persons if p.role in ("executive", "supervisory_board")
        ]

        sc.enrichment_status = "done"
        sc.enriched_at       = datetime.now(timezone.utc)
        db.commit()

        logger.info(
            "enrich_one_shadow OK: '%s' — FY%s rev=%.1fM shareholders=%d",
            sc.name, sc.fiscal_year,
            sc.revenue_eur_mn or 0.0,
            len(sc.shareholders or []),
        )
        return True

    except Exception as e:
        sc.retry_count = (sc.retry_count or 0) + 1
        if sc.retry_count >= MAX_RETRIES:
            sc.enrichment_status = "exhausted"
            logger.warning(
                "enrich_one_shadow: '%s' — exhausted nach %d Versuchen (Exception: %s)",
                sc.name, sc.retry_count, e,
            )
        else:
            sc.enrichment_status = "pending"
            logger.warning(
                "enrich_one_shadow FAILED '%s' (Versuch %d/%d): %s",
                sc.name, sc.retry_count, MAX_RETRIES, e,
            )
        db.commit()
        return False
