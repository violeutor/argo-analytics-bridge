"""
BA-Bridge — Main
=================
Eigenständiger FastAPI-Service.
Stellt Bundesanzeiger-Daten als strukturiertes JSON bereit.

Endpoints:
  GET /health
  GET /ba/company/{name}

Cron: täglich 03:00 UTC — fetch_and_store + parse_pending für alle Companies
"""
import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config import settings
from src.database import init_db, SessionLocal
from src.routes.company import router as company_router
from src.routes.yahoo import router as yahoo_router
from src.routes.shadow import router as shadow_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Cron Job ──────────────────────────────────────────────────────────────────

def _cron_enrich_all() -> None:
    """
    Täglicher Cron 03:00 UTC: alle Companies in ba_reports refreshen + pending parsen.
    Holt distinct company_names aus ba_reports → re-fetch → parse_pending.
    """
    from src.ba_fetcher import fetch_and_store, get_pending_reports
    from src.ba_parser import parse_pending

    db = SessionLocal()
    try:
        from src.models import BAReport
        from sqlalchemy import distinct

        names = [
            row[0]
            for row in db.query(distinct(BAReport.company_name)).all()
        ]
        logger.info("Cron: %d Companies zu refreshen", len(names))

        for name in names:
            try:
                fetch_and_store(name, db)
            except Exception as e:
                logger.warning("Cron fetch failed für '%s': %s", name, e)

        parsed = parse_pending(db, limit=100)
        logger.info("Cron abgeschlossen: %d Reports geparst", parsed)

        # KPI-Zeitreihen in Argo Supabase schreiben
        from src.routes.company import _push_kpi_to_argo
        total_pushed = 0
        for name in names:
            try:
                total_pushed += _push_kpi_to_argo(name, db)
            except Exception as e:
                logger.warning("KPI-Push failed für '%s': %s", name, e)
        logger.info("Cron KPI-Push: %d Rows total in Argo Supabase geschrieben", total_pushed)

    except Exception as e:
        logger.error("Cron _cron_enrich_all failed: %s", e)
    finally:
        db.close()


def _cron_shadow_seed() -> None:
    """
    Täglich 03:30 UTC: neue BA-Companies in Shadow-Queue aufnehmen.
    Prio-Score via Wikipedia Pageviews API.
    """
    from src.shadow_enrichment import seed_shadow_queue
    db = SessionLocal()
    try:
        added = seed_shadow_queue(db)
        logger.info("Cron _cron_shadow_seed: %d neue Companies in Queue", added)
    except Exception as e:
        logger.error("Cron _cron_shadow_seed failed: %s", e)
    finally:
        db.close()


def _cron_shadow_enrich() -> None:
    """
    Alle 2.5h: 1 pending Shadow-Company anreichern.
    ≈10 Companies/Tag — sequentiell, kein CAPTCHA-Risiko.
    """
    from src.shadow_enrichment import enrich_one_shadow
    db = SessionLocal()
    try:
        enrich_one_shadow(db)
    except Exception as e:
        logger.error("Cron _cron_shadow_enrich failed: %s", e)
    finally:
        db.close()


def _cron_beta_update() -> None:
    """
    Täglicher Cron 22:00 UTC: Beta-Kennzahlen für alle is_listed Ticker
    aus Argo-Supabase neu berechnen + in beta_cache schreiben.
    Läuft nach US-Börsenschluss (~21:00 UTC) für frische Tagesdaten.
    """
    from src.price_fetcher import run as run_price_fetcher

    try:
        logger.info("Cron _cron_beta_update: Start")
        run_price_fetcher()
        logger.info("Cron _cron_beta_update: Fertig")
    except Exception as e:
        logger.error("Cron _cron_beta_update failed: %s", e)


# ── App Lifecycle ─────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("BA-Bridge starting up …")
    init_db()
    # Shadow-DB Tabelle anlegen falls nicht vorhanden
    from src.models_shadow import ShadowCompany  # noqa: F401 — triggers table creation
    from src.database import engine, Base
    Base.metadata.create_all(bind=engine, tables=[ShadowCompany.__table__])
    logger.info("Shadow-DB initialisiert")

    scheduler.add_job(
        _cron_enrich_all,
        trigger="cron",
        hour=settings.cron_hour,
        minute=settings.cron_minute,
        id="daily_enrich",
        replace_existing=True,
    )
    scheduler.add_job(
        _cron_beta_update,
        trigger="cron",
        hour=22,
        minute=0,
        id="daily_beta_update",
        replace_existing=True,
    )
    scheduler.add_job(
        _cron_shadow_seed,
        trigger="cron",
        hour=3,
        minute=30,
        id="daily_shadow_seed",
        replace_existing=True,
    )
    scheduler.add_job(
        _cron_shadow_enrich,
        trigger="interval",
        minutes=150,            # alle 2.5h → ≈10 Companies/Tag
        id="shadow_enrich",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Crons gestartet: BA-Enrich täglich %02d:%02d UTC · Beta-Update täglich 22:00 UTC",
        settings.cron_hour, settings.cron_minute,
    )

    yield

    # Shutdown
    scheduler.shutdown(wait=False)
    logger.info("BA-Bridge shut down")


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="BA-Bridge",
    description="Bundesanzeiger → strukturiertes JSON für Argo Analytics",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Intern only — kein Public Access
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(company_router)
app.include_router(yahoo_router)
app.include_router(shadow_router)


@app.get("/health")
def health():
    return {"status": "ok", "service": "ba-bridge"}
