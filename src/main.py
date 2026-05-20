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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Cron Job ──────────────────────────────────────────────────────────────────

def _cron_enrich_all() -> None:
    """
    Täglicher Cron: alle Companies in ba_reports refreshen + pending parsen.
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

    except Exception as e:
        logger.error("Cron _cron_enrich_all failed: %s", e)
    finally:
        db.close()


# ── App Lifecycle ─────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("BA-Bridge starting up …")
    init_db()
    logger.info("Shadow-DB initialisiert")

    scheduler.add_job(
        _cron_enrich_all,
        trigger="cron",
        hour=settings.cron_hour,
        minute=settings.cron_minute,
        id="daily_enrich",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Cron gestartet: täglich %02d:%02d UTC",
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


@app.get("/health")
def health():
    return {"status": "ok", "service": "ba-bridge"}
