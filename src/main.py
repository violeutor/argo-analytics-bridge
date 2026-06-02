"""
BA-Bridge — Main
=================
Eigenständiger FastAPI-Service.
Beta-Kennzahlen für börsennotierte Companies (Yahoo Finance).

Endpoints:
  GET  /health
  GET  /yahoo/beta/{ticker}
  POST /yahoo/beta/bulk

Cron: täglich 22:00 UTC — Beta-Update für alle is_listed Ticker aus Supabase.

BA-Teil (Bundesanzeiger) entfernt — S47.
Bundesanzeiger seit 2022 strukturell leer (Daten im Unternehmensregister).
Shadow-Queue + BA-Scraper + Shadow-Routes ebenfalls entfernt.
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from src.config import settings
from src.database import init_db, SessionLocal
from src.routes.yahoo import router as yahoo_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Cron Job ──────────────────────────────────────────────────────────────────

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

    scheduler.add_job(
        _cron_beta_update,
        trigger="cron",
        hour=22,
        minute=0,
        id="daily_beta_update",
        replace_existing=True,
    )
    scheduler.start()

    logger.info("Cron gestartet: Beta-Update 22:00 UTC")

    yield

    # Shutdown
    scheduler.shutdown(wait=False)
    logger.info("BA-Bridge shut down")


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="BA-Bridge",
    description="Beta-Kennzahlen via Yahoo Finance für Argo Analytics",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Intern only — kein Public Access
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(yahoo_router)


@app.get("/health")
def health():
    return {"status": "ok", "service": "ba-bridge"}


# ── Manueller Trigger (Debugging / Testing) ───────────────────────────────────

@app.post("/admin/beta/trigger")
async def trigger_beta_update(background_tasks: BackgroundTasks):
    """
    Manueller Trigger für _cron_beta_update.
    Beta-Kennzahlen sofort neu berechnen ohne auf 22:00 UTC zu warten.
    """
    background_tasks.add_task(_cron_beta_update)
    return {"status": "triggered", "job": "_cron_beta_update"}


