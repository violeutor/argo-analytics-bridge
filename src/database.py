"""
Database — Railway Postgres (Shadow-DB)
Alle Tabellen werden beim Start automatisch angelegt (create_all).
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from src.config import settings

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    """FastAPI Dependency — DB-Session pro Request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Legt alle Tabellen an wenn nicht vorhanden. Beim App-Start aufrufen."""
    from src import models  # noqa: F401 — importiert damit SQLAlchemy die Tabellen kennt
    Base.metadata.create_all(bind=engine)
