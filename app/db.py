"""
ScionAgricos - Database Connection
AWS RDS PostgreSQL connection via SQLAlchemy.
Set DATABASE_URL in your environment or .env file.
"""

import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

logger = logging.getLogger(__name__)

# Load from environment (set this in your .env or AWS environment)
DATABASE_URL = os.getenv("DATABASE_URL", "")

Base = declarative_base()

_engine = None
SessionLocal = None


def get_engine():
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL environment variable is not set. "
                "Example: postgresql://user:password@your-rds-host:5432/dbname"
            )
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        logger.info("Connected to PostgreSQL RDS")
    return _engine


def get_session():
    global SessionLocal
    if SessionLocal is None:
        SessionLocal = sessionmaker(bind=get_engine(), autocommit=False, autoflush=False)
    return SessionLocal()


def init_tables():
    """Create all tables if they don't exist yet."""
    from app import models  # noqa: F401 — registers models with Base
    Base.metadata.create_all(bind=get_engine())
    logger.info("RDS tables verified/created: transit_time, seasonality")


def is_db_configured() -> bool:
    return bool(DATABASE_URL)
