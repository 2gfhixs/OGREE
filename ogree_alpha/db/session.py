"""Database session management."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return database_url


_ENGINE: Engine | None = None
SessionLocal: sessionmaker[Session] | None = None


def get_engine(database_url: Optional[str] = None) -> Engine:
    global _ENGINE
    if _ENGINE is None or database_url is not None:
        url = database_url or get_database_url()
        _ENGINE = create_engine(url, future=True)
    return _ENGINE


def get_session_factory(database_url: Optional[str] = None) -> sessionmaker[Session]:
    global SessionLocal
    if SessionLocal is None or database_url is not None:
        engine = get_engine(database_url)
        SessionLocal = sessionmaker(bind=engine, future=True)
    return SessionLocal


@contextmanager
def get_session(database_url: Optional[str] = None) -> Generator[Session, None, None]:
    session_factory = get_session_factory(database_url)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
