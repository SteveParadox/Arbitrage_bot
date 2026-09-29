from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from api.settings import settings


class Base(DeclarativeBase):
    pass


@lru_cache
def get_engine():
    return create_engine(
        settings.arb_database_url,
        pool_pre_ping=True,
        pool_recycle=300,
    )


@lru_cache
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(
        bind=get_engine(),
        expire_on_commit=False,
        autoflush=False,
    )


def get_db() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()
