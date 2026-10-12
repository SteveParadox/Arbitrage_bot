from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from api.settings import settings
from api.infrastructure import postgres_options


class Base(DeclarativeBase):
    pass


@lru_cache
def get_engine():
    return create_engine(
        settings.arb_database_url,
        connect_args=postgres_options(settings),
        pool_size=settings.arb_database_pool_size,
        max_overflow=settings.arb_database_pool_overflow,
        pool_timeout=settings.arb_database_pool_timeout,
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
