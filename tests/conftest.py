import pytest
import asyncio
from typing import List, Any, Dict

from pydantic import ValidationError
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncSession


from pgutils.core import Database, MultiDatabase
from pgutils.models import DatabaseSettings
from pgutils.testing import prepare_database

DEFAULT_PORT=5433

# Database configuration constants
DB_NAME = "mydb"
ADMIN_SYNC_URL=f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/postgres"
SYNC_DB_URL = f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"
ASYNC_DB_URL = f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"


@pytest.fixture(scope="module")
def multidatabase_settings():
    settings_dict = {
        "sync": {
            "uri": f"postgresql+psycopg://postgres:postgres@localhost:{DEFAULT_PORT}/db1",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": False,
            "auto_create_db": True
        },
        "async": {
            "uri": f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/db2",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": True,
            "auto_create_db": True
        }
    }
    
    return {
        settings_name: DatabaseSettings(**settings_values)
        for settings_name, settings_values in settings_dict.items()
    }


@pytest.fixture(scope="module")
def multidatabase(multidatabase_settings: Dict[str, DatabaseSettings]):
    for settings_name, settings in multidatabase_settings.items():
        prepare_database(ADMIN_SYNC_URL, str(settings.uri), settings.db_name)
    
    return MultiDatabase(multidatabase_settings)

@pytest.fixture()
def invalid_uri_config():
    return {
        "uri": "invalid_uri",
        "admin_username": "postgres",
        "admin_password": "postgres",
        "async_mode": False,
        "pool_size": 10,
        "max_overflow": 5,
    }


@pytest.fixture(scope="function")
def sync_database(multidatabase_settings):
    sync_settings=multidatabase_settings['sync']
    db = Database(sync_settings)
    yield db


@pytest.fixture(scope="function")
def async_database(multidatabase_settings):
    async_settings=multidatabase_settings['async']
    db = Database(async_settings)
    yield db

@pytest.fixture(scope="module")
def sync_db_engine():
    """Create a test PostgreSQL database engine and ensure the database exists."""
    prepare_database(ADMIN_SYNC_URL, SYNC_DB_URL, DB_NAME)

    # Create the main engine for the tests, now connecting to database
    engine = create_engine(SYNC_DB_URL)

    yield engine


@pytest.fixture(scope="module")
def sync_session_factory(sync_db_engine):
    """Create a session factory for the database."""
    return sessionmaker(bind=sync_db_engine)


@pytest.fixture(scope="module")
async def async_session_factory():
    """Provide a reusable async session factory."""
    async_engine = create_async_engine(ASYNC_DB_URL, echo=True)
    
    async_session_maker = sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session_maker() as async_session:
        yield async_session  # Yield the session for usage in tests

    # Ensure engine disposal after the session is done
    await async_engine.dispose()



