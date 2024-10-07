import pytest
import asyncio
from typing import List, Any, Dict

from pydantic import ValidationError
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncSession

from pgutils.core import Database, Datasource, MultiDatasource
from pgutils.models import DatabaseSettings, DatasourceSettings
from pgutils.testing import prepare_database

DEFAULT_PORT=5432

# Database configuration constants
DB_NAME = "mydb"
ADMIN_SYNC_URL=f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/postgres"
SYNC_DB_URL = f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"
ASYNC_DB_URL = f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"

@pytest.fixture()
def invalid_uri_config():
    return {
        "uri": "invalid_uri",
        "admin_username": "postgres",
        "admin_password": "postgres",
        "async_mode": False,
        "pool_size": 10,
        "max_overflow": 5,
        "auto_create_db": False
    }

@pytest.fixture(scope="module")
def databases_settings():
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
def same_database_settings():
    settings_dict = {
        "sync": {
            "uri": f"postgresql+psycopg://postgres:postgres@localhost:{DEFAULT_PORT}/db",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": False,
            "auto_create_db": True
        },
        "async": {
            "uri": f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/db",
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
def sync_settings_without_auto_create():
    return DatabaseSettings(**{
        "uri": f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/db_test",
        "admin_username": "postgres",
        "admin_password": "postgres",
        "async_mode": False,
        "auto_create_db": False
    })

@pytest.fixture(scope="function")
def database_without_auto_create(sync_settings_without_auto_create):
    db = Database(sync_settings_without_auto_create)
    yield db

@pytest.fixture(scope="function")
def sync_database(databases_settings):
    db = Database(databases_settings['sync'])
    yield db

@pytest.fixture(scope="function")
def async_database(databases_settings):
    db = Database(databases_settings['async'])
    yield db

@pytest.fixture(scope="module")
def datasource_settings(databases_settings: Dict[str, DatabaseSettings]):

    # Create the DatasourceSettings fixture
    return DatasourceSettings(
        name="Datasource object",
        databases=list(databases_settings.values()),
        description="Datasource with both sync and async databases"
    )

@pytest.fixture(scope="module")
def datasource(datasource_settings: DatasourceSettings):
    for database in datasource_settings.databases:
        prepare_database(str(database.admin_uri), str(database.uri), database.name)
    
    return Datasource(datasource_settings)

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



