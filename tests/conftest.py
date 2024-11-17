import pytest
import asyncio
import random
from typing import List, Any, Dict
from unittest.mock import MagicMock, patch, create_autospec

import factory
from pydantic import ValidationError, AnyUrl
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncSession

from pgbase.core import AsyncDatabase, Datasource, DataCluster
from pgbase.models import DatabaseSettings, DatasourceSettings
from tests.testing import prepare_database

DEFAULT_PORT=5432

# Database configuration constants
DB_NAME = "mydb"
ADMIN_SYNC_URL=f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/postgres"
SYNC_DB_URL = f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"
ASYNC_DB_URL = f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"

@pytest.fixture
def default_port():
    return DEFAULT_PORT

@pytest.fixture(scope="session")
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

@pytest.fixture(scope="session")
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

@pytest.fixture(scope="session")
def database_settings():
    settings_dict = {
        "sync": {
            "uri": f"postgresql+psycopg://postgres:postgres@localhost:{DEFAULT_PORT}/db",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "auto_create_db": True
        },
        "async": {
            "uri": f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/db",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "auto_create_db": True
        },
        "invalid": {
            "uri": f"postgresql+asyncpg://postgres:postgres@anotherhost:{DEFAULT_PORT}/db",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "auto_create_db": True
        }
    }
    
    return {
        settings_name: DatabaseSettings(**settings_values)
        for settings_name, settings_values in settings_dict.items()
    }


@pytest.fixture(scope="session")
def sync_settings_without_auto_create():
    return DatabaseSettings(**{
        "uri": f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/db_test",
        "admin_username": "postgres",
        "admin_password": "postgres",
        "auto_create_db": False
    })

@pytest.fixture(scope="session")
def sync_settings_without_db_name():
    return DatabaseSettings(**{
        "uri": f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}",
        "admin_username": "postgres",
        "admin_password": "postgres",
        "auto_create_db": False
    })

@pytest.fixture(scope="function")
def database_without_db_name(sync_settings_without_db_name):
    db = AsyncDatabase(sync_settings_without_db_name)
    yield db


@pytest.fixture(scope="function")
def database_without_auto_create(sync_settings_without_auto_create):
    db = AsyncDatabase(sync_settings_without_auto_create)
    yield db

@pytest.fixture(scope="function")
def sync_database(databases_settings):
    db = Database(databases_settings['sync'])
    yield db

@pytest.fixture(scope="function")
def async_database(databases_settings):
    db = AsyncDatabase(databases_settings['async'])
    yield db

@pytest.fixture(scope="session")
def datasource_settings(databases_settings: Dict[str, DatabaseSettings]):
    # Create the DatasourceSettings fixture
    return DatasourceSettings(
        name="Datasource object",
        databases=list(databases_settings.values()),
        description="Datasource with both sync and async databases"
    )

@pytest.fixture(scope="session", autouse=True)
def setup_database(datasource_settings: DatasourceSettings):
    for database in datasource_settings.databases:
        prepare_database(str(database.admin_uri), str(database.uri), database.name)

@pytest.fixture
def datasource(datasource_settings: DatasourceSettings):
    datasource = Datasource(datasource_settings)
    yield datasource
    datasource.disconnect_all()

@pytest.fixture(scope="session")
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

    # Yield the session for usage in tests
    async with async_session_maker() as async_session:
        yield async_session

    # Ensure engine disposal after the session is done
    await async_engine.dispose()

@pytest.fixture(scope='function')
def mock_logger():
    """Fixture for mocking a logger."""
    return MagicMock()

class LoggerMock:
    def __init__(self):
        self.info = MagicMock()
        self.error = MagicMock()
        self.warning = MagicMock()

class DatabaseSettingsFactory(factory.Factory):
    class Meta:
        model = DatabaseSettings

    name = factory.Sequence(lambda n: f"db{n}")
    uri = factory.LazyFunction(
        lambda: f"postgresql://user:password@localhost:5432/db{random.randint(1, 1000)}")  # Generating a valid URL string
    admin_username = "admin"
    admin_password = "password"
    default_port = 5432
    async_mode = False
    pool_size = 10
    max_overflow = 5
    auto_create_db = False

class DatasourceSettingsFactory(factory.Factory):
    class Meta:
        model = DatasourceSettings

    name = "TestDataSource"
    admin_username = "admin"
    admin_password = "password"
    databases = factory.List([
        factory.SubFactory(DatabaseSettingsFactory),
        factory.SubFactory(DatabaseSettingsFactory)
    ])
    connection_timeout = 30
    retry_attempts = 3

@pytest.fixture
def mock_datasource():
    """Fixture for mocking a Datasource instance."""
    return create_autospec(Datasource)

def create_mocked_datasource():
    """Create a mocked Datasource instance using factories."""
    # Generate DatasourceSettings using the factory
    datasource_settings = DatasourceSettingsFactory()

    # Create the Datasource instance with the generated datasource settings
    mock_logger = LoggerMock()
    datasource = Datasource(datasource_settings, mock_logger)

    # Mock the databases within the Datasource
    datasource.databases = {
        db.name: MagicMock(
            spec=DatabaseSettings, 
            name=db.name
        ) for db in datasource_settings.databases
    }

    # Mock methods for each database
    for db in datasource.databases.values():
        db.connect = MagicMock()
        db.disconnect = MagicMock()
        db.create_database = MagicMock()

    return datasource

def create_mocked_data_cluster():
    """Create a mocked DataCluster instance using factories."""
    # Generate DatasourceSettings using the factory
    datasource_settings: Dict[
        str, DatasourceSettingsFactory
    ] = {
        "ds1": DatasourceSettingsFactory(name="ds1"),
        "ds2": DatasourceSettingsFactory(name="ds2"),
    }
    
    # Create a mock logger
    mock_logger = LoggerMock()

    # Create the DataCluster instance with the generated datasource settings
    data_cluster = DataCluster(datasource_settings, mock_logger)

    # Mock the datasources within the DataCluster
    data_cluster.datasources = {
        ds.name: MagicMock(
            spec=DatasourceSettings, 
            name=ds.name
        ) for ds in datasource_settings.values()
    }

    # Mock methods for each datasource
    for ds in data_cluster.datasources.values():
        ds.disconnect_all = MagicMock()
        ds.create_tables_all = MagicMock()

    return data_cluster

@pytest.fixture
def datasource_settings_factory():
    """Fixture to create mock DatasourceSettings for tests."""
    return DatasourceSettingsFactory

@pytest.fixture(scope="function")
def mock_data_cluster(datasource_settings_factory, mock_logger):
    """Fixture to create a DataCluster instance for tests."""
    settings_dict = {
        "ds1": datasource_settings_factory(name="ds1"),
        "ds2": datasource_settings_factory(name="ds2"),
    }
    return DataCluster(settings_dict, logger=mock_logger)