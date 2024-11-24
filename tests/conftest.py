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

from pgbase.base import BaseDatabase
from pgbase.core import AsyncDatabase, Datasource, DataGrid
from pgbase.models import DatabaseSettings, DatasourceSettings, ColumnIndex
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
        "max_overflow": 5
    }


@pytest.fixture(scope="session")
def databases_settings():
    settings_dict = {
        "db1": {
            "uri": f"postgresql+psycopg://postgres:postgres@localhost:{DEFAULT_PORT}/db1",
            "admin_username": "postgres",
            "admin_password": "postgres",
        },
        "db2": {
            "uri": f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/db2",
            "admin_username": "postgres",
            "admin_password": "postgres",
        }
    }
    
    return {
        settings_name: DatabaseSettings(**settings_values)
        for settings_name, settings_values in settings_dict.items()
    }



@pytest.fixture(scope="session")
def async_settings_without_auto_create():
    return DatabaseSettings(**{
        "uri": f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/db_test",
        "admin_username": "postgres",
        "admin_password": "postgres"
    })


@pytest.fixture(scope="session")
def sync_settings_without_db_name():
    return DatabaseSettings(**{
        "uri": f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}",
        "admin_username": "postgres",
        "admin_password": "postgres"
    })


@pytest.fixture(scope="function")
def database_without_db_name(sync_settings_without_db_name):
    db = AsyncDatabase(sync_settings_without_db_name)
    yield db


@pytest.fixture(scope="function")
def database_without_auto_create(async_settings_without_auto_create):
    db = AsyncDatabase(async_settings_without_auto_create)
    yield db


@pytest.fixture(scope="function")
def async_database(databases_settings):
    db = AsyncDatabase(databases_settings['db1'])
    yield db


@pytest.fixture(scope="session")
def datasource_settings(databases_settings: Dict[str, DatabaseSettings]):
    # Create the DatasourceSettings fixture
    return DatasourceSettings(
        name="Datasource object",
        description="Datasource with both sync and async databases",
        databases=list(databases_settings.values())
    )


@pytest.fixture(scope="session", autouse=True)
def setup_database(datasource_settings: DatasourceSettings):
    for database in datasource_settings.databases:
        prepare_database(str(database.admin_uri), str(database.uri), database.name)


@pytest.fixture
def datasource(datasource_settings: DatasourceSettings):
    datasource = Datasource(datasource_settings)
    return datasource


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
        lambda: f"postgresql+asyncpg://user:password@localhost:5432/db{random.randint(1, 1000)}")  # Generating a valid URL string
    admin_username = "admin"
    admin_password = "password"
    default_port = 5432
    async_mode = False
    pool_size = 10
    max_overflow = 5


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


def create_mocked_data_grid():
    """Create a mocked DataGrid instance using factories."""
    # Generate DatasourceSettings using the factory
    datasource_settings: Dict[
        str, DatasourceSettingsFactory
    ] = {
        "ds1": DatasourceSettingsFactory(name="ds1"),
        "ds2": DatasourceSettingsFactory(name="ds2"),
    }
    
    # Create a mock logger
    mock_logger = LoggerMock()

    # Create the DataGrid instance with the generated datasource settings
    data_grid = DataGrid(datasource_settings, mock_logger)

    # Mock the datasources within the DataGrid
    data_grid.datasources = {
        ds.name: MagicMock(
            spec=DatasourceSettings, 
            name=ds.name
        ) for ds in datasource_settings.values()
    }

    # Mock methods for each datasource
    for ds in data_grid.datasources.values():
        ds.disconnect_all = MagicMock()
        ds.create_tables_all = MagicMock()

    return data_grid


@pytest.fixture
def datasource_settings_factory():
    """Fixture to create mock DatasourceSettings for tests."""
    return DatasourceSettingsFactory


@pytest.fixture(scope="function")
def mock_data_grid(datasource_settings_factory, mock_logger):
    """Fixture to create a DataGrid instance for tests."""
    settings_dict = {
        "ds1": datasource_settings_factory(name="ds1"),
        "ds2": datasource_settings_factory(name="ds2"),
    }
    return DataGrid(settings_dict, logger=mock_logger)


class TestDatabase(BaseDatabase):
    def _create_engine(self):
        return "test_engine"

    def _create_admin_engine(self):
        return "test_admin_engine"

    def _create_sessionmaker(self):
        return "test_sessionmaker"

    def get_session(self):
        return "test_session"

    def create_database(self, db_name: str = None):
        return f"Database {db_name or self.settings.database} created."

    def drop_database(self, db_name: str = None):
        return f"Database {db_name or self.settings.database} dropped."

    def database_exists(self, db_name: str = None) -> bool:
        return True

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        return True

    def create_indexes(self, indexes: List[ColumnIndex]):
        return "Indexes created."

    def schema_exists(self, schema_name):
        return True

    def health_check(self, use_admin_uri=False, timeout=10, max_retries=3) -> bool:
        return True

    def execute(self, query: str, params: dict = None):
        return "Query executed."

    async def list_tables(self, schema_name: str = 'public'):
        return ["table1", "table2"]

    async def list_schemas(self):
        return ["schema1", "schema2"]

    async def list_indexes(self, table_name: str):
        return ["index1", "index2"]

    async def list_views(self, table_schema='public'):
        return ["view1", "view2"]

    async def list_sequences(self):
        return ["sequence1", "sequence2"]

    async def list_constraints(self, table_name: str, table_schema: str = 'public'):
        return ["constraint1", "constraint2"]

    async def list_triggers(self, table_name: str):
        return ["trigger1", "trigger2"]

    async def list_functions(self):
        return ["function1", "function2"]

    async def list_procedures(self):
        return ["procedure1", "procedure2"]

    async def list_materialized_views(self):
        return ["materialized_view1", "materialized_view2"]

    async def list_columns(self, table_name: str, table_schema: str = 'public'):
        return ["column1", "column2"]

    async def list_types(self):
        return ["type1", "type2"]

    async def list_roles(self):
        return ["role1", "role2"]

    async def list_extensions(self) -> list:
        return ["extension1", "extension2"]

@pytest.fixture
def database_settings():
    return DatabaseSettings(
        uri="postgresql://user:password@localhost:5432/testdb",
        admin_username = "admin", admin_password = "password"
    )

@pytest.fixture
def test_db(database_settings):
    return TestDatabase(settings=database_settings, logger=MagicMock())