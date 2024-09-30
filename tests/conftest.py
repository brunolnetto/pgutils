import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncSession
from typing import List, Any

from pgutils.core import Database, MultiDatabase
from pgutils.models import DatabaseSettings


DEFAULT_PORT=5433

# Database configuration constants
DB_NAME = "mydb"
ADMIN_SYNC_URL=f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/postgres"
SYNC_DB_URL = f"postgresql://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"
ASYNC_DB_URL = f"postgresql+asyncpg://postgres:postgres@localhost:{DEFAULT_PORT}/{DB_NAME}"


@pytest.fixture()
def multidatabase_settings():
    settings_dict = {
        "sync": {
            "uri": f"postgresql+psycopg://localhost:{DEFAULT_PORT}/db1",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": False
        },
        "async": {
            "uri": f"postgresql+asyncpg://localhost:{DEFAULT_PORT}/db2",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": True
        }
    }
    
    return {
        settings_name: DatabaseSettings(**settings_values)
        for settings_name, settings_values in settings_dict.items()
    }


@pytest.fixture
def invalid_uri_config():
    return {
        "uri": "invalid_uri",
        "admin_username": "postgres",
        "admin_password": "postgres",
        "async_mode": False,
        "pool_size": 10,
        "max_overflow": 5,
    }


@pytest.fixture
def sync_settings(multidatabase_settings):
    return multidatabase_settings['sync']


@pytest.fixture
def async_settings(multidatabase_settings):
    return multidatabase_settings['async']


@pytest.fixture
def sync_database(sync_settings: DatabaseSettings):
    db = Database(sync_settings)
    yield db
    db.drop_database_if_exists(sync_settings.db_name)


@pytest.fixture
def async_database(sync_settings: DatabaseSettings):
    db = Database(sync_settings)
    yield db
    db.drop_database_if_exists(sync_settings.db_name)


def create_database(uri: str, db_name: str):
    # Connect to the default PostgreSQL database
    default_engine = create_engine(uri, isolation_level="AUTOCOMMIT")
    
    # Drop the database if it exists
    with default_engine.connect() as conn:
        try:
            query=text(f"DROP DATABASE IF EXISTS {db_name};")
            conn.execute(query)
            print(f"Dropped database '{db_name}' if it existed.")
        except Exception as e:
            print(f"Error dropping database: {e}")

    # Create the database
    try:
        with default_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {db_name};"))
            print(f"Database '{db_name}' created.")
    except Exception as e:
        print(f"Error creating database: {e}")

def populate_database(uri: str, db_name: str):
    default_engine = create_engine(uri, isolation_level="AUTOCOMMIT")
    
    # Create the test table and populate it with data
    with default_engine.connect() as conn:
        try:
            # Create the table
            conn.execute(
                text("""
                    CREATE TABLE IF NOT EXISTS test_table (
                        id SERIAL PRIMARY KEY, 
                        name TEXT
                    );
                """)
            )

            # Check available tables
            result = conn.execute(
                text("""
                    SELECT 
                        table_name 
                    FROM 
                        information_schema.tables 
                    WHERE 
                        table_schema='public';
                """)
            )
            tables = [row[0] for row in result]

            # Clear existing data
            conn.execute(text("DELETE FROM test_table;"))

            # Insert new data
            conn.execute(
                text("""
                    INSERT INTO test_table (name) 
                    VALUES ('Alice'), ('Bob'), ('Charlie'), ('David');
                """)
            )
            conn.commit()

        except Exception as e:
            print(f"Error during table operations: {e}")
            pytest.fail(f"Table creation or data insertion failed: {e}")

def prepare_database():
    create_database(ADMIN_SYNC_URL, DB_NAME)
    populate_database(SYNC_DB_URL, DB_NAME)

@pytest.fixture(scope="module")
def sync_db_engine():
    """Create a test PostgreSQL database engine and ensure the database exists."""
    prepare_database()

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



