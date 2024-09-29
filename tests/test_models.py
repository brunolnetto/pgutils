import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncSession
from typing import List, Any

# Assuming the following imports based on your original code
from pgutils.models import DatabaseConfig, Index, Paginator

# Database configuration constants
DB_NAME = "mydb"
DB_URL = f"postgresql://postgres:postgres@localhost:5432/{DB_NAME}"

@pytest.fixture(scope="module")
def db_engine():
    """Create a test PostgreSQL database engine and ensure the database exists."""
    # Connect to the default PostgreSQL database
    default_engine = create_engine(
        "postgresql://postgres:postgres@localhost:5432/postgres",
        isolation_level="AUTOCOMMIT"
    )

    # Drop the database if it exists
    with default_engine.connect() as conn:
        try:
            conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME};"))
            print(f"Dropped database '{DB_NAME}' if it existed.")
        except Exception as e:
            print(f"Error dropping database: {e}")

    # Create the database
    try:
        with default_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {DB_NAME};"))
            print(f"Database '{DB_NAME}' created.")
    except Exception as e:
        print(f"Error creating database: {e}")
        pytest.fail(f"Database creation failed: {e}")

    # Create the main engine for the tests, now connecting to mydb
    engine = create_engine(DB_URL)

    # Create the test table and populate it with data
    with engine.connect() as conn:
        try:
            # Create the table
            conn.execute(text("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT);"))

            # Check available tables
            result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';"))
            tables = [row[0] for row in result]

            # Clear existing data
            conn.execute(text("DELETE FROM test_table;"))

            # Insert new data
            conn.execute(text("INSERT INTO test_table (name) VALUES ('Alice'), ('Bob'), ('Charlie'), ('David');"))
            conn.commit()

        except Exception as e:
            print(f"Error during table operations: {e}")
            pytest.fail(f"Table creation or data insertion failed: {e}")

    yield engine

@pytest.fixture(scope="module")
def session_factory(db_engine):
    """Create a session factory for the database."""
    return sessionmaker(bind=db_engine)

def test_database_config_valid():
    config = DatabaseConfig(
        uri='postgresql://user:password@localhost:5432/mydatabase',
        admin_username='admin',
        admin_password='strongpassword'
    )
    
    assert config.db_name == 'mydatabase'
    assert str(config.admin_uri) == 'postgresql://admin:***@localhost:5432'
    assert str(config.complete_uri) == 'postgresql://user:***@localhost:5432/mydatabase'

def test_database_config_invalid_uri():
    with pytest.raises(ValidationError) as excinfo:
        DatabaseConfig(
            uri='invalid_uri',
            admin_username='admin',
            admin_password='strongpassword'
        )
    assert 'relative URL without a base' in str(excinfo.value)

def test_database_config_invalid_pool_size():
    with pytest.raises(ValidationError) as excinfo:
        DatabaseConfig(
            uri='postgresql://user:password@localhost:5432/mydatabase',
            admin_username='admin',
            admin_password='strongpassword',
            pool_size=-1
        )
    assert 'should be greater than 0' in str(excinfo.value)

def test_database_config_invalid_indexes():
    with pytest.raises(ValidationError) as excinfo:
        DatabaseConfig(
            uri='postgresql://user:password@localhost:5432/mydatabase',
            admin_username='admin',
            admin_password='strongpassword',
            indexes={'my_table': {'type': 'invalid_type'}}  # Invalid index type
        )
    assert "must include 'columns' as a list." in str(excinfo.value)


def test_valid_index_btree():
    index = Index(type='btree', columns=['column1', 'column2'])
    assert index.type == 'btree'
    assert index.columns == ['column1', 'column2']
    assert index.expression is None
    assert index.condition is None

def test_valid_index_expression():
    index = Index(type='expression', columns=['column1', 'column2'], expression='column1 + column2')
    assert index.type == 'expression'
    assert index.columns == ['column1', 'column2']
    assert index.expression == 'column1 + column2'

def test_valid_index_partial():
    index = Index(type='partial', columns=['column1'], condition='column1 IS NOT NULL')
    assert index.type == 'partial'
    assert index.columns == ['column1']
    assert index.condition == 'column1 IS NOT NULL'

def test_invalid_index_type():
    with pytest.raises(ValidationError) as exc_info:
        Index(type='invalid_type', columns=['column1'])
    assert "Index type must be one of" in str(exc_info.value)

def test_columns_must_be_list():
    with pytest.raises(ValidationError) as exc_info:
        Index(type='btree', columns='not_a_list')
    
    assert "Input should be a valid list" in str(exc_info.value)

def test_expression_required_for_expression_index():
    with pytest.raises(ValidationError) as exc_info:
        Index(type='expression', columns=['column1'])
    
    assert "Expression index must include 'expression'." in str(exc_info.value)

def test_condition_required_for_partial_index():
    with pytest.raises(ValidationError) as exc_info:
        Index(type='partial', columns=['column1'])
    
    assert "Partial index must include 'condition'." in str(exc_info.value)


# Example test function to check table visibility
def test_table_data(db_engine):
    with db_engine.connect() as conn:
        query=text("SELECT * FROM public.test_table;")
        result = conn.execute(query).fetchall()

        assert len(result) == 4, "Expected 4 rows in test_table."

def test_database_config_validation():
    # Valid case
    valid_config = DatabaseConfig(
        uri="postgresql://postgres:postgres@localhost:5432/mydb",
        admin_username="postgres",
        admin_password="postgres",
        async_mode=True,
    )
    assert valid_config.db_name == DB_NAME
    assert str(valid_config.admin_uri) == "postgresql://postgres:***@localhost:5432"

    # Invalid URI
    with pytest.raises(ValidationError) as exc_info:
        DatabaseConfig(
            uri="invalid_uri",
            admin_username="admin",
            admin_password="strong_password",
        )
    assert "Input should be a valid URL" in str(exc_info.value)

    # Missing columns in index
    with pytest.raises(ValidationError) as exc_info:
        DatabaseConfig(
            uri="postgresql://user:password@localhost:5432/mydb",
            admin_username="admin",
            admin_password="strong_password",
            indexes={"mytable": {"type": "btree"}}  # Missing 'columns'
        )
    assert "must include 'columns' as a list." in str(exc_info.value)

@pytest.mark.asyncio
async def test_paginator(session_factory):
    # Initialize paginator with a query
    session = session_factory()
    paginator = Paginator(session, "SELECT * FROM public.test_table")

    # Fetch paginated results
    results = []
    async for batch in paginator.paginate():
        results.extend(batch)

    assert len(results) == 4  # Total number of inserted records
    assert results[0] == (1, 'Alice')
    assert results[1] == (2, 'Bob')
    assert results[2] == (3, 'Charlie')
    assert results[3] == (4, 'David')

@pytest.mark.asyncio
async def test_paginator_with_params(session_factory):
    session = session_factory()
    paginator = Paginator(
        session, 
        "SELECT * FROM public.test_table WHERE name LIKE :name", 
        params={'name': 'A%'}
    )

    # Fetch paginated results
    results = []
    async for batch in paginator.paginate():
        results.extend(batch)

    assert len(results) == 1  # Only 'Alice' matches the condition
    assert results[0] == (1, 'Alice')

@pytest.mark.asyncio
async def test_async_paginator():
    # Create an async engine for testing
    async_engine = create_async_engine(f"postgresql+asyncpg://postgres:postgres@localhost:5432/{DB_NAME}")
    
    async with async_engine.begin() as conn:
        await conn.execute(text("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT)"))
        await conn.execute(text("DELETE FROM test_table"))  # Clear existing data
        await conn.execute(text("INSERT INTO test_table (name) VALUES ('Alice'), ('Bob'), ('Charlie'), ('David')"))

    async_session = AsyncSession(async_engine)

    paginator = Paginator(async_session, "SELECT * FROM test_table")


# Example test for db_name property
def test_db_name():
    config = DatabaseConfig(
        uri="postgresql://localhost/mydb",
        admin_username="admin",
        admin_password="password"
    )
    assert config.db_name == "mydb"