import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncSession
from typing import List, Any

# Assuming the following imports based on your original code
from pgutils.models import DatabaseSettings, Index, Paginator

# Database configuration constants
DB_NAME = "mydb"
SYNC_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{DB_NAME}"
ASYNC_DB_URL = f"postgresql+asyncpg://postgres:postgres@localhost:5432/{DB_NAME}"

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
    engine = create_engine(SYNC_DB_URL)

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

@pytest.fixture(scope="module")
async def async_db_engine():
    """Create a test PostgreSQL database engine and ensure the database exists."""
    default_engine = create_async_engine(
        "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres",
        isolation_level="AUTOCOMMIT"
    )

    # Drop the database if it exists
    async with default_engine.connect() as conn:
        await conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME};"))
        print(f"Dropped database '{DB_NAME}' if it existed.")

    # Create the database
    async with default_engine.connect() as conn:
        await conn.execute(text(f"CREATE DATABASE {DB_NAME};"))
        print(f"Database '{DB_NAME}' created.")

    # Create the main async engine for the tests
    engine = create_async_engine(ASYNC_DB_URL)

    # Create the test table and populate it with data
    async with engine.connect() as conn:
        await conn.execute(text("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT);"))
        await conn.execute(text("DELETE FROM test_table;"))
        await conn.execute(text("INSERT INTO test_table (name) VALUES ('Alice'), ('Bob'), ('Charlie'), ('David');"))
        await conn.commit()

    return engine

def test_database_config_valid():
    config = DatabaseSettings(
        uri='postgresql://user:password@localhost:5432/mydatabase',
        admin_username='admin',
        admin_password='strongpassword'
    )
    
    assert config.db_name == 'mydatabase'
    assert str(config.admin_uri) == 'postgresql+psycopg2://admin:***@localhost:5432/'
    assert str(config.complete_uri) == 'postgresql://user:***@localhost:5432/mydatabase'

def test_database_config_invalid_uri():
    with pytest.raises(ValidationError) as excinfo:
        DatabaseSettings(
            uri='invalid_uri',
            admin_username='admin',
            admin_password='strongpassword'
        )
    assert 'relative URL without a base' in str(excinfo.value)

def test_database_config_invalid_pool_size():
    with pytest.raises(ValidationError) as excinfo:
        DatabaseSettings(
            uri='postgresql://user:password@localhost:5432/mydatabase',
            admin_username='admin',
            admin_password='strongpassword',
            pool_size=-1
        )
    assert 'should be greater than 0' in str(excinfo.value)

def test_database_config_invalid_pool_size():
    with pytest.raises(ValidationError) as excinfo:
        DatabaseSettings(
            uri='mysql://user:password@localhost:5432/mydatabase',
            admin_username='admin',
            admin_password='strongpassword',
            pool_size=1
        )

    assert 'URI must start with' in str(excinfo.value)

def test_database_config_invalid_pool_size():
    with pytest.raises(ValidationError) as excinfo:
        DatabaseSettings(
            uri='postgresql://user:password@localhost:5432/mydatabase',
            admin_username='admin',
            admin_password='strongpassword',
            pool_size=-1
        )

    assert 'should be greater than 0' in str(excinfo.value)


def test_valid_index_btree():
    index = Index(
        table_name='my_table', 
        type='btree', 
        columns=['column1', 'column2']
    )
    assert index.type == 'btree'
    assert index.columns == ['column1', 'column2']
    assert index.expression is None
    assert index.condition is None

def test_invalid_index_btree_duplicate_indexes():
    with pytest.raises(ValidationError) as excinfo:
        Index(
            table_name='my_table', 
            type='btree', 
            columns=['column1', 'column1']
        )

    assert "Index cannot have duplicate columns." in str(excinfo.value)


def test_valid_index_expression():
    index = Index(
        table_name='my_table', 
        type='expression', 
        columns=['column1', 'column2'], 
        expression='column1 + column2'
    )
    assert index.type == 'expression'
    assert index.columns == ['column1', 'column2']
    assert index.expression == 'column1 + column2'

def test_valid_index_partial():
    index = Index(
        table_name='my_table', 
        type='partial', 
        columns=['column1'], 
        condition='column1 IS NOT NULL'
    )
    assert index.type == 'partial'
    assert index.columns == ['column1']
    assert index.condition == 'column1 IS NOT NULL'

def test_invalid_index_type():
    with pytest.raises(ValidationError) as exc_info:
        Index(type='invalid_type', columns=['column1'])
    assert "Index type must be one of" in str(exc_info.value)

def test_columns_must_be_list():
    with pytest.raises(ValidationError) as exc_info:
        Index(
            table_name='my_table', 
            type='btree', 
            columns='not_a_list'
        )
    
    assert "Input should be a valid list" in str(exc_info.value)

def test_expression_required_for_expression_index():
    with pytest.raises(ValidationError) as exc_info:
        Index(
            table_name='my_table', 
            type='expression', 
            columns=['column1']
        )
    
    assert "Expression index must include 'expression'." in str(exc_info.value)

def test_condition_required_for_partial_index():
    with pytest.raises(ValidationError) as exc_info:
        Index(
            table_name='my_table', 
            type='partial', 
            columns=['column1']
        )
    
    assert "Partial index must include 'condition'." in str(exc_info.value)


# Example test function to check table visibility
def test_table_data(db_engine):
    with db_engine.connect() as conn:
        query=text("SELECT * FROM public.test_table;")
        result = conn.execute(query).fetchall()

        assert len(result) == 4, "Expected 4 rows in test_table."

def test_database_config_validation():
    # Valid case
    valid_config = DatabaseSettings(
        uri="postgresql://postgres:postgres@localhost:5432/mydb",
        admin_username="postgres",
        admin_password="postgres",
        async_mode=True,
    )
    assert valid_config.db_name == DB_NAME
    assert str(valid_config.admin_uri) == "postgresql+psycopg2://postgres:***@localhost:5432/"

    # Invalid URI
    with pytest.raises(ValidationError) as exc_info:
        DatabaseSettings(
            uri="invalid_uri",
            admin_username="admin",
            admin_password="strong_password",
        )
    assert "Input should be a valid URL" in str(exc_info.value)


@pytest.mark.asyncio
async def test_paginator(session_factory):
    # Initialize paginator with a query
    session = session_factory()
    paginator = Paginator(
        session, 
        "SELECT * FROM public.test_table",
        batch_size=2
    )

    # Fetch and assert paginated results in batches
    expected_batches = [
        [(1, 'Alice'), (2, 'Bob')],
        [(3, 'Charlie'), (4, 'David')]
    ]

    results = []
    
    async for batch in paginator.paginate():
        results.append(batch)

    # Assert total number of batches
    assert len(results) == len(expected_batches)

    # Assert each batch
    for i, expected_batch in enumerate(expected_batches):
        assert results[i] == expected_batch

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
async def test_sync_paginator_batches(session_factory):
    session = session_factory()
    paginator = Paginator(
        conn=session,
        query="SELECT * FROM public.test_table",
        batch_size=2
    )

    batches = []
    async for batch in paginator.paginate():
        batches.append(batch)

    assert len(batches) == 2  # Should have 2 batches of 2 records
    assert batches[0] == [(1, 'Alice'), (2, 'Bob')]
    assert batches[1] == [(3, 'Charlie'), (4, 'David')]

def test_get_batch_query(session_factory):
    session=session_factory()
    
    # Prepare the paginator
    paginator = Paginator(
        conn=session,
        query="SELECT * FROM test_table",
        batch_size=2
    )

    # Test the get_batch_query method
    expected_query = "SELECT * FROM test_table LIMIT :limit OFFSET :offset"
    assert paginator.get_batch_query() == expected_query
    
def test_get_total_count(session_factory):
    session=session_factory()

    # Prepare the paginator
    paginator = Paginator(
        conn=session,
        query="SELECT * FROM test_table",
        batch_size=2
    )
    
    assert paginator._get_total_count() == 4

# Example test for db_name property
def test_db_name():
    config = DatabaseSettings(
        uri="postgresql://localhost/mydb",
        admin_username="admin",
        admin_password="password"
    )
    assert config.db_name == "mydb"