import pytest
import asyncio
from pydantic import ValidationError
from typing import Dict
from unittest.mock import patch, MagicMock

from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.exc import OperationalError

from pgutils.core import DatabaseSettings, Database, MultiDatabase


def test_create_and_drop_tables(sync_database: Database):
    db = sync_database
    db.create_tables()
    assert db.health_check() is True, "Health check after table creation should pass."
    db.drop_tables()
    assert db.health_check() is True, "Health check after dropping tables should pass."


def test_invalid_pool_size():
    with pytest.raises(ValidationError):
        DatabaseSettings(
            uri="postgresql+psycopg://localhost:5432/mydatabase",
            admin_username="postgres",
            admin_password="postgres",
            async_mode=False,
            pool_size=-5,  # Invalid pool size
            max_overflow=5,
            db_name="mydatabase"
        )


# Test for check_database_exists
def test_check_database_exists_true(sync_database):
    """Test when the database exists (synchronous)."""
    # Call the method using the sync_database fixture
    result = sync_database.check_database_exists()

    # Check that the method returned True
    assert result is True


def test_check_database_doesnt_exist(database_without_auto_create):
    """Test when an error occurs during the check (synchronous)."""
    # Check that the method returned False due to the error
    db_exists = database_without_auto_create.check_database_exists()
    
    assert db_exists is False
    
    db_exists = database_without_auto_create.check_database_exists('test_db_')

    assert db_exists is False

#def test_drop_database(sync_database: Database):
#    db_name=sync_database.config.db_name
#    db_exists = sync_database.check_database_exists()
#    assert db_exists == True
#
#    sync_database.drop_database_if_exists()
#    db_exists = sync_database.check_database_exists()
#    assert db_exists == False
#
#    sync_database.create_database_if_not_exists(db_name)
#    tables = sync_database.list_tables()
#
#    db_exists = sync_database.check_database_exists()
#    assert db_exists == True


def test_get_parallel_queries(database_without_auto_create):
    queries=database_without_auto_create._get_parallel_queries()
    assert queries == [
        "SET max_parallel_workers = 8;",
        "SET max_parallel_workers_per_gather = 4;",
        "SET min_parallel_table_scan_size = '8MB';",
        "SET min_parallel_index_scan_size = '512kB';"    
    ]

def test_repr(sync_database: Database):
    database_repr = str(sync_database)
    assert "***" in database_repr, "Sensitive data should be masked."
    assert f"async_mode={str(sync_database.async_mode)}" in database_repr, "Async mode must be present"

def test_paginate_sync(sync_database: Database):
    with sync_database.get_session() as session:
        # Fetch and assert paginated results in batches
        expected_batches = [
            [(1, 'Alice'), (2, 'Bob')],
            [(3, 'Charlie'), (4, 'David')]
        ]

        results = []

        query_str="SELECT * FROM test_table"
        for batch in sync_database.paginate(session, query_str, batch_size = 2):
            results.append(batch)

        # Assertion to verify the result
        assert len(results) == 2

def test_query(sync_database: Database):
    results = sync_database.query( "SELECT * FROM test_table")

    # Assertion to verify the result
    assert len(results) == 4

def test_list_columns(sync_database: Database):
    results = sync_database.list_columns('test_table')

    # Assertion to verify the result
    assert results == [('id', ), ('name', )]

def test_columns_exist_sync(sync_database: Database):
    assert sync_database.column_exists('test_table', 'name')
    
def test_columns_exist_async(async_database: Database):
    assert async_database.column_exists('test_table', 'id')

def test_list_schemas(sync_database: Database):
    results = sync_database.list_schemas()
    
    # Assertion to verify the result
    assert results == [
        ('pg_toast', ), 
        ('pg_catalog', ), 
        ('public', ), 
        ('information_schema', )
    ]

def test_list_views(sync_database: Database):
    results = sync_database.list_views('public')

    # Assertion to verify the result
    assert results == []

def test_list_sequences(sync_database: Database):
    results = sync_database.list_sequences()

    # Assertion to verify the result
    assert results == [('test_table_id_seq', )]

def test_list_constraints(sync_database: Database):
    results = sync_database.list_constraints('test_table')
    print(results)
    # Assertion to verify the result
    assert results == [('test_table_pkey', 'PRIMARY KEY', 'test_table', 'id', 'test_table', 'id')]

def test_list_triggers(sync_database: Database):
    results = sync_database.list_triggers('test_table')

    # Assertion to verify the result
    assert results == []

def test_list_functions(sync_database: Database):
    results = sync_database.list_functions()

    # Assertion to verify the result
    assert len(results) > 0

def test_list_procedures(sync_database: Database):
    results = sync_database.list_procedures()

    # Assertion to verify the result
    assert results == []

def test_list_materialized_views(sync_database: Database):
    results = sync_database.list_materialized_views()

    # Assertion to verify the result
    assert results == []

def test_list_types(sync_database: Database):
    results = sync_database.list_types()

    # Assertion to verify the result
    assert results == []

def test_list_roles(sync_database: Database):
    results = sync_database.list_roles()

    # Assertion to verify the result
    assert len(results) > 0

def test_list_extensions(sync_database: Database):
    results = sync_database.list_extensions()

    # Assertion to verify the result
    assert len(results) == 1

def test_list_tables(sync_database: Database):
    # Define your table model using the Database's Base
    def get_test_table_model(database: Database):
        class TestTable(database.base):
            __tablename__ = 'test_table'
            
            id = Column(Integer, primary_key=True)
            name = Column(String, nullable=False)
        
        return TestTable

    TestTable = get_test_table_model(sync_database)  # Get the table model
    
    # Create the table if it does not exist
    sync_database.base.metadata.create_all(sync_database.engine)
    
    tables = sync_database.list_tables()
    
    assert ('test_table', ) in tables, "test_table should be listed in the database tables."


def test_create_index_statement(sync_database: Database):
    index_query=f"CREATE INDEX IF NOT EXISTS test_table_name_idx ON test_table USING btree (name);"
    assert str(sync_database._create_index_statement('test_table', 'name', 'btree')) == index_query


def test_multi_database_health_check(multidatabase: MultiDatabase):
    health_checks = multidatabase.health_check_all()
    assert all(health_checks.values()), "Health check for all databases should pass."


def test_list_tables_async(async_database: Database):
    tables = async_database.list_tables()
    
    assert tables == [('test_table', )]

def test_list_schemas_sync(sync_database: Database):
    schemas = sync_database.list_schemas()
    assert schemas == [
        ('pg_toast', ), 
        ('pg_catalog', ), 
        ('public', ), 
        ('information_schema', )
    ]

def test_list_schemas_async(async_database: Database):
    schemas = async_database.list_schemas()

    assert schemas == [
        ('pg_toast', ), 
        ('pg_catalog', ), 
        ('public',), 
        ('information_schema',)
    ]


def test_list_triggers_sync(sync_database: Database):
    triggers = sync_database.list_triggers('test_table')

    assert triggers == []

def test_list_triggers_async(async_database: Database):
    triggers = async_database.list_triggers('test_table')
    
    assert triggers == []

def test_list_indexes_sync(sync_database: Database):
    indexes = sync_database.list_indexes('test_table')
    assert indexes == [('test_table_pkey', )]
    index_exists = sync_database._index_exists('test_table', 'test_table_pkey')
    assert index_exists == True

def test_list_indexes_async(async_database: Database):
    indexes = async_database.list_indexes('test_table')
    assert indexes == [('test_table_pkey', )]

    index_exists = async_database._index_exists('test_table', 'test_table_pkey')
    assert index_exists == True