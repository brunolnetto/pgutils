import pytest
import asyncio
from pydantic import ValidationError
from typing import Dict
from unittest.mock import patch, MagicMock

from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.exc import OperationalError

from pgutils.core import (
    DatabaseSettings, 
    DatasourceSettings, 
    TableConstraint, 
    Database, 
    Datasource, 
    MultiDatasource
)


def test_create_and_drop_tables(sync_database: Database):
    db = sync_database
    db.create_tables()
    assert db.health_check() is True, "Health check after table creation should pass."
    db.drop_tables()
    assert db.health_check() is True, "Health check after dropping tables should pass."

def test_create_and_drop_tables(async_database: Database):
    db = async_database
    db.create_tables()
    assert db.health_check() is True, "Health check after table creation should pass."
    db.drop_tables()
    assert db.health_check() is True, "Health check after dropping tables should pass."


def test_create_and_drop_tables_with_admin(sync_database: Database):
    db = sync_database
    db.create_tables()
    assert db.health_check(use_admin_uri=True) is True, "Health check after table creation should pass."
    db.drop_tables()
    assert db.health_check(use_admin_uri=True) is True, "Health check after dropping tables should pass."


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

def test_query_sync(sync_database: Database):
    results = sync_database.query( "SELECT * FROM test_table")

    # Assertion to verify the result
    assert len(results) == 4

def test_query_async(async_database: Database):
    results = async_database.query( "SELECT * FROM test_table")

    # Assertion to verify the result
    assert len(results) == 4

def test_list_columns(sync_database: Database, async_database: Database):
    sync_results = sync_database.list_columns('test_table')
    async_results = async_database.list_columns('test_table')

    # Assertion to verify the result
    assert sync_results == ['id', 'name']
    assert async_results == ['id', 'name']

def test_columns_exist(
    sync_database: Database,
    async_database: Database
):
    assert sync_database.column_exists('test_table', 'name')
    assert async_database.column_exists('test_table', 'name')
    

def test_list_schemas(sync_database: Database, async_database: Database):
    sync_results = sync_database.list_schemas()
    async_results = async_database.list_schemas()
    
    # Assertion to verify the result
    expected=[ 'pg_toast', 'pg_catalog', 'public', 'information_schema' ]
    assert sync_results == expected
    assert async_results == expected

def test_list_views(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    expected = []
    
    sync_results = sync_database.list_views('public')
    async_results = async_database.list_views('public')

    # Assertion to verify the result
    assert sync_results == expected
    assert async_results == expected

    ds_results = datasource.list_views('db1', 'public')
    assert ds_results == expected

def test_list_constraints(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    # Assertion to verify the result
    expected=[
        TableConstraint(
            constraint_name='test_table_pkey', 
            constraint_type='PRIMARY KEY', 
            table_name='test_table', 
            column_name='id', 
            foreign_table_name='test_table', 
            foreign_column_name='id'
        )
    ]
    
    sync_results = sync_database.list_constraints('test_table')
    async_results = async_database.list_constraints('test_table')

    assert async_results == expected
    assert sync_results == expected
    
    ds_results = datasource.list_constraints('db1', 'public', 'test_table')
    
    assert ds_results == expected

def test_list_sequences(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    expected={'test_table_id_seq', 'test_table_audit_id_seq'}
    
    sync_results = sync_database.list_sequences()
    async_results = sync_database.list_sequences()

    # Assertion to verify the result
    assert set(sync_results) == expected
    assert set(async_results) == expected
    
    ds_results = datasource.list_sequences('db1')
    assert set(ds_results) == expected

def test_audit_trigger(sync_database: Database, async_database: Database):
    sync_database.add_audit_trigger('test_table')
    async_database.add_audit_trigger('test_table')
    
    sync_results = sync_database.list_triggers('test_table')
    async_results = async_database.list_triggers('test_table')

    # Assertion to verify the result
    assert len(sync_results) == 3
    assert len(async_results) == 3

def test_audit_trigger_with_error(sync_database: Database):
    with pytest.raises(ValueError, match='Invalid table name provided.'):
        sync_database.add_audit_trigger('invalid table name')

def test_list_functions(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    sync_results = sync_database.list_functions()
    async_results = async_database.list_functions()

    # Assertion to verify the result
    assert len(sync_results) > 0
    assert len(async_results) > 0

    ds_results = datasource.list_functions('db1')
    
    assert len(ds_results) > 0

def test_list_procedures(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    expected = []
    
    sync_results = sync_database.list_procedures()
    async_results = async_database.list_procedures()

    # Assertion to verify the result
    assert sync_results == expected
    assert async_results == expected
    
    ds_results = datasource.list_procedures('db1')
    
    assert ds_results == expected

def test_list_materialized_views(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    expected = []
    
    sync_results = sync_database.list_materialized_views()
    async_results = sync_database.list_materialized_views()

    # Assertion to verify the result
    assert sync_results == expected
    assert async_results == expected
    
    ds_results = datasource.list_materialized_views('db1')
    
    assert ds_results == expected

def test_list_types(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    expected = []

    sync_results = sync_database.list_types()
    async_results = async_database.list_types()

    # Assertion to verify the result
    assert sync_results == expected
    assert async_results == expected
    
    ds_results = datasource.list_types('db1')
    
    assert ds_results == expected

def test_list_roles(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    sync_results = sync_database.list_roles()
    async_results = async_database.list_roles()

    # Assertion to verify the result
    assert len(sync_results) > 0
    assert len(async_results) > 0
    
    ds_results = datasource.list_roles('db1')
    
    assert len(ds_results) > 0

def test_list_extensions(
    sync_database: Database, async_database: Database):
    sync_results = sync_database.list_extensions()
    async_results = sync_database.list_extensions()

    # Assertion to verify the result
    assert len(sync_results) == 1
    assert len(async_results) == 1

def test_list_tables(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    # Define your table model using the Database's Base
    def get_test_table_model(database: Database):
        class TestTable(database.base):
            __tablename__ = 'test_table'
            
            id = Column(Integer, primary_key=True)
            name = Column(String, nullable=False)
        
        return TestTable

    # Get the table model
    sync_test_table = get_test_table_model(sync_database)
    async_test_table = get_test_table_model(async_database)

    # Create the table if it does not exist
    sync_database.create_tables()
    async_database.create_tables()
    ds_db1_tables=datasource.list_tables('db1', 'public')
    ds_db2_tables=datasource.list_tables('db2', 'public')

    sync_tables = sync_database.list_tables()
    async_tables = async_database.list_tables()
    
    assert 'test_table' in sync_tables, \
        "test_table should be listed in the sync database tables."
    assert 'test_table' in async_tables, \
        "test_table should be listed in the async database tables."
    assert 'test_table' in ds_db1_tables, \
        "test_table should be listed in the async database tables."
    assert 'test_table' in ds_db2_tables, \
        "test_table should be listed in the async database tables."
    


def test_list_schemas(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    sync_schemas = sync_database.list_schemas()
    async_schemas = async_database.list_schemas()
    
    expected = [ 'pg_toast', 'pg_catalog', 'public', 'information_schema' ]
    
    assert sync_schemas == expected
    assert async_schemas == expected
    
    ds_db1_schemas=datasource.list_schemas('db1')
    ds_db2_schemas=datasource.list_schemas('db2')
    
    assert ds_db1_schemas == expected
    assert ds_db2_schemas == expected

def test_list_indexes(
    sync_database: Database, 
    async_database: Database,
    datasource: Datasource
):
    sync_indexes = sync_database.list_indexes('test_table')
    async_indexes = sync_database.list_indexes('test_table')

    expected=['test_table_pkey']
    
    assert async_indexes == expected
    assert sync_indexes == expected

    sync_index_exists = sync_database._index_exists('test_table', 'test_table_pkey')
    async_index_exists = sync_database._index_exists('test_table', 'test_table_pkey')
    
    assert sync_index_exists == True
    assert async_index_exists == True
    
    ds_db1_indexes=datasource.list_indexes('db1', 'test_table')
    ds_db2_indexes=datasource.list_indexes('db2', 'test_table')
    
    assert ds_db2_indexes == expected
    assert ds_db1_indexes == expected

def test_multi_datasource_health_check(datasource: Datasource):
    health_checks = datasource.health_check_all()
    assert all(health_checks.values()), "Health check for all databases should pass."

def test_multi_datasource_get_database(
    datasource: Datasource, 
    datasource_settings: Dict[str, DatasourceSettings]
):
    gotten_database: Database = datasource.get_database('db1')
    assert gotten_database.name == 'db1', "Health check for all databases should pass."

def test_multi_datasource_get_item(
    datasource: Datasource, 
    datasource_settings: Dict[str, DatasourceSettings]
):
    gotten_database: Database = datasource['db1']
    assert gotten_database.name == 'db1', "Health check for all databases should pass."

def test_multi_datasource_get_database_with_exception(datasource: Datasource):
    with pytest.raises(KeyError):
        datasource.get_database('db3')

@pytest.mark.parametrize(
    "table_name, expected_result", [
        ("table-name", False),               # Hyphen in the name
        ("123table", False),                 # Starts with a number
        ("table!name", False),               # Special character '!'
        ("table_name; DROP TABLE users;", False),  # SQL injection attempt
        ("table_name'; --", False),          # SQL injection with comment
        ("table_name' OR '1'='1", False),    # SQL injection OR logic
        (" ", False),                        # Blank name (space)
        ("SELECT", False),                   # SQL keyword
        ("__", False),                       # Only underscores
        ("1tablename", False),               # Starts with a number
        ("@tablename", False),               # Starts with a special character
        ("valid_table_name", True)           # Valid name, should return True
    ]
)
def test_is_valid_table_name(sync_database: Database, table_name: str, expected_result: bool):
    """
    Test the _is_valid_table_name method with various invalid table names.
    
    Args:
        sync_database (Database): Fixture that provides a database instance.
        table_name (str): The table name to validate.
        expected_result (bool): The expected result of the validation (True/False).
    """
    # Assuming `sync_database` has a method _is_valid_table_name
    assert sync_database._is_valid_table_name(table_name) == expected_result


def test_corrupted_datasource_settings():
    # Create the DatasourceSettings fixture
    with pytest.raises(ValueError):
        DatasourceSettings(
            name="Corrupted datasource object",
            databases=[],
            description="Datasource with both sync and async databases"
        )

def test_same_database_database_settings(same_database_settings):
    # Create the DatasourceSettings fixture
    with pytest.raises(ValueError):
        DatasourceSettings(
            name="Corrupted datasource object",
            databases=list(same_database_settings.values()),
            description="Datasource with both sync and async databases"
        )

def test_datasource_representation(datasource_settings: DatasourceSettings):
    datasource_repr=f"<DataSourceSettings(name={datasource_settings.name}, databases=2)>"
    assert datasource_settings.__repr__() == datasource_repr
