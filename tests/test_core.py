import pytest
from pydantic import ValidationError
from typing import Dict

from sqlalchemy import Column, Integer, String

from pgbase.core import AsyncDatabase, Datasource
from pgbase.models import DatabaseSettings, DatasourceSettings, TableConstraint, ColumnIndex
from pgbase.utils import mask_sensitive_data, is_entity_name_valid

from .conftest import DatasourceSettingsFactory

# Dummy test to initialize module scoped Datasource
def test_database_initialization(datasource: Datasource):
    assert 1==1


@pytest.mark.asyncio
async def test_create_and_drop_tables(async_database: AsyncDatabase):
    db = async_database
    await db.create_tables()
    assert await db.health_check() is True, "Health check after table creation should pass."
    await db.drop_tables()
    assert await db.health_check() is True, "Health check after dropping tables should pass."


@pytest.mark.asyncio
async def test_create_and_drop_tables_with_admin(async_database: AsyncDatabase):
    db = async_database
    await db.create_tables()
    assert await db.health_check(use_admin_uri=True) is True, "Health check after table creation should pass."
    await db.drop_tables()
    assert await db.health_check(use_admin_uri=True) is True, "Health check after dropping tables should pass."


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


def test_check_database_exists_true(async_database: AsyncDatabase):
    """Test when the AsyncDatabase exists (synchronous)."""
    # Check that the method returned True
    assert async_database.database_exists() is True


def test_check_database_doesnt_exist(database_without_auto_create: AsyncDatabase):
    """Test when an error occurs during the check (synchronous)."""
    db=database_without_auto_create
    
    # Check that the method returned False due to the error
    db.drop_database()
    assert db.database_exists() is False

    assert db.database_exists('test_db_') is False

    db.create_database()
    assert db.database_exists() is True

    db.drop_database()
    assert db.database_exists() is False


def test_check_database_doesnt_exist_without_db_name(database_without_db_name: AsyncDatabase):
    """Test when an error occurs during the check (synchronous)."""
    # Check that the method returned False due to the error    
    assert database_without_db_name.database_exists() is False


def test_repr_sync_database(async_database: AsyncDatabase):
    AsyncDatabase_repr = str(async_database)
    assert "***" in AsyncDatabase_repr, "Sensitive data should be masked."


@pytest.mark.asyncio
async def test_paginate_sync(async_database: AsyncDatabase):
    # Fetch and assert paginated results in batches
    results = []

    async with async_database.get_session() as session:
        query_str="SELECT name FROM test_table"
        async for batch in async_database.paginate(session, query_str, batch_size = 2):
            results.append(batch)

        # Assertion to verify the result
        assert len(results) == 2


def test_datasource_repr(datasource: Datasource):
    expected=f"Datasource({datasource.databases.keys()})"

    # Assertion to verify the result
    assert datasource.__repr__() == expected


@pytest.mark.asyncio
async def test_query(async_database: AsyncDatabase):
    query_str="SELECT * FROM test_table"
    async_results = await async_database.execute(query_str)
    assert len(async_results) == 4


@pytest.mark.asyncio
async def test_list_columns(async_database: AsyncDatabase, datasource: Datasource):
    expected=['id', 'name']
    
    async_results = await async_database.list_columns('test_table')
    assert async_results == expected
    
    ds_results = await datasource.list_columns('db1', 'public', 'test_table')
    assert ds_results == expected

@pytest.mark.asyncio
async def test_columns_exists(async_database: AsyncDatabase, datasource: Datasource):
    assert await async_database.column_exists('public', 'test_table', 'name')
    assert await datasource.column_exists('db1', 'public', 'test_table', 'name')


@pytest.mark.asyncio
async def test_list_views(async_database: AsyncDatabase, datasource: Datasource):
    expected = []
    async_results = await async_database.list_views('public')
    assert async_results == expected

    ds_results = await datasource.list_views('db1', 'public')
    assert ds_results == expected


@pytest.mark.asyncio
async def test_list_constraints(async_database: AsyncDatabase, datasource: Datasource):
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

    async_results = await async_database.list_constraints('test_table')
    assert async_results == expected
    
    ds_results = await datasource.list_constraints('db1', 'public', 'test_table')    
    assert ds_results == expected


@pytest.mark.asyncio
async def test_list_sequences(async_database: AsyncDatabase, datasource: Datasource):
    expected={'test_table_id_seq'}
    async_results = await async_database.list_sequences()
    assert set(async_results) == expected
    
    ds_results = await datasource.list_sequences('db1')
    assert set(ds_results) == expected


@pytest.mark.asyncio
async def test_audit_trigger(async_database: AsyncDatabase, datasource: Datasource):
    await async_database.add_audit_trigger('test_table')
    async_results = await async_database.list_triggers('test_table')
    assert len(async_results) == 3

    await datasource.get_database('db2').add_audit_trigger('test_table')
    ds_results = await datasource.list_triggers('db1', 'test_table')
    assert len(ds_results) == 3

@pytest.mark.asyncio
async def test_audit_trigger_with_error(async_database: AsyncDatabase):
    with pytest.raises(ValueError, match='Invalid table name'):
        await async_database.add_audit_trigger('invalid table name')

@pytest.mark.asyncio
async def test_list_functions(async_database: AsyncDatabase, datasource: Datasource):
    async_results = await async_database.list_functions()
    assert len(async_results) > 0

    ds_results = await datasource.list_functions('db1')
    
    assert len(ds_results) > 0


@pytest.mark.asyncio
async def test_list_procedures(async_database: AsyncDatabase, datasource: Datasource):
    expected = []
    async_results = await async_database.list_procedures()
    assert async_results == expected
    
    ds_results = await datasource.list_procedures('db1')
    assert ds_results == expected


@pytest.mark.asyncio
async def test_list_materialized_views(
    async_database: AsyncDatabase,
    datasource: Datasource
):
    expected = []
    async_results = await async_database.list_materialized_views()
    assert async_results == expected
    
    ds_results = await datasource.list_materialized_views('db1')
    assert ds_results == expected


@pytest.mark.asyncio
async def test_list_types(async_database: AsyncDatabase, datasource: Datasource):
    expected = []
    async_results = await async_database.list_types()
    assert async_results == expected
    
    ds_results = await datasource.list_types('db1')    
    assert ds_results == expected


@pytest.mark.asyncio
async def test_list_roles(async_database: AsyncDatabase, datasource: Datasource):
    async_results = await async_database.list_roles()
    assert len(async_results) > 0
    
    ds_results = await datasource.list_roles('db1')
    assert len(ds_results) > 0


@pytest.mark.asyncio
async def test_list_extensions(async_database: AsyncDatabase, datasource: Datasource):
    async_results = await async_database.list_extensions()
    assert len(async_results) == 1
    
    ds_results = await datasource.list_extensions('db1')
    assert len(ds_results) == 1


@pytest.mark.asyncio
async def test_list_tables(async_database: AsyncDatabase, datasource: Datasource):
    # Define your table model using the AsyncDatabase's Base
    def add_test_table_model(database: AsyncDatabase):
        class TestTable(database.base):
            __tablename__ = 'test_table'
            
            id = Column(Integer, primary_key=True)
            name = Column(String, nullable=False)
        
        return TestTable

    # Get the table model
    add_test_table_model(async_database)

    # Create the table if it does not exist
    await async_database.create_tables()
    ds_db1_tables=await datasource.list_tables('db1', 'public')
    ds_db2_tables=await datasource.list_tables('db2', 'public')

    async_tables = await async_database.list_tables()
    
    assert 'test_table' in async_tables, \
        "test_table should be listed in the async AsyncDatabase tables."
    assert 'test_table' in ds_db1_tables, \
        "test_table should be listed in the async AsyncDatabase tables."
    assert 'test_table' in ds_db2_tables, \
        "test_table should be listed in the async AsyncDatabase tables."


@pytest.mark.asyncio
async def test_create_indexes_async(async_database: AsyncDatabase):
    """Test the create_indexes method for sync."""
    indexes = [
        ColumnIndex(
            table_name='test_table', 
            column_names=['name'], 
            type='btree'
        )
    ]

    # Run the method
    await async_database.create_indexes(indexes)
    indexes=await async_database.list_indexes('test_table')
    assert len(indexes) == 2

    assert await async_database.create_indexes([]) is None

    invalid_indexes = [
        ColumnIndex(
            table_name='test_table', 
            column_names=['invalid'], 
            type='btree'
        )
    ]

    with pytest.raises(ValueError):
        await async_database.create_indexes(invalid_indexes)


@pytest.mark.asyncio
async def test_list_schemas(
    async_database: AsyncDatabase,
    datasource: Datasource
):
    async_schemas = await async_database.list_schemas()

    expected = [ 'pg_toast', 'pg_catalog', 'public', 'information_schema' ]

    assert async_schemas == expected

    ds_db1_schemas=await datasource.list_schemas('db1')
    ds_db2_schemas=await datasource.list_schemas('db2')

    assert ds_db1_schemas == expected
    assert ds_db2_schemas == expected


@pytest.mark.asyncio
async def test_list_indexes(
    async_database: AsyncDatabase,
    datasource: Datasource
):
    async_indexes = await async_database.list_indexes('test_table')
    assert len(async_indexes) == 2

    async_index_exists = await async_database._index_exists('test_table', 'test_table_pkey')
    assert async_index_exists is True

    ds_db1_indexes=await datasource.list_indexes('db1', 'test_table')
    ds_db2_indexes=await datasource.list_indexes('db2', 'test_table')

    assert len(ds_db1_indexes) == 2
    assert len(ds_db2_indexes) == 1

@pytest.mark.asyncio
async def test_multi_datasource_health_check(datasource: Datasource):
    health_checks = await datasource.health_check_all()
    assert all(health_checks.values()), "Health check for all AsyncDatabases should pass."


def test_multi_datasource_get_database(
    datasource_settings: Dict[str, DatasourceSettings], datasource: Datasource, 
):
    gotten_database: AsyncDatabase = datasource.get_database('db1')
    assert gotten_database.settings.name == 'db1', "Health check for all AsyncDatabases should pass."


def test_multi_datasource_get_item(
    datasource_settings: Dict[str, DatasourceSettings], datasource: Datasource
):
    gotten_database: AsyncDatabase = datasource['db1']
    assert gotten_database.settings.name == 'db1', "Health check for all AsyncDatabases should pass."


def test_multi_datasource_get_database_with_exception(datasource: Datasource):
    with pytest.raises(KeyError):
        datasource.get_database('db3')


@pytest.mark.parametrize(
    "table_name, expected_result", [
        ("table-name", False),                      # Hyphen in the name
        ("123table", False),                        # Starts with a number
        ("table!name", False),                      # Special character '!'
        ("table_name; DROP TABLE users;", False),   # SQL injection attempt
        ("table_name'; --", False),                 # SQL injection with comment
        ("table_name' OR '1'='1", False),           # SQL injection OR logic
        (" ", False),                               # Blank name (space)
        ("SELECT", False),                          # SQL keyword
        ("__", False),                              # Only underscores
        ("1tablename", False),                      # Starts with a number
        ("@tablename", False),                      # Starts with a special character
        ("valid_table_name", True)                  # Valid name, should return True
    ]
)
def test_is_valid_table_name(async_database: AsyncDatabase, table_name: str, expected_result: bool):
    """
    Test the _is_valid_table_name method with various invalid table names.
    
    Args:
        sync_database (Database): Fixture that provides a AsyncDatabase instance.
        table_name (str): The table name to validate.
        expected_result (bool): The expected result of the validation (True/False).
    """
    # Assuming `sync_database` has a method _is_valid_table_name
    assert is_entity_name_valid(table_name, 'table') == expected_result


def test_factory_boy_example():
    datasource = DatasourceSettingsFactory(name="ds1")
    assert datasource.name == 'ds1'
    assert len(datasource.databases) == 2
    assert 'db' in datasource.databases[0].name 
    assert 'db' in datasource.databases[1].name


def test_corrupted_datasource_settings():
    """Test corrupted DatasourceSettings raises ValueError."""
    with pytest.raises(ValueError):
        DatasourceSettings(
            name="Corrupted datasource object",
            AsyncDatabases=[],
            description="Datasource with invalid configuration"
        )


def test_datasource_settings_representation(datasource_settings: DatasourceSettings):
    """Test the string representation of DatasourceSettings."""
    datasource_repr = f"<DataSourceSettings(name={datasource_settings.name}, databases=2)>"
    assert datasource_settings.__repr__() == datasource_repr


def test_initialization(mock_data_grid, mock_logger):
    """Test that DataGrid initializes correctly with valid settings."""
    assert len(mock_data_grid.datasources) == 2
    assert "ds1" in mock_data_grid.datasources
    assert "ds2" in mock_data_grid.datasources
    mock_logger.info.assert_called()


def test_get_datasource(mock_data_grid):
    """Test that get_datasource returns the correct instance."""
    datasource = mock_data_grid.get_datasource("ds1")
    assert datasource.name == "ds1"


def test_datagrid_repr(mock_data_grid):
    """Test that get_datasource returns the correct instance."""
    expected="<DataGrid(datasources=['ds1', 'ds2'])>"
    assert mock_data_grid.__repr__() == expected


def test_get_datasource_not_found(mock_data_grid):
    """Test that get_datasource raises KeyError for non-existent datasource."""
    with pytest.raises(KeyError, match="Datasource 'unknown' not found."):
        mock_data_grid.get_datasource("unknown")


@pytest.mark.asyncio
async def test_health_check_all(mock_data_grid, mock_datasource):
    """Test health check for all datasources."""
    mock_datasource.health_check_all.return_value = {"db1": True, "db2": True}
    mock_data_grid.datasources["ds1"] = mock_datasource
    mock_data_grid.datasources["ds2"] = mock_datasource

    results = await mock_data_grid.health_check_all()

    assert results["ds1"] == {"db1": True, "db2": True}
    assert results["ds2"] == {"db1": True, "db2": True}


@pytest.mark.asyncio
async def test_health_check_all_error(mock_data_grid, mock_datasource, mock_logger):
    """Test health check logs errors on failure."""
    mock_datasource.health_check_all.side_effect = Exception("Health check failed")
    mock_data_grid.datasources["ds1"] = mock_datasource

    results = await mock_data_grid.health_check_all()

    assert results["ds1"] == {'error': 'Health check failed'}
    assert "Health check failed" in str(mock_logger.error.call_args)


@pytest.mark.asyncio
async def test_create_tables_all(mock_data_grid, mock_datasource):
    """Test disconnect_all method."""
    mock_data_grid.datasources["ds1"] = mock_datasource
    await mock_data_grid.create_tables_all()
    mock_datasource.create_tables_all.assert_called()


@pytest.mark.asyncio
async def test_disconnect_all(mock_data_grid, mock_datasource):
    """Test disconnect_all method."""
    mock_data_grid.datasources["ds1"] = mock_datasource
    await mock_data_grid.disconnect_all()
    mock_datasource.disconnect_all.assert_called()


def test_create_database_if_not_exists(test_db):
    result = test_db.create_database("test_db")
    assert result == "Database test_db created."


def test_check_database_exists(test_db):
    assert test_db.database_exists("test_db") is True


def test_execute(test_db):
    result = test_db.execute("SELECT 1;")
    assert result == "Query executed."


@pytest.mark.asyncio
async def test_list_tables_testdatabase(test_db):
    result = await test_db.list_tables()
    assert result == ["table1", "table2"]


@pytest.mark.asyncio
async def test_list_extensions_testdatabase(test_db):
    result = await test_db.list_extensions()
    assert result == ["extension1", "extension2"]


def test_database_repr(test_db):
    db_repr = test_db.__repr__()
    masked_credentials=mask_sensitive_data(test_db.uri)
    assert db_repr == f"<Database(uri={masked_credentials})>"


