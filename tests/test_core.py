import pytest
import asyncio
from pydantic import ValidationError
from typing import Dict

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

def test_mask_sensitive_data(sync_settings: DatabaseSettings):
    db = Database(sync_settings)
    masked_uri = db.mask_sensitive_data()
    assert "***" in masked_uri, "Sensitive data should be masked."


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
    
    tables = sync_database.list_tables()  # Your method to list tables
    assert 'test_table' in tables, "test_table should be listed in the database tables."


@pytest.mark.asyncio
async def test_check_columns_exist(async_database: Database):
    """Test the check_columns_exist function."""
    
    # Create a test table with columns
    table_name = "test_table"
    columns = ["id", "name"]

    # Call the method to check for the column existence
    missing_columns_task = async_database.check_columns_exist(table_name, columns)

    missing_columns = await missing_columns_task

    # Check that no columns are missing since 'id' and 'name' should exist
    assert missing_columns == [], f"Missing columns: {missing_columns}"

    # Now, test with non-existent columns
    non_existent_columns = ["non_existent_col"]
    missing_columns_task = async_database.check_columns_exist(table_name, non_existent_columns)

    missing_columns = await missing_columns_task

    # We expect the non-existent column to be returned as missing
    assert missing_columns == ["non_existent_col"], f"Expected missing column not found: {missing_columns}"

def test_multi_database_health_check(multidatabase: MultiDatabase):
    health_checks = multidatabase.health_check_all()
    assert all(health_checks.values()), "Health check for all databases should pass."
