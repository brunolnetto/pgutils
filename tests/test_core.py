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


def test_create_index_statement(sync_database: Database):
    index_query=f"CREATE INDEX IF NOT EXISTS test_table_name_idx ON test_table USING btree (name);"
    assert str(sync_database._create_index_statement('test_table', 'name', 'btree')) == index_query

def test_multi_database_health_check(multidatabase: MultiDatabase):
    health_checks = multidatabase.health_check_all()
    assert all(health_checks.values()), "Health check for all databases should pass."
