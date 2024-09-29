import pytest
from pydantic import ValidationError

from sqlalchemy import Column, Integer, String
from sqlalchemy.exc import OperationalError

from pgutils.core import DatabaseSettings, Database, MultiDatabase


# Define your table model using the Database's Base
def get_test_table_model(database):
    class TestTable(database.base):
        __tablename__ = 'test_table'
        
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
    
    return TestTable


def test_create_and_drop_tables(sync_database):
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


def test_multi_database_health_check():
    db_configs = {
        "db1": {
            "uri": "postgresql+psycopg://localhost:5432/db1",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": False
        },
        "db2": {
            "uri": "postgresql+asyncpg://localhost:5432/db2",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": True
        }
    }
    multi_db = MultiDatabase(db_configs)
    health_checks = multi_db.health_check_all()
    assert all(health_checks.values()), "Health check for all databases should pass."


def test_mask_sensitive_data(sync_config):
    db = Database(sync_config)
    masked_uri = db.mask_sensitive_data()
    assert "***" in masked_uri, "Sensitive data should be masked."


def test_list_tables(sync_database):
    TestTable = get_test_table_model(sync_database)  # Get the table model
    
    # Create the table if it does not exist
    sync_database.base.metadata.create_all(sync_database.engine)
    
    tables = sync_database.list_tables()  # Your method to list tables
    assert 'test_table' in tables, "test_table should be listed in the database tables."