import pytest
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError
from pgutils.core import DatabaseConfig, Database, MultiDatabase


def test_sync_config(sync_database):
    db = sync_database
    assert db.health_check() is True, "Health check should pass with valid config."


def test_invalid_uri_raises_error(invalid_uri_config):
    with pytest.raises(ValidationError):
        DatabaseConfig(**invalid_uri_config)


def test_invalid_admin_credentials(sync_config):
    sync_config.admin_password = "wrongpassword"
    db = Database(sync_config)

    print(db.admin_uri.password)

    # Indicate that the health check should use the admin URI
    assert db.health_check(use_admin_uri=True) is False, "Health check should fail with incorrect admin credentials."


def test_async_config(async_database):
    db = async_database
    assert db.async_mode is True, "Database should be configured for async mode."
    assert db.health_check() is True, "Async database health check should pass."


def test_create_and_drop_tables(sync_database):
    db = sync_database
    db.create_tables()
    assert db.health_check() is True, "Health check after table creation should pass."
    db.drop_tables()
    assert db.health_check() is True, "Health check after dropping tables should pass."


def test_invalid_pool_size():
    with pytest.raises(ValidationError):
        DatabaseConfig(
            uri="postgresql+psycopg://localhost:5432/mydatabase",
            admin_username="postgres",
            admin_password="postgres",
            async_mode=False,
            pool_size=-5,  # Invalid pool size
            max_overflow=5,
            db_name="mydatabase"
        )


def test_multi_database_health_check(sync_config, async_config):
    db_configs = {
        "db1": {
            "uri": "postgresql+psycopg://localhost:5432/db1",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": False,
            "pool_size": 10,
            "max_overflow": 5
        },
        "db2": {
            "uri": "postgresql+asyncpg://localhost:5432/db2",
            "admin_username": "postgres",
            "admin_password": "postgres",
            "async_mode": True,
            "pool_size": 10,
            "max_overflow": 5
        }
    }
    multi_db = MultiDatabase(db_configs)
    health_checks = multi_db.health_check_all()
    assert all(health_checks.values()), "Health check for all databases should pass."


def test_mask_sensitive_data(sync_config):
    db = Database(sync_config)
    masked_uri = db.mask_sensitive_data()
    assert "***" in masked_uri, "Sensitive data should be masked."


def test_disconnect_all(sync_config):
    db = Database(sync_config)
    db.disconnect()
    assert db.health_check() is False, "Health check should fail after disconnect."


@pytest.mark.asyncio
async def test_async_create_tables(async_config):
    db = Database(async_config)
    db.create_tables()
    assert db.health_check() is True, "Health check after async table creation should pass."
    await db.drop_tables()
    assert db.health_check() is True, "Health check after async table drop should pass."
