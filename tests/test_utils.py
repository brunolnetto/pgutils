import pytest

from pgutils.utils import validate_postgresql_uri

# Test valid URIs for psycopg
@pytest.mark.parametrize("valid_psycopg_uri", [
    "postgresql://user:password@localhost:5432/mydatabase",
    "postgresql://user:password@127.0.0.1:5432/mydatabase",
    "postgresql://user:password@db.example.com:5432/mydatabase",
    "postgresql://user@localhost:5432/",  # Missing password
    "postgresql://:password@localhost:5432/mydatabase"  # Missing username
])
def test_valid_psycopg_uri(valid_psycopg_uri):
    assert validate_postgresql_uri(valid_psycopg_uri) == valid_psycopg_uri


# Test valid URIs for asyncpg (when allow_async=True)
@pytest.mark.parametrize("valid_asyncpg_uri", [
    "postgresql+asyncpg://user:password@localhost:5432/mydatabase",
    "postgresql+asyncpg://user:password@127.0.0.1:5432/mydatabase",
    "postgresql+asyncpg://localhost:5432/",
])
def test_valid_asyncpg_uri(valid_asyncpg_uri):
    assert validate_postgresql_uri(valid_asyncpg_uri, allow_async=True) == valid_asyncpg_uri


# Test valid URIs for psycopg
@pytest.mark.parametrize("valid_psycopg_uri", [
    "postgresql://user:password@localhost:5432/mydatabase",
    "postgresql://localhost:5432/mydatabase",  # Valid without user and password
    "postgresql://localhost:5432/",  # Missing database
])
def test_valid_psycopg_uri(valid_psycopg_uri):
    assert validate_postgresql_uri(valid_psycopg_uri) == valid_psycopg_uri


# Test invalid prefixes
@pytest.mark.parametrize("invalid_prefix_uri", [
    "mysql://user:password@localhost:5432/dbname",  # Invalid scheme
    "postgresql+invalid://user:password@localhost:5432/dbname",  # Invalid prefix
    "http://user:password@localhost:5432/dbname"  # Completely invalid
])
def test_invalid_prefix(invalid_prefix_uri):
    with pytest.raises(ValueError, match="URI must start with"):
        validate_postgresql_uri(invalid_prefix_uri, allow_async=True)


# Test valid psycopg URI with allow_async=False should raise ValueError for asyncpg
@pytest.mark.parametrize("invalid_async_uri", [
    "postgresql+asyncpg://user:password@localhost:5432/dbname",
    "postgresql+psycopg://user:password@localhost:5432/dbname"
])
def test_invalid_async_uri_when_not_allowed(invalid_async_uri):
    with pytest.raises(ValueError, match="URI must start with one of the following"):
        validate_postgresql_uri(invalid_async_uri, allow_async=False)


# Test invalid URIs with missing components and allow_async=True
@pytest.mark.parametrize("invalid_async_uri", [
    "postgresql+psycopg://user@localhost:5432/dbname",
    "postgresql+psycopg://user@localhost:5432",
    "postgresql+asyncpg://user@localhost:5432/dbname",
    "postgresql+psycopg://:password@localhost:5432/mydatabase",
])
def test_invalid_asyncpg_uris_with_missing_components(invalid_async_uri):
    with pytest.raises(ValueError, match='Both username and password must be provided together.'):
        validate_postgresql_uri(invalid_async_uri, allow_async=True)



# Test invalid URIs with missing components and allow_async=True
@pytest.mark.parametrize("invalid_async_uri", [
    "postgresql://user:password@:5432/dbname",
    "postgresql://user:password@localhost/dbname",
    "postgresql://user:password@localhost",
])
def test_invalid_asyncpg_uris_with_missing_components(invalid_async_uri):
    with pytest.raises(ValueError, match='Port is missing in the URI|'):
        validate_postgresql_uri(invalid_async_uri)


# Test invalid URIs with missing components
@pytest.mark.parametrize("invalid_uri", [
    "postgresql://user:password@:5432/mydatabase",  # Missing host
    "postgresql://localhost:/mydatabase",  # Missing port
    "postgresql://localhost",  # Missing port and database
])
def test_invalid_uris(invalid_uri):
    error_msgs="Host is missing in the URI|Port is missing in the URI|Invalid PostgreSQL URI format."
    with pytest.raises(ValueError, match=error_msgs):
        validate_postgresql_uri(invalid_uri)
