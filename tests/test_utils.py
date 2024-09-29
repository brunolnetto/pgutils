import pytest
import asyncio
from unittest.mock import patch

from pgutils.utils import validate_postgresql_uri, run_async_method

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
    with pytest.raises(ValueError, match="Invalid URI scheme"):
        validate_postgresql_uri(invalid_prefix_uri, allow_async=True)


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
])
def test_invalid_asyncpg_uris_with_missing_components(invalid_async_uri):
    with pytest.raises(ValueError, match='Port is missing in the URI|'):
        validate_postgresql_uri(invalid_async_uri)


# Test invalid URIs with missing components
@pytest.mark.parametrize("invalid_uri", [
    "postgresql://user:password@:5432/mydatabase",  # Missing host
    "postgresql://localhost:/mydatabase",  # Missing port
])
def test_invalid_uris(invalid_uri):
    error_msgs="Host is missing in the URI|Invalid URI"
    with pytest.raises(ValueError, match=error_msgs):
        validate_postgresql_uri(invalid_uri)


@pytest.mark.asyncio
async def test_run_async_method_no_event_loop():
    """Test running an async method with no current event loop."""
    async def sample_async_method(x):
        return x * 2

    task = run_async_method(sample_async_method, 5)
    
    result = await task
    
    assert result == 10

@pytest.mark.asyncio
async def test_run_async_method_with_running_event_loop():
    """Test running an async method with an existing running event loop."""
    async def sample_async_method(x):
        return x * 2

    # Manually create and run the event loop
    loop = asyncio.get_event_loop()
    task = run_async_method(sample_async_method, 5)
    result = await task
    assert result == 10

@pytest.mark.asyncio
async def test_run_async_method_with_non_async_function():
    """Test calling a synchronous function using run_async_method."""
    async def sample_sync_function(x):
        return x * 2

    task = run_async_method(sample_sync_function, 5)
    
    # Await the task to get the result
    result = await task
    
    assert result == 10

@pytest.mark.asyncio
async def test_run_async_method_with_exception():
    """Test running an async method that raises an exception."""
    async def sample_async_method():
        raise ValueError("An error occurred")

    with pytest.raises(ValueError, match="An error occurred"):
        task = run_async_method(sample_async_method)
        await task

@pytest.mark.asyncio
async def test_run_async_method_with_kwargs():
    """Test running an async method with keyword arguments."""
    async def sample_async_method(x, y):
        return x + y

    task = run_async_method(sample_async_method, 3, y=7)
    
    # Await the task to get the result
    result = await task

    assert result == 10

def test_run_async_method_no_event_loop():
    """Test that a new event loop is created when there is no current loop."""
    async def sample_async_method():
        return 42

    with patch('asyncio.get_event_loop', side_effect=RuntimeError("No current event loop")):
        result = run_async_method(sample_async_method)
        assert result == 42  # Check if the result is as expected