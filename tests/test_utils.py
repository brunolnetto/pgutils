import pytest
import asyncio
from unittest.mock import patch

from unittest.mock import AsyncMock

from pgbase.core import AsyncDatabase
from pgbase.utils import (
    validate_postgresql_uri,
    mask_sensitive_data,
    validate_entity_name,
    is_entity_name_valid,
    retry_async,
)


# Test valid URIs for psycopg
@pytest.mark.parametrize(
    "valid_psycopg_uri",
    [
        "postgresql://user:password@localhost:5432/mydatabase",
        "postgresql://user:password@127.0.0.1:5432/mydatabase",
        "postgresql://user:password@db.example.com:5432/mydatabase",
    ],
)
def test_valid_psycopg_uri(valid_psycopg_uri):
    assert validate_postgresql_uri(valid_psycopg_uri) == valid_psycopg_uri


# Test valid URIs for asyncpg (when allow_async=True)
@pytest.mark.parametrize(
    "valid_asyncpg_uri",
    [
        "postgresql+asyncpg://user:password@localhost:5432/mydatabase",
        "postgresql+asyncpg://user:password@127.0.0.1:5432/mydatabase",
        "postgresql+asyncpg://localhost:5432/",
    ],
)
def test_valid_asyncpg_uri(valid_asyncpg_uri):
    assert validate_postgresql_uri(valid_asyncpg_uri, allow_async=True) == valid_asyncpg_uri


# Test invalid prefixes
@pytest.mark.parametrize(
    "invalid_prefix_uri",
    [
        "mysql://user:password@localhost:5432/dbname",  # Invalid scheme
        "postgresql+invalid://user:password@localhost:5432/dbname",  # Invalid prefix
        "http://user:password@localhost:5432/dbname",  # Completely invalid
    ],
)
def test_invalid_prefix(invalid_prefix_uri):
    with pytest.raises(ValueError, match="Invalid URI scheme"):
        validate_postgresql_uri(invalid_prefix_uri, allow_async=True)


# Test invalid URIs with missing components and allow_async=True
@pytest.mark.parametrize(
    "invalid_async_uri",
    [
        "postgresql+psycopg://user@localhost:5432/dbname",
        "postgresql+psycopg://user@localhost:5432",
        "postgresql+asyncpg://user@localhost:5432/dbname",
        "postgresql+psycopg://:password@localhost:5432/mydatabase",
        "postgresql://user@localhost:5432/",
        "postgresql://:password@localhost:5432/mydatabase",
    ],
)
def test_invalid_asyncpg_uris_with_missing_components(invalid_async_uri):
    with pytest.raises(ValueError, match="Both username and password must be provided together."):
        validate_postgresql_uri(invalid_async_uri, allow_async=True)


# Test invalid URIs with missing components
@pytest.mark.parametrize(
    "invalid_uri",
    [
        "postgresql://user:password@:5432/mydatabase",  # Missing host
        "postgresql://localhost:/mydatabase",  # Missing port
    ],
)
def test_invalid_uris(invalid_uri):
    error_msgs = "Host is missing in the URI|Invalid URI"
    with pytest.raises(ValueError, match=error_msgs):
        validate_postgresql_uri(invalid_uri)


# Test cases for is_valid_schema_name
@pytest.mark.parametrize(
    "schema_name,expected",
    [
        ("valid_schema", True),  # Valid schema name
        ("_underscore_start", True),  # Valid schema name starting with underscore
        ("valid123", True),  # Valid schema name with numbers
        ("123invalid", False),  # Invalid: starts with digit
        ("invalid-schema", False),  # Invalid: contains hyphen
        ("invalid schema", False),  # Invalid: contains space
        ("", False),  # Invalid: empty string
        ("valid_schema_with_length_63_", True),  # Valid schema with max length
        ("1_invalid_start", False),  # Invalid: starts with a digit
    ],
)
def test_is_valid_schema_name(schema_name, expected):
    assert is_entity_name_valid(schema_name, "schema") == expected


# Test cases for validate_schema_name
@pytest.mark.parametrize(
    "schema_name",
    [
        "valid_schema",
        "_underscore_start",
        "valid123",
    ],
)
def test_validate_entity_name_valid(schema_name):
    # Should not raise an exception for valid schema names
    validate_entity_name(schema_name, "schema")


@pytest.mark.parametrize(
    "schema_name",
    [
        "123invalid",
        "invalid-schema",
        "invalid schema",
        "",
    ],
)
def test_validate_schema_name_invalid(schema_name):
    # Should raise ValueError for invalid schema names
    with pytest.raises(ValueError, match=r"Invalid schema name: .*"):
        validate_entity_name(schema_name, "schema")


def test_missing_username():
    # Case 1: Missing username but password is provided
    uri = "postgresql://:password@localhost/dbname"
    with pytest.raises(
        ValueError, match="Both username and password must be provided together, or neither."
    ):
        validate_postgresql_uri(uri)


def test_missing_password():
    # Case 2: Missing password but username is provided
    uri = "postgresql://username:@localhost/dbname"
    with pytest.raises(
        ValueError, match="Both username and password must be provided together, or neither."
    ):
        validate_postgresql_uri(uri)


def test_valid_username_and_password():
    # Case 3: Both username and password are provided correctly
    uri = "postgresql://username:password@localhost/dbname"
    assert validate_postgresql_uri(uri) == uri


def test_no_username_or_password():
    # Case 4: Neither username nor password is provided
    uri = "postgresql://localhost/dbname"
    assert validate_postgresql_uri(uri) == uri


def test_mask_sensitive_data(async_database: AsyncDatabase):
    masked_uri = mask_sensitive_data(async_database.uri)
    assert "***" in masked_uri, "Sensitive data should be masked."


# Small retry timeout
DELTA_TIME = 0.01


@pytest.mark.asyncio
async def test_retry_async_success_first_attempt():
    """Test when the action succeeds on the first attempt."""
    mock_action = AsyncMock(return_value=True)
    result = await retry_async(
        action=mock_action,  # Simulated action
        max_retries=3,
        timeout=DELTA_TIME,  # Reduced timeout for testing
        delay_factor=2,  # Reduced delay factor
        max_delay=DELTA_TIME,  # Reduced max delay
    )
    assert result is True
    mock_action.assert_awaited_once()


@pytest.mark.asyncio
async def test_retry_async_success_after_retries():
    """Test when the action succeeds after a few retries."""
    mock_action = AsyncMock(side_effect=[Exception("Fail"), Exception("Fail"), True])
    result = await retry_async(
        action=mock_action,  # Simulated action
        max_retries=3,
        timeout=DELTA_TIME,  # Reduced timeout for testing
        delay_factor=2,  # Reduced delay factor
        max_delay=DELTA_TIME,  # Reduced max delay
    )
    assert result is True
    assert mock_action.await_count == 3


@pytest.mark.asyncio
async def test_retry_async_max_retries_exceeded():
    """Test when the action fails and max retries are exceeded."""
    mock_action = AsyncMock(side_effect=Exception("Fail"))
    result = await retry_async(
        action=mock_action,  # Simulated action
        max_retries=3,
        timeout=DELTA_TIME,  # Reduced timeout for testing
        delay_factor=2,  # Reduced delay factor
        max_delay=DELTA_TIME,  # Reduced max delay
    )
    assert result is False
    assert mock_action.await_count == 3


@pytest.mark.asyncio
async def test_retry_async_timeout_handling():
    """Test when the action times out."""
    mock_action = AsyncMock(side_effect=asyncio.TimeoutError("Timeout"))
    result = await retry_async(
        action=mock_action,  # Simulated action
        max_retries=3,
        timeout=DELTA_TIME,  # Reduced timeout for testing
        delay_factor=2,  # Reduced delay factor
        max_delay=DELTA_TIME,  # Reduced max delay
    )
    assert result is False
    assert mock_action.await_count == 3


@pytest.mark.asyncio
async def test_retry_async_exponential_backoff():
    """Test that exponential backoff works as expected."""
    mock_action = AsyncMock(side_effect=Exception("Fail"))
    with patch("pgbase.utils.sleep", new_callable=AsyncMock) as mock_sleep:
        await retry_async(
            action=mock_action,  # Simulated action
            max_retries=3,
            timeout=DELTA_TIME,  # Reduced timeout for testing
            delay_factor=2,  # Reduced delay factor
            max_delay=DELTA_TIME,  # Reduced max delay
            jitter=False,
        )
        # Expected delays: x^1, x^2, x^3
        expected_delays = [DELTA_TIME, DELTA_TIME, DELTA_TIME]
        actual_delays = [call.args[0] for call in mock_sleep.await_args_list]
        assert actual_delays == expected_delays
    assert mock_action.await_count == 3


@pytest.mark.asyncio
async def test_retry_async_exponential_backoff_with_jitter():
    """Test that exponential backoff with jitter works as expected."""
    mock_action = AsyncMock(side_effect=Exception("Fail"))
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await retry_async(
            action=mock_action,  # Simulated action
            max_retries=3,
            timeout=DELTA_TIME,  # Reduced timeout for testing
            delay_factor=DELTA_TIME,  # Reduced delay factor
            max_delay=DELTA_TIME,  # Reduced max delay
            jitter=True,
        )
        # Verify jitter introduces randomness
        delays = [call.args[0] for call in mock_sleep.await_args_list]
        assert all(0 < delay <= 1 for delay in delays)  # Delays should vary due to jitter


@pytest.mark.asyncio
async def test_retry_async_handles_other_exceptions():
    """Test when the action raises a non-timeout exception."""
    mock_action = AsyncMock(side_effect=ValueError("Random error"))
    result = await retry_async(
        action=mock_action,  # Simulated action
        max_retries=3,
        timeout=DELTA_TIME,  # Reduced timeout for testing
        delay_factor=DELTA_TIME,  # Reduced delay factor
        max_delay=DELTA_TIME,  # Reduced max delay
    )
    assert result is False
    assert mock_action.await_count == 3
