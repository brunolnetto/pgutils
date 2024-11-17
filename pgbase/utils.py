from typing import Any, Callable, Optional
from pydantic import AnyUrl
import asyncio
import random
import re

from sqlalchemy.engine.url import make_url, URL

from .constants import VALID_SCHEMES, VALID_SYNC_SCHEMES, DEFAULT_RETRY_TIMEOUT_S


def validate_postgresql_uri(uri: str, allow_async: bool = False):
    """Validates if a URI is a valid PostgreSQL URI using SQLAlchemy's make_url."""
    
    # Parse the URI using make_url
    try:
        parsed_url = make_url(uri)
    except Exception as e:
        raise ValueError(f"Invalid URI: {e}")

    # Check if the scheme is correct
    valid_schemes = VALID_SCHEMES if allow_async else VALID_SYNC_SCHEMES
    
    if parsed_url.drivername not in valid_schemes:
        raise ValueError(\
            f"Invalid URI scheme '{parsed_url.drivername}'. " \
            f"Allowed schemes: {', '.join(valid_schemes)}" \
        )
    
    # Host is mandatory, so we check if the host is present
    if parsed_url.host is None:
        raise ValueError("Host is missing in the URI.")

    # Both username and password must be provided together, if any
    hasnt_password=parsed_url.username and not parsed_url.password
    hasnt_username=parsed_url.password and not parsed_url.username
    hasnt_username_or_password=hasnt_password or hasnt_username
    if hasnt_username_or_password:
        raise ValueError("Both username and password must be provided together, or neither.")

    # Return valid URI if all checks pass
    return uri


def is_valid_schema_name(schema_name: str) -> bool:
    # Schema names should only contain alphanumeric characters or underscores and must not start with a digit.
    return bool(re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', schema_name))


def validate_schema_name(schema_name: str):
    if not is_valid_schema_name(schema_name):
        raise ValueError(f"Invalid schema name: {schema_name}")


def run_async_method(async_method: Callable, *args, **kwargs) -> Any:
    """Run an arbitrary asynchronous method in an agnostic way."""
    # Check if async_method is indeed callable
    if not callable(async_method):
        raise ValueError("The provided method must be callable.")

    try:
        # Get the current event loop
        loop = asyncio.get_event_loop()
        if loop.is_running():
            
            # If the event loop is running, create a task for it
            return asyncio.ensure_future(async_method(*args, **kwargs))
        else:
            # If no running loop, use asyncio.run for synchronous environments
            return loop.run_until_complete(async_method(*args, **kwargs))
    except RuntimeError:
        # If there's no event loop, we can create a new one and run the async method
        return asyncio.run(async_method(*args, **kwargs))


def mask_sensitive_data(uri: URL) -> str:
    replaced_uri=uri._replace(password="******", username="******")
    return str(replaced_uri)


def construct_uri(
    drivername: str, username: str, password: str, host: str, port: int, database: str
) -> AnyUrl:
    """Constructs a PostgreSQL URI from the provided components, excluding slash if the database is empty."""
    database_part = f"/{database}" if database else ""
    return make_url(f"{drivername}://{username}:{password}@{host}:{port}{database_part}")


def construct_complete_uri(uri: AnyUrl, username: str, password: str, default_port: int) -> AnyUrl:
    """Constructs the complete URI for a database connection."""
    parsed_uri = make_url(str(uri))
    return construct_uri(
        parsed_uri.drivername,
        username or parsed_uri.username,
        password or parsed_uri.password,
        parsed_uri.host,
        parsed_uri.port or default_port,
        parsed_uri.database or ''
    )


def construct_admin_uri(uri: AnyUrl, username: str, password: str) -> AnyUrl:
    """Constructs an admin URI from the given details."""
    return construct_uri('postgresql+psycopg2', username, password, uri.host, uri.port, '')


async def retry_async(
    action: Callable[[], asyncio.Future], 
    max_retries: int = 3, 
    timeout: Optional[int] = DEFAULT_RETRY_TIMEOUT_S, 
    delay_factor: float = 2.0,
    max_delay: Optional[int] = DEFAULT_RETRY_TIMEOUT_S,
    jitter: bool = True
) -> bool:
    """Retries an asynchronous action with exponential backoff and optional jitter."""
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            # Execute the action
            result = await asyncio.wait_for(action(), timeout=timeout)
            return result

        except Exception as e:
            # Log the error based on the exception
            # This allows flexibility in handling different types of exceptions
            if isinstance(e, asyncio.TimeoutError):
                message = f"Timeout on attempt {attempt}: {str(e)}"
            else:
                message = f"Error on attempt {attempt}: {str(e)}"
            print(message)  # Use logging in production code

        # Exponential backoff with jitter
        wait_time = min(delay_factor ** attempt + (random.uniform(0, 1) if jitter else 0), max_delay)
        print(f"Retrying in {wait_time:.2f} seconds...")
        await asyncio.sleep(wait_time)

    return False
