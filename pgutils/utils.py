import asyncio
from typing import Any

from sqlalchemy.engine.url import make_url

def validate_postgresql_uri(uri: str, allow_async: bool = False):
    """Validates if a URI is a valid PostgreSQL URI using SQLAlchemy's make_url."""
    
    # Parse the URI using make_url
    try:
        parsed_url = make_url(uri)
    except Exception as e:
        raise ValueError(f"Invalid URI: {e}")

    # Check if the scheme is correct
    if allow_async:
        valid_schemes = ["postgresql", "postgresql+psycopg", "postgresql+asyncpg"]
    else:
        valid_schemes = ["postgresql", "postgresql+psycopg"]
    
    if parsed_url.drivername not in valid_schemes:
        raise ValueError(f"Invalid URI scheme '{parsed_url.drivername}'. "
                         f"Allowed schemes: {', '.join(valid_schemes)}")
    
    # Host is mandatory, so we check if the host is present
    if parsed_url.host is None:
        raise ValueError("Host is missing in the URI.")

    # Both username and password must be provided together, if any
    if (parsed_url.username and not parsed_url.password) or (parsed_url.password and not parsed_url.username):
        raise ValueError("Both username and password must be provided together, or neither.")
    
    return uri  # Return valid URI if all checks pass


def run_async_method(async_method, *args, **kwargs) -> Any:
    """Run an arbitrary asynchronous method in an agnostic way."""
    try:
        # Attempt to get the current event loop
        loop = asyncio.get_event_loop()
    except RuntimeError:  # No current event loop
        # Create a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        # Create a task if the loop is running
        task = loop.create_task(async_method(*args, **kwargs))
        return task  # Optionally return the task
    else:
        # No running loop, use asyncio.run safely
        return loop.run_until_complete(async_method(*args, **kwargs))