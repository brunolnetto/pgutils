import asyncio
from typing import Any

from sqlalchemy.engine.url import make_url
from .constants import VALID_SCHEMES, VALID_SYNC_SCHEMES

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

