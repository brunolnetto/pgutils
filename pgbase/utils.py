from typing import Callable, Optional
from pydantic import AnyUrl
from asyncio import Future, sleep, TimeoutError, wait_for
from random import uniform
from logging import getLogger, Logger
from re import match

from sqlalchemy.engine.url import make_url, URL

from .constants import (
    VALID_SCHEMES, 
    VALID_SYNC_SCHEMES, 
)


# Utils constants
MAX_RETRIES = 3
DEFAULT_RETRY_TIMEOUT_S = 5
DEFAULT_DELAY_FACTOR = 2.0


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


def get_jitter(attempt: int, has_jitter: bool, delay_factor: float, max_delay: float) -> float:
    rand_value=uniform(0, 1) if has_jitter else 0
    return min(delay_factor ** attempt + rand_value, max_delay)


async def retry_async(
    action: Callable[[], Future],
    max_retries: int = 3,
    timeout: Optional[int] = DEFAULT_RETRY_TIMEOUT_S,
    delay_factor: float = 2.0,
    max_delay: Optional[int] = DEFAULT_RETRY_TIMEOUT_S,
    jitter: bool = True,
    logger: Optional[Logger] = None
) -> bool:
    """
    Retries an asynchronous action with exponential backoff and optional jitter.
    Parameters:
        - action: The asynchronous function to execute.
        - max_retries: Maximum number of retry attempts.
        - timeout: Maximum time in seconds to wait for the action to complete.
        - delay_factor: Factor to increase the delay between retries.
        - max_delay: Maximum delay between retries.
        - jitter: Whether to add random jitter to the delay.
        - logger: Logger instance for logging messages.
    Returns:
        - The result of the action if successful, or False if all retries fail.
    """ 
    
    logger = logger or getLogger("retry_async")

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            result = await wait_for(action(), timeout=timeout)
            logger.info(f"Action succeeded on attempt {attempt}")
            return result
        except Exception as e:
            if isinstance(e, TimeoutError):
                logger.warning(f"Timeout on attempt {attempt}: {str(e)}")
            else:
                logger.error(f"Error on attempt {attempt}: {str(e)}")

        wait_time = get_jitter(attempt, jitter, delay_factor, max_delay)
        logger.info(f"Retrying in {wait_time:.2f} seconds...")
        await sleep(wait_time)

    logger.error("All retry attempts failed.")
    return False

RESERVED_KEYWORDS = {
    "SELECT", "INSERT", "DELETE", "UPDATE", "DROP", "CREATE", "FROM", "WHERE", "JOIN", "TABLE", "INDEX"
}

def is_entity_name_valid(entity_name: str, entity_category: str, logger: Optional[Logger] = None) -> bool:
    """Validate entity name against SQL injection, reserved keywords, and special characters."""
    logger = logger or getLogger("entity_name_validator")

    # Ensure it starts with a letter or underscore and only contains valid characters
    pattern = r'^[A-Za-z_][A-Za-z0-9_]*$'
    is_valid = match(pattern, entity_name) is not None

    upper_table_name = entity_name.upper()

    # Check if table name is exactly a reserved keyword
    if is_valid:
        if upper_table_name in RESERVED_KEYWORDS:
            logger.warning(f"Invalid {entity_category} name attempted: '{entity_name}' (reserved keyword)")
            is_valid = False

    # Ensure the name is not composed entirely of special characters
    valid_table_pattern=r"^[^a-zA-Z0-9]+$"
    if is_valid and match(valid_table_pattern, entity_name):
        logger.warning(f"Invalid {entity_category} name attempted: '{entity_name}' (special characters only)")
        is_valid = False

    # Log detailed reasons for failure
    if not is_valid:
        if not entity_name.strip():
            logger.warning(f"Invalid {entity_category} name attempted: '{entity_name}' (empty or whitespace)")
        elif match(valid_table_pattern, entity_name):
            logger.warning(f"Invalid {entity_category} name attempted: '{entity_name}' (special characters only)")
        elif upper_table_name in RESERVED_KEYWORDS:
            logger.warning(f"Invalid {entity_category} name attempted: '{entity_name}' (reserved keyword)")

    return is_valid


def validate_entity_name(entity_name: str, entity_category: str, logger: Optional[Logger] = None):
    if not is_entity_name_valid(entity_name, entity_category, logger):
        raise ValueError(f"Invalid {entity_category} name: \'{entity_name}\'")
