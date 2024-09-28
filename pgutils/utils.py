import re

def validate_postgresql_uri(uri: str, allow_async: bool = False):
    """Validates if a URI is a valid PostgreSQL URI for psycopg or asyncpg."""
    # Define valid prefixes based on async allowance
    if allow_async:
        valid_prefixes = ("postgresql+psycopg://", "postgresql+asyncpg://")
    else:
        valid_prefixes = ("postgresql://",)

    # Check for valid prefix
    if not uri.startswith(valid_prefixes):
        raise ValueError(f"URI must start with one of the following: {', '.join(valid_prefixes)}")

    # General URI structure validation
    regex = re.compile(
        r"^postgresql(\+.*)?://"
        r"(?:(?P<user>[a-zA-Z0-9._%+-]+)(?::(?P<password>[^@]+))?@)?"  # Optional user:password
        r"(?P<host>[^:/]+)?"  # Optional host
        r"(?::(?P<port>\d+))?"  # Optional port
        r"(?:/(?P<database>[^/]*))?$"  # Optional database
    )

    # Validate URI structure
    match = regex.match(uri)
    if not match:
        raise ValueError("Invalid PostgreSQL URI format.")

    user = match.group('user')
    password = match.group('password')
    host = match.group('host')
    port = match.group('port')
    database = match.group('database')

    # Validate username and password requirements
    if (user is None and password is not None) or (user is not None and password is None):
        raise ValueError("Both username and password must be provided together.")

    # Check for missing host
    if host is None:
        raise ValueError("Host is missing in the URI.")
    
    # Check for missing port
    if port is None:
        raise ValueError("Port is missing in the URI.")

    return uri
