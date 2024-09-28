import re

def validate_postgresql_uri(uri: str, allow_async: bool = False):
    """Validates if a URI is a valid PostgreSQL URI for psycopg or asyncpg."""
    if allow_async:
        valid_prefixes = ("postgresql+psycopg://", "postgresql+asyncpg://")
    else:
        valid_prefixes = ("postgresql://",)

    if not uri.startswith(valid_prefixes):
        raise ValueError(f"URI must start with one of the following: {', '.join(valid_prefixes)}")

    # General URI structure validation
    regex = re.compile(r"^postgresql(\+.*)?://[a-zA-Z0-9._%+-]+:[^@]+@[^:/]+:\d+/.+$")
    if not regex.match(uri):
        raise ValueError("Invalid PostgreSQL URI format.")
    return uri