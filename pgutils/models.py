from typing import List, Any, Generator, Union
import re

from pydantic import BaseModel, AnyUrl, ValidationError, field_validator, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncConnection

from .utils import validate_postgresql_uri

# Constants
DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 10
NOT_EMPTY_STR_COUNT = 1
DEFAULT_MINIMUM_PASSWORD_SIZE = 1

class DatabaseConfig(BaseModel):
    uri: AnyUrl  # Database URI for regular operations
    admin_username: str = Field(min_length=NOT_EMPTY_STR_COUNT) 
    admin_password: str = Field(min_length=DEFAULT_MINIMUM_PASSWORD_SIZE)
    async_mode: bool = False
    pool_size: int = Field(default=DEFAULT_POOL_SIZE, gt=0)  # Must be greater than 0
    max_overflow: int = Field(default=DEFAULT_MAX_OVERFLOW, ge=0)  # Must be 0 or greater

    @property
    def db_name(self) -> str:
        """Extracts the database name from the URI."""
        return self.uri.path.lstrip('/') if self.uri.path else None

    @property
    def admin_uri(self) -> AnyUrl:
        """Constructs the admin URI using the username and password, falling back to defaults if missing."""
        username = self.admin_username
        password = self.admin_password
        
        admin_uri = f"postgresql://{username}:{password}@{self.uri.host}:{self.uri.port}"
        # Validate the admin URI using shared validation logic
        validate_postgresql_uri(admin_uri, allow_async=False)
        return make_url(admin_uri)

    @field_validator('uri')
    def validate_uri(cls, value: AnyUrl):
        """Validates the URI format to assert PostgreSQL with psycopg or asyncpg."""
        # Check the scheme directly
        if value.scheme not in ("postgresql", "postgresql+psycopg", "postgresql+asyncpg"):
            raise ValueError("URI must start with 'postgresql', 'postgresql+psycopg', or 'postgresql+asyncpg'.")

        # Optionally, you can also check the full URI structure here.
        regex = re.compile(r"^postgresql(\+.*)?://[a-zA-Z0-9._%+-]+:[^@]+@[^:/]+:\d+/.+$")
        if not regex.match(str(value)):
            raise ValueError("Invalid PostgreSQL URI format.")
        
        return value

    @field_validator('pool_size', 'max_overflow')
    def validate_pool_params(cls, value):
        if value < 0:
            raise ValueError("Pool size and max overflow must be non-negative")
        return value


class Paginator:
    def __init__(self, query: str, params: dict = None, batch_size: int = 1000):
        self.query = query
        self.params = params
        self.batch_size = batch_size
        self.current_offset = 0
        self.total_count = None

    def _get_batch_query(self) -> str:
        """Construct a paginated query with LIMIT and OFFSET."""
        return f"{self.query} LIMIT :limit OFFSET :offset"

    def _get_total_count(self, conn) -> int:
        """Fetch the total count of records (optional)."""
        count_query = f"SELECT COUNT(*) FROM ({self.query}) as total"
        result = conn.execute(text(count_query).bindparams(**(self.params or {})))
        return result.scalar()  # Assuming a single result

    def _sync_paginated_query(self, conn: Connection) -> Generator[List[Any], None, None]:
        """Generator to fetch results batch by batch synchronously."""
        if self.total_count is None:
            self.total_count = self._get_total_count(conn)

        while self.current_offset < self.total_count:
            result = conn.execute(text(self._get_batch_query()).bindparams(
                limit=self.batch_size,
                offset=self.current_offset,
                **(self.params or {})
            ))
            batch = result.fetchall()

            if not batch:
                break

            yield batch
            self.current_offset += self.batch_size

    async def _async_paginated_query(self, conn: AsyncConnection) -> Generator[List[Any], None, None]:
        """Generator to fetch results batch by batch asynchronously."""
        if self.total_count is None:
            self.total_count = await self._get_total_count_async(conn)

        while self.current_offset < self.total_count:
            result = await conn.execute(text(self._get_batch_query()).bindparams(
                limit=self.batch_size,
                offset=self.current_offset,
                **(self.params or {})
            ))
            batch = await result.fetchall()

            if not batch:
                break

            yield batch
            self.current_offset += self.batch_size

    async def paginate(self, conn: Union[Connection, AsyncConnection]) -> Union[Generator[List[Any], None, None], Generator]:
        """Unified paginate method to handle both sync and async queries."""
        if isinstance(conn, AsyncConnection):
            # Asynchronous pagination
            async for batch in self._async_paginated_query(conn):
                yield batch
        else:
            # Synchronous pagination
            for batch in self._sync_paginated_query(conn):
                yield batch

    async def _get_total_count_async(self, conn) -> int:
        """Fetch the total count of records asynchronously."""
        count_query = f"SELECT COUNT(*) FROM ({self.query}) as total"
        result = await conn.execute(text(count_query).bindparams(**(self.params or {})))
        return result.scalar()

    def paginated_query(self, conn) -> Union[Generator[List[Any], None, None], Generator]:
        """Unified interface for paginated queries."""
        if isinstance(conn, AsyncSession):
            # Return async generator
            return self._async_paginated_query(conn)
        else:
            # Return sync generator
            return self._sync_paginated_query(conn)
