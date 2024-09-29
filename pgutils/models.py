from typing import (
    Dict, List, Any, 
    Generator, AsyncGenerator, 
    Union, Optional
)
from typing_extensions import Self
import re

from pydantic import (
    BaseModel, 
    AnyUrl, 
    ValidationError, 
    field_validator, 
    model_validator, 
    Field 
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncConnection

from .utils import validate_postgresql_uri
from pgutils.constants import (
    PAGINATION_BATCH_SIZE,
    DEFAULT_POOL_SIZE,
    DEFAULT_MAX_OVERFLOW,
    NOT_EMPTY_STR_COUNT,
    DEFAULT_MINIMUM_PASSWORD_SIZE,
    VALID_SCHEMES,
)


class Index(BaseModel):
    type: str
    columns: List[str]
    expression: Optional[str] = None
    condition: Optional[str] = None

    @model_validator(mode='before')
    def validate_index_type(self) -> Self:
        index_type = self.get('type')
        valid_index_types = {
            'btree', 'gin', 'gist', 'hash', 
            'spgist', 'brin', 'expression', 'partial'
        }
        
        if index_type not in valid_index_types:
            raise ValueError(f"Index type must be one of {valid_index_types}.")

        return self
    
    @model_validator(mode='before')
    def validate_obj(self) -> Self:
        index_type = self.get('type')
        if index_type == 'expression' and 'expression' not in self.keys():
            raise ValueError("Expression index must include 'expression'.")
        if index_type == 'partial' and 'condition' not in self.keys():
            raise ValueError("Partial index must include 'condition'.")

        return self

    @field_validator('columns')
    def check_columns_field(cls, columns):
        if not isinstance(columns, list):
            raise ValueError("Index must include 'columns' as a list.")
        return columns


class DatabaseConfig(BaseModel):
    uri: AnyUrl  # Database URI for regular operations
    admin_username: str = Field(min_length=NOT_EMPTY_STR_COUNT) 
    admin_password: str = Field(min_length=DEFAULT_MINIMUM_PASSWORD_SIZE)
    default_port: int = 5432
    async_mode: bool = False
    pool_size: int = Field(default=DEFAULT_POOL_SIZE, gt=0)  # Must be greater than 0
    max_overflow: int = Field(default=DEFAULT_MAX_OVERFLOW, ge=0)  # Must be 0 or greater
    indexes: Optional[Dict[str, Dict[str, List[str]]]] = None  # New index configuration

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

    @property
    def complete_uri(self) -> AnyUrl:
        """Builds the complete URI, filling in missing username, password, and port."""
        # Parse the existing URI
        parsed_uri = make_url(str(self.uri))
        
        scheme = parsed_uri.drivername

        # Use admin username and password if they're missing
        username = parsed_uri.username or self.admin_username
        password = parsed_uri.password or self.admin_password
        
        # Use the provided port or default to `default_port`
        port = parsed_uri.port or self.default_port

        # Build the complete URI
        complete_uri = f"{scheme}://{username}:{password}@{parsed_uri.host}:{port}/{parsed_uri.database or ''}"
        
        # Validate the newly constructed URI
        validate_postgresql_uri(complete_uri, allow_async=self.async_mode)
        
        return make_url(complete_uri)

    @field_validator('uri')
    def validate_uri(cls, value: AnyUrl):
        """Validates the URI format to assert PostgreSQL with psycopg or asyncpg."""
        
        
        # Check the scheme directly
        if value.scheme not in VALID_SCHEMES:
            raise ValueError(f"URI must start with {VALID_SCHEMES}.")

        # Optionally, you can also check the full URI structure here.
        validate_postgresql_uri(str(value), allow_async = True)
        
        return value

    @field_validator('pool_size', 'max_overflow')
    def validate_pool_params(cls, value):
        if value < 0:
            raise ValueError("Pool size and max overflow must be non-negative")
        return value

    @model_validator(mode='before')
    def validate_indexes(cls, values):
        """Validates the index configuration."""
        indexes = values.get('indexes', {})

        # Define valid index types and their requirements
        valid_index_types = {
            'btree': {'columns': True},
            'gin': {'columns': True},
            'gist': {'columns': True},
            'hash': {'columns': True},
            'spgist': {'columns': True},
            'brin': {'columns': True},
            'expression': {'columns': True, 'expression': True},
            'partial': {'columns': True, 'condition': True},
        }

        for table_name, index_info in indexes.items():
            errors = []  # Collect errors for each index

            # Validate 'columns' field
            if 'columns' not in index_info or not isinstance(index_info['columns'], list):
                errors.append(f"Index for table '{table_name}' must include 'columns' as a list.")

            # Validate 'type' field and its requirements
            index_type = index_info.get('type', None)
            if index_type not in valid_index_types:
                errors.append(f"Index type for table '{table_name}' must be one of {list(valid_index_types.keys())}.")
            else:
                requirements = valid_index_types[index_type]
                for req_field, is_required in requirements.items():
                    if is_required and req_field not in index_info:
                        errors.append(f"Index for table '{table_name}' of type '{index_type}' must include '{req_field}'.")

            # Raise a combined error message if there are any errors
            if errors:
                raise ValueError('\n'.join(errors))

        return values


class Paginator:
    def __init__(
        self, 
        conn: Union[Connection, AsyncConnection],
        query: str, 
        params: dict = None, 
        batch_size: int = PAGINATION_BATCH_SIZE
    ):
        self.conn =  conn
        self.query = query
        self.params = params
        self.batch_size = batch_size
        self.current_offset = 0
        self.total_count = None

    def _get_batch_query(self) -> str:
        """Construct a paginated query with LIMIT and OFFSET."""
        return f"{self.query} LIMIT :limit OFFSET :offset"

    def _get_total_count(self) -> int:
        """Fetch the total count of records (optional)."""
        count_query = f"SELECT COUNT(*) FROM ({self.query}) as total"
        result = self.conn.execute(text(count_query).bindparams(**(self.params or {})))
        return result.scalar()  # Assuming a single result

    def _sync_paginated_query(self) -> Generator[List[Any], None, None]:
        """Generator to fetch results batch by batch synchronously."""
        if self.total_count is None:
            self.total_count = self._get_total_count()

        while self.current_offset < self.total_count:
            batch_query=text(self._get_batch_query()).bindparams(
                limit=self.batch_size,
                offset=self.current_offset,
                **(self.params or {})
            )
            result = self.conn.execute(batch_query)
            batch = result.fetchall()

            if not batch:
                break

            yield batch
            self.current_offset += self.batch_size


    async def _async_paginated_query(self):
        """Async generator to fetch results batch by batch."""
        if self.total_count is None:
            self.total_count = await self._get_total_count()  # Await the total count

        while self.current_offset < self.total_count:
            batch_query=text(self._get_batch_query()).bindparams(
                limit=self.batch_size,
                offset=self.current_offset,
                **(self.params or {})
            )
            result = await self.conn.execute(batch_query)
            batch = result.fetchall()

            if not batch:
                break

            yield batch
            self.current_offset += self.batch_size

    async def paginate(self) -> Union[AsyncGenerator[List[Any], None], AsyncGenerator]:
        """Unified paginate method to handle both sync and async queries."""
        if isinstance(self.conn, AsyncConnection):
            # Asynchronous pagination
            async for batch in self._async_paginated_query():
                yield batch
        else:
            # Synchronous pagination
            for batch in self._sync_paginated_query():
                yield batch

    def _get_total_count_async(self) -> int:
        """Fetch the total count of records asynchronously."""
        count_query = f"SELECT COUNT(*) FROM ({self.query}) as total"
        result = self.conn.execute(text(count_query).bindparams(**(self.params or {})))
        return result.scalar()

    def paginated_query(self) -> Union[Generator[List[Any], None, None], Generator]:
        """Unified interface for paginated queries."""
        if isinstance(self.conn, AsyncSession):
            # Return async generator
            return self._async_paginated_query()
        else:
            # Return sync generator
            return self._sync_paginated_query()
    
    def get_batch_query(self) -> str:
        """Generate the batched query for fetching data."""
        return f"{self.query} LIMIT :limit OFFSET :offset"
