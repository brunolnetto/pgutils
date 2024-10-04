from typing import (
    Dict, List, Any, 
    Generator, AsyncGenerator, 
    Union, Optional
)
from typing_extensions import Self
from pydantic import BaseModel
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
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection

from .utils import validate_postgresql_uri, construct_uri
from .constants import (
    PAGINATION_BATCH_SIZE,
    DEFAULT_POOL_SIZE,
    DEFAULT_MAX_OVERFLOW,
    DEFAULT_ADMIN_USERNAME,
    DEFAULT_ADMIN_PASSWORD,
    NOT_EMPTY_STR_COUNT,
    DEFAULT_MINIMUM_PASSWORD_SIZE,
    VALID_SCHEMES,
    VALID_INDEX_TYPES,
)


class ColumnIndex(BaseModel):
    type: str
    table_name: str
    columns: List[str]
    expression: Optional[str] = None
    condition: Optional[str] = None

    @model_validator(mode='before')
    def validate_index_type(self) -> Self:
        index_type = self.get('type')

        if index_type not in VALID_INDEX_TYPES:
            raise ValueError(f"Index type must be one of {list(VALID_INDEX_TYPES.keys())}.")

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
    def check_columns(cls, columns):        
        if len(set(columns)) != len(columns):
            raise ValueError("Index cannot have duplicate columns.")
        
        return columns


class DatabaseSettings(BaseModel):
    uri: AnyUrl
    admin_username: str = Field(default=DEFAULT_ADMIN_USERNAME, min_length=NOT_EMPTY_STR_COUNT)
    admin_password: str = Field(default=DEFAULT_ADMIN_PASSWORD, min_length=DEFAULT_MINIMUM_PASSWORD_SIZE)
    default_port: int = 5432
    async_mode: bool = False
    pool_size: int = Field(default=DEFAULT_POOL_SIZE, gt=0)
    max_overflow: int = Field(default=DEFAULT_MAX_OVERFLOW, ge=0)
    auto_create_db: bool = Field(default=False)

    @property
    def db_name(self) -> str:
        """Extracts the database name from the URI."""
        return self.uri.path.lstrip('/') if self.uri.path else None

    @property
    def admin_uri(self) -> AnyUrl:
        """Constructs the admin URI."""
        admin_uri = f"postgresql+psycopg2://{self.admin_username}:{self.admin_password}@{self.uri.host}:{self.uri.port}"
        validate_postgresql_uri(admin_uri, allow_async=False)
        return make_url(admin_uri)

    @property
    def complete_uri(self) -> AnyUrl:
        """Builds the complete URI."""
        parsed_uri = make_url(str(self.uri))
        drivername = parsed_uri.drivername
        username = parsed_uri.username or self.admin_username
        password = parsed_uri.password or self.admin_password
        port = parsed_uri.port or self.default_port

        return construct_uri(
            drivername, username, password,
            parsed_uri.host, port, parsed_uri.database or ''
        )


    @field_validator('uri')
    def validate_uri(cls, value: AnyUrl) -> AnyUrl:
        """Validates the URI format."""
        if value.scheme not in VALID_SCHEMES:
            raise ValueError(f"URI must start with {VALID_SCHEMES}.")
        validate_postgresql_uri(str(value), allow_async=True)
        return value


class DataSourceSettings(BaseModel):
    """Configuration settings for a DataSource."""
    name: str
    base_uri: AnyUrl                                    # Base URI for the data source
    databases: List[DatabaseSettings]                   # List of databases in the data source
    description: Optional[str] = None                   # Optional field for a description of the data source
    connection_timeout: int = Field(default=30, ge=0)   # Timeout for connections in seconds
    retry_attempts: int = Field(default=3, ge=0)        # Number of attempts to connect to the database
    failover_strategy: Optional[str] = None             # Failover strategy, if any

    @field_validator('databases')
    def check_databases(cls, values):
        """Ensures that at least one database configuration is provided."""
        databases = values.get('databases', [])
        if not databases:
            raise ValueError("At least one database must be defined in the data source.")
        return values

    @field_validator('databases')
    def validate_databases(cls, values):
        """Validates that all databases reference the base URI and have unique names."""
        base_uri = values.get('base_uri')
        databases = values.get('databases', [])
        
        # Check for unique database names
        names = set()
        for db in databases:
            if db.name in names:
                raise ValueError(f"Database name '{db.name}' must be unique within the data source.")
            names.add(db.name)
        
        for db in databases:
            # Ensure each database has the same base URI
            if not db.complete_uri.startswith(str(base_uri)):
                raise ValueError(f"Database '{db.name}' must reference the base URI '{base_uri}'.")

        return values

    def __repr__(self):
        return f"<DataSourceSettings(name={self.name}, databases={len(self.databases)})>"


class TableConstraint(BaseModel):
    constraint_name: str
    constraint_type: str
    table_name: str
    column_name: Optional[str]
    foreign_table_name: Optional[str]
    foreign_column_name: Optional[str]
    

class Paginator:
    def __init__(
        self, 
        conn: Union[Session, AsyncSession, Connection, AsyncConnection],
        query: str, 
        batch_size: int = PAGINATION_BATCH_SIZE,
        params: dict = None
    ):
        self.conn =  conn
        self.query = query
        self.params = params
        self.batch_size = batch_size
        self.current_offset = 0
        self.total_count = None

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

            yield batch
            self.current_offset += self.batch_size

    async def _get_total_count_async(self) -> int:
        """Fetch the total count of records asynchronously."""
        count_query = f"SELECT COUNT(*) FROM ({self.query}) as total"
        query=text(count_query).bindparams(**(self.params or {}))
        result = await self.conn.execute(query)
        return result.scalar()

    async def _async_paginated_query(self):
        """Async generator to fetch results batch by batch."""
        if self.total_count is None:
            self.total_count = await self._get_total_count_async()  # Await the total count

        while self.current_offset < self.total_count:
            batch_query_text=text(self._get_batch_query())
            batch_query=batch_query_text.bindparams(
                limit=self.batch_size,
                offset=self.current_offset,
                **(self.params or {})
            )
            result = await self.conn.execute(batch_query)
            batch = result.fetchall()

            yield batch
            self.current_offset += self.batch_size

    def paginate(self) -> Union[AsyncGenerator[List[Any], None], Generator[List[Any], None, None]]:
        """Unified paginate method to handle both sync and async queries."""
        if isinstance(self.conn, (AsyncConnection, AsyncSession)):
            # Asynchronous pagination
            async def async_generator():
                async for batch in self._async_paginated_query():
                    yield batch
            return async_generator()  # Return the async generator
            
        else:
            # Synchronous pagination
            return self._sync_paginated_query()  # Return the sync generator

    def _get_batch_query(self) -> str:
        """Construct a paginated query with LIMIT and OFFSET."""
        return f"{self.query} LIMIT :limit OFFSET :offset"
