from typing import Dict, List, Any, Optional, Generator, AsyncGenerator, Union
from typing_extensions import Self
from pydantic import BaseModel
import warnings
import re

from pydantic import (
    AnyUrl, 
    field_validator, 
    model_validator, 
    Field 
)
from sqlalchemy import text
from sqlalchemy.engine import Result, Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.orm import Session

from .utils import (
    validate_postgresql_uri, 
    construct_admin_uri,
    construct_complete_uri, 
    run_async_method
)
from .constants import (
    PAGINATION_BATCH_SIZE,
    DEFAULT_HEALTHCHECK_TIMEOUT_S,
    VALID_INDEX_TYPES,
    VALID_SCHEMES,
)

# Model types
AsyncPageGenerator = AsyncGenerator[List[Any], None] 
SyncPageGenerator = Generator[List[Any], None, None]
PageGenerator = Union[AsyncPageGenerator, SyncPageGenerator]
AsyncDatabaseInteraction=(AsyncConnection, AsyncSession)
DatabaseConnection = Union[Session, AsyncSession, Connection, AsyncConnection]

# Models constants
DEFAULT_ADMIN_USERNAME='postgres'
DEFAULT_ADMIN_PASSWORD='postgres'

DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 10

DEFAULT_MIN_LENGTH = 1
DEFAULT_MINIMUM_PASSWORD_SIZE = 1


class QueryValidationError(Exception):
    """Exception for invalid queries."""
    pass


class ExcessiveSelectWarning(Warning):
    """Warning raised for the use of SELECT * in SQL queries."""
    pass


class DatabaseSettings(BaseModel):
    uri: AnyUrl
    admin_username: str = Field(
        default=DEFAULT_ADMIN_USERNAME, min_length=DEFAULT_MIN_LENGTH
    )
    admin_password: str = Field(
        default=DEFAULT_ADMIN_PASSWORD, min_length=DEFAULT_MINIMUM_PASSWORD_SIZE
    )
    default_port: int = 5432
    pool_size: int = Field(default=DEFAULT_POOL_SIZE, gt=0)
    max_overflow: int = Field(default=DEFAULT_MAX_OVERFLOW, ge=0)

    @property
    def name(self) -> str:
        """Extracts the database name from the URI."""
        return self.uri.path.lstrip('/') if self.uri.path else None

    @property
    def admin_uri(self) -> AnyUrl:
        """Constructs the admin URI."""
        return construct_admin_uri(
            self.uri, self.admin_username, self.admin_password
        )

    @property
    def complete_uri(self) -> AnyUrl:
        """Builds the complete URI."""
        return construct_complete_uri(
            self.uri, self.uri.username, self.uri.password, self.default_port
        )

    @field_validator('uri')
    def validate_uri(cls, uri: AnyUrl) -> Any:
        """Validates the URI format."""
        if uri.scheme not in VALID_SCHEMES:
            raise ValueError(f"URI must start with {VALID_SCHEMES}.")

        validate_postgresql_uri(str(uri), allow_async=True)

        return uri


class DatasourceSettings(BaseModel):
    """Configuration settings for a DataSource."""
    name: str
    admin_username: str = Field(
        default=DEFAULT_ADMIN_USERNAME, min_length=DEFAULT_MIN_LENGTH
    )
    admin_password: str = Field(
        default=DEFAULT_ADMIN_PASSWORD,  min_length=DEFAULT_MINIMUM_PASSWORD_SIZE
    )
    databases: List[DatabaseSettings]                   # List of databases in the data source
    description: Optional[str] = None                   # Optional field for a description of the data source
    connection_timeout: int = Field(default=30, ge=0)   # Timeout for connections in seconds
    retry_attempts: int = Field(default=3, ge=0)        # Number of attempts to connect to the database

    @field_validator('databases')
    def validate_databases(cls, databases: List[DatabaseSettings]) -> List[DatabaseSettings]:
        """Validates that all databases are properly configured."""
        # Ensure that there is at least one database
        if not databases:
            raise ValueError("At least one database must be defined.")

        return databases

    def __repr__(self):
        return f"<DataSourceSettings(name={self.name}, databases={len(self.databases)})>"


class ColumnIndex(BaseModel):
    schema_name: str = 'public'
    table_name: str
    column_names: List[str]
    type: str
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

    @field_validator('column_names')
    def check_column_names(cls, column_names: List[str]) -> List[str]:
        if len(set(column_names)) != len(column_names):
            raise ValueError("Index cannot have duplicate columns.")
        
        return column_names


class TableConstraint(BaseModel):
    constraint_name: str
    constraint_type: str
    table_name: str
    column_name: Optional[str]
    foreign_table_name: Optional[str]
    foreign_column_name: Optional[str]


class Trigger(BaseModel):
    trigger_catalog: str = Field(..., 
        description="The catalog (database) where the trigger exists")
    trigger_schema: str = Field(..., 
        description="The schema where the trigger is defined")
    trigger_name: str = Field(..., 
        description="The name of the trigger")
    event_manipulation: str = Field(..., 
        description="The event that causes the trigger to fire, such as INSERT, DELETE, UPDATE")
    event_object_catalog: str = Field(..., 
        description="The catalog (database) where the table exists")
    event_object_schema: str = Field(..., 
        description="The schema where the table exists")
    event_object_table: str = Field(..., 
        description="The table associated with the trigger")
    action_order: int = Field(..., 
        description="The order in which trigger actions are executed")
    action_condition: Optional[str] = Field(None, 
        description="The condition under which the trigger fires")
    action_statement: str = Field(..., 
        description="The SQL statement executed when the trigger fires")
    action_orientation: str = Field(..., 
        description="Whether the trigger fires per row or per statement")
    action_timing: str = Field(..., 
        description="When the trigger fires relative to the event, such as BEFORE or AFTER")
    action_reference_old_table: Optional[str] = Field(None, 
        description="Used in INSTEAD OF triggers to reference an old table")
    action_reference_new_table: Optional[str] = Field(None, 
        description="Used in INSTEAD OF triggers to reference a new table")
    action_reference_old_row: Optional[str] = Field(None, 
        description="Used to reference the old row")
    action_reference_new_row: Optional[str] = Field(None, 
        description="Used to reference the new row")
    created: Optional[str] = Field(None, 
        description="The timestamp when the trigger was created")


class QueryValidator:
    """Utility class to validate queries used for pagination."""

    def __init__(self, query: str):
        self.query = query.strip()

    def validate(self) -> None:
        """Perform query validation."""
        self._validate_sql_syntax()
        self._check_limit_offset()

    def _validate_sql_syntax(self) -> None:
        """Simple validation to ensure query contains necessary clauses."""
        upper_query = self.query.upper()
        
        # Check for maximum length
        max_length = 1000  # Set a reasonable maximum length
        if len(self.query) > max_length:
            raise QueryValidationError("Query exceeds maximum length.")

        # Ensure query contains SELECT and FROM
        if 'SELECT' not in upper_query or 'FROM' not in upper_query:
            raise QueryValidationError("Query must contain SELECT and FROM clauses.")

        # Ensure no multiple semicolons or unsafe characters
        if re.search(r";\s*;", self.query):
            raise QueryValidationError("Query contains multiple semicolons, which is unsafe.")
        
        # Check for balanced parentheses
        if self.query.count('(') != self.query.count(')'):
            raise QueryValidationError("Query contains unbalanced parentheses.")

        # Ensure that the SELECT statement does not contain disallowed clauses
        disallowed_clauses = ['DROP', 'DELETE', 'UPDATE']
        for clause in disallowed_clauses:
            if clause in upper_query:
                raise QueryValidationError(f"Query contains disallowed clause: {clause}.")

        # Check for excessive SELECT *
        if 'SELECT *' in upper_query:
            warnings.warn(
                "Using SELECT * is discouraged. Specify the columns instead.",
                ExcessiveSelectWarning
            )

    def _check_limit_offset(self) -> None:
        """Ensure no pre-existing LIMIT or OFFSET in query."""
        if re.search(r"\bLIMIT\b", self.query, re.IGNORECASE):
            raise QueryValidationError("Query should not contain a predefined LIMIT clause.")
        if re.search(r"\bOFFSET\b", self.query, re.IGNORECASE):
            raise QueryValidationError("Query should not contain a predefined OFFSET clause.")


class TablePaginator:
    def __init__(
        self, 
        conn: DatabaseConnection,
        query: str, 
        batch_size: int = PAGINATION_BATCH_SIZE,
        params: Optional[Dict[str, Any]] = None
    ):
        self.conn = conn
        self.query = query
        self.params = params or {}
        self.batch_size = batch_size
        self.current_offset = 0
        self.total_count = None

        # Validate query upon initialization
        self._validate_query()

    def _validate_query(self) -> None:
        """Validate the query before using it for pagination."""
        validator = QueryValidator(self.query)
        validator.validate()

    async def get_total_count(self) -> int:
        """Fetch the total count using the run_async_method utility."""
        count_query = f"SELECT COUNT(1) FROM ({self.query}) as total"
        result = await self.conn.execute(text(count_query).bindparams(**self.params))
        return result.scalar()

    def _get_batch_query(self) -> str:
        """Construct a paginated query with LIMIT and OFFSET."""
        return f"{self.query} LIMIT :limit OFFSET :offset"

    async def _fetch_batch_async(self) -> List[Any]:
        """Fetch a single batch asynchronously."""
        query_text=text(self._get_batch_query())
        batch_query = query_text.bindparams(
            limit=self.batch_size, offset=self.current_offset, **self.params
        )
        result = await self.conn.execute(batch_query)
        return result.fetchall()

    async def _paginated_query_async(self) -> AsyncPageGenerator:
        """Asynchronous generator to fetch results batch by batch."""
        self.total_count = await self.get_total_count()

        while self.current_offset < self.total_count:
            batch = await self._fetch_batch_async()
            yield batch
            self.current_offset += self.batch_size

    async def paginate(self) -> PageGenerator:
        """Unified paginate method to handle both sync and async queries."""
        self.total_count = await self.get_total_count()

        while self.current_offset < self.total_count:
            batch = await self._fetch_batch_async()
            yield batch
            self.current_offset += self.batch_size

    def fetch_total_count(self) -> int:
        """Fetch the total count using the run_async_method utility."""
        if isinstance(self.conn, AsyncDatabaseInteraction):
            return run_async_method(self._get_total_count_async)
        return self.get_total_count()
