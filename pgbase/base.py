from logging import Logger, getLogger
from typing import Any, List, Optional, TypeAlias
from abc import ABC, abstractmethod


from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import declarative_base

from .models import DatabaseSettings, TableConstraint, ColumnIndex
from .constants import DEFAULT_HEALTHCHECK_TIMEOUT_S, MAX_RETRIES
from .utils import mask_sensitive_data


class BaseDatabase(ABC):
    """
    Abstract base class for managing PostgreSQL database connections and operations.
    It supports both synchronous and asynchronous implementations.
    """

    def __init__(self, settings: DatabaseSettings, logger: Optional[Logger] = None):
        self.settings = settings
        self.uri = make_url(str(settings.complete_uri))
        self.admin_uri = settings.admin_uri
        self.base: TypeAlias = declarative_base()
        self.logger = logger or getLogger(__name__)

    @abstractmethod
    async def _create_engine(self):
        """
        Create the main database engine.
        To be implemented for sync/async based engines.
        """
        pass

    @abstractmethod
    async def _create_admin_engine(self):
        """
        Create the admin engine for administrative tasks.
        To be implemented for sync/async based engines.
        """
        pass

    @abstractmethod
    async def _create_sessionmaker(self):
        """
        Initialize and return the sessionmaker.
        To be implemented for sync/async based sessions.
        """
        pass

    @abstractmethod
    async def get_session(self):
        """
        Return a session for interacting with the database.
        To be implemented for sync/async sessions.
        """
        pass

    @abstractmethod
    def create_database(self, db_name: Optional[str] = None):
        """Creates the database if it doesn't exist."""
        pass

    @abstractmethod
    def drop_database(self, db_name: Optional[str] = None):
        """Drops the database if it exists."""
        pass

    @abstractmethod
    def database_exists(self, db_name: Optional[str] = None) -> bool:
        """Checks if the database exists."""
        pass

    @abstractmethod
    async def add_audit_trigger(self, table_name: str):
        """Create an audit to table with name 'table_name'"""
        pass

    @abstractmethod
    async def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        """Checks if a column exists in the table."""
        pass

    @abstractmethod
    async def create_indexes(self, indexes: List[ColumnIndex]):
        """Creates indexes for the specified tables."""
        pass

    @abstractmethod
    async def schema_exists(self, schema_name):
        """Checks if the schema exists."""
        pass

    @abstractmethod
    async def health_check(
        self,
        use_admin_uri: bool = False,
        timeout: Optional[int] = DEFAULT_HEALTHCHECK_TIMEOUT_S,
        max_retries: int = MAX_RETRIES,
    ) -> bool:
        """Performs a health check on the database connection."""
        pass

    @abstractmethod
    async def execute(self, query: str, params: Optional[dict] = None):
        """Executes a SQL query."""
        pass

    # 1. List tables
    @abstractmethod
    async def list_tables(self, schema_name: str = "public") -> List[Any]:
        """List all tables in the specified schema."""
        pass

    # 2. List Schemas
    @abstractmethod
    async def list_schemas(self) -> List[Any]:
        """List all schemas in the database."""
        pass

    # 3. List Indexes
    @abstractmethod
    async def list_indexes(self, table_name: str) -> List[Any]:
        """List all indexes for a given table."""
        pass

    # 4. List Views
    @abstractmethod
    async def list_views(self, table_schema: str = "public") -> List[Any]:
        """List all views in the specified schema."""
        pass

    # 5. List Sequences
    @abstractmethod
    async def list_sequences(self) -> List[Any]:
        """List all sequences in the database."""
        pass

    # 6. List Constraints
    @abstractmethod
    async def list_constraints(
        self, table_name: str, table_schema: str = "public"
    ) -> List[TableConstraint]:
        """List all constraints for a specified table."""
        pass

    # 7. List Triggers
    @abstractmethod
    async def list_triggers(self, table_name: str) -> List[Any]:
        """List all triggers for a specified table."""
        pass

    # 8. List Functions
    @abstractmethod
    async def list_functions(self) -> List[Any]:
        """List all functions in the database."""
        pass

    # 9. List Procedures
    @abstractmethod
    async def list_procedures(self) -> List[Any]:
        """List all procedures in the database."""
        pass

    # 10. List Materialized Views
    @abstractmethod
    async def list_materialized_views(self) -> List[Any]:
        """List all materialized views in the database."""
        pass

    # 11. List Columns
    @abstractmethod
    async def list_columns(self, table_name: str, schema_name: str = "public") -> List[Any]:
        """List all columns for a specified table in the schema."""
        pass

    # 12. List User-Defined Types
    @abstractmethod
    async def list_types(self) -> List[Any]:
        """List all user-defined types in the database."""
        pass

    # 13. List Roles
    @abstractmethod
    async def list_roles(self) -> List[Any]:
        """List all roles in the database."""
        pass

    # 14. List Extensions
    @abstractmethod
    async def list_extensions(self) -> List[Any]:
        """List all extensions installed in the database."""
        pass

    def __repr__(self):
        return f"<Database(uri={mask_sensitive_data(self.uri)})>"
