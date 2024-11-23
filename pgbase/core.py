from typing import AsyncGenerator, Optional, Union, Tuple, Dict, List, Any
from contextlib import asynccontextmanager
from logging import getLogger, Logger
from asyncio import Lock
from re import match
import asyncio
from asyncpg import create_pool, Connection
import psutil

from sqlalchemy import DDL
from sqlalchemy.sql.schema import MetaData
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import ProgrammingError, OperationalError
from ping3 import ping, errors

from .models import (
    DatasourceSettings, 
    DatabaseSettings, 
    TableConstraint, 
    Trigger, 
    ColumnIndex, 
    TablePaginator
)
from .utils import (
    mask_sensitive_data, validate_schema_name, retry_async
)
from .constants import PAGINATION_BATCH_SIZE, DEFAULT_HEALTHCHECK_TIMEOUT_S, MAX_RETRIES
from .models import DatabaseConnection
from .base import BaseDatabase

class SyncDatabase(BaseDatabase):
    pass

class AsyncDatabase(BaseDatabase):
    """
    Database class for managing PostgreSQL connections and operations.
    Supports both synchronous and asynchronous operations.
    """
    # Caching for index existence and column existence checks
    index_cache = {}
    column_cache = {}

    RESERVED_KEYWORDS = {
        "SELECT", "INSERT", "DELETE", "UPDATE", "DROP", "CREATE", "FROM", "WHERE", "JOIN", "TABLE", "INDEX"
    }


    # Constants for dynamic batch size adjustment
    MIN_BATCH_SIZE = 1
    MAX_BATCH_SIZE = 10
    LOAD_THRESHOLD = 0.75


    def __init__(self, settings: DatabaseSettings, logger: Logger = None):
        self.settings = settings
        self.uri = make_url(str(settings.complete_uri))
        self.admin_uri = settings.admin_uri
        self.base = declarative_base(metadata=MetaData())
        self.logger = logger or getLogger(__name__)

        # Create engines and sessionmakers
        self.admin_engine = self._create_admin_engine()
        self.engine = self._create_engine()

        # Create sessionmaker
        self.session_maker = self._create_sessionmaker()

        # Initialize index cache
        self.indexes_lock = Lock()
        self.pool = None

    async def init(self):
        await self.init_pool()
        await self.create_database_if_not_exists(self.settings.name)                 

    def _create_engine(self):
        """Creates and returns the main async database engine."""
        self.logger.debug(f"Creating main engine with URI: {str(self.uri)}")
        return create_async_engine(
            str(self.uri),
            isolation_level="AUTOCOMMIT",
            pool_size=self.settings.pool_size,
            max_overflow=self.settings.max_overflow,
            pool_pre_ping=True
        )

    def _create_admin_engine(self):
        """Creates and returns the admin engine for administrative tasks."""
        self.logger.debug(f"Creating admin engine with masked URI: {mask_sensitive_data(self.admin_uri)}")
        return create_engine(
            str(self.admin_uri),
            isolation_level="AUTOCOMMIT",
            pool_size=self.settings.pool_size,
            max_overflow=self.settings.max_overflow,
            pool_pre_ping=True
        )

    def _create_sessionmaker(self):
        """Initializes and returns an async sessionmaker for database interactions."""
        return sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=False)

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Provide an async database session with proper exception handling."""
        async with self.session_maker() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                self.logger.error(f"Session rollback due to exception: {e}")
                raise
            finally:
                await session.close()

    async def init_pool(self):
        """
        Initializes the connection pool for the PostgreSQL database.
        """
        try:
            # Create a connection pool for the database
            credentials=f"{self.uri.username}:{self.uri.password}"
            route=f"{self.uri.host}:{self.uri.port}/{self.uri.database}"
            dsn = f"postgresql://{credentials}@{route}"
            self.pool = await create_pool(
                dsn=dsn,
                min_size=1,                             # Minimum number of connections in the pool
                max_size=self.settings.pool_size,       # Maximum number of connections in the pool
                max_inactive_connection_lifetime=300,   # Close inactive connections after 5 minutes
            )
            self.logger.info("Database connection pool initialized.")
        except Exception as e:
            self.logger.error(f"Failed to initialize database pool: {e}")



    async def close_pool(self):
        """
        Closes the connection pool when the application shuts down.
        """
        if self.pool:
            await self.pool.close()
            self.logger.info("Database connection pool closed.")


    @asynccontextmanager
    async def _acquire_connection(self):
        """
        Acquires a connection from the pool.
        
        Returns:
            asyncpg.Connection: A database connection object.
        """
        if self.pool is None:
            await self.init_pool()
        async with self.pool.acquire() as connection:
            yield connection


    def _is_valid_table_name(self, table_name: str) -> bool:
        """Validate table name against SQL injection, reserved keywords, and special characters."""
        # Ensure it is not empty or just spaces
        if not table_name.strip():
            self.logger.warning(f"Invalid table name attempted: '{table_name}' (empty or whitespace)")
            return False

        # Ensure it starts with a letter or underscore and only contains valid characters
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        is_valid = match(pattern, table_name) is not None

        upper_table_name = table_name.upper()

        # Check if table name is exactly a reserved keyword
        if is_valid:
            if upper_table_name in self.RESERVED_KEYWORDS:
                self.logger.warning(f"Invalid table name attempted: '{table_name}' (reserved keyword)")
                is_valid = False

        # Ensure the name is not composed entirely of special characters
        valid_table_pattern=r"^[^a-zA-Z0-9]+$"
        if is_valid and match(valid_table_pattern, table_name):
            self.logger.warning(f"Invalid table name attempted: '{table_name}' (special characters only)")
            is_valid = False

        # Log detailed reasons for failure
        if not is_valid:
            if not table_name.strip():
                self.logger.warning(f"Invalid table name attempted: '{table_name}' (empty or whitespace)")
            elif match(valid_table_pattern, table_name):
                self.logger.warning(f"Invalid table name attempted: '{table_name}' (special characters only)")
            elif upper_table_name in self.RESERVED_KEYWORDS:
                self.logger.warning(f"Invalid table name attempted: '{table_name}' (reserved keyword)")

        return is_valid


    def check_database_exists(self, db_name: Optional[str] = None) -> bool:
        """Checks if the specified database exists using the admin engine."""
        db_name_to_check = db_name or self.settings.name
        if not db_name_to_check:
            self.logger.error("No database name provided or configured.")
            return False

        try:
            with self.admin_engine.connect() as connection:
                query = text("SELECT 1 FROM pg_database WHERE datname = :db_name;")
                result = connection.execute(query, {"db_name": db_name_to_check})
                return result.scalar() is not None
        except OperationalError as e:
            self.logger.error(f"Error checking database existence: {e}")
            return False


    def create_database_if_not_exists(self, db_name: Optional[str] = None):
        """Creates the database if it does not already exist."""
        db_name = db_name or self.settings.name
        if not db_name:
            self.logger.error("No database name provided.")
            return

        try:
            with self.admin_engine.connect() as connection:
                query = text(f"CREATE DATABASE {db_name};")
                connection.execute(query)
                self.logger.info(f"Database '{db_name}' created successfully.")
        except ProgrammingError as e:
            if "already exists" not in str(e):
                self.logger.error(f"Error creating database: {e}")
                raise
            self.logger.info(f"Database '{db_name}' already exists.")


    def drop_database_if_exists(self, db_name: Optional[str] = None):
        """Drops the database if it exists."""
        db_name = db_name or self.settings.name
        if not db_name:
            self.logger.error("No database name provided.")
            return

        try:
            with self.admin_engine.connect() as connection:
                terminate_query = text(""" 
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = :db_name
                    AND pid <> pg_backend_pid();
                """)
                connection.execute(terminate_query, {"db_name": db_name})
                self.logger.info(f"Terminated active connections for database '{db_name}'.")

                drop_query = text(f"DROP DATABASE IF EXISTS \"{db_name}\";")
                connection.execute(drop_query)
                self.logger.info(f"Database '{db_name}' dropped successfully.")
        except OperationalError as e:
            self.logger.error(f"Error dropping database: {e}")


    async def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        """Checks if the specified column exists in the table."""
        async with self._acquire_connection() as connection:
            try:
                query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = $1
                    AND table_schema = $2;
                """
                # Execute the query
                result = await connection.fetch(query, table_name, schema_name)
                
                # Extract column names from the result and check if the column exists
                columns = {row['column_name'] for row in result}
                return column_name in columns
            except Exception as e:
                self.logger.error(f"Error checking column existence: {e}")
                return False


    def _measure_network_latency(self, host: str = "8.8.8.8") -> float:
        """
        Measures network latency by sending a ping to a given host.
        
        Args:
            host (str): The IP address or hostname to ping (default is Google's public DNS).
        
        Returns:
            float: The round-trip time (RTT) in seconds. Returns None if the ping fails.
        """
        try:
            # Send a single ping to the host and get the round-trip time in seconds
            rtt = ping(host, timeout=2)
            if rtt is None:
                self.logger.warning(f"Failed to measure latency for {host}")
                return float('inf')
            return rtt
        except errors.PingError as e:
            self.logger.error(f"Ping error when measuring latency to {host}: {e}")
            return float('inf')


    def _adjust_batch_size(self) -> int:
        """Adjust the batch size based on system load."""
        cpu_usage = psutil.cpu_percent(interval=1) / 100  # Get CPU usage percentage
        mem_usage = psutil.virtual_memory().percent / 100  # Get memory usage percentage
        disk_usage = psutil.disk_usage('/').percent / 100  # Disk usage
        psutil.net_io_counters()  # Network I/O
        latency = self._measure_network_latency()

        # Calculate system load based on all parameters
        load_factor = max(cpu_usage, mem_usage, disk_usage, latency)
        
        if load_factor > self.LOAD_THRESHOLD:
            return max(self.MIN_BATCH_SIZE, self.MAX_BATCH_SIZE // 2)
        return self.MAX_BATCH_SIZE  # Use maximum batch size under normal conditions


    async def _execute_in_batches(self, tasks, batch_size: int):
        """Execute tasks in smaller batches to avoid overloading the database."""
        # Process tasks in batches
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            try:
                # Gather and execute each batch concurrently
                await asyncio.gather(*batch)
                self.logger.info(f"Batch of {len(batch)} index creation tasks completed successfully.")
            except Exception as e:
                self.logger.error(f"An error occurred during batch execution: {e}")
                raise


    async def create_indexes(self, indexes: List[ColumnIndex]):
        """Creates indexes based on the provided configuration, asynchronously with batching and caching."""

        if not indexes:
            self.logger.warning("No indexes provided for creation.")
            return

        # Dynamically adjust batch size based on system load
        batch_size = self._adjust_batch_size()

        # Store existing indexes for each table (only once, cached)
        indexes_names = {}

        # Cache the list of indexes
        async with self.indexes_lock:
            for index_info in indexes:
                table_name = index_info.table_name
                if table_name not in self.index_cache:
                    self.index_cache[table_name] = await self.list_indexes(table_name)
                indexes_names[table_name] = self.index_cache[table_name]

        # Function to check if the index exists on a table
        def has_index_alias(table_name: str, index_name: str) -> bool:
            return any(index == index_name for index in indexes_names.get(table_name, []))

        # Prepare the tasks to be executed concurrently
        tasks = []

        for index_info in indexes:
            schema_name = index_info.schema_name
            column_names = index_info.column_names
            table_name = index_info.table_name
            index_type = index_info.type

            # Check that all columns exist before proceeding (with caching)
            for column_name in column_names:
                if column_name not in self.column_cache:
                    self.column_cache[column_name] = await self.column_exists(schema_name, table_name, column_name)

                if not self.column_cache[column_name]:
                    message = f"Column {column_name} does not exist in table '{schema_name}.{table_name}'."
                    self.logger.error(message)
                    raise ValueError(message)

            # Create the index if not already existing
            for column_name in column_names:
                index_name = f"{table_name}_{column_name}_idx"

                if has_index_alias(table_name, index_name):
                    self.logger.info(f"Index {index_name} already exists on table {table_name}.")
                    continue  # Skip if the index already exists

                # Build the CREATE INDEX statement
                index_stmt = DDL(
                    f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    ON {table_name} USING {index_type} ({column_name});
                    """
                )

                # Add the task to the list to execute with connection pool
                tasks.append(self._execute_create_index(index_stmt))

        # Run the index creation tasks in batches to avoid overloading the database
        if tasks:
            await self._execute_in_batches(tasks, batch_size)


    async def _execute_create_index(self, index_stmt: DDL):
        """Executes the index creation statement using a connection from the pool."""
        async with self._acquire_connection() as connection:  # Acquire connection from the pool
            try:
                # Execute the index creation statement
                await connection.execute(str(index_stmt))
            except Exception as e:
                self.logger.error(f"Error creating index: {e}")
                raise


    async def _index_exists(self, table_name: str, index_name: str) -> bool:
        """Check if the specified index exists on the given table."""
        indexes = await self.list_indexes(table_name)
        return index_name in indexes


    async def schema_exists(self, schema_name):
        try:
            validate_schema_name(schema_name)
        except ValueError:
            self.logger.error(f'Invalid schema name {schema_name}.')
            raise

        query_str = """
            SELECT schema_name FROM information_schema.schemata WHERE schema_name = :table_schema
        """
        result = await self.execute(query_str, {'table_schema': schema_name})
        return bool(result)


    async def check_active_connections(self) -> bool:
        """Checks if there are active connections in the database."""
        query = "SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'"
        result = await self.execute(query)  # Execute the raw SQL query
        active_connections = result[0]["count"]  # Access the count from the first row
        return active_connections > 0


    async def check_replication_status(self) -> bool:
        """Checks if replication is active in the database (if applicable)."""
        query = "SELECT COUNT(*) FROM pg_stat_replication WHERE state = 'streaming'"
        result = await self.execute(query)  # Execute the raw SQL query
        replication_connections = result[0]["count"]  # Access the count from the first row
        return replication_connections > 0


    async def health_check(
        self, 
        use_admin_uri: bool = False, 
        timeout: Optional[int] = DEFAULT_HEALTHCHECK_TIMEOUT_S, 
        max_retries: int = MAX_RETRIES
    ) -> bool:
        """Performs a health check on the database connection with retries."""

        # Define the health check action to be retried
        async def action(): 
            try:
                # Perform checks
                if not await self.check_active_connections():
                    self.logger.warning("No active database connections found.")
                    return False

                if not await self.check_replication_status():
                    self.logger.warning("Replication is not active.")

                self.logger.info("Database health check passed.")
                return True

            except Exception as e:
                self.logger.error(f"Health check failed: {e}")
                return False

        # Call the retry_async function for retry logic
        return await retry_async(action, max_retries=max_retries, timeout=timeout)


    async def create_tables(self):
        """Create tables based on SQLAlchemy models."""
        self.base.metadata.create_all
        self.logger.info("Successfully created all tables asynchronously.")


    async def drop_tables(self):
        """Unified method to drop tables synchronously or asynchronously."""
        self.base.metadata.drop_all
        self.logger.info("Successfully dropped all tables asynchronously.")


    async def execute(self, query: str, params: dict = None):
        """
        Execute a SQL query and return results asynchronously.
        """
        try:
            # Acquire a connection from the pool
            async with self._acquire_connection() as connection:
                if params:
                    # Convert named placeholders to positional placeholders for asyncpg
                    query, param_values = self._convert_to_asyncpg_query(query, params)
                    result = await connection.fetch(query, *param_values)
                else:
                    result = await connection.fetch(query)

                # Return the result as a list of rows
                return result

        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise

    def _convert_to_asyncpg_query(self, query: str, params: dict):
        """
        Convert query with named placeholders (:name) to asyncpg-style positional placeholders ($1, $2, ...).
        """
        # Replace each named placeholder with a positional placeholder
        positional_query = query
        positional_values = []
        for i, (key, value) in enumerate(params.items(), start=1):
            positional_query = positional_query.replace(f":{key}", f"${i}")
            positional_values.append(value)
        
        return positional_query, positional_values

    async def add_audit_trigger(self, table_name: str):
        """Add an audit trigger to the specified table."""
        
        # Validate table names to prevent SQL injection
        if not self._is_valid_table_name(table_name):
            raise ValueError("Invalid table name provided.")

        audit_table_name = table_name + '_audit'

        # Prepare queries
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {audit_table_name} (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                old_data JSONB,
                new_data JSONB,
                changed_at TIMESTAMP DEFAULT NOW()
            );
        """
        create_function_query = f"""
            CREATE OR REPLACE FUNCTION log_changes() RETURNS TRIGGER AS $$
            BEGIN
            INSERT INTO {audit_table_name} (table_name, operation, old_data, new_data, changed_at)
            VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW), NOW());
            RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """
        create_trigger_query = f"""
            CREATE TRIGGER {table_name}_audit_trigger
            AFTER INSERT OR UPDATE OR DELETE ON {table_name}
            FOR EACH ROW EXECUTE FUNCTION log_changes();
        """

        # Execute queries
        await self.execute(create_table_query)
        await self.execute(create_function_query)
        await self.execute(create_trigger_query)


    async def disconnect(self):
        """Cleans up and closes the database connections, synchronous or asynchronously."""
        await self.engine.dispose()


    async def paginate(
        self, conn: DatabaseConnection, 
        query: str, params: Optional[Dict[str, Any]] = None, 
        batch_size: int = PAGINATION_BATCH_SIZE
    ) -> AsyncGenerator:
        """Unified paginate interface for synchronous and asynchronous queries."""
        paginator = TablePaginator(conn, query, params=params, batch_size=batch_size)
        async for page in paginator._paginated_query_async():
            yield page


    # 1. List tables
    async def list_tables(self, schema_name: str = 'public'):
        """List all tables in the specified schema."""
        if not await self.schema_exists(schema_name):
            error_message=f"Schema '{schema_name}' does not exist."
            self.logger.error(error_message)
            raise ValueError(error_message)

        try:
            query_str = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :table_schema;
            """
            result = await self.execute(query_str, {'table_schema': schema_name})
            return [table[0] for table in result]
        except Exception as e:
            self.logger.error(f"Failed to list tables in schema '{schema_name}': {e}")
            return []


    # 2. List Schemas
    async def list_schemas(self):
        """List all schemas in the database."""
        sync_query = "SELECT schema_name FROM information_schema.schemata;"
        result = await self.execute(sync_query)
        return [schema[0] for schema in result]


    # 3. List Indexes
    async def list_indexes(self,  table_name: str, schema_name: str = 'public'):
        """List all indexes for a given table."""
        sync_query = """
            SELECT indexname FROM pg_indexes 
            WHERE 
                schemaname = :schema_name and
                tablename = :table_name;
        """
        params={
            'table_name': table_name,
            'schema_name': schema_name
        }
        result = await self.execute(sync_query, params)
        return [index[0] for index in result]


    # 4. List Views
    async def list_views(self, schema_name='public'):
        """List all views in the specified schema."""
        sync_query = """
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = :table_schema;
        """
        result = await self.execute(sync_query, {'table_schema': schema_name})
        return [view[0] for view in result]


    # 5. List Sequences
    async def list_sequences(self):
        """List all sequences in the database."""
        sync_query = "SELECT sequence_name FROM information_schema.sequences;"
        result = await self.execute(sync_query)
        return [sequence[0] for sequence in result]


    # 6. List Constraints
    async def list_constraints(self, table_name: str, schema_name: str = 'public') -> List[TableConstraint]:
        """List all constraints for a specified table."""
        sync_query = """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage AS ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE 
                tc.table_name = :table_name AND 
                tc.table_schema = :table_schema;
        """
        params={
            'table_schema': schema_name, 
            'table_name': table_name
        }
        result = await self.execute(sync_query, params)
        return [
            TableConstraint(
                constraint_name=constraint_name,
                constraint_type=constraint_type,
                table_name=table_name,
                column_name=column_name,
                foreign_table_name=foreign_table_name,
                foreign_column_name=foreign_column_name
            ) 
            for constraint_name, constraint_type, 
            table_name, column_name, 
            foreign_table_name, foreign_column_name in result
        ]


    # 7. List Triggers
    async def list_triggers(self, table_name: str, schema_name: str = 'public'):
        """List all triggers for a specified table."""
        sync_query = """
            SELECT * 
            FROM information_schema.triggers 
            WHERE 
                event_object_table = :table_name AND
                event_object_schema = :schema_name;
        """

        # Assuming list_columns method exists
        columns = await self.list_columns('triggers', 'information_schema')
        params={
            'schema_name': schema_name, 
            'table_name': table_name
        }
        result = await self.execute(sync_query, params)
        return [
            Trigger(
                **dict(zip(columns, trigger_info))
            ) for trigger_info in result
        ]


    # 8. List Functions
    async def list_functions(self):
        """List all functions in the database."""
        sync_query = """
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE routine_type = 'FUNCTION';
        """
        return await self.execute(sync_query)


    # 9. List Procedures
    async def list_procedures(self):
        """List all procedures in the database."""
        sync_query = """
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE routine_type = 'PROCEDURE';
        """
        return await self.execute(sync_query)


    # 10. List Materialized Views
    async def list_materialized_views(self):
        """List all materialized views in the database."""
        sync_query = "SELECT matviewname FROM pg_matviews;"
        return await self.execute(sync_query)


    # 11. List Columns
    async def list_columns(self, table_name: str, schema_name: str = 'public'):
        """List all columns for a specified table in the schema."""
        sync_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = :table_schema 
            AND table_name = :table_name;
        """
        params={
            'table_schema': schema_name, 
            'table_name': table_name
        }
        result = await self.execute(sync_query, params)
        return [column[0] for column in result]


    # 12. List User-Defined Types
    async def list_types(self):
        """List all user-defined types in the database."""
        sync_query = "SELECT typname FROM pg_type WHERE typtype = 'e';"
        return await self.execute(sync_query)


    # 13. List Roles
    async def list_roles(self):
        """List all roles in the database."""
        sync_query = "SELECT rolname FROM pg_roles;"
        return await self.execute(sync_query)


    # 14. List Extensions
    async def list_extensions(self) -> list:
        """List all extensions installed in the database."""
        sync_query = "SELECT extname FROM pg_extension;"
        return await self.execute(sync_query)


    def __repr__(self):
        return f"<AsyncDatabase(uri={mask_sensitive_data(self.uri)})>"

class Datasource:
    """
    Manages multiple Database instances.

    Args:
        settings (DatasourceSettings): Settings containing all database configurations.
        logger (Optional[Logger]): Logger for logging information and errors. Defaults to a logger with the class name.

    Attributes:
        logger (Logger): Logger used for logging.
        databases (Dict[str, Database]): Dictionary of initialized database instances.
    """

    def __init__(self, settings: DatasourceSettings, logger: Optional[Logger] = None):
        self.logger = logger or getLogger(__name__)
        self.name = settings.name
        self.description = settings.description
        self.settings = settings

        # Initialize and validate database instances
        self.databases: Dict[str, AsyncDatabase] = {}
        for database_settings in settings.databases:
            self.databases[database_settings.name] = AsyncDatabase(database_settings)

    async def _call_database_method(self, database_name: str, method_name: str, *args: Any):
        """General method to call a database method dynamically."""
        database = self.get_database(database_name)
        method = getattr(database, method_name)
        return await method(*args)

    def get_database(self, name: str) -> BaseDatabase:
        """Returns the database instance for the given name."""
        if name not in self.databases:
            raise KeyError(f"Database '{name}' not found.")
        return self.databases[name]

    async def list_tables(self, database_name: str, table_schema: str):
        """List tables from a specified database or from all databases."""
        return await self._call_database_method(database_name, "list_tables", table_schema)

    async def list_schemas(self, database_name: str):
        """List indexes for a table in a specified database or across all databases."""
        return await self._call_database_method(database_name, "list_schemas")

    async def list_indexes(self, database_name: str, table_name: str):
        """List indexes for a table in a specified database."""
        return await self._call_database_method(database_name, "list_indexes", table_name)

    async def list_views(self, database_name: str, table_schema: str):
        """List views from a specified database."""
        return await self._call_database_method(database_name, "list_views", table_schema)

    async def list_sequences(self, database_name: str):
        """List sequences from a specified database."""
        return await self._call_database_method(
            database_name, "list_sequences"
        )

    async def list_constraints(self, database_name: str, table_schema: str, table_name: str):
        """List constraints for a table in a specified database."""
        return await self._call_database_method(
            database_name, "list_constraints", table_name, table_schema
        )

    async def list_triggers(self, database_name: str, table_name: str):
        """List triggers for a table in a specified database."""
        return await self._call_database_method(database_name, "list_triggers", table_name)

    async def list_functions(self, database_name: str):
        """List functions from a specified database."""
        return await self._call_database_method(database_name, "list_functions")

    async def list_procedures(self, database_name: str):
        """List procedures from a specified database."""
        return await self._call_database_method(database_name, "list_procedures")

    async def list_materialized_views(self, database_name: str):
        """List materialized views from a specified database."""
        return await self._call_database_method(
            database_name, "list_materialized_views"
        )

    async def list_columns(self, database_name: str, table_schema: str, table_name: str):
        """List columns for a table in a specified database."""
        return await self._call_database_method(
            database_name, "list_columns", table_name, table_schema
        )

    async def column_exists(self, database_name: str, table_schema: str, table_name: str, column: str):
        """Check if a column exists in a specified table and database."""
        return await self._call_database_method(
            database_name, "column_exists", table_schema, table_name, column
        )

    async def list_types(self, database_name: str):
        """List user-defined types from a specified database."""
        return await self._call_database_method(database_name, "list_types")

    async def list_roles(self, database_name: str):
        """List roles from a specified database."""
        return await self._call_database_method(database_name, "list_roles")

    async def list_extensions(self, database_name: str):
        """List extensions from a specified database."""
        return await self._call_database_method(database_name, "list_extensions")

    async def _call_database_method_all(self, method_name: str, *args: Any) -> Dict[str, Union[Any, str]]:
        """
        Calls a method on all databases and returns the results concurrently.

        Args:
            method_name (str): The name of the method to call on each database.
            *args: Arguments to pass to the database method.

        Returns:
            A dictionary with database names as keys and the result of the method call or an error message.
        """
        async def call_method(name: str, db: AsyncDatabase) -> Tuple[str, Union[Any, str]]:
            """Helper function to call a method on a single database."""
            self.logger.info(f"Starting {method_name} for database '{name}'")
            try:
                # Dynamically get the method from the database instance
                method = getattr(db, method_name)

                # Ensure the method is callable before attempting to invoke it
                if not callable(method):
                    raise AttributeError(f"Method '{method_name}' is not callable on database '{name}'.")

                # Call the method with provided arguments
                result = await method(*args)
                self.logger.info(f"{method_name} for database '{name}' succeeded.")
                return name, result
            except AttributeError:
                # Handle cases where the method does not exist
                error_message = f"Method '{method_name}' not found for database '{name}'."
                self.logger.error(error_message)
                return name, error_message
            except Exception as e:
                # Log and store the exception for any other errors
                error_message = f"{method_name} failed for database '{name}': {e}"
                self.logger.error(error_message)
                return name, error_message

        # Use asyncio.gather to run the method calls concurrently
        tasks = [
            call_method(name, db) for name, db in self.databases.items()
        ]
        results = await asyncio.gather(*tasks)

        # Convert the list of results into a dictionary
        return dict(results)

    async def health_check_all(self) -> Dict[str, bool]:
        """Performs health checks on all databases."""
        return await self._call_database_method_all('health_check')

    async def create_tables_all(self):
        """Creates tables for all databases."""
        await self._call_database_method_all('create_tables')

    async def disconnect_all(self):
        """Disconnects all databases."""
        await self._call_database_method_all('disconnect')

    def __getitem__(self, database_name: str):
        return self.get_database(database_name)

    def __repr__(self):
        return f'Datasource({self.databases.keys()})'


class DataGrid:
    """
    Manages multiple Datasource instances.

    Args:
        settings_dict (Dict[str, DatasourceSettings]): A dictionary containing datasource names and their settings.
        logger (Optional[Logger]): Logger for logging information and errors. Defaults to a logger with the class name.

    Attributes:
        logger (Logger): Logger used for logging.
        datasources (Dict[str, Datasource]): Dictionary of initialized datasource instances.
    """

    def __init__(
        self, settings_dict: Dict[str, DatasourceSettings], logger: Optional[Logger] = None
    ):
        self.logger = logger or getLogger(self.__class__.__name__)

        self.datasources: Dict[str, Datasource] = {}
        for name, settings in settings_dict.items():
            # Initialize and validate datasource instance
            self.datasources[name] = Datasource(settings, self.logger)
            self.logger.info(f"Initialized datasource '{name}' successfully.")

    def get_datasource(self, name: str) -> Datasource:
        """Returns the datasource instance for the given name."""
        if name not in self.datasources:
            self.logger.error(f"Datasource '{name}' not found.")
            raise KeyError(f"Datasource '{name}' not found.")

        return self.datasources[name]

    async def _call_datasource_method_all(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Calls a method on all datasources and collects the results.

        Args:
            method_name (str): The name of the method to call on each datasource.
            *args: Positional arguments to pass to the datasource method.
            **kwargs: Keyword arguments to pass to the datasource method.

        Returns:
            A dictionary with datasource names as keys and the results of the method call as values.
        """
        results = {}
        for name, datasource in self.datasources.items():
            self.logger.info(f"Calling {method_name} for datasource '{name}'")
            try:
                method = getattr(datasource, method_name)
                results[name] = await method(*args, **kwargs)
                self.logger.info(f"{method_name} for datasource '{name}' completed successfully.")
            except Exception as e:
                self.logger.error(f"{method_name} failed for datasource '{name}': {e}")
                results[name] = {'error': str(e)}
        return results

    async def health_check_all(self) -> Dict[str, Dict[str, bool]]:
        """Performs health checks on all datasources."""
        return await self._call_datasource_method_all('health_check_all')

    async def create_tables_all(self):
        """Creates tables for all datasources."""
        await self._call_datasource_method_all('create_tables_all')

    async def disconnect_all(self):
        """Disconnects all datasources."""
        await self._call_datasource_method_all('disconnect_all')

    def __repr__(self) -> str:
        return f"<DataGrid(datasources={list(self.datasources.keys())})>"
