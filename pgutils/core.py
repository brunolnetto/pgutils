from contextlib import contextmanager, asynccontextmanager
from typing import Union, List, Any, Generator, AsyncGenerator, Dict, Optional

from logging import getLogger, Logger 
from re import match
import asyncio

from pydantic import ValidationError, TypeAdapter
from sqlalchemy import DDL
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from sqlalchemy.exc import (
    ProgrammingError, 
    OperationalError, 
    ResourceClosedError, 
    SQLAlchemyError
)

from .models import DatabaseSettings, TableConstraint, ColumnIndex, Paginator
from .utils import run_async_method, mask_sensitive_data
from .constants import PAGINATION_BATCH_SIZE

class Database:
    """
    Database class for managing PostgreSQL connections and operations.
    Supports both synchronous and asynchronous operations.
    """

    def __init__(self, config: DatabaseSettings, logger: Logger = None):
        self.config = config
        self.uri = make_url(str(config.complete_uri))
        self.admin_uri = config.admin_uri
        self.base = declarative_base()
        self.async_mode = config.async_mode
        self.logger = logger or getLogger(__name__)

        self.admin_engine = self._create_admin_engine()
        self.engine = self._create_engine()
        self.session_maker = self._create_sessionmaker()

        if config.db_name and config.auto_create_db:
            self._db_name = config.db_name
            self.create_database_if_not_exists(config.db_name)       

    def _create_engine(self):
        """Create and return the database engine based on async mode."""
        uri_str = str(self.uri)
        self.logger.debug(f"Creating engine with URI: {uri_str}")
        engine = create_async_engine(
            uri_str,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_pre_ping=True
        ) if self.async_mode else create_engine(
            uri_str,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_pre_ping=True
        )
        return engine
    
    def _create_admin_engine(self):
        """Create an admin engine to execute administrative tasks."""
        admin_uri_str = str(self.admin_uri)
        self.logger.debug(f"Creating admin engine with URI: {mask_sensitive_data(self.admin_uri)}")
        return create_engine(
            admin_uri_str, 
            isolation_level="AUTOCOMMIT",
            pool_pre_ping=True
        )

    def _create_sessionmaker(self):
        """Create and return a sessionmaker for the database engine."""
        return sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=False) \
            if self.async_mode else sessionmaker(bind=self.engine)

    def _get_parallel_queries(self):
        """Return the list of queries for parallel configuration."""
        return [
            "SET max_parallel_workers = 8;",                 # Number of parallel workers allowed in total
            "SET max_parallel_workers_per_gather = 4;",      # Number of workers allowed per query
            "SET min_parallel_table_scan_size = '8MB';",     # Minimum table size for parallel scan
            "SET min_parallel_index_scan_size = '512kB';"    # Minimum index size for parallel scan
        ]

    def set_parallel_queries_sync(self):
        """Synchronous configuration for parallel queries."""
        queries = self._get_parallel_queries()
        for query in queries:
            self._execute_query_sync(query)

    async def set_parallel_queries_async(self):
        """Asynchronous configuration for parallel queries."""
        queries = self._get_parallel_queries()
        for query in queries:
            await self._execute_query_async(query)

    def configure_parallel_queries(self):
        """Configure PostgreSQL parallel query settings lazily."""
        if self.async_mode:
            # Detect and handle the async loop
            run_async_method(self.set_parallel_queries_async)
        else:
            self.set_parallel_queries_sync()

    def check_database_exists(self, db_name: str = None):
        """Checks if the database exists."""
        db_name_to_check = db_name or self.config.db_name

        if not db_name_to_check:
            self.logger.error("No database name provided or configured.")
            return False

        with self.admin_engine.connect() as connection:
            try:
                create_query=text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name_to_check}';")
                result = connection.execute(create_query)
                exists = result.scalar() is not None  # Check if any row was returned
                return exists
            except (OperationalError, ProgrammingError) as e:
                self.logger.error(f"Error while checking if database '{db_name_to_check}' exists: {e}")
                return False

    def create_database_if_not_exists(self, db_name: str = None):
        db_name_to_check = db_name or self.config.db_name

        """Creates the database if it does not exist."""
        # Create a temporary engine for the default database
        temp_engine = create_engine(str(self.admin_uri), isolation_level="AUTOCOMMIT")

        # Use the temporary engine to create the specified database if it doesn't exist
        with temp_engine.connect() as connection:
            try:
                # Attempt to create the database
                query=text(f"CREATE DATABASE {db_name_to_check};")
                
                connection.execute(query)
            except ProgrammingError as e:
                # Check if the error indicates the database already exists
                if 'already exists' not in str(e):
                    raise  # Reraise if it's a different error

    def drop_database_if_exists(self):
        """Drops the database if it exists."""
        if self._db_name:
            admin_uri_str=str(self.admin_uri)
            sync_engine=create_engine(
                admin_uri_str, 
                isolation_level="AUTOCOMMIT"
            )
            
            with sync_engine.connect() as connection:
                try:
                    connection.execute(text(f"""
                        SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = '{self._db_name}'
                        AND pid <> pg_backend_pid();
                    """))
                    self.logger.info(f"Terminated connections for database '{self._db_name}'.")
                    connection.execute(text(f"DROP DATABASE IF EXISTS \"{self._db_name}\""))
                    self.logger.info(f"Database '{self._db_name}' dropped successfully.")
                except (OperationalError, ProgrammingError) as e:
                    self.logger.error(f"Error while dropping database '{self._db_name}': {e}")

    def _are_columns_present(self, columns: List[str], actual_columns: set) -> bool:
        """Helper method to check if all specified columns are present in the actual columns."""
        return all(col in actual_columns for col in columns)
    
    async def _check_columns_exist_async(self, table_name: str, columns: List[str]) -> bool:
        """Check if the specified columns exist in the table asynchronously."""
        async with self.get_session() as session:
            query=text(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                """
            )
            result = await session.execute(query, {"table_name": table_name})
            
            actual_columns = {row for row in result}
            print(actual_columns)
            print(columns)
            return self._are_columns_present(columns, actual_columns)

    def _check_columns_exist_sync(self, table_name: str, columns: List[str]) -> bool:
        """Check if the specified columns exist in the table synchronously."""
        inspector = inspect(self.engine)
        actual_columns = {column['name'] for column in inspector.get_columns(table_name)}
        
        return self._are_columns_present(columns, actual_columns)

    def check_columns_exist(self, table_name: str, columns: List[str]) -> List[str]:
        """Check if the specified columns exist in the table, supporting both async and sync modes."""
        if self.async_mode:
            return run_async_method(self._check_columns_exist_async, table_name, columns)
        else:
            return self._check_columns_exist_sync(table_name, columns)

    def _create_index_statement(self, table_name: str, column_name: str, index_type: str) -> DDL:
        """Generate the DDL statement for creating an index."""
        return DDL(
            f"CREATE INDEX IF NOT EXISTS {table_name}_{column_name}_idx "
            f"ON {table_name} USING {index_type} ({column_name});"
        )

    async def create_indexes(self, indexes: Dict[str, ColumnIndex]):
        """Creates indexes based on the provided configuration."""
        if not indexes:
            self.logger.warning("No indexes provided for creation.")
            return  # No indexes to create

        for table_name, index_info in indexes.items():
            missing_columns = await self.check_columns_exist(table_name, index_info['columns'])
            if missing_columns:
                self.logger.error(f"Missing columns {missing_columns} in table '{table_name}'.")
                raise ValueError(f"Columns {missing_columns} do not exist in table '{table_name}'.")

            await self._create_index(table_name, index_info)

    async def _create_index(self, table_name: str, index_info: Dict[str, Any]):
        """Helper method to create a single index."""
        columns = index_info['columns']
        index_type = index_info.get('type', 'btree')
        index_name = f"{table_name}_{'_'.join(columns)}_idx"

        async with AsyncSession(self.engine) as session:
            for column in columns:
                if await self._index_exists(session, table_name, index_name):
                    self.logger.info(f"Index {index_name} already exists on table {table_name}.")
                    return

                index_stmt = await self._create_index_statement(table_name, columns, index_type)
                await self._execute_index_creation(session, index_stmt, table_name)


    def _index_exists(self, session: AsyncSession, table_name: str, index_name: str) -> bool:
        """Check if the index exists in the specified table."""
        inspector = inspect(session.bind)
        existing_indexes = inspector.get_indexes(table_name)
        return any(index['name'] == index_name for index in existing_indexes)

    async def _execute_index_creation(self, session: AsyncSession, index_stmt: DDL, table_name: str):
        """Execute the index creation statement."""
        try:
            async with session.begin():
                await session.execute(index_stmt)
        except Exception as e:
            self.logger.error(f"Error creating index on {table_name}: {e}")

    @asynccontextmanager
    async def _get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Async method to get a database session."""
        async with self.session_maker() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                raise e
            finally:
                await session.close()

    @contextmanager
    def _get_sync_session(self) -> Generator[Session, None, None]:
        """Synchronous session manager."""
        session = self.session_maker()
        try:
            yield session
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_session(self) -> Union[asynccontextmanager, contextmanager]:
        """Unified session manager for synchronous and asynchronous operations."""
        return self._get_async_session() if self.async_mode else self._get_sync_session()

    def _health_check_sync(self):
        """Execute health check logic."""
        try:
            with self.get_session() as connection:
                # Execute a simple query to test the connection
                connection.execute(text("SELECT 1"))
                return True
        except ProgrammingError as e:
            self.logger.error(f"Health check failed: {e}")
            return False
        except Exception:
            return False

    async def _health_check_async(self) -> bool:
        """Checks if the database connection is alive."""    
        # Implement your async health check logic here        
        async with self.get_session() as session:    
            query=text("SELECT 1")
            result = await session.execute(query)
            is_healthy=result.scalar() == 1

            return is_healthy

    async def health_check_async(self) -> bool:
        """Asynchronous health check for database."""
        try:
            return await self._health_check_async()
        except Exception as e:
            self.logger.error(f"Async health check failed: {e}")
            return False

    def health_check(self, use_admin_uri: bool = False) -> bool:
        """Checks database connection, synchronous or asynchronously."""
        try:
            if use_admin_uri:
                # Use the admin URI for the health check
                temp_engine = create_engine(str(self.admin_uri))
                with temp_engine.connect() as connection:
                    check_query = text("SELECT 1")
                    connection.execute(check_query)
                    return True
            else:
                if self.async_mode:
                    # Run the async function in the current event loop
                    return run_async_method(self._health_check_async)
                else:
                    return self._health_check_sync()
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False

    def execute(self, sync_query: str, async_query: callable, params: dict = None):
        """General method for synchronous and asynchronous query execution."""        
        if self.async_mode:
            return run_async_method(async_query, params)

        with self.get_session() as session:
            # Prepare the query with bound parameters
            query = text(sync_query).bindparams(**params) if params else text(sync_query)
            compiled_query=query.compile(compile_kwargs={"literal_binds": True})
            
            # Execute the query
            conn_result = session.execute(query)
            rows = conn_result.fetchall()
            
            # Extract results
            result = [row for row in rows]
            
            return list(rows)

    async def async_query_method(self, query: str, params: Dict):
        """Helper method for running async queries."""
        async with self.get_session() as session:
            query_text=text(query).bindparams(**params) if params else text(query)
            
            result = await session.execute(query_text)
            return [row for row in result]

    def _execute_query_sync(self, query: str, params: Dict[str, Any] = None) -> List[Any]:
        """Execute a prepared query and return results synchronously."""
        with self.get_session() as conn:
            try:
                # Prepare and execute the query
                query=text(query).bindparams(**params) if params else text(query)
                result = conn.execute(query)
        
                return result.fetchall()
            except ResourceClosedError:
                return []

    async def _execute_query_async(self, query: str, params: Dict[str, Any] = None) -> List[Any]:
        """Execute a prepared query and return results asynchronously."""
        async with self.get_session() as conn:
            try:
                # Prepare and execute the query
                query=text(query).bindparams(**params) if params else text(query)
                result = await conn.execute(query)
                return await result.fetchall()
            except ResourceClosedError:
                    return []

    def _execute_list_query(self, query: str, params: dict = None):
        """Execute a list query and return results synchronously or asynchronously."""
        async_handler = lambda p: self.async_query_method(query, p)
        return self.execute(query, async_handler, params)

    def create_tables(self):
        """Creates tables based on SQLAlchemy models, synchronously or asynchronously."""
        # Synchronous version
        if self.async_mode:
            # Asynchronous version
            async def create_tables_async():
                async with self.engine.begin() as conn:
                    try:
                        await conn.run_sync(self.base.metadata.create_all)
                    except Exception as e:
                        self.logger.error(f"Async error creating tables: {e}")

            # Run the asynchronous method if async_mode is True
            run_async_method(create_tables_async)
            
        self.base.metadata.create_all(self.engine)
        return

    # 1. List Tables
    def list_tables(self, table_schema: str = 'public'):
        sync_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :table_schema;
        """
        return self._execute_list_query(sync_query, {'table_schema': table_schema})

    # 2. List Schemas
    def list_schemas(self):
        sync_query = "SELECT schema_name FROM information_schema.schemata;"
        return self._execute_list_query(sync_query)

    # 3. List Indexes
    def list_indexes(self, table_name: str):
        sync_query = """
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = :table_name;
        """
        return self._execute_list_query(sync_query, {'table_name': table_name})

    # 4. List Views
    def list_views(self, table_schema='public'):
        sync_query = """
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = :table_schema;
        """
        return self._execute_list_query(sync_query, {'table_schema': table_schema})

    # 5. List Sequences
    def list_sequences(self):
        sync_query = "SELECT sequence_name FROM information_schema.sequences;"
        return self._execute_list_query(sync_query)

    # 6. List Constraints
    def list_constraints(self, table_name: str, table_schema: str = 'public') -> List[TableConstraint]:
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
            WHERE tc.table_name = :table_name
                AND tc.table_schema = :table_schema;
        """
        return self._execute_list_query(sync_query, {
            'table_schema': table_schema,
            'table_name': table_name
        })

    # 7. List Triggers
    def list_triggers(self, table_name: str):
        sync_query = """
            SELECT trigger_name 
            FROM information_schema.triggers 
            WHERE event_object_table = :table_name;
        """
        return self._execute_list_query(sync_query, {'table_name': table_name})

    # 8. List Functions
    def list_functions(self):
        sync_query = """
            SELECT routine_name FROM information_schema.routines WHERE routine_type = 'FUNCTION';
        """
        return self._execute_list_query(sync_query)


    # 9. List Procedures
    def list_procedures(self):
        sync_query = """
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE routine_type = 'PROCEDURE';
        """
        return self._execute_list_query(sync_query)

    # 10. List Materialized Views
    def list_materialized_views(self):
        sync_query = """
            SELECT matviewname 
            FROM pg_matviews;
        """
        return self._execute_list_query(sync_query)

    # 11. List Columns
    def list_columns(self, table_name: str, table_schema: str = 'public'):
        sync_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE 
                table_schema = :table_schema and
                table_name = :table_name;
        """
        return self._execute_list_query(sync_query, {
            'table_schema': table_schema,
            'table_name': table_name
        })

    # 12. List User-Defined Types
    def list_types(self):
        sync_query = """
            SELECT typname 
            FROM pg_type 
            WHERE typtype = 'e';  -- Only enumerated types
        """
        return self._execute_list_query(sync_query)

    # 13. List Roles
    def list_roles(self):
        sync_query = "SELECT rolname FROM pg_roles;"
        return self._execute_list_query(sync_query)

    # 14. List Extensions
    def list_extensions(self) -> list:
        """Lists extensions installed in the database."""
        sync_query = "SELECT extname FROM pg_extension;"
        return self._execute_list_query(sync_query)

    def drop_tables(self):
        """Unified method to drop tables synchronously or asynchronously."""
        
        def _drop_tables_sync():
            """Drops all tables in the database synchronously."""
            try:
                self.base.metadata.drop_all(self.engine)
                self.logger.info("Successfully dropped all tables synchronously.")
            except Exception as e:
                self.logger.error(f"Error dropping tables synchronously: {e}")

        async def _drop_tables_async():
            """Asynchronously drops all tables in the database."""
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(self.base.metadata.drop_all)
                    self.logger.info("Successfully dropped all tables asynchronously.")
            except Exception as e:
                self.logger.error(f"Error dropping tables asynchronously: {e}")

        if self.async_mode:
            run_async_method(_drop_tables_async)  # Pass the function reference
        else:
            _drop_tables_sync()
            
    async def add_audit_trigger(self, audit_table_name: str, table_name: str):
        """Add an audit trigger to the specified table."""
        # Validate table names to prevent SQL injection
        is_audit_tablename_valid=not self._is_valid_table_name(audit_table_name)
        is_table_name_valid=not self._is_valid_table_name(table_name)
        are_tablenames_valid=is_audit_tablename_valid or is_table_name_valid
        if are_tablenames_valid:
            raise ValueError("Invalid table name provided.")

        # Prepare queries
        create_audit_table_query = f"""
            CREATE TABLE IF NOT EXISTS {audit_table_name} (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                old_data JSONB,
                new_data JSONB,
                changed_at TIMESTAMP DEFAULT NOW()
            );
        """

        create_trigger_function_query = f"""
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
        try:
            await self._execute_query_async(create_audit_table_query)
            await self._execute_query_async(create_trigger_function_query)
            await self._execute_query_async(create_trigger_query)
        except SQLAlchemyError as e:
            raise Exception(f"An error occurred while adding the audit trigger: {str(e)}")

    def _is_valid_table_name(self, table_name: str) -> bool:
        """Validate table name against SQL injection."""
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        is_valid = match(pattern, table_name) is not None
        if not is_valid:
            self.logger.warning(f"Invalid table name attempted: {table_name}")
        return is_valid

    def disconnect(self):
        """Cleans up and closes the database connections, synchronous or asynchronously."""
        
        if self.async_mode:
            run_async_method(self._disconnect_async)
        else:
            self._disconnect_sync()

    async def _disconnect_async(self):
        """Asynchronously cleans up and closes the database connections."""
        await self.engine.dispose()

    def _disconnect_sync(self):
        """Synchronously cleans up and closes the database connections."""
        self.engine.dispose()

    def query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Unified method to execute queries synchronously or asynchronously."""
        try:
            if self.async_mode:
                return run_async_method(self._execute_query_async, query, params)
            
            return self._execute_query_sync(query, params)
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return []
    
    def paginate(
        self, 
        query: str, 
        params: Optional[dict] = None, 
        batch_size: int = PAGINATION_BATCH_SIZE
    ) -> Generator[List[Any], None, None]:
        """Unified paginate interface for synchronous and asynchronous queries."""
        paginator = Paginator(query, params, batch_size)

        try:
            if self.async_mode:
                async_pages = run_async_method(paginator._async_paginated_query)
                for page in async_pages:
                    yield page
            else:
                for page in paginator._sync_paginated_query():
                    yield page
        except Exception as e:
            self.logger.error(f"Pagination failed: {e}")
            yield []  # Return an empty page in case of failure
    
    def __repr__(self):
        return f"<Database(uri={mask_sensitive_data(self.uri)}, async_mode={self.async_mode})>"


class MultiDatabase:
    """
    Manages multiple Database instances.

    Args:
        settings_dict (Dict[str, DatabaseSettings]): A dictionary containing database names and their settings.
        logger (Optional[Logger]): Logger for logging information and errors. Defaults to a logger with the class name.

    Attributes:
        logger (Logger): Logger used for logging.
        databases (Dict[str, Database]): Dictionary of initialized database instances.
    """

    def __init__(self, settings_dict: Dict[str, DatabaseSettings], logger: Optional[Logger] = None):
        self.logger = logger or getLogger(__name__)

        self.databases = {}
        for name, settings in settings_dict.items():
            try:
                # Initialize and validate database instance
                self.databases[name] = Database(settings, self.logger)
            except ValidationError as e:
                self.logger.error(f"Invalid configuration for database '{name}': {e}")

    def get_database(self, name: str) -> Database:
        """Returns the database instance for the given name."""
        if name not in self.databases:
            raise KeyError(f"Database '{name}' not found.")
        return self.databases[name]

    def health_check_all(self) -> Dict[str, bool]:
        """
        Performs health checks on all databases.
        
        Returns:
            A dictionary with database names as keys and the result of the health check (True/False) as values.
        """
        results = {}
        for name, db in self.databases.items():
            self.logger.info(f"Starting health check for database '{name}'")
            try:
                results[name] = db.health_check()
                self.logger.info(f"Health check for database '{name}' succeeded.")
            except Exception as e:
                self.logger.error(f"Health check failed for database '{name}': {e}")
                results[name] = False
        return results

    def create_tables_all(self):
        """Creates tables for all databases."""
        for db in self.databases.values():
            db.create_tables()

    def disconnect_all(self):
        """Disconnects all databases."""
        for db in self.databases.values():
            db.disconnect()

    @asynccontextmanager
    async def async_context(self):
        """
        Async context manager to manage connections and disconnections for async operations.
        """
        try:
            yield self
        finally:
            await self.disconnect_all()

    @contextmanager
    def sync_context(self):
        """
        Sync context manager to manage connections and disconnections for sync operations.
        """
        try:
            yield self
        finally:
            self.disconnect_all()

    def __repr__(self):
        return f"<MultiDatabase(databases={list(self.databases.keys())})>"