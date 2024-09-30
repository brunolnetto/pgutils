from contextlib import contextmanager, asynccontextmanager
from typing import Union, List, Any, Generator, AsyncGenerator, Dict
from logging import getLogger, Logger 
from re import match
import asyncio

from pydantic import ValidationError
from sqlalchemy import DDL
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from sqlalchemy.exc import (
    ProgrammingError, 
    OperationalError, 
    ResourceClosedError, 
    SQLAlchemyError
)

from .models import DatabaseSettings, Index, Paginator 
from .utils import run_async_method

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

        self.engine = self._create_engine()
        self.session_maker = self._create_sessionmaker()

        if config.db_name:
            self.__create_database_if_not_exists(config.db_name)

        self.configure_parallel_queries()

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

    def __create_database_if_not_exists(self, db_name: str):
        """Creates the database if it does not exist."""
        # Create a temporary engine for the default database
        temp_engine = create_engine(str(self.admin_uri), isolation_level="AUTOCOMMIT")

        # Use the temporary engine to create the specified database if it doesn't exist
        with temp_engine.connect() as connection:
            try:
                # Attempt to create the database
                connection.execute(text(f"CREATE DATABASE \"{db_name}\""))
            except ProgrammingError as e:
                # Check if the error indicates the database already exists
                if 'already exists' not in str(e):
                    raise  # Reraise if it's a different error
    
    def drop_database_if_exists(self, db_name: str):
        """Drops the database if it exists."""
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
                    WHERE pg_stat_activity.datname = '{db_name}'
                    AND pid <> pg_backend_pid();
                """))
                self.logger.info(f"Terminated connections for database '{db_name}'.")
                connection.execute(text(f"DROP DATABASE IF EXISTS \"{db_name}\""))
                self.logger.info(f"Database '{db_name}' dropped successfully.")
            except (OperationalError, ProgrammingError) as e:
                self.logger.error(f"Error while dropping database '{db_name}': {e}")

    async def check_columns_exist_async(self, table_name: str, columns: List[str]) -> List[str]:
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
            actual_columns = {row['column_name'] for row in result}
            print(actual_columns)
            missing_columns = [col for col in columns if col not in actual_columns]
            return missing_columns

    def check_columns_exist_sync(self, table_name: str, columns: List[str]) -> List[str]:
        """Check if the specified columns exist in the table synchronously."""
        inspector = inspect(self.engine)
        actual_columns = {column['name'] for column in inspector.get_columns(table_name)}
        missing_columns = [col for col in columns if col not in actual_columns]
        return missing_columns

    def check_columns_exist(self, table_name: str, columns: List[str]) -> List[str]:
        """Check if the specified columns exist in the table, supporting both async and sync modes."""
        if self.async_mode:
            return run_async_method(self.check_columns_exist_async, table_name, columns)
        else:
            return self.check_columns_exist_sync(table_name, columns)

    async def create_indexes(self, indexes: Dict[str, Index]):
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
            if await self._index_exists(session, table_name, index_name):
                self.logger.info(f"Index {index_name} already exists on table {table_name}.")
                return

            index_stmt = await self._create_index_statement(table_name, columns, index_type)
            await self._execute_index_creation(session, index_stmt, table_name)

    async def _create_index_statement(self, table_name: str, columns: list, index_type: str) -> DDL:
        """Generate the DDL statement for creating an index."""
        return DDL(
            f"CREATE INDEX IF NOT EXISTS {table_name}_{'_'.join(columns)}_idx "
            f"ON {table_name} USING {index_type} ({', '.join(columns)});"
        )

    async def _index_exists(self, session: AsyncSession, table_name: str, index_name: str) -> bool:
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
        if self.async_mode:
            return self._get_async_session()
        else:
            return self._get_sync_session()

    def mask_sensitive_data(self) -> str:
        """Masks sensitive data (e.g., password) in the database URI."""
        masked_url = self.uri.set(password="******")
        if self.uri.username:
            masked_url = masked_url.set(username="******")
        return str(masked_url)

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
            print(is_healthy)
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

    async def _create_tables_async(self):
        """Asynchronously creates tables based on SQLAlchemy models."""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.base.metadata.create_all)
        except Exception as e:
            self.logger.error(f"Async error creating tables: {e}")

    def _create_tables_sync(self):
        """Creates tables based on SQLAlchemy models synchronously."""
        self.base.metadata.create_all(self.engine)

    def create_tables(self):
        """Creates tables based on SQLAlchemy models, synchronous or asynchronously."""
        if self.async_mode:
            # Check if there is an active event loop
            run_async_method(self._create_tables_async)
        else:
            self._create_tables_sync()

    def list_tables(self):
        """Lists tables in the database synchronously."""
        # Use synchronous execution to retrieve table names
        if self.async_mode:
            return run_async_method(self._list_tables_async())
        
        with self.engine.connect() as connection:
            inspector = inspect(connection)
            return inspector.get_table_names()

    async def _list_tables_async(self):
        """Lists tables in the database asynchronously."""
        async with self.get_session() as connection:
            inspector = inspect(connection)
            return await connection.run_sync(inspector.get_table_names)
    
    def _drop_tables_sync(self):
        """Drops all tables in the database synchronously."""
        self.base.metadata.drop_all(self.engine)

    async def _drop_tables_async(self):
        """Asynchronously drops all tables in the database."""
        async with self.engine.begin() as conn:
            await conn.run_sync(self.base.metadata.drop_all)

    def drop_tables(self):
        """Unified method to drop tables synchronously or asynchronously."""
        if self.async_mode:
            run_async_method(self._drop_tables_async())
        else:
            self._drop_tables_sync() 

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
        # Check for valid table name using a regex pattern: 
        #   Alphanumeric and underscore, starting with a letter or underscore
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        return match(pattern, table_name) is not None

    async def _disconnect_async(self):
        """Asynchronously cleans up and closes the database connections."""
        await self.engine.dispose()

    def disconnect(self, async_mode: bool = None):
        """Cleans up and closes the database connections, synchronous or asynchronously."""
        if self.async_mode:
            run_async_method(self._disconnect_async())
        else:
            self.engine.dispose()

    def _execute_query_sync(self, query: str, params: Dict[str, Any] = None) -> List[Any]:
        """Execute a prepared query and return results synchronously."""
        with self.engine.begin() as conn:    
            try:
                # Prepare and execute the query
                result = conn.execute(text(query).bindparams(**params) if params else text(query))
        
                return result.fetchall() if self.async_mode else result.fetchall()
            except ResourceClosedError:
                return []

    async def _execute_query_async(self, query: str, params: Dict[str, Any] = None) -> List[Any]:
        """Execute a prepared query and return results asynchronously."""
        async with self.engine.begin() as conn:
            try:
                # Prepare and execute the query
                result = await conn.execute(text(query).bindparams(**params) if params else text(query))
                return await result.fetchall()
            except ResourceClosedError:
                    return []

    def query(self, query: str, params: Dict[str, Any] = None) -> List[Any]:
        """Unified method to execute queries synchronously or asynchronously."""
        # Lazy initialization: configure parallel queries if not already done
        if self.async_mode:
            return run_async_method(self._execute_query_async, query, params)
        else:
            return self._execute_query_sync(query, params)
    
    async def paginate(
        self, query: str, params: dict = None, batch_size: int = 100
    ) -> AsyncGenerator[List[Any], None]:
        """Unified paginate interface for synchronous and asynchronous queries."""
        paginator = Paginator(query, params, batch_size)

        if self.async_mode:
            async for page in paginator._async_paginated_query(self):
                yield page
        else:
            for page in paginator._sync_paginated_query(self):
                yield page
    
    def __repr__(self):
        return f"<Database(uri={self.mask_sensitive_data()}, async_mode={self.async_mode})>"


class MultiDatabase:
    """
    Class to manage multiple Database instances.
    """

    def __init__(
        self, 
        settings_dict: Dict[str, DatabaseSettings], 
        logger: Logger = None
    ):
        self.logger = logger or getLogger(__name__)

        self.databases = {}
        for name, settings in settings_dict.items():
            try:
                # Validate config
                self.databases[name] = Database(settings, logger)
            except ValidationError as e:
                self.logger.error(f"Invalid configuration for database '{name}': {e}")

    def get_database(self, name: str) -> Database:
        """Get a specific database instance."""
        return self.databases.get(name)

    def health_check_all(self) -> dict:
        """Health check for all databases (sync and async)."""
        results = {}
        for name, db in self.databases.items():
            self.logger.info(f"Starting health check for database '{name}'")
            try:
                results[name] = db.health_check()
                self.logger.info(f"Health check for database '{name}' succeeded: {results[name]}")
            except Exception as e:
                self.logger.error(f"Health check failed for database '{name}': {e}")
                results[name] = False
        return results

    def create_tables_all(self):
        """Create tables for all databases (sync and async)."""
        for db in self.databases.values():
            db.create_tables()

    def disconnect_all(self):
        """Disconnect all databases (sync and async)."""
        for db in self.databases.values():
            db.disconnect()

    def __repr__(self):
        return f"<MultiDatabase(databases={list(self.databases.keys())})>"

    async def __aenter__(self):
        try:
            yield self
        finally:
            await self.disconnect_all()

    def __enter__(self):
        try:
            yield self
        finally:
            self.disconnect_all()