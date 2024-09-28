from contextlib import contextmanager, asynccontextmanager
from typing import Union, List, Any
import logging
import asyncio
import re


from pydantic import BaseModel, AnyUrl, ValidationError, field_validator, Field
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ProgrammingError

from .utils import validate_postgresql_uri

logger = logging.getLogger(__name__)

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
    

class Database:
    """
    Database class for managing PostgreSQL connections and operations.
    Supports both synchronous and asynchronous operations.
    """

    def __init__(self, config: DatabaseConfig):
        self.uri = make_url(str(config.uri))
        self.admin_uri = config.admin_uri
        self.base = declarative_base()
        self.async_mode = config.async_mode

        self.engine = (
            create_async_engine(str(config.uri), pool_size=config.pool_size, max_overflow=config.max_overflow)
            if self.async_mode
            else create_engine(str(config.uri), pool_size=config.pool_size, max_overflow=config.max_overflow)
        )
        self.session_maker = (
            sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=False)
            if self.async_mode
            else sessionmaker(bind=self.engine)
        )

        # Create the database if a name is provided and it doesn't exist
        if config.db_name:  
            self.__create_database_if_not_exists(config.db_name)

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
        # Create a temporary engine for the admin connection
        temp_uri=str(self.admin_uri)
        temp_engine = create_engine(temp_uri, isolation_level="AUTOCOMMIT")

        # First, terminate all active connections to the database
        with temp_engine.connect() as connection:
            try:
                # Terminate active connections
                connection.execute(text(f"""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{db_name}'
                    AND pid <> pg_backend_pid();
                """))
            except Exception as e:
                logger.error(f"Error while terminating connections: {e}")

        # Now, proceed to drop the database
        with temp_engine.connect() as connection:
            try:
                # Attempt to drop the database
                connection.execute(text(f"DROP DATABASE IF EXISTS \"{db_name}\""))
            except ProgrammingError as e:
                logger.error(f"Error while dropping the database: {e}")

    async def _get_async_session(self):
        """Async method to get a database session."""
        async with self.session_maker() as session:
            yield session

    @contextmanager
    def _get_sync_session(self):
        """Synchronous session manager."""
        session = self.session_maker()
        try:
            yield session
        finally:
            session.close()

    def get_session(self) -> Union[asynccontextmanager, contextmanager]:
        """Unified session manager for synchronous and asynchronous operations."""
        if self.async_mode:
            return self._get_async_session()
        else:
            return self._get_sync_session()

    async def get_async_session(self):
        """Async context manager for a database session."""
        async with self.session_maker() as session:
            yield session

    def mask_sensitive_data(self) -> str:
        """Masks sensitive data (e.g., password) in the database URI."""
        masked_url = self.uri.set(password="******")
        if self.uri.username:
            masked_url = masked_url.set(username="******")
        return str(masked_url)

    def _health_check_sync(self):
        """Execute health check logic."""
        try:
            with self.engine.connect() as connection:
                print(self.uri.password)
                # Execute a simple query to test the connection
                connection.execute(text("SELECT 1"))
                return True
        except ProgrammingError as e:
            logger.error(f"Health check failed: {e}")
            return False
        except Exception:
            return False

    async def _health_check_async(self):
        """Checks if the database connection is alive."""
        try:
            async with self.engine.connect() as connection:
                # Execute a simple query to test the connection
                await connection.execute(text("SELECT 1"))
                return True
        except ProgrammingError as e:
            logger.error(f"Health check failed: {e}")
            return False
        except Exception:
            return False

    def health_check(self, use_admin_uri: bool = False) -> bool:
        """Checks database connection, synchronous or asynchronously."""
        try:
            if use_admin_uri:
                # Use the admin URI for the health check
                temp_uri=str(self.admin_uri)
                temp_engine = create_engine(temp_uri)
                with temp_engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                    return True
            else:
                if self.async_mode:
                    # Check if there's an existing event loop
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # If the loop is running, use a future to run the coroutine
                        return loop.run_until_complete(self._health_check_async())
                    else:
                        return asyncio.run(self._health_check_async())
                else:
                    return self._health_check_sync()
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


    async def _create_tables_async(self):
        """Asynchronously creates tables based on SQLAlchemy models."""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.base.metadata.create_all)
        except Exception as e:
            logger.error(f"Async error creating tables: {e}")

    def _create_tables_sync(self):
        """Creates tables based on SQLAlchemy models synchronously."""
        self.base.metadata.create_all(self.engine)

    def create_tables(self):
        """Creates tables based on SQLAlchemy models, synchronous or asynchronously."""
        if self.async_mode:
            # Check if there is an active event loop
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop, we can safely use asyncio.run
                asyncio.run(self._create_tables_async())
            else:
                # Running in an event loop, create a task
                loop.create_task(self._create_tables_async())
        else:
            self._create_tables_sync()

    def list_tables(self):
        """Lists tables in the database synchronously."""
        # Use synchronous execution to retrieve table names
        if self.async_mode:
            return asyncio.run(self._list_tables_async())
        
        with self.engine.connect() as connection:
            inspector = inspect(connection)
            return inspector.get_table_names()

    async def _list_tables_async(self):
        """Lists tables in the database asynchronously."""
        async with self.engine.connect() as connection:
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
            asyncio.run(self._drop_tables_async())
        else:
            self._drop_tables_sync() 

    async def _disconnect_async(self):
        """Asynchronously cleans up and closes the database connections."""
        await self.engine.dispose(close=False)

    def disconnect(self, async_mode: bool = None):
        """Cleans up and closes the database connections, synchronous or asynchronously."""
        if self.async_mode:
            asyncio.run(self._disconnect_async())
        else:
            self.engine.dispose(close=False)

    def _execute_query(self, query: str, params: dict = None) -> List[Any]:
        """Execute a query and return results synchronously."""
        with self.engine.begin() as conn:
            result = conn.execute(text(query), params)
            return result.fetchall()

    async def _async_execute_query(self, query: str, params: dict = None) -> List[Any]:
        """Execute a query and return results asynchronously."""
        async with self.engine.begin() as conn:
            result = await conn.execute(text(query), params)
            return await result.fetchall()

    def query(self, query: str, params: dict = None) -> List[Any]:
        """Unified method to execute queries synchronously or asynchronously."""
        if self.async_mode:
            return asyncio.run(self._async_execute_query(query, params))
        else:
            return self._execute_query(query, params)

    def __repr__(self):
        return f"<Database(uri={self.mask_sensitive_data()}, async_mode={self.async_mode})>"


class MultiDatabase:
    """
    Class to manage multiple Database instances.
    """

    def __init__(self, databases: dict):
        self.databases = {}
        for name, config in databases.items():
            try:
                # Validate config
                db_config = DatabaseConfig(**config)
                self.databases[name] = Database(db_config)
            except ValidationError as e:
                logger.error(f"Invalid configuration for database '{name}': {e}")


    def get_database(self, name: str) -> Database:
        """Get a specific database instance."""
        return self.databases.get(name)

    def health_check_all(self) -> dict:
        """Health check for all databases (sync and async)."""
        results = {}
        for name, db in self.databases.items():
            results[name] = db.health_check()
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
