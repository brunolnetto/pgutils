from contextlib import contextmanager, asynccontextmanager
from typing import Union, List, Any
import logging
import asyncio
from asyncio import iscoroutinefunction
import re


from pydantic import BaseModel, HttpUrl, Field, ValidationError, field_validator, Field, constr
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.url import make_url

from .utils import validate_postgresql_uri

logger = logging.getLogger(__name__)

DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 10


class DatabaseConfig(BaseModel):
    uri: HttpUrl  # Database URI for regular operations
    admin_username: str = Field(min_length=1)  # Admin username must not be empty
    admin_password: str = Field(min_length=8)  # Admin password must be at least 8 characters
    async_mode: bool = False
    pool_size: int = Field(default=DEFAULT_POOL_SIZE, gt=0)  # Must be greater than 0
    max_overflow: int = Field(default=DEFAULT_MAX_OVERFLOW, ge=0)  # Must be 0 or greater
    db_name: str = Field(min_length=1)  # Database name must not be empty

    @property
    def admin_uri(self) -> str:
        """Constructs the admin URI using the username and password."""
        admin_uri = f"postgresql://{self.admin_username}:{self.admin_password}@{self.uri.host}:{self.uri.port}/{self.db_name}"
        # Validate the admin URI using shared validation logic
        validate_postgresql_uri(admin_uri, allow_async=False)
        return admin_uri

    @field_validator('uri')
    def validate_uri(cls, value):
        """Validates the URI format to assert PostgreSQL with psycopg or asyncpg."""
        return validate_postgresql_uri(value, allow_async=True)

    @field_validator('uri')
    def validate_uri(cls, value):
        """Validates the URI format to assert PostgreSQL with psycopg or asyncpg."""
        if not value.startswith(("postgresql+psycopg://", "postgresql+asyncpg://")):
            raise ValueError("URI must start with 'postgresql+psycopg://' or 'postgresql+asyncpg://'.")
        
        # Optionally, you can also check the full URI structure here.
        regex = re.compile(r"^postgresql\+.*://[a-zA-Z0-9._%+-]+:[^@]+@[^:/]+:\d+/.+$")
        if not regex.match(value):
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
        self.url = make_url(config.uri)
        self.base = declarative_base()
        self.async_mode = config.async_mode

        self.engine = (
            create_async_engine(config.uri, pool_size=config.pool_size, max_overflow=config.max_overflow)
            if self.async_mode
            else create_engine(config.uri, pool_size=config.pool_size, max_overflow=config.max_overflow)
        )
        self.session_maker = (
            sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=False)
            if self.async_mode
            else sessionmaker(bind=self.engine)
        )

    @contextmanager
    def get_session(self):
        """Context manager for a database session."""
        session = self.session_maker()
        try:
            yield session
        finally:
            session.close()

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
        masked_url = self.url.set(password="******")
        if self.url.username:
            masked_url = masked_url.set(username="******")
        return str(masked_url)

    def _execute_health_check(self):
        """Execute health check logic."""
        with self.engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        return True

    async def _async_execute_health_check(self):
        """Asynchronously execute health check logic."""
        async with self.engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        return True

    def health_check(self, async_mode: bool = None) -> bool:
        """Checks database connection, synchronous or asynchronously."""
        try:
            if self.async_mode:
                return asyncio.run(self._async_execute_health_check())
            else:
                return self._execute_health_check()
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def _async_create_tables(self):
        """Asynchronously creates tables based on SQLAlchemy models."""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.base.metadata.create_all)
        except Exception as e:
            logger.error(f"Async error creating tables: {e}")

    def create_tables(self):
        """Creates tables based on SQLAlchemy models, synchronous or asynchronously."""
        if self.async_mode:
            asyncio.run(self._async_create_tables())
        else:
            self.base.metadata.create_all(self.engine)

    def drop_tables_sync(self):
        """Drops all tables in the database synchronously."""
        self.base.metadata.drop_all(self.engine)

    async def _async_drop_tables(self):
        """Asynchronously drops all tables in the database."""
        async with self.engine.begin() as conn:
            await conn.run_sync(self.base.metadata.drop_all)

    def _drop_tables_async(self):
        """Drops all tables in the database asynchronously."""
        asyncio.run(self._async_drop_tables())

    def drop_tables(self, async_mode: bool = None):
        """Unified method to drop tables synchronously or asynchronously."""
        if self.async_mode:
            self.drop_tables_async()
        else:
            self.drop_tables_sync()

    async def _async_disconnect(self):
        """Asynchronously cleans up and closes the database connections."""
        await self.engine.dispose()

    def disconnect(self, async_mode: bool = None):
        """Cleans up and closes the database connections, synchronous or asynchronously."""
        if self.async_mode:
            asyncio.run(self._async_disconnect())
        else:
            self.engine.dispose()

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
