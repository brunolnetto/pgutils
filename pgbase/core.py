from contextlib import contextmanager, asynccontextmanager
from typing import Union, List, Any, Generator, AsyncGenerator, Dict, Optional

from logging import getLogger, Logger 
from re import match

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

from .models import (
    DatasourceSettings, 
    DatabaseSettings, 
    TableConstraint, Trigger, ColumnIndex, TablePaginator
)
from .utils import (
    run_async_method, mask_sensitive_data, construct_admin_uri, construct_complete_uri
)
from .constants import PAGINATION_BATCH_SIZE
from .types import DatabaseConnection

class Database:
    """
    Database class for managing PostgreSQL connections and operations.
    Supports both synchronous and asynchronous operations.
    """
    RESERVED_KEYWORDS = {
        "SELECT", "INSERT", "DELETE", "UPDATE", "DROP", "CREATE", "FROM", "WHERE", "JOIN", "TABLE", "INDEX"
    }

    def __init__(self, config: DatabaseSettings, logger: Logger = None):
        self.config = config
        self.uri = make_url(str(config.complete_uri))
        self.admin_uri = config.admin_uri
        self.base  = declarative_base()
        self.async_mode = config.async_mode
        self.logger = logger or getLogger(__name__)

        self.admin_engine = self._create_admin_engine()
        self.engine = self._create_engine()
        self.session_maker = self._create_sessionmaker()

        if config.name and config.auto_create_db:
            self.name = config.name
            self.create_database_if_not_exists(config.name)       

    def _create_engine(self):
        """Create and return the database engine based on async mode."""
        uri_str = str(self.uri)
        self.logger.debug(f"Creating engine with URI: {uri_str}")
        engine = create_async_engine(
            uri_str,
            isolation_level="AUTOCOMMIT",
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_pre_ping=True
        ) if self.async_mode else create_engine(
            uri_str,
            isolation_level="AUTOCOMMIT",
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

    def check_database_exists(self, db_name: str = None):
        """Checks if the database exists."""
        db_name_to_check = db_name or self.config.name

        if not db_name_to_check:
            self.logger.error("No database name provided or configured.")
            return False

        with self.admin_engine.connect() as connection:
            create_query=text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name_to_check}';")
            result = connection.execute(create_query)
            exists = result.scalar() is not None  # Check if any row was returned
            return exists

    def create_database_if_not_exists(self, db_name: str = None):
        db_name_to_check = db_name or self.config.name

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
        db_name=self.config.name
        
        if db_name:
            admin_uri_str=str(self.admin_uri)
            sync_engine=create_engine(
                admin_uri_str, 
                isolation_level="AUTOCOMMIT"
            )
            
            with sync_engine.connect() as connection:
                connection.execute(text(f"""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{db_name}'
                    AND pid <> pg_backend_pid();
                """))
                self.logger.info(f"Terminated connections for database '{db_name}'.")
                connection.execute(text(f"DROP DATABASE IF EXISTS \"{db_name}\""))
                self.logger.info(f"Database '{db_name}' dropped successfully.")
    
    async def _column_exists_async(self, table_schema: str, table_name: str, column_name: str) -> bool:
        """Check if the specified columns exist in the table asynchronously."""
        async with self.get_session() as session:
            query=text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name and 
                table_schema = :table_schema
                """
            )
            params={
                "table_schema": table_schema,
                "table_name": table_name
            }
            result = await session.execute(query, params)
            
            actual_columns = {row[0] for row in result}
            return column_name in actual_columns

    def _column_exists_sync(self, table_schema: str, table_name: str, column_name: str) -> bool:
        """Check if the specified columns exist in the table synchronously."""
        with self.get_session() as session:
            query=text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name and 
                table_schema = :table_schema
                """
            )
            params={
                "table_schema": table_schema,
                "table_name": table_name
            }
            result = session.execute(query, params)
            
            actual_columns = {row[0] for row in result}
            return column_name in actual_columns

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if the specified columns exist in the table, supporting both async and sync modes."""
        if self.async_mode:
            return run_async_method(self._column_exists_async, schema_name, table_name, column_name)
        else:
            return self._column_exists_sync(schema_name, table_name, column_name)

    async def create_indexes(self, indexes: Dict[str, ColumnIndex]):
        """Creates indexes based on the provided configuration."""
        if not indexes:
            self.logger.warning("No indexes provided for creation.")
            return

        for table_name, index_info in indexes.items():
            missing_columns = await self.column_exists(table_name, index_info['columns'])
            if missing_columns:
                self.logger.error(f"Missing columns {missing_columns} in table '{table_name}'.")
                raise ValueError(f"Columns {missing_columns} do not exist in table '{table_name}'.")

            await self._create_index(table_name, index_info)

    def _create_index(self, table_name: str, index_info: Dict[str, Any]):
        """Helper method to create a single index."""
        columns = index_info['columns']
        index_type = index_info.get('type', 'btree')
        index_name = f"{table_name}_{'_'.join(columns)}_idx"

        for column_name in columns:
            if self._index_exists(table_name, index_name):
                self.logger.info(f"Index {index_name} already exists on table {table_name}.")
                return

            index_stmt = DDL(
                f"CREATE INDEX IF NOT EXISTS {table_name}_{column_name}_idx "
                f"ON {table_name} USING {index_type} ({column_name});"
            )
            
            with self.get_session() as session:
                try:
                    with session.begin():
                        session.execute(index_stmt)
                except Exception as e:
                    self.logger.error(f"Error creating index on {table_name}: {e}")

    def _index_exists(self, table_name: str, index_name: str) -> bool:
        """Check if the index exists in the specified table, supporting both sync and async modes."""
        return any(index == index_name for index in self.list_indexes(table_name))

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

    async def health_check_async(self) -> bool:
        """Asynchronous health check for database."""
        try:
            async with self.get_session() as session:    
                query=text("SELECT 1")
                result = await session.execute(query)
                is_healthy=result.scalar() == 1

                return is_healthy
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
                    return run_async_method(self.health_check_async)
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
            return [row for row in rows]

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
                return result.fetchall()
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
            
            return
            
        self.base.metadata.create_all(self.engine)
        return

    # 1. List Tables
    def list_tables(self, table_schema: str = 'public'):
        sync_query = """
            SELECT table_name
            FROM information_schema.tables 
            WHERE table_schema = :table_schema;
        """
        return [
            table_tuple[0]
            for table_tuple in self._execute_list_query(sync_query, {'table_schema': table_schema})
        ]

    # 2. List Schemas
    def list_schemas(self):
        sync_query = "SELECT schema_name FROM information_schema.schemata;"
        return [
            schema_tuple[0] for schema_tuple in self._execute_list_query(sync_query)
        ]

    # 3. List Indexes
    def list_indexes(self, table_name: str):
        sync_query = """
            SELECT indexname FROM pg_indexes 
            WHERE tablename = :table_name;
        """

        return [
            index_tuple[0]
            for index_tuple in self._execute_list_query(sync_query, {'table_name': table_name})
        ]

    # 4. List Views
    def list_views(self, table_schema='public'):
        sync_query = """
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = :table_schema;
        """
        return [
            views_tuple[0]
            for views_tuple in self._execute_list_query(sync_query, {'table_schema': table_schema})
        ]

    # 5. List Sequences
    def list_sequences(self):
        sync_query = "SELECT sequence_name FROM information_schema.sequences;"
        return [
            sequence_tuple[0]
            for sequence_tuple in self._execute_list_query(sync_query)
        ]

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
        return [
            TableConstraint(
                constraint_name=constraint_name,
                constraint_type=constraint_type,
                table_name=table_name,
                column_name=column_name,
                foreign_table_name=table_name,
                foreign_column_name=column_name
            ) for constraint_name, constraint_type, table_name, 
            column_name, table_name, column_name
            in self._execute_list_query(sync_query, {
            'table_schema': table_schema,
            'table_name': table_name
            })
        ]

    # 7. List Triggers
    def list_triggers(self, table_name: str):
        sync_query = """
            SELECT * 
            FROM information_schema.triggers 
            WHERE event_object_table = :table_name;
        """
        columns = self.list_columns('triggers', 'information_schema')
        return [
            Trigger(**dict(zip(columns, trigger_info)))
            for trigger_info in self._execute_list_query(sync_query, {'table_name': table_name})
        ]

    # 8. List Functions
    def list_functions(self):
        sync_query = """
            SELECT routine_name FROM information_schema.routines WHERE routine_type = 'FUNCTION';
        """
        return self._execute_list_query(sync_query)


    # 9. List Procedures
    def list_procedures(self):
        sync_query = """
            SELECT routine_name FROM information_schema.routines WHERE routine_type = 'PROCEDURE';
        """
        return self._execute_list_query(sync_query)

    # 10. List Materialized Views
    def list_materialized_views(self):
        sync_query = "SELECT matviewname FROM pg_matviews;"
        return self._execute_list_query(sync_query)

    # 11. List Columns
    def list_columns(self, table_name: str, table_schema: str = 'public'):
        sync_query = """
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = :table_schema and table_name = :table_name;
        """
        return [
            column_tuple[0]
            for column_tuple in self._execute_list_query(sync_query, {
                'table_schema': table_schema,
                'table_name': table_name
            })
        ]

    # 12. List User-Defined Types
    def list_types(self):
        sync_query = "SELECT typname FROM pg_type WHERE typtype = 'e';"
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

    def add_audit_trigger(self, table_name: str):
        """Add an audit trigger to the specified table."""
        
        # Validate table names to prevent SQL injection
        if not self._is_valid_table_name(table_name):
            raise ValueError("Invalid table name provided.")

        audit_table_name = table_name+'_audit'

        # Validate table names to prevent SQL injection
        is_audit_tablename_valid=not self._is_valid_table_name(audit_table_name)
        is_table_name_valid=not self._is_valid_table_name(table_name)
        are_tablenames_valid=is_audit_tablename_valid or is_table_name_valid
        if are_tablenames_valid:
            raise ValueError("Invalid table name provided.")

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
        try:
            self.query(create_table_query)
            self.query(create_function_query)
            self.query(create_trigger_query)
        except SQLAlchemyError as e:
            raise Exception(f"An error occurred while adding the audit trigger: {str(e)}")

    def _is_valid_table_name(self, table_name: str) -> bool:
        """Validate table name against SQL injection, reserved keywords, and special characters."""
        # Ensure it is not empty or just spaces
        if not table_name.strip():
            self.logger.warning(f"Invalid table name attempted: '{table_name}' (empty or whitespace)")
            return False

        # Ensure it starts with a letter or underscore and only contains valid characters
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        is_valid = match(pattern, table_name) is not None

        # Check if table name is exactly a reserved keyword
        if is_valid:
            upper_table_name = table_name.upper()
            if upper_table_name in self.RESERVED_KEYWORDS:
                is_valid = False

        # Ensure the name is not composed entirely of special characters
        if is_valid and match(r"^[^a-zA-Z0-9]+$", table_name):
            is_valid = False

        if not is_valid:
            self.logger.warning(f"Invalid table name attempted: '{table_name}'")

        return is_valid

    async def _disconnect_async(self):
        """Asynchronously cleans up and closes the database connections."""
        await self.engine.dispose()

    def _disconnect_sync(self):
        """Synchronously cleans up and closes the database connections."""
        self.engine.dispose()

    def disconnect(self):
        """Cleans up and closes the database connections, synchronous or asynchronously."""
        
        if self.async_mode:
            run_async_method(self._disconnect_async)
        else:
            self._disconnect_sync()

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
        conn: DatabaseConnection,
        query: str, 
        params: Optional[Dict[str, Any]] = None, 
        batch_size: int = PAGINATION_BATCH_SIZE
    ) -> Generator[List[Any], None, None]:
        """Unified paginate interface for synchronous and asynchronous queries."""
        paginator = TablePaginator(conn, query, params=params, batch_size=batch_size)

        try:
            if self.async_mode:
                async_pages = run_async_method(paginator._paginated_query_async)
                for page in async_pages:
                    yield page
            else:
                for page in paginator._paginated_query_sync():
                    yield page
        except Exception as e:
            self.logger.error(f"Pagination failed: {e}")
            yield []  # Return an empty page in case of failure
    
    def __repr__(self):
        return f"<Database(uri={mask_sensitive_data(self.uri)}, async_mode={self.async_mode})>"

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

        self.databases: Dict[str, Database] = {}
        for database_settings in settings.databases:
            # Initialize and validate database instance
            self.databases[database_settings.name] = Database(database_settings)

    def get_database(self, name: str) -> Database:
        """Returns the database instance for the given name."""
        if name not in self.databases:
            raise KeyError(f"Database '{name}' not found.")
        return self.databases[name]

    def list_tables(self, database_name: str, table_schema: str):
        """List tables from a specified database or from all databases."""
        return self.get_database(database_name).list_tables(table_schema)

    def list_schemas(self, database_name: str):
        """List indexes for a table in a specified database or across all databases."""
        return self.get_database(database_name).list_schemas()

    def list_indexes(self, database_name: str, table_name: str):
        """List indexes for a table in a specified database or across all databases."""
        return self.get_database(database_name).list_indexes(table_name)

    def list_views(self, database_name: str, table_schema: str):
        """List views from a specified database or from all databases."""
        return self.get_database(database_name).list_views(table_schema)

    def list_sequences(self, database_name: str):
        """List sequences from a specified database or from all databases."""
        return self.get_database(database_name).list_sequences()

    def list_constraints(self, database_name: str, table_schema: str, table_name: str):
        """List constraints for a table in a specified database or across all databases."""
        return self.get_database(database_name).list_constraints(table_name, table_schema=table_schema)

    def list_triggers(self, database_name: str, table_name: str):
        """List triggers for a table in a specified database or across all databases."""
        return self.get_database(database_name).list_triggers(table_name)

    def list_functions(self, database_name: str):
        """List functions from a specified database or from all databases."""
        return self.get_database(database_name).list_functions()

    def list_procedures(self, database_name: str):
        """List procedures from a specified database or from all databases."""
        return self.get_database(database_name).list_procedures()

    def list_materialized_views(self, database_name: str):
        """List materialized views from a specified database or from all databases."""
        return self.get_database(database_name).list_materialized_views()

    def list_columns(self, database_name: str, table_schema: str, table_name: str):
        """List columns for a table in a specified database or across all databases."""
        print(self.get_database(database_name))
        return self.get_database(database_name).list_columns(table_name, table_schema)

    def column_exists(
        self, database_name: str, table_schema: str, table_name: str, column: str
    ):
        """List columns for a table in a specified database or across all databases."""
        return self.get_database(database_name).column_exists(table_schema, table_name, column)

    def list_types(self, database_name: str):
        """List user-defined types from a specified database or from all databases."""
        return self.get_database(database_name).list_types()

    def list_roles(self, database_name: str):
        """List roles from a specified database or from all databases."""
        return self.get_database(database_name).list_roles()

    def list_extensions(self, database_name: str):
        """List extensions installed in a specified database or from all databases."""
        return self.get_database(database_name).list_extensions()

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
            
    def __getitem__(self, database_name: str):
        return self.get_database(database_name)

    def __repr__(self):
        return f'Datasource({self.databases.keys()})'

class DataCluster:
    """
    Manages multiple Datasource instances.

    Args:
        settings_dict (Dict[str, DatasourceSettings]): A dictionary containing datasource names and their settings.
        logger (Optional[Logger]): Logger for logging information and errors. Defaults to a logger with the class name.

    Attributes:
        logger (Logger): Logger used for logging.
        datasources (Dict[str, Datasource]): Dictionary of initialized datasource instances.
    """

    def __init__(self, settings_dict: Dict[str, DatasourceSettings], logger: Optional[Logger] = None):
        self.logger = logger or getLogger(self.__class__.__name__)

        self.datasources: Dict[str, Datasource] = {}
        for name, settings in settings_dict.items():
            try:
                # Initialize and validate datasource instance
                self.datasources[name] = Datasource(settings, self.logger)
                self.logger.info(f"Initialized datasource '{name}' successfully.")
            except ValidationError as e:
                self.logger.error(f"Invalid configuration for datasource '{name}': {e}")

    def get_datasource(self, name: str) -> Datasource:
        """Returns the datasource instance for the given name."""
        if name not in self.datasources:
            self.logger.error(f"Datasource '{name}' not found.")
            raise KeyError(f"Datasource '{name}' not found.")

        return self.datasources[name]

    def health_check_all(self) -> Dict[str, Dict[str, bool]]:
        """
        Performs health checks on all datasources.

        Returns:
            A dictionary with datasource names as keys and a dictionary of their databases' health check results.
        """
        results = {}
        for name, datasource in self.datasources.items():
            self.logger.info(f"Starting health check for datasource '{name}'")
            try:
                results[name] = datasource.health_check_all()
                self.logger.info(f"Health check for datasource '{name}' completed successfully.")
            except Exception as e:
                self.logger.error(f"Health check for datasource '{name}' failed: {e}")
                results[name] = {'error': str(e)}
        return results

    def create_tables_all(self):
        """Creates tables for all datasources."""
        for name, datasource in self.datasources.items():
            try:
                datasource.create_tables_all()
                self.logger.info(f"Tables created for datasource '{name}' successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create tables for datasource '{name}': {e}")

    def disconnect_all(self):
        """Disconnects all datasources."""
        for name, datasource in self.datasources.items():
            try:
                datasource.disconnect_all()
                self.logger.info(f"Disconnected datasource '{name}' successfully.")
            except Exception as e:
                self.logger.error(f"Failed to disconnect datasource '{name}': {e}")

    def __repr__(self) -> str:
        return f"<DataCluster(datasources={list(self.datasources.keys())})>"