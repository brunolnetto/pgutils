from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from pgbase.utils import convert_to_sync_dsn


def create_database(uri: str, db_name: str):
    # Connect to the default PostgreSQL database
    default_engine = create_engine(uri, isolation_level="AUTOCOMMIT")

    # Drop the database if it exists
    with default_engine.connect() as conn:
        query = text(f"DROP DATABASE IF EXISTS {db_name};")
        conn.execute(query)
        print(f"Dropped database '{db_name}' if it existed.")

        conn.execute(text(f"CREATE DATABASE {db_name};"))
        print(f"Database '{db_name}' created.")


def populate_database(uri: str, db_name: str):
    # Create an engine and session for interacting with the database
    default_engine = create_engine(uri, isolation_level="AUTOCOMMIT")

    try:
        with default_engine.connect() as conn:
            # Create the test table if not exists
            create_query = text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                );
            """)
            conn.execute(create_query)

            # Check if the test_table exists and is available
            select_table = text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'test_table';
            """)
            result = conn.execute(select_table)
            if not result.fetchone():
                raise ValueError("The table 'test_table' does not exist.")

            # Clear existing data from the table
            conn.execute(text("DELETE FROM test_table;"))

            # Insert new data into the test_table
            insert_data_query = text("""
                INSERT INTO test_table (name)
                VALUES ('Alice'), ('Bob'), ('Charlie'), ('David');
            """)
            conn.execute(insert_data_query)

            # Commit the transaction
            conn.commit()

    except SQLAlchemyError as e:
        # Handle any SQLAlchemy errors
        print(f"An error occurred while populating the database: {e}")
        conn.rollback()
    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")


def prepare_database(admin_uri: str, data_uri: str, db_name: str):
    admin_uri_ = convert_to_sync_dsn(admin_uri)
    data_uri_ = convert_to_sync_dsn(data_uri)

    create_database(admin_uri_, db_name)
    populate_database(data_uri_, db_name)
