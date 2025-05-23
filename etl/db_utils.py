import psycopg2
import os
from psycopg2 import OperationalError, sql # Import sql for safe query construction if needed later

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database using environment variables.
    Returns a psycopg2 connection object or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "db"),  # Default to 'db' as per docker-compose
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "etl_data"),
            user=os.getenv("DB_USER", "etl_user"),
            password=os.getenv("DB_PASSWORD", "etl_password")
        )
        print("Successfully connected to PostgreSQL database.")
        return conn
    except OperationalError as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

def execute_ddl(conn, ddl_statements: list[str]):
    """
    Executes a list of DDL statements.
    Typically used for creating tables (CREATE TABLE IF NOT EXISTS ...).
    """
    if not conn:
        print("No database connection available to execute DDL.")
        return False
    
    try:
        with conn.cursor() as cur:
            for statement in ddl_statements:
                cur.execute(statement)
        conn.commit()
        print("DDL statements executed successfully.")
        return True
    except Exception as e:
        print(f"Error executing DDL: {e}")
        conn.rollback() # Rollback in case of error during DDL execution
        return False

if __name__ == '__main__':
    # Example usage:
    # Ensure Docker Compose is running with the 'db' service.
    # Environment variables should be set if running outside docker-compose context for this test.
    # For testing within the app container via docker-compose, env vars will be available.

    print("Attempting to connect to database from db_utils.py...")
    connection = get_db_connection()

    if connection:
        print("Connection successful. Now attempting to close.")
        # Example DDL (not actually creating tables here in __main__, just testing syntax)
        # test_ddl = [
        #     "CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name VARCHAR(100));",
        #     "INSERT INTO test_table (name) VALUES ('Test Name');" # Not DDL, but for testing execute
        # ]
        # if execute_ddl(connection, test_ddl[:1]): # Only create table
        #     print("Test DDL for table creation executed.")
        #     # You could add a SELECT here to verify, then DROP the table.
        #     with connection.cursor() as cur:
        #         cur.execute("SELECT * FROM test_table;")
        #         print("Test table content:", cur.fetchall())
        #         cur.execute("DROP TABLE test_table;")
        #     connection.commit()
        #     print("Test table dropped.")

        connection.close()
        print("Database connection closed.")
    else:
        print("Failed to connect to the database from db_utils.py.")
