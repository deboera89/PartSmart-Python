import psycopg2
from psycopg2 import sql

# Database configuration
db_name = "downtime_data"
db_user = "postgres"
db_password = "1T1I1m1e"
db_host = "localhost"

try:
    # Connect to the default PostgreSQL database to drop the specific database
    conn = psycopg2.connect(
        dbname="postgres",  # Connect to the default database to manage other databases
        user=db_user,
        password=db_password,
        host=db_host
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Drop the database if it exists
    cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    print(f"Database '{db_name}' dropped successfully.")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"An error occurred: {e}")
