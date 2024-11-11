import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import sql
from urllib.parse import urlparse
import logging
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
db_name = "downtime_data"
db_user = "postgres"
db_password = "1T1I1m1e"
db_host = "localhost"

# database_utils.py
import os
from sqlalchemy import create_engine, text


def initialize_database():
    # Get the Heroku database URL from the environment variable
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set.")

    # Replace postgres:// with postgresql://
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    # Connect to the database using SQLAlchemy
    engine = create_engine(DATABASE_URL)

    # Define SQL for creating the 'downtime' table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS downtime (
        mc TEXT,
        date DATE,
        downtime_start TIMESTAMP,
        downtime_finish TIMESTAMP,
        downtime_total_minutes DOUBLE PRECISION,
        downtime_reason TEXT,
        machine_state TEXT,
        shift_code TEXT,
        part_number TEXT,
        part_description TEXT,
        user_id TEXT,
        day TEXT,
        UNIQUE (mc, date, downtime_start, downtime_finish)
    );
    """

    # Execute the table creation SQL
    with engine.connect() as connection:
        connection.execute(text(create_table_sql))
        print("Database tables created successfully.")



def verify_connection():
    """Test database connection and return status"""
    try:
        # Get the DATABASE_URL environment variable
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL is None:
            raise Exception("DATABASE_URL environment variable not set")

        # Replace 'postgres://' with 'postgresql://' if needed
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

        # Parse the DATABASE_URL
        result = urlparse(DATABASE_URL)
        dbname = result.path[1:]  # Remove the leading '/' from the path
        user = result.username
        password = result.password
        host = result.hostname
        port = result.port

        # Connect to the database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.close()
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def get_current_count():
    """Get current count of rows in the 'downtime' table"""
    try:
        # Get the DATABASE_URL environment variable
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL is None:
            raise Exception("DATABASE_URL environment variable not set")

        # Replace 'postgres://' with 'postgresql://' if needed
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

        # Parse the DATABASE_URL
        result = urlparse(DATABASE_URL)
        dbname = result.path[1:]  # Remove the leading '/' from the path
        user = result.username
        password = result.password
        host = result.hostname
        port = result.port

        # Connect to the database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        # Execute the query to get the row count
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM downtime")
        count = cursor.fetchone()[0]

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return count
    except Exception as e:
        logger.error(f"Error getting current count: {e}")
        return None


import os
import psycopg2
import pandas as pd
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)

def setup_and_insert_data(file):
    """Enhanced version with proper transaction management"""
    try:
        # Get initial count
        initial_count = get_current_count()
        logger.info(f"Initial row count in database: {initial_count}")

        # Read and process the CSV file
        logger.info(f"Reading file: {file}")
        df = pd.read_csv(file)
        logger.info(f"CSV file read successfully. Shape: {df.shape}")

        # Data cleaning
        df.columns = ["mc", "date", "downtime_start", "downtime_finish", "downtime_total",
                      "remove_one", "remove_two", "remove_three", "downtime_reason",
                      "machine_state", "shift_code", "part_number", "part_description", "user_id"]

        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        df.drop_duplicates(subset=['mc', 'date', 'downtime_start', 'downtime_finish'], keep='first', inplace=True)
        df.drop(["remove_one", "remove_two", "remove_three"], axis=1, inplace=True)

        # Convert date/time columns
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)
        df['downtime_start'] = pd.to_datetime(df['downtime_start'], format="%H:%M").dt.time
        df['downtime_finish'] = pd.to_datetime(df['downtime_finish'], format="%H:%M").dt.time

        # Combine date and time
        df['downtime_start'] = df.apply(lambda row: pd.Timestamp.combine(row['date'], row['downtime_start']), axis=1)
        df['downtime_finish'] = df.apply(lambda row: pd.Timestamp.combine(row['date'], row['downtime_finish']), axis=1)

        # Handle overnight shifts
        df.loc[df['downtime_finish'] < df['downtime_start'], 'downtime_finish'] += pd.Timedelta(days=1)

        df['downtime_total_minutes'] = (df['downtime_finish'] - df['downtime_start']).dt.total_seconds() / 60
        df.drop(columns=['downtime_total'], inplace=True)
        df['day'] = df['date'].dt.day_name()

        # Parse DATABASE_URL for Heroku compatibility
        DATABASE_URL = os.getenv("DATABASE_URL").replace("postgres://", "postgresql://", 1)
        result = urlparse(DATABASE_URL)
        dbname = result.path[1:]  # Remove the leading '/' from the path
        user = result.username
        password = result.password
        host = result.hostname
        port = result.port

        # Use psycopg2 for transaction control
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        inserted_count = 0
        skipped_count = 0

        logger.info("Beginning data insertion with transaction...")

        try:
            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO downtime (
                        mc, date, downtime_start, downtime_finish, downtime_total_minutes,
                        downtime_reason, machine_state, shift_code, part_number,
                        part_description, user_id, day
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mc, date, downtime_start, downtime_finish) DO NOTHING
                    RETURNING mc;
                """, (
                    row['mc'], row['date'], row['downtime_start'], row['downtime_finish'],
                    row['downtime_total_minutes'], row['downtime_reason'], row['machine_state'],
                    row['shift_code'], row['part_number'], row['part_description'],
                    row['user_id'], row['day']
                ))

                if cursor.fetchone() is not None:
                    inserted_count += 1
                else:
                    skipped_count += 1

                if idx % 100 == 0:
                    conn.commit()  # Commit every 100 rows
                    logger.info(f"Processed and committed {idx + 1}/{len(df)} rows")

            conn.commit()  # Final commit for any remaining rows
            logger.info(f"Data insertion complete. Inserted: {inserted_count}, Skipped: {skipped_count}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error during insertion, rolling back: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

        # Verify the insertion
        final_count = get_current_count()
        logger.info(f"Final row count in database: {final_count}")
        logger.info(f"Net change in rows: {final_count - initial_count}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise



if __name__ == "__main__":
    if verify_connection():
        setup_and_insert_data("your_file.csv")  # Replace with your actual file path
    else:
        logger.error("Failed to establish database connection. Please check credentials and database status.")