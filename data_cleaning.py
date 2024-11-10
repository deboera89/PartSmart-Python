import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import sql

# Database configuration
db_name = "downtime_data"
db_user = "postgres"
db_password = "1T1I1m1e"
db_host = "localhost"

# Establish a connection to PostgreSQL to create the database if it doesn’t exist
try:
    conn = psycopg2.connect(
        dbname="postgres",  # Connect to the default database to check if 'downtime_data' exists
        user=db_user,
        password=db_password,
        host=db_host
    )
    conn.autocommit = True  # Enable autocommit to allow database creation
    cursor = conn.cursor()

    # Check if the database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    if not cursor.fetchone():
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Database '{db_name}' already exists.")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"An error occurred: {e}")

# Connect to the newly created or existing database
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}')

# Create a connection and add the unique constraint if it doesn’t already exist
with engine.connect() as connection:
    # Try creating the downtime table with the unique constraint if it doesn’t exist
    connection.execute(text("""
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
            UNIQUE (mc, date, downtime_start, downtime_finish)  -- Add unique constraint here
        );
    """))

# Load and clean the data
df = pd.read_csv('datasource/Data.csv', header=None)
df.columns = ["mc", "date", "downtime_start", "downtime_finish", "downtime_total", "remove_one", "remove_two", "remove_three", "downtime_reason", "machine_state", "shift_code", "part_number", "part_description", "user_id"]

df.dropna(inplace=True)
df.drop_duplicates(inplace=True)
df.drop_duplicates(subset=['mc', 'date', 'downtime_start', 'downtime_finish'], keep='first', inplace=True)
df.drop(["remove_one", "remove_two", "remove_three"], axis=1, inplace=True)

df['downtime_start'] = pd.to_datetime("1900-01-01 " + df['downtime_start'], format="%Y-%m-%d %H:%M")
df['downtime_finish'] = pd.to_datetime("1900-01-01 " + df['downtime_finish'], format="%Y-%m-%d %H:%M")
df.loc[df['downtime_finish'] < df['downtime_start'], 'downtime_finish'] += pd.Timedelta(days=1)

df['downtime_total_minutes'] = (df['downtime_finish'] - df['downtime_start']).dt.total_seconds() / 60
df.drop(columns=['downtime_total'], inplace=True)

df['date'] = pd.to_datetime(df['date'], dayfirst=True)
df['day'] = df['date'].dt.day_name()

# Save the data to the 'downtime' table
try:
    df.to_sql('downtime', con=engine, if_exists='append', index=False)
    print("Data successfully stored in PostgreSQL.")
except Exception as e:
    print(f"An error occurred while inserting data: {e}")
