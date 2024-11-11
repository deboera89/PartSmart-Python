import os

# Retrieve environment variables for database connection
DB_NAME = os.getenv("DB_NAME", "downtime_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1T1I1m1e")
DB_HOST = os.getenv("DB_HOST", "localhost")

DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# FOR HEROKU
# Get the DATABASE_URL from the environment and replace 'postgres://' with 'postgresql://'
# DATABASE_URL = os.getenv("DATABASE_URL").replace("postgres://", "postgresql://", 1)
