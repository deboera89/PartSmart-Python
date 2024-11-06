import pandas as pd
from sqlalchemy import create_engine


#Load the data
df = pd.read_csv('datasource/Data.csv', header=None)

# Define a list of column names
df.columns = ["MC", "Date", "downtime_start", "downtime_finish", "downtime_total", "remove_one", "remove_two", "remove_three", "downtime_reason", "machine_state", "shift_code", "part_number", "part_description", "user_id"]  # Replace with appropriate names

# Remove empty rows
df.dropna(inplace=True)

# Remove duplicates
df.drop_duplicates(inplace=True)

# Drop duplicates based on the first four columns
# Replace 'column1', 'column2', 'column3', 'column4' with the actual column names
df.drop_duplicates(subset=['MC', 'Date', 'downtime_start', 'downtime_finish'], keep='first', inplace=True)

# Drop useless columns
df.drop(["remove_one", "remove_two", "remove_three"], axis=1, inplace=True)

# Define the datetime format based on your data
datetime_format = "%H:%M"

# Parse with a placeholder date so we can perform arithmetic
df['downtime_start'] = pd.to_datetime("1900-01-01 " + df['downtime_start'], format="%Y-%m-%d %H:%M")
df['downtime_finish'] = pd.to_datetime("1900-01-01 " + df['downtime_finish'], format="%Y-%m-%d %H:%M")

# Adjust for overnight shifts: add one day to downtime_finish if it’s earlier than downtime_start
df.loc[df['downtime_finish'] < df['downtime_start'], 'downtime_finish'] += pd.Timedelta(days=1)


# Calculate downtime total as a timedelta
df['downtime_total'] = df['downtime_finish'] - df['downtime_start']

# Convert downtime_total to minutes as a new column (or replace downtime_total if you prefer)
df['downtime_total_minutes'] = df['downtime_total'].dt.total_seconds() / 60  # for minutes

# Drop the original downtime_total column if no longer needed
df.drop(columns=['downtime_total'], inplace=True)


# Set Pandas to show all columns
pd.set_option('display.max_columns', None)

###########################################
#
#   Save to Database
#
###########################################

# Define the database connection URL for PostgreSQL
engine = create_engine('postgresql://postgres:1T1I1m1e@localhost/downtime_data')

# Assuming df is your cleaned DataFrame, store it in PostgreSQL
df.to_sql('downtime', con=engine, if_exists='replace', index=False)

print("Data successfully stored in PostgreSQL.")

