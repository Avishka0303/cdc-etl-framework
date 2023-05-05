import psycopg2
import time
import random

from datetime import datetime

# Connect to the database
source_connection = psycopg2.connect(
    dbname="source_db",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)

# Connect to the database
target_connection = psycopg2.connect(
    dbname="target_db",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)

SCHEMA_NAME: str = 'test'
IS_INITIAL_LOAD = True


# initial full load for both tables
def initial_full_load():
    # Open a cursor to perform database operations
    source_cursor = source_connection.cursor()
    target_cursor = target_connection.cursor()

    trans_id = 0

    if IS_INITIAL_LOAD:
        # delete tables' data from both source and target
        query = f"delete from {SCHEMA_NAME}.transaction"

        source_cursor.execute(query)
        target_cursor.execute(query)

        # Commit the transaction
        source_connection.commit()
        target_connection.commit()

    else:
        # delete tables' data from both source and target
        query = f"select max(trans_id) from {SCHEMA_NAME}.transaction"

        source_cursor.execute(query)

        trans_id = source_cursor.fetchone()[0] + 1

    # Loop indefinitely and insert a new record every minute
    while True:
        # Generate a random user ID between 1 and 10
        user_id = random.randint(1, 10)

        # Get the current datetime with milliseconds
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        # Get the primary key of the inserted record

        # Build the insert query
        query = f"INSERT INTO {SCHEMA_NAME}.transaction (trans_id, usr_id, trans_date) VALUES ({trans_id}, {user_id}, '{now}')"
        print(query)

        # Execute the insert query
        source_cursor.execute(query)
        if IS_INITIAL_LOAD:
            target_cursor.execute(query)

        # Commit the transaction
        source_connection.commit()
        target_connection.commit()

        trans_id += 1

        # Wait for 2 seconds before inserting the next record
        time.sleep(2)

        if trans_id > 30:
            break

    # Close the cursor and database connection
    source_cursor.close()
    target_cursor.close()
    source_connection.close()
    target_connection.close()
