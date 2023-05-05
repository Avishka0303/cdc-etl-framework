import psycopg2
from datetime import datetime, timedelta
import random

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
IS_INITIAL_LOAD = False

# Open a cursor to perform database operations
source_cursor = source_connection.cursor()
target_cursor = target_connection.cursor()

batch_id = 0

if IS_INITIAL_LOAD:
    # delete tables' data from both source and target
    query = f"delete from {SCHEMA_NAME}.batch"

    source_cursor.execute(query)
    target_cursor.execute(query)

    # Commit the transaction
    source_connection.commit()
    target_connection.commit()

    print(' data in both tables are deleted. ')

else:
    # delete tables' data from both source and target
    query = f"select max(batch_id) from {SCHEMA_NAME}.batch"

    source_cursor.execute(query)

    batch_id = source_cursor.fetchone()[0] + 1

# generate 10 random batches
for i in range(10):
    # generate random values for batch_id, batch_name, and batch_inserted_ts
    batch_id += 1
    batch_name = f"Batch {batch_id}"
    batch_inserted_ts = datetime.now() - timedelta(minutes=random.randint(1, 60))

    # set batch_updated_ts to None initially
    batch_updated_ts = None

    # check if the batch should be updated
    if random.random() < 0.5:
        # generate a new random timestamp for batch_updated_ts
        batch_updated_ts = datetime.now() - timedelta(minutes=random.randint(1, 60))

        # make sure that the inserted and updated timestamps are not the same
        while batch_updated_ts == batch_inserted_ts:
            batch_updated_ts = datetime.now() - timedelta(minutes=random.randint(1, 60))

    # insert the batch into the database
    source_cursor.execute(
        f"INSERT INTO {SCHEMA_NAME}.batch (batch_id, batch_name, batch_inserted_ts, batch_updated_ts) VALUES (%s, %s, %s, %s)",
        (batch_id, batch_name, batch_inserted_ts, batch_updated_ts))

    if IS_INITIAL_LOAD:
        target_cursor.execute(
            f"INSERT INTO {SCHEMA_NAME}.batch (batch_id, batch_name, batch_inserted_ts, batch_updated_ts) VALUES (%s, %s, %s, %s)",
            (batch_id, batch_name, batch_inserted_ts, batch_updated_ts))

    # Commit the transaction
    source_connection.commit()
    target_connection.commit()

# Close the cursor and database connection
source_cursor.close()
target_cursor.close()
source_connection.close()
target_connection.close()
