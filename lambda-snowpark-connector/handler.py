import snowflake.connector as sf

def lambda_handler(event, context):
    snowflake_params = {
        'user': 'test',
        'password': 'test',
        'account': 'test',
        'warehouse': 'test',
        'database': 'test',
        'schema': 'TEST_SCHEMA',
        'host': 'snowflake.localhost.localstack.cloud'
    }

    # Establish a connection
    connection = sf.connect(**snowflake_params)

    try:
        # Create a cursor object
        cursor = connection.cursor()

        # Execute the query to create a table
        cursor.execute("create or replace table ability(name string, skill string )")

        # Rows to insert
        rows_to_insert = [('John', 'SQL'), ('Alex', 'Java'), ('Pete', 'Snowflake')]

        # Execute the insert query with executemany
        cursor.executemany("insert into ability (name, skill) values (%s, %s)", rows_to_insert)

        # Execute a query to select data from the table
        cursor.execute("select name, skill from ability")

        # Fetch the results
        result = cursor.fetchall()
        print("Total # of rows:", len(result))
        print("Row-1 =>", result[0])
        print("Row-2 =>", result[1])

        # Execute a query to get the current timestamp
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        current_timestamp = cursor.fetchone()[0]
        print("Current timestamp from Snowflake:", current_timestamp)

    finally:
        # Close the cursor
        cursor.close()

        # Close the connection
        connection.close()

    return {
        'statusCode': 200,
        'body': "Successfully connected to Snowflake and inserted rows!"
    }
