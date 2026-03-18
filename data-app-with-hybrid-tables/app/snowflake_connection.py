import snowflake.connector
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

load_dotenv(override=True)


def getConnection() -> snowflake.connector.SnowflakeConnection:
    creds = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'client_session_keep_alive': True
        }
    
    snowflake.connector.paramstyle='qmark'
    connection = snowflake.connector.connect(**creds)

    return connection

def getSession() -> Session:
    return Session.builder.configs({"connection": getConnection() }).create()

def getSQLAlchemyURI() -> str:

    uri = f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}@{os.getenv('SNOWFLAKE_ACCOUNT')}"
    return uri
