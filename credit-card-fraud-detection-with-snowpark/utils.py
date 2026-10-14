"""
Utility classes, methods, and variables.
"""

import os


def connection_parameters() -> dict:
    """
    Returns the Snowflake connection parameters from the environment, leaving
    out the empty ones. With `.env.sample` as-is this connects to the LocalStack
    Snowflake emulator through HOST, and with the Snowflake Cloud settings to
    your Snowflake account.
    """
    parameters = {
        "account": os.getenv("ACCOUNTNAME"),
        "user": os.getenv("USERNAME"),
        "password": os.getenv("PASSWORD"),
        "role": os.getenv("ROLENAME"),
        "database": os.getenv("DBNAME"),
        "schema": os.getenv("SCHEMANAME"),
        "warehouse": os.getenv("WAREHOUSENAME"),
        "host": os.getenv("HOST"),
        "port": os.getenv("PORT"),
    }
    return {key: value for key, value in parameters.items() if value}
