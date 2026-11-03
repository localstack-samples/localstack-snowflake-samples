"""Snowflake Scripting transaction semantics the apply procedure relies on.

OPS.APPLY_ORDERS_CDC wraps its whole run in one explicit transaction and rolls
it back in the exception handler, so a failure after the first MERGE never
leaves a half-applied batch.
"""

from __future__ import annotations

import uuid

import pytest
from snowflake.connector.errors import ProgrammingError

from pipeline import snowflake_db


@pytest.fixture
def scratch_schema(conn):
    database = f"O2C_S_{uuid.uuid4().hex[:8]}".upper()
    snowflake_db.query(conn, f"CREATE DATABASE {database}")
    snowflake_db.query(conn, f"CREATE TABLE {database}.PUBLIC.T (N INTEGER)")
    yield f"{database}.PUBLIC"
    snowflake_db.drop(conn, database)


def test_rollback_in_handler_discards_the_failed_transaction(conn, scratch_schema):
    snowflake_db.query(conn, f"""
        CREATE PROCEDURE {scratch_schema}.P() RETURNS VARCHAR LANGUAGE SQL AS
        $$
        DECLARE
          boom EXCEPTION (-20002, 'boom');
        BEGIN
          BEGIN TRANSACTION;
          INSERT INTO {scratch_schema}.T VALUES (1);
          RAISE boom;
        EXCEPTION
          WHEN OTHER THEN
            ROLLBACK;
            RAISE;
        END;
        $$""")
    with pytest.raises(ProgrammingError) as excinfo:
        snowflake_db.query(conn, f"CALL {scratch_schema}.P()")
    assert excinfo.value.errno == -20002
    assert snowflake_db.query(conn, f"SELECT N FROM {scratch_schema}.T") == []

