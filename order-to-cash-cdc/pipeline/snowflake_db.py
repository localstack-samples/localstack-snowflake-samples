"""Snowflake connections and pipeline deployment."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector import SnowflakeConnection

from pipeline.settings import SAMPLE_ROOT, Settings

QUERY_TAG = "order-to-cash-cdc"
INIT_DIR = SAMPLE_ROOT / "init"
# Init files put account-level setup (role, warehouse, the O2C database) above
# this marker and database-relative objects below it.
DEPLOY_MARKER = "-- @deploy:"


def private_key_der(settings: Settings) -> bytes:
    """The unencrypted PKCS#8 key as DER bytes.

    Loaded here rather than passed as private_key_file: connector 5.x refuses
    key files that are group- or world-writable, which is every file on some
    mounted filesystems (WSL /mnt/c, Docker Desktop bind mounts).
    """
    key = serialization.load_pem_private_key(settings.private_key_path.read_bytes(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect(settings: Settings, database: str | None = None) -> SnowflakeConnection:
    """Open a key-pair (SNOWFLAKE_JWT) connection with an explicit role and warehouse."""
    return snowflake.connector.connect(
        account=settings.account,
        host=settings.host,
        port=settings.port,
        protocol=settings.protocol,
        user=settings.user,
        authenticator="SNOWFLAKE_JWT",
        private_key=private_key_der(settings),
        role=settings.role,
        warehouse=settings.warehouse,
        database=database,
        schema="OPS" if database else None,
        session_parameters={"QUERY_TAG": QUERY_TAG},
        client_session_keep_alive=False,
        # Always log out on close; no async queries outlive a connection here.
        enable_server_session_keep_alive_auto_detection=False,
    )


def query(conn: SnowflakeConnection, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []


def scalar(conn: SnowflakeConnection, sql: str, params: Sequence[Any] | None = None) -> Any:
    rows = query(conn, sql, params)
    return rows[0][0] if rows else None


def deployable_sql(settings: Settings) -> str:
    """Database-relative DDL from init/*.sf.sql, in file order.

    The warehouse name is the one in the init files unless SNOWFLAKE_WAREHOUSE
    names another (real accounts rarely have O2C_WH).
    """
    parts = []
    for path in sorted(INIT_DIR.glob("*.sf.sql")):
        text = path.read_text(encoding="utf-8")
        if DEPLOY_MARKER not in text:
            raise RuntimeError(f"{path.name} has no '{DEPLOY_MARKER}' marker")
        parts.append(text.split(DEPLOY_MARKER, 1)[1].split("\n", 1)[1])
    sql = "\n".join(parts)
    return re.sub(r"\bWAREHOUSE = O2C_WH\b", f"WAREHOUSE = {settings.warehouse}", sql)


def deploy(conn: SnowflakeConnection, settings: Settings, database: str) -> None:
    """Create `database` from scratch with every pipeline object in it."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE OR REPLACE DATABASE {database}")
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"GRANT USAGE ON DATABASE {database} TO ROLE {settings.connector_role}")
    for cur in conn.execute_string(deployable_sql(settings), remove_comments=True):
        cur.close()
    query(conn, f"USE SCHEMA {database}.OPS")


def drop(conn: SnowflakeConnection, database: str) -> None:
    query(conn, f"DROP DATABASE IF EXISTS {database}")
