"""Wait until the init hooks have created TENANTBOARD with its policies and grants.

The emulator accepts connections before it has finished applying
init/*.sf.sql, so `make up` waits for the end result: ORDERS with its row
access policy and its reader grant.
"""

from __future__ import annotations

import sys
import time

from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.errors import Error

from app import provisioning
from app.db import connect_admin
from app.settings import get_settings

TIMEOUT_SECONDS = 90


def ready(conn: SnowflakeConnection) -> bool:
    """ORDERS exists with its row access policy (03_tables) and its reader grant (04_grants, the last file)."""
    database = get_settings().tb_database
    try:
        refs = provisioning.policy_refs(conn, database, "ORDERS")
        with conn.cursor(DictCursor) as cur:
            cur.execute(f"SHOW GRANTS ON TABLE {database}.APP.ORDERS")
            grants = {(g["privilege"], g["grantee_name"]) for g in cur.fetchall()}
    except Error:
        return False
    return any(r.kind == "ROW_ACCESS_POLICY" for r in refs) and ("SELECT", "TENANT_READER") in grants


def main() -> int:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    with connect_admin(get_settings()) as conn:
        while time.monotonic() < deadline:
            if ready(conn):
                print("Init hooks applied: TENANTBOARD.APP tables, policies and grants are in place")
                return 0
            time.sleep(1)
    print(
        f"TENANTBOARD was not fully initialised within {TIMEOUT_SECONDS}s. Check the init hook output:\n"
        "  docker compose -p snfs-tenantboard logs snowflake | grep init_hooks",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
