"""Snowflake connections: key-pair auth, the per-role session cache, query helpers.

The API keeps one Snowflake session per *role* it serves (TENANT_ACME,
TENANT_ACME_ADMIN, PLATFORM_ADMIN, ...), because Snowflake evaluates row access
and masking policies against the session's role. Sessions are opened lazily,
shared by concurrent requests for the same role, and bounded: the least
recently used one is closed once more than `max_size` are open.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.aio import DictCursor, SnowflakeConnection

from app.settings import Settings

log = logging.getLogger(__name__)

APP_NAME = "tenantboard"


def private_key_der(settings: Settings) -> bytes:
    """The unencrypted PKCS#8 key as DER bytes.

    Loaded here rather than passed as private_key_file: connector 5.x refuses
    key files that are group- or world-writable, which is every file on some
    mounted filesystems (WSL /mnt/c, Docker Desktop bind mounts).
    """
    key = serialization.load_pem_private_key(settings.snowflake_private_key_path.read_bytes(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect_kwargs(settings: Settings, *, user: str, role: str, database: str | None) -> dict[str, Any]:
    return {
        "account": settings.account,
        "host": settings.host,
        "port": settings.port,
        "protocol": "https",
        "user": user,
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": private_key_der(settings),
        "role": role,
        "warehouse": settings.snowflake_warehouse,
        "database": database,
        "schema": settings.tb_schema if database else None,
        # Every bind in this sample is a qmark bind (server-side binding).
        "paramstyle": "qmark",
        "application": APP_NAME,
        # Always log out on close; no server-side async queries outlive a session here.
        "enable_server_session_keep_alive_auto_detection": False,
    }


async def open_role_session(settings: Settings, role: str, *, database: str | None = None) -> SnowflakeConnection:
    """Open an async session for the app's service user under `role`."""
    conn = SnowflakeConnection(
        **_connect_kwargs(settings, user=settings.tb_app_user, role=role, database=database or settings.tb_database),
        session_parameters={
            "QUERY_TAG": APP_NAME,
            # Dashboards re-run identical aggregate queries; the result cache
            # serves repeat loads without warehouse time. Each session runs as
            # one tenant role, so a reused result is that tenant's own. Set
            # explicitly so an account-level default cannot turn it off.
            "USE_CACHED_RESULT": True,
            "TIMEZONE": "UTC",
        },
        client_session_keep_alive=True,
    )
    await conn.connect()
    return conn


def connect_sync(
    settings: Settings, *, user: str, role: str, database: str | None = None, **session_parameters: Any
) -> snowflake.connector.SnowflakeConnection:
    """Synchronous session for tooling and tests (never used on the request path)."""
    return snowflake.connector.connect(
        **_connect_kwargs(settings, user=user, role=role, database=database),
        session_parameters={"QUERY_TAG": f"{APP_NAME}:tooling", "TIMEZONE": "UTC", **session_parameters},
        client_session_keep_alive=False,
    )


def connect_admin(settings: Settings, database: str | None = None) -> snowflake.connector.SnowflakeConnection:
    """Admin session for deployment, provisioning and seeding."""
    return connect_sync(settings, user=settings.snowflake_user, role=settings.admin_role, database=database)


class _Entry:
    __slots__ = ("conn", "leases", "retired")

    def __init__(self, conn: SnowflakeConnection) -> None:
        self.conn = conn
        self.leases = 0
        self.retired = False


class RoleSessionCache:
    """Bounded LRU of role -> session, with leases so eviction never closes a busy session."""

    def __init__(self, factory: Callable[[str], Awaitable[SnowflakeConnection]], max_size: int) -> None:
        self._factory = factory
        self._max_size = max_size
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        self._opening: dict[str, asyncio.Lock] = {}
        self._closed = False

    @property
    def open_roles(self) -> list[str]:
        return list(self._entries)

    async def _get(self, role: str) -> _Entry:
        entry = self._entries.get(role)
        if entry is None or entry.conn.is_closed():
            lock = self._opening.setdefault(role, asyncio.Lock())
            async with lock:
                entry = self._entries.get(role)
                if entry is None or entry.conn.is_closed():
                    entry = _Entry(await self._factory(role))
                    self._entries[role] = entry
                    log.info("opened Snowflake session for role %s (%d open)", role, len(self._entries))
        self._entries.move_to_end(role)
        return entry

    def _evict(self) -> list[_Entry]:
        idle: list[_Entry] = []
        while len(self._entries) > self._max_size:
            _, entry = self._entries.popitem(last=False)
            entry.retired = True
            if entry.leases == 0:
                idle.append(entry)
        return idle

    @contextlib.asynccontextmanager
    async def lease(self, role: str) -> AsyncIterator[SnowflakeConnection]:
        if self._closed:
            raise RuntimeError("session cache is closed")
        entry = await self._get(role)
        entry.leases += 1
        for stale in self._evict():
            await stale.conn.close()
        try:
            yield entry.conn
        finally:
            entry.leases -= 1
            if entry.retired and entry.leases == 0:
                await entry.conn.close()

    async def aclose(self) -> None:
        self._closed = True
        entries, self._entries = list(self._entries.values()), OrderedDict()
        await asyncio.gather(*(entry.conn.close() for entry in entries), return_exceptions=True)


def query_tag(endpoint: str) -> str:
    """Per-endpoint query tag, so QUERY_HISTORY attributes warehouse time to API routes."""
    return f"{APP_NAME}:{endpoint}"


async def fetch_dicts(
    conn: SnowflakeConnection, sql: str, params: Sequence[Any] | None = None, *, tag: str
) -> list[dict[str, Any]]:
    async with conn.cursor(DictCursor) as cur:
        # _statement_params sets QUERY_TAG for this statement only, so requests
        # for different endpoints can share one session without ALTER SESSION races.
        await cur.execute(sql, params, _statement_params={"QUERY_TAG": tag})
        return list(await cur.fetchall())


async def fetch_frame(
    conn: SnowflakeConnection, sql: str, params: Sequence[Any] | None = None, *, tag: str
) -> pd.DataFrame:
    """Run a query and fetch the result as Arrow batches straight into pandas."""
    async with conn.cursor() as cur:
        await cur.execute(sql, params, _statement_params={"QUERY_TAG": tag})
        return await cur.fetch_pandas_all()


async def execute(
    conn: SnowflakeConnection, sql: str, params: Sequence[Any] | None = None, *, tag: str
) -> tuple[Any, ...] | None:
    async with conn.cursor() as cur:
        await cur.execute(sql, params, _statement_params={"QUERY_TAG": tag})
        return await cur.fetchone() if cur.description else None
