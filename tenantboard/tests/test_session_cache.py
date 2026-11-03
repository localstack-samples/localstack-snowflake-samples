"""The per-role session cache's contract: bounded, shared per role, never closes a busy session.

Uses stand-in sessions (no Snowflake): the contract is about lifecycle, and a
real login per case would only add latency.
"""

from __future__ import annotations

import asyncio

from app.db import RoleSessionCache


class FakeSession:
    def __init__(self, role: str) -> None:
        self.role = role
        self.closed = False

    def is_closed(self) -> bool:
        return self.closed

    async def close(self) -> None:
        self.closed = True


def make_cache(max_size: int):
    opened: list[FakeSession] = []

    async def factory(role: str) -> FakeSession:
        await asyncio.sleep(0.01)  # a login takes a while; concurrent callers must not open twice
        session = FakeSession(role)
        opened.append(session)
        return session

    return RoleSessionCache(factory, max_size=max_size), opened


def test_concurrent_requests_for_one_role_share_one_session():
    async def run():
        cache, opened = make_cache(max_size=4)

        async def use(role: str) -> FakeSession:
            async with cache.lease(role) as session:
                return session

        sessions = await asyncio.gather(*(use("TENANT_ACME") for _ in range(10)))
        return sessions, opened

    sessions, opened = asyncio.run(run())

    assert len(opened) == 1
    assert {id(s) for s in sessions} == {id(opened[0])}


def test_least_recently_used_session_is_closed_beyond_the_bound():
    async def run():
        cache, opened = make_cache(max_size=2)
        for role in ("TENANT_A", "TENANT_B", "TENANT_A", "TENANT_C"):
            async with cache.lease(role):
                pass
        return cache, opened

    cache, opened = asyncio.run(run())

    assert cache.open_roles == ["TENANT_A", "TENANT_C"]
    assert [(s.role, s.closed) for s in opened] == [("TENANT_A", False), ("TENANT_B", True), ("TENANT_C", False)]


def test_an_evicted_session_stays_open_until_its_lease_ends():
    async def run():
        cache, opened = make_cache(max_size=1)
        async with cache.lease("TENANT_A") as busy:
            async with cache.lease("TENANT_B"):
                still_open = not busy.closed
        return still_open, busy.closed, cache.open_roles

    still_open, closed_after, roles = asyncio.run(run())

    assert still_open is True
    assert closed_after is True
    assert roles == ["TENANT_B"]


def test_aclose_closes_every_session():
    async def run():
        cache, opened = make_cache(max_size=4)
        for role in ("TENANT_A", "TENANT_B"):
            async with cache.lease(role):
                pass
        await cache.aclose()
        return opened

    assert all(s.closed for s in asyncio.run(run()))
