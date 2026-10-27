"""Fixtures: one template database per run, one CLONE of it per xdist worker.

The controller process (or the only process, without xdist) builds
TB_TEMPLATE from init/*.sf.sql and the seeded dataset before any worker
starts, and drops it at the end. Each worker then clones it into
TB_TEST_<WORKER>, points its own API instance at the clone, and drops the clone
when its session ends. Expected values come from regenerating the same seeded
dataset in Python, never from the code under test.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
import snowflake.connector
from fastapi.testclient import TestClient

from app import provisioning
from app.db import connect_admin, connect_sync
from app.main import create_app
from app.settings import Settings
from scripts import seed

TEMPLATE_DB = "TB_TEMPLATE"


def _is_controller(config: pytest.Config) -> bool:
    return not hasattr(config, "workerinput")


def pytest_sessionstart(session: pytest.Session) -> None:
    if not _is_controller(session.config):
        return
    settings = Settings()
    dataset = seed.generate()
    with connect_admin(settings) as conn:
        provisioning.provision_tenants(conn, settings, dataset.tenant_ids)
        provisioning.deploy_database(conn, TEMPLATE_DB)
        provisioning.load_tables(conn, TEMPLATE_DB, dataset.frames())


def pytest_sessionfinish(session: pytest.Session) -> None:
    if not _is_controller(session.config):
        return
    with connect_admin(Settings()) as conn:
        provisioning.drop_database(conn, TEMPLATE_DB)


@pytest.fixture(scope="session")
def dataset() -> seed.Dataset:
    return seed.generate()


@pytest.fixture(scope="session")
def worker_db(request: pytest.FixtureRequest) -> Iterator[str]:
    """This worker's own database: a CLONE of the template, with the template's policies and grants."""
    worker = getattr(request.config, "workerinput", {}).get("workerid", "main")
    name = f"TB_TEST_{worker.upper()}"
    with connect_admin(Settings()) as conn:
        provisioning.clone_database(conn, TEMPLATE_DB, name)
    yield name
    with connect_admin(Settings()) as conn:
        provisioning.drop_database(conn, name)


@pytest.fixture(scope="session")
def settings(worker_db: str) -> Settings:
    return Settings(tb_database=worker_db)


@pytest.fixture(scope="session")
def admin(settings: Settings) -> Iterator[snowflake.connector.SnowflakeConnection]:
    with connect_admin(settings, database=settings.tb_database) as conn:
        yield conn


@pytest.fixture(scope="session")
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as test_client:
        yield test_client


@pytest.fixture(scope="session")
def connect_as(settings: Settings) -> Iterator[Callable[[str], snowflake.connector.SnowflakeConnection]]:
    """Raw sessions of the app's service user under a given role, outside the API.

    USE_CACHED_RESULT is off so every assertion is computed from storage under
    the session's own role, not served from a result an earlier test cached.
    """
    opened: dict[str, snowflake.connector.SnowflakeConnection] = {}

    def factory(role: str) -> snowflake.connector.SnowflakeConnection:
        if role not in opened:
            opened[role] = connect_sync(
                settings, user=settings.tb_app_user, role=role, database=settings.tb_database, USE_CACHED_RESULT=False
            )
        return opened[role]

    yield factory
    for conn in opened.values():
        conn.close()
