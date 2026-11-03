"""Shared fixtures: one freshly deployed database per test session.

The session deploys the real migrations with schemachange into
``PHI_TEST_<random>``, loads the pinned Synthea extract, and drops the database
afterwards (set ``KEEP_TEST_DB=1`` to keep it for debugging). Roles and users
are account-level and created idempotently by V1.0.0, so sessions do not
interfere with each other or with ``make migrate``.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
from snowflake.connector import SnowflakeConnection

from phi_guardrails import settings as cfg
from phi_guardrails.contract import Contract, load_contract
from phi_guardrails.deploy import deploy
from phi_guardrails.load import load
from phi_guardrails.matrix import Matrix
from phi_guardrails.settings import Settings
from phi_guardrails.synthea import Dataset, load_dataset


def unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8].upper()}"


@pytest.fixture(scope="session")
def contract() -> Contract:
    return load_contract()


@pytest.fixture(scope="session")
def dataset() -> Dataset:
    return load_dataset()


@pytest.fixture(scope="session")
def deployed() -> Iterator[Settings]:
    """Settings pointing at a database with every migration applied and data loaded."""
    settings = cfg.load().with_database(unique("PHI_TEST"))
    try:
        deploy(settings)
        load(settings)
        yield settings
    finally:
        if not os.getenv("KEEP_TEST_DB"):
            with cfg.connect(settings) as conn:
                conn.cursor().execute(f"DROP DATABASE IF EXISTS {settings.database}")


@pytest.fixture(scope="session")
def admin(deployed: Settings) -> Iterator[SnowflakeConnection]:
    """The deploy identity, with the test database as the current database."""
    conn = cfg.connect(deployed)
    conn.cursor().execute(f"USE DATABASE {deployed.database}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def matrix(deployed: Settings, contract: Contract, dataset: Dataset) -> Iterator[Matrix]:
    m = Matrix(deployed, contract, dataset)
    yield m
    m.close()


@pytest.fixture
def scratch_schema(admin: SnowflakeConnection, deployed: Settings) -> Iterator[str]:
    """A throwaway schema in the test database, as ``DB.SCHEMA``."""
    name = f"{deployed.database}.{unique('SCRATCH')}"
    admin.cursor().execute(f"CREATE SCHEMA {name}")
    yield name
    admin.cursor().execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
