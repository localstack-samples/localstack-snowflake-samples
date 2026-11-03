"""Shared fixtures: one throwaway database per test session.

The session loads the downsampled TLC extract into <db>.RAW.TRIPS and builds
production state once (dbt build --target prod in a temporary copy of the
project). Tests then build pull request schemas against that state. The
database is dropped at the end, so runs never touch SLIM_CI or each other.
"""

from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from slim_ci_lab import loader, settings, tlc
from slim_ci_lab.dbt_runner import copy_project, dbt_env, run_dbt, targets

EMULATOR = settings.is_emulator()


@dataclass(frozen=True)
class ProdState:
    database: str
    project_dir: Path
    state_dir: Path
    env: dict[str, str]


@pytest.fixture(scope="session")
def database():
    name = f"SLIM_CI_TEST_{uuid.uuid4().hex[:8].upper()}"
    with settings.connect() as conn:
        conn.cursor().execute(f"CREATE DATABASE {name}")
    yield name
    with settings.connect() as conn:
        conn.cursor().execute(f"DROP DATABASE IF EXISTS {name}")


@pytest.fixture(scope="session")
def conn(database):
    with settings.connect() as connection:
        connection.cursor().execute(f"USE DATABASE {database}")
        yield connection


@pytest.fixture(scope="session")
def sample_file() -> Path:
    return tlc.sample_file()


@pytest.fixture(scope="session")
def first_load(database, sample_file) -> loader.LoadResult:
    return loader.load(database, sample_file)


@pytest.fixture(scope="session")
def prod_state(database, first_load, tmp_path_factory) -> ProdState:
    """Production state: a full `dbt build` plus its saved manifest.json."""
    work = tmp_path_factory.mktemp("prod")
    project = copy_project(work / "dbt")
    env = dbt_env(DBT_DATABASE=database)
    env.pop("DBT_PR_NUMBER", None)
    result = run_dbt(["build", "--target", targets()[0]], project, env)
    assert result.returncode == 0, result.stdout[-4000:]
    state_dir = work / "state"
    state_dir.mkdir()
    shutil.copy(project / "target" / "manifest.json", state_dir / "manifest.json")
    return ProdState(database=database, project_dir=project, state_dir=state_dir, env=env)
