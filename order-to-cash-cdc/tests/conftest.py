"""Fixtures: one connection per session, one isolated pipeline per scenario.

Each pipeline gets its own database (fresh DDL from init/*.sf.sql), Kafka topic
and Snowflake sink connector, and all three are removed afterwards, so runs
never see each other's data and the O2C database `make seed` fills is never
touched.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from pipeline import settings as settings_module
from pipeline import snowflake_db
from pipeline.harness import IsolatedPipeline, Pipeline, TaskRun
from producer.produce import Scenario, initial_scenario

SEED = 42


@pytest.fixture(scope="session")
def settings() -> settings_module.Settings:
    return settings_module.load()


@pytest.fixture(scope="session")
def conn(settings):
    connection = snowflake_db.connect(settings)
    yield connection
    connection.close()


@pytest.fixture
def isolated_pipeline(settings, conn) -> Iterator[Pipeline]:
    with IsolatedPipeline(settings, conn) as pipeline:
        yield pipeline


@dataclass
class Loaded:
    """A pipeline after batch 1 has been produced, landed and applied once."""

    pipeline: Pipeline
    scenario: Scenario
    first_run: TaskRun


def load_initial_batch(pipeline: Pipeline) -> Loaded:
    scenario = initial_scenario(SEED)
    produced = pipeline.produce(scenario.events)
    pipeline.wait_for_landing(produced)
    run = pipeline.run_task()
    assert run.state == "SUCCEEDED", f"initial apply failed: {run}"
    pipeline.refresh_mart()
    return Loaded(pipeline, scenario, run)


@pytest.fixture(scope="module")
def loaded(settings, conn) -> Iterator[Loaded]:
    """Shared, read-only state for tests that only inspect results."""
    with IsolatedPipeline(settings, conn) as pipeline:
        yield load_initial_batch(pipeline)
