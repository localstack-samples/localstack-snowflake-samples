"""End-to-end tests: Kafka -> connector -> stream -> task -> core tables -> mart.

Every expected value comes from tests/model.py, which recomputes the business
rules from the producer's events. No test waits on the task's schedule: runs
are triggered with EXECUTE TASK and observed through TASK_HISTORY().
"""

from __future__ import annotations

from collections import Counter

import pytest
from snowflake.connector.errors import ProgrammingError

from producer.produce import next_batch
from tests import model
from tests.conftest import load_initial_batch


# --- ingest ---------------------------------------------------------------------


def test_connector_lands_every_produced_event_once(loaded):
    p, events = loaded.pipeline, loaded.scenario.events
    rows = p.q(
        f"""SELECT RECORD_METADATA:topic::VARCHAR, RECORD_METADATA:partition::INT,
                   RECORD_METADATA:offset::INT, RECORD_CONTENT:source:lsn::NUMBER
            FROM {p.fq('RAW.ORDERS_CDC')}"""
    )
    assert len(rows) == len(events)
    assert {r[0] for r in rows} == {p.topic}
    assert len({(r[1], r[2]) for r in rows}) == len(rows), "a Kafka offset landed twice"
    assert {r[1] for r in rows} <= {0, 1, 2}
    assert Counter(r[3] for r in rows) == Counter(e.lsn for e in events)


# --- current state and history --------------------------------------------------------


def test_core_orders_hold_newest_version_of_every_order(loaded):
    assert loaded.pipeline.orders() == model.expected_orders(loaded.scenario.events)


def test_order_delete_is_a_soft_delete(loaded):
    p, events = loaded.pipeline, loaded.scenario.events
    expected_deleted = {row[0]: row for row in model.expected_orders(events) if row[7]}
    assert expected_deleted, "scenario must contain deleted orders"
    actual_deleted = {row[0]: row for row in p.orders() if row[7]}
    # The row survives with the attributes of the delete's "before" image.
    assert actual_deleted == expected_deleted


def test_customer_delete_closes_the_open_scd2_version(loaded):
    p, events = loaded.pipeline, loaded.scenario.events
    deleted = model.deleted_customers(events)
    assert deleted, "scenario must contain deleted customers"
    history = p.customer_history()
    for key, deleted_at in deleted.items():
        versions = [row for row in history if row[0] == key]
        assert versions, f"customer {key} has no history"
        assert not any(row[7] for row in versions), f"customer {key} still has a current version"
        assert versions[-1][6] == deleted_at, f"customer {key}: last VALID_TO is not the delete time"


def test_customer_history_has_one_version_per_change(loaded):
    p, events = loaded.pipeline, loaded.scenario.events
    history = p.customer_history()
    assert history == model.expected_customer_history(events)
    # Contiguous validity: each closed version ends where the next one starts.
    for prev, nxt in zip(history, history[1:], strict=False):
        if prev[0] == nxt[0]:
            assert prev[6] == nxt[5]


def test_duplicate_and_late_events_do_not_create_rows_or_regress_state(loaded):
    p, events = loaded.pipeline, loaded.scenario.events
    assert any(e.kind == "duplicate" for e in events) and any(e.kind == "late" for e in events)
    assert p.q(f"SELECT O_ORDERKEY FROM {p.fq('CORE.ORDERS')} GROUP BY 1 HAVING COUNT(*) > 1") == []
    assert p.q(
        f"SELECT C_CUSTKEY, SOURCE_LSN FROM {p.fq('CORE.DIM_CUSTOMER_HISTORY')} GROUP BY 1, 2 HAVING COUNT(*) > 1"
    ) == []
    applied_lsn = {row[0]: row[8] for row in p.orders()}
    for late in (e for e in events if e.kind == "late" and e.table == "orders"):
        assert applied_lsn[late.key] > late.lsn, f"late event for order {late.key} overwrote a newer one"


def test_invalid_events_are_dead_lettered_not_applied(loaded):
    p, events = loaded.pipeline, loaded.scenario.events
    assert p.dead_letters() == model.expected_dead_letters(events)
    assert p.scalar(f"SELECT COUNT(*) FROM {p.fq('CORE.ORDERS')} WHERE O_ORDERKEY IS NULL") == 0


# --- mart -----------------------------------------------------------------------


def test_daily_bookings_match_totals_computed_from_events(loaded):
    expected = model.expected_daily_bookings(loaded.scenario.events)
    assert len({row[0] for row in expected}) > 1, "scenario must span several booking dates"
    assert "UNKNOWN" in {row[1] for row in expected}, "scenario must book orders of a deleted customer"
    assert loaded.pipeline.daily_bookings() == expected


# --- error paths ----------------------------------------------------------------------


def test_merge_without_dedup_fails_with_duplicate_row_error(loaded):
    """Why the procedure ranks with QUALIFY ROW_NUMBER(): CDC batches carry several
    events per key, and a MERGE that matches one target row with several source
    rows is rejected (ERROR_ON_NONDETERMINISTIC_MERGE)."""
    p = loaded.pipeline
    before = p.orders()
    with pytest.raises(ProgrammingError) as excinfo:
        p.q(
            f"""MERGE INTO {p.fq('CORE.ORDERS')} t
                USING (SELECT * FROM {p.fq('OPS.APPLY_BATCH')}
                       WHERE SOURCE_TABLE = 'orders' AND REJECT_REASON IS NULL) s
                  ON t.O_ORDERKEY = s.ENTITY_KEY
                WHEN MATCHED THEN UPDATE SET O_ORDERSTATUS = s.IMAGE:o_orderstatus::VARCHAR"""
        )
    assert excinfo.value.errno == 100090
    assert "Duplicate row detected during DML action" in str(excinfo.value)
    assert p.orders() == before


def test_replaying_the_same_events_changes_nothing(isolated_pipeline):
    p = isolated_pipeline
    loaded = load_initial_batch(p)
    events = loaded.scenario.events
    orders, history, bookings = p.orders(), p.customer_history(), p.daily_bookings()

    # The source (or a connector reset) redelivers every event of batch 1.
    p.produce(events)
    p.wait_for_landing(2 * len(events))
    run = p.run_task()
    p.refresh_mart()

    assert run.state == "SUCCEEDED", run
    assert p.orders() == orders == model.expected_orders(events)
    assert p.customer_history() == history == model.expected_customer_history(events)
    assert p.daily_bookings() == bookings
    # Each poison *delivery* is a distinct Kafka record, so it is dead-lettered once per delivery.
    assert p.dead_letters() == sorted(model.expected_dead_letters(events) * 2, key=lambda r: (r[1], r[0]))


def test_forced_failure_rolls_back_then_next_run_applies_each_event_exactly_once(isolated_pipeline):
    """Headline: a run that fails after its MERGEs leaves no trace, and the retry
    applies batch 2 exactly once."""
    p = isolated_pipeline
    loaded = load_initial_batch(p)
    scenario = loaded.scenario
    batch2 = next_batch(scenario)
    p.produce(batch2)
    p.wait_for_landing(len(scenario.events))
    orders, history, dead = p.orders(), p.customer_history(), p.dead_letters()
    assert orders != model.expected_orders(scenario.events), "batch 2 must change core.orders"

    p.set_fail_next_run(True)
    failed = p.run_task()

    assert failed.state == "FAILED"
    assert str(failed.error_code) == "-20001"
    assert "FAIL_NEXT_RUN is TRUE" in failed.error_message
    assert p.orders() == orders, "core.orders changed although the run failed"
    assert p.customer_history() == history, "dim_customer_history changed although the run failed"
    assert p.dead_letters() == dead
    assert p.stream_has_data() is True, "stream offset advanced although the run failed"

    p.set_fail_next_run(False)
    retried = p.run_task()
    p.refresh_mart()

    assert retried.state == "SUCCEEDED", retried
    assert p.orders() == model.expected_orders(scenario.events)
    assert p.customer_history() == model.expected_customer_history(scenario.events)
    assert p.daily_bookings() == model.expected_daily_bookings(scenario.events)
    assert p.dead_letters() == dead
