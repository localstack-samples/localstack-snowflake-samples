"""Checks on the test oracle itself (no emulator needed).

tests/model.py derives expected tables from the delivered events. Here it is
compared with the simulated source database's final rows, a second
independent view, so a mistake in the oracle cannot silently pass the
pipeline tests.
"""

from __future__ import annotations

from decimal import Decimal

from producer.produce import initial_scenario, next_batch
from tests import model


def test_expected_state_matches_the_source_database():
    scenario = initial_scenario(42)
    next_batch(scenario)
    events = scenario.events
    live_orders = {row[0]: row for row in model.expected_orders(events) if not row[7]}
    assert sorted(live_orders) == sorted(scenario.source.orders)
    for key, src in scenario.source.orders.items():
        assert live_orders[key][2] == src["o_orderstatus"]
        assert live_orders[key][3] == Decimal(src["o_totalprice"])
    current = {row[0]: row for row in model.expected_customer_history(events) if row[7]}
    assert sorted(current) == sorted(scenario.source.customers)
    for key, src in scenario.source.customers.items():
        assert current[key][4] == src["c_mktsegment"]
        assert current[key][3] == Decimal(src["c_acctbal"])


def test_scenario_is_deterministic_and_covers_every_event_kind():
    first, second = initial_scenario(42).events, initial_scenario(42).events
    assert [e.kafka_value() for e in first] == [e.kafka_value() for e in second]
    kinds = {e.kind for e in first}
    assert kinds == {"change", "duplicate", "late", "poison"}
    assert {e.op for e in first if e.kind != "poison"} == {"r", "c", "u", "d"}
