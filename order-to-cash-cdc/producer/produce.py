"""Seeded Debezium-style change generator for TPC-H shaped `orders` and `customer`.

A small simulated source database applies inserts, updates and deletes and
writes one change event per commit, with a monotonically increasing log
sequence number (LSN), like Debezium's PostgreSQL connector. Delivery then
adds what real CDC pipelines see on the wire:

* duplicates: an event delivered twice (at-least-once redelivery),
* late events: an older change for a key delivered after a newer one,
* poison events: records the pipeline must reject (truncate op, missing key,
  a table the pipeline does not own).

The same seed always yields the same events, so tests can compute expected
results from the events alone.

CLI: python -m producer.produce --topic orders.cdc --seed 42
"""

from __future__ import annotations

import argparse
import copy
import json
import random
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

SEGMENTS = ("AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY")
PRIORITIES = ("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW")
EPOCH = date(1970, 1, 1)
FIRST_ORDER_DATE = date(2026, 10, 1)
ORDER_DATE_SPAN_DAYS = 5
DEBEZIUM_VERSION = "2.7.3.Final"


@dataclass(frozen=True)
class ChangeEvent:
    """One record on the `orders.cdc` topic.

    `kind` is the generator's ground truth: "change" (first delivery),
    "duplicate", "late" or "poison". `reject_reason` is set for poison
    events only.
    """

    table: str
    op: str
    key: int | None
    lsn: int
    ts_ms: int
    before: dict | None
    after: dict | None
    kind: str = "change"
    reject_reason: str | None = None
    envelope_override: dict | None = field(default=None, compare=False)

    @property
    def image(self) -> dict | None:
        return self.before if self.op == "d" else self.after

    def envelope(self) -> dict:
        if self.envelope_override is not None:
            return self.envelope_override
        return {
            "before": self.before,
            "after": self.after,
            "source": {
                "version": DEBEZIUM_VERSION,
                "connector": "postgresql",
                "name": "tpch",
                "ts_ms": self.ts_ms,
                "db": "tpch",
                "schema": "public",
                "table": self.table,
                "lsn": self.lsn,
            },
            "op": self.op,
            "ts_ms": self.ts_ms + 250,
        }

    def kafka_key(self) -> str:
        key_column = "o_orderkey" if self.table == "orders" else "c_custkey"
        return json.dumps({key_column: self.key})

    def kafka_value(self) -> str:
        return json.dumps(self.envelope(), separators=(",", ":"))


def money(value: Decimal) -> str:
    # Debezium decimal.handling.mode=string
    return str(value.quantize(Decimal("0.01")))


class SourceDatabase:
    """The upstream OLTP database: current rows plus a change log."""

    def __init__(self, seed: int, start_lsn: int = 24_000_000, start: datetime | None = None) -> None:
        self.rng = random.Random(seed)
        self.lsn = start_lsn
        self.now = start or datetime(2026, 10, 1, 8, 0, 0, tzinfo=UTC)
        self.customers: dict[int, dict] = {}
        self.orders: dict[int, dict] = {}
        self.next_custkey = 1
        self.next_orderkey = 1

    # -- change log ---------------------------------------------------------
    def _commit(self, table: str, op: str, key: int, before: dict | None, after: dict | None) -> ChangeEvent:
        self.lsn += self.rng.randint(8, 96)
        self.now += timedelta(milliseconds=self.rng.randint(150, 4_000))
        return ChangeEvent(
            table=table,
            op=op,
            key=key,
            lsn=self.lsn,
            ts_ms=int(self.now.timestamp() * 1000),
            before=copy.deepcopy(before),
            after=copy.deepcopy(after),
        )

    # -- customer -----------------------------------------------------------
    def insert_customer(self, op: str = "c") -> ChangeEvent:
        key = self.next_custkey
        self.next_custkey += 1
        row = {
            "c_custkey": key,
            "c_name": f"Customer#{key:09d}",
            "c_nationkey": self.rng.randint(0, 24),
            "c_acctbal": money(Decimal(self.rng.randint(-99_999, 999_999)) / 100),
            "c_mktsegment": self.rng.choice(SEGMENTS),
        }
        self.customers[key] = row
        return self._commit("customer", op, key, None, row)

    def update_customer(self, key: int | None = None) -> ChangeEvent:
        key = key if key is not None else self.rng.choice(sorted(self.customers))
        before = self.customers[key]
        after = dict(before)
        if self.rng.random() < 0.6:
            after["c_mktsegment"] = self.rng.choice([s for s in SEGMENTS if s != before["c_mktsegment"]])
        after["c_acctbal"] = money(Decimal(before["c_acctbal"]) + Decimal(self.rng.randint(-50_000, 50_000)) / 100)
        self.customers[key] = after
        return self._commit("customer", "u", key, before, after)

    def delete_customer(self, key: int | None = None) -> ChangeEvent:
        key = key if key is not None else self.rng.choice(sorted(self.customers))
        before = self.customers.pop(key)
        return self._commit("customer", "d", key, before, None)

    # -- orders -------------------------------------------------------------
    def insert_order(self) -> ChangeEvent:
        key = self.next_orderkey
        self.next_orderkey += 1
        order_date = FIRST_ORDER_DATE + timedelta(days=self.rng.randrange(ORDER_DATE_SPAN_DAYS))
        row = {
            "o_orderkey": key,
            "o_custkey": self.rng.choice(sorted(self.customers)),
            "o_orderstatus": "O",
            "o_totalprice": money(Decimal(self.rng.randint(90_000, 45_000_000)) / 100),
            "o_orderdate": (order_date - EPOCH).days,  # Debezium io.debezium.time.Date
            "o_orderpriority": self.rng.choice(PRIORITIES),
            "o_clerk": f"Clerk#{self.rng.randint(1, 1000):09d}",
        }
        self.orders[key] = row
        return self._commit("orders", "c", key, None, row)

    def update_order(self, key: int | None = None) -> ChangeEvent:
        key = key if key is not None else self.rng.choice(sorted(self.orders))
        before = self.orders[key]
        after = dict(before)
        after["o_orderstatus"] = {"O": "P", "P": "F", "F": "F"}[before["o_orderstatus"]]
        if self.rng.random() < 0.5:
            after["o_totalprice"] = money(Decimal(before["o_totalprice"]) * Decimal("0.95"))
        self.orders[key] = after
        return self._commit("orders", "u", key, before, after)

    def delete_order(self, key: int | None = None) -> ChangeEvent:
        key = key if key is not None else self.rng.choice(sorted(self.orders))
        before = self.orders.pop(key)
        return self._commit("orders", "d", key, before, None)

    # -- workload -----------------------------------------------------------
    def snapshot(self, customers: int) -> list[ChangeEvent]:
        """Initial snapshot read (op "r"), as Debezium emits on first start."""
        return [self.insert_customer(op="r") for _ in range(customers)]

    def workload(self, changes: int) -> list[ChangeEvent]:
        events: list[ChangeEvent] = []
        for _ in range(changes):
            roll = self.rng.random()
            if roll < 0.40 or not self.orders:
                events.append(self.insert_order())
            elif roll < 0.68:
                events.append(self.update_order())
            elif roll < 0.74 and len(self.orders) > 3:
                events.append(self.delete_order())
            elif roll < 0.80:
                events.append(self.insert_customer())
            elif roll < 0.97:
                events.append(self.update_customer())
            elif len(self.customers) > 3:
                events.append(self.delete_customer())
            else:
                events.append(self.update_customer())
        return events


def deliver(events: list[ChangeEvent], rng: random.Random, duplicates: int, late: int) -> list[ChangeEvent]:
    """Delivery order on the topic: commit order plus duplicates and late events."""
    delivered = list(events)
    # Late: an update delivered after the next change for the same key.
    candidates = []
    for i, ev in enumerate(events):
        if ev.op != "u":
            continue
        nxt = next((j for j in range(i + 1, len(events)) if (events[j].table, events[j].key) == (ev.table, ev.key)), None)
        if nxt is not None:
            candidates.append((i, nxt))
    moved: set[int] = set()
    for i, j in rng.sample(candidates, min(late, len(candidates))):
        if i in moved or j in moved:
            continue
        moved.update((i, j))  # j stays put so i really arrives after it
        delivered.remove(events[i])
        delivered.insert(delivered.index(events[j]) + 1, replace(events[i], kind="late"))
    # Duplicates: redeliver a few events a little later (same LSN, same payload).
    for ev in rng.sample([e for k, e in enumerate(events) if k not in moved], duplicates):
        pos = min(len(delivered), delivered.index(ev) + rng.randint(1, 10))
        delivered.insert(pos, replace(ev, kind="duplicate"))
    return delivered


def poison_events(source: SourceDatabase) -> list[ChangeEvent]:
    """Records the pipeline must dead-letter instead of applying."""
    source.now += timedelta(seconds=1)
    ts_ms = int(source.now.timestamp() * 1000)
    base = {"version": DEBEZIUM_VERSION, "connector": "postgresql", "name": "tpch", "db": "tpch", "schema": "public"}
    truncate = {
        "before": None,
        "after": None,
        "source": {**base, "ts_ms": ts_ms, "table": "orders", "lsn": source.lsn + 1},
        "op": "t",
        "ts_ms": ts_ms,
    }
    missing_key = {
        "before": None,
        "after": {"o_custkey": 1, "o_orderstatus": "O", "o_totalprice": "10.00"},
        "source": {**base, "ts_ms": ts_ms, "table": "orders", "lsn": source.lsn + 2},
        "op": "c",
        "ts_ms": ts_ms,
    }
    foreign_table = {
        "before": None,
        "after": {"l_orderkey": 1, "l_linenumber": 1},
        "source": {**base, "ts_ms": ts_ms, "table": "lineitem", "lsn": source.lsn + 3},
        "op": "c",
        "ts_ms": ts_ms,
    }
    source.lsn += 3
    return [
        ChangeEvent("orders", "t", None, source.lsn - 2, ts_ms, None, None, "poison", "UNKNOWN_OP", truncate),
        ChangeEvent("orders", "c", None, source.lsn - 1, ts_ms, None, None, "poison", "MISSING_KEY", missing_key),
        ChangeEvent("lineitem", "c", None, source.lsn, ts_ms, None, None, "poison", "UNKNOWN_SOURCE_TABLE",
                    foreign_table),
    ]


@dataclass
class Scenario:
    """A source database and the batches of events it has delivered so far."""

    seed: int
    source: SourceDatabase
    batches: list[list[ChangeEvent]] = field(default_factory=list)

    @property
    def events(self) -> list[ChangeEvent]:
        return [ev for batch in self.batches for ev in batch]


def initial_scenario(seed: int = 42) -> Scenario:
    """Batch 1: snapshot, a mixed workload, guaranteed edge cases, poison."""
    source = SourceDatabase(seed)
    rng = random.Random(seed + 1)
    commits = source.snapshot(customers=12)
    commits += source.workload(changes=70)
    # Guaranteed coverage regardless of the random mix: a customer updated
    # twice, a customer deleted while one of their orders is still live (its
    # bookings move to segment UNKNOWN), an order soft-deleted.
    twice = min(source.customers)
    commits += [source.update_customer(twice), source.update_customer(twice)]
    with_orders = sorted({o["o_custkey"] for o in source.orders.values()} - {twice})
    commits.append(source.delete_customer(with_orders[-1]))
    commits.append(source.delete_order(min(source.orders)))
    batch = deliver(commits, rng, duplicates=6, late=4) + poison_events(source)
    return Scenario(seed=seed, source=source, batches=[batch])


def next_batch(scenario: Scenario, changes: int = 30) -> list[ChangeEvent]:
    """A follow-up batch from the same source (higher LSNs), with duplicates.

    It also redelivers an already superseded order event and customer event
    from earlier batches (a connector or source restart replaying old
    offsets), which the pipeline must recognise as stale by LSN.
    """
    rng = random.Random(scenario.seed + len(scenario.batches) + 1)
    earlier = [e for e in scenario.events if e.kind == "change"]
    batch = deliver(scenario.source.workload(changes), rng, duplicates=3, late=2)
    for table in ("orders", "customer"):
        stale = next(e for e in earlier if e.table == table and any(
            later.key == e.key and later.lsn > e.lsn for later in earlier if later.table == table))
        batch.insert(rng.randrange(len(batch) + 1), replace(stale, kind="duplicate"))
    scenario.batches.append(batch)
    return batch


def publish(events: list[ChangeEvent], topic: str, bootstrap: str) -> int:
    """Produce events in delivery order, keyed by primary key, and wait for acks."""
    from confluent_kafka import Producer

    errors: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors.append(str(err))

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "client.id": "o2c-cdc-producer",
    })
    for ev in events:
        producer.produce(topic, key=ev.kafka_key(), value=ev.kafka_value(), on_delivery=on_delivery)
    remaining = producer.flush(60)
    if remaining or errors:
        raise RuntimeError(f"{remaining} events not delivered to {topic}; errors: {errors[:3]}")
    return len(events)


def main() -> None:
    import os

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--topic", default="orders.cdc")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bootstrap", default=f"localhost:{os.getenv('KAFKA_PORT', '19092')}")
    args = parser.parse_args()
    scenario = initial_scenario(args.seed)
    count = publish(scenario.events, args.topic, args.bootstrap)
    kinds = {k: sum(1 for e in scenario.events if e.kind == k) for k in ("change", "duplicate", "late", "poison")}
    print(f"Produced {count} events to {args.topic} (seed {args.seed}): {kinds}")


if __name__ == "__main__":
    main()
