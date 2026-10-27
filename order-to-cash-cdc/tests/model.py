"""Expected pipeline results, computed in plain Python from the emitted events.

This is the test oracle. It applies the business rules directly to the
producer's events (newest LSN wins per key, deletes are soft, one SCD2 version
per customer change, poison events are dead-lettered) without touching any
SQL from the pipeline, so a bug in the procedure cannot hide in both places.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from producer.produce import EPOCH, ChangeEvent


def _ts(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, UTC).replace(tzinfo=None)


def _valid(events: list[ChangeEvent], table: str) -> list[ChangeEvent]:
    return [e for e in events if e.kind != "poison" and e.table == table]


def _latest_by_key(events: list[ChangeEvent]) -> dict[int, ChangeEvent]:
    latest: dict[int, ChangeEvent] = {}
    for ev in events:
        if ev.key not in latest or ev.lsn > latest[ev.key].lsn:
            latest[ev.key] = ev
    return latest


def expected_orders(events: list[ChangeEvent]) -> list[tuple]:
    """Rows of CORE.ORDERS, in the column order of Pipeline.orders()."""
    rows = []
    for key, ev in sorted(_latest_by_key(_valid(events, "orders")).items()):
        img = ev.image
        rows.append((
            key,
            img["o_custkey"],
            img["o_orderstatus"],
            Decimal(img["o_totalprice"]),
            EPOCH + timedelta(days=img["o_orderdate"]),
            img["o_orderpriority"],
            img["o_clerk"],
            ev.op == "d",
            ev.lsn,
        ))
    return rows


def expected_customer_history(events: list[ChangeEvent]) -> list[tuple]:
    """Rows of CORE.DIM_CUSTOMER_HISTORY, in the column order of Pipeline.customer_history()."""
    changes: dict[int, dict[int, ChangeEvent]] = defaultdict(dict)
    for ev in _valid(events, "customer"):
        changes[ev.key][ev.lsn] = ev  # a redelivered duplicate has the same LSN
    rows = []
    for key in sorted(changes):
        ordered = [changes[key][lsn] for lsn in sorted(changes[key])]
        for i, ev in enumerate(ordered):
            if ev.op == "d":
                continue
            valid_to = _ts(ordered[i + 1].ts_ms) if i + 1 < len(ordered) else None
            img = ev.image
            rows.append((
                key,
                img["c_name"],
                img["c_nationkey"],
                Decimal(img["c_acctbal"]),
                img["c_mktsegment"],
                _ts(ev.ts_ms),
                valid_to,
                valid_to is None,
                ev.lsn,
            ))
    return rows


def deleted_customers(events: list[ChangeEvent]) -> dict[int, datetime]:
    """Customer keys whose newest change is a delete, with the delete time."""
    latest = _latest_by_key(_valid(events, "customer"))
    return {k: _ts(e.ts_ms) for k, e in latest.items() if e.op == "d"}


def expected_daily_bookings(events: list[ChangeEvent]) -> list[tuple]:
    """Rows of MART.DAILY_BOOKINGS: live orders by order date and current segment."""
    segment = {}
    for key, ev in _latest_by_key(_valid(events, "customer")).items():
        if ev.op != "d":
            segment[key] = ev.image["c_mktsegment"]
    totals: dict[tuple[date, str], list] = defaultdict(lambda: [0, Decimal("0.00")])
    for _key, ev in _latest_by_key(_valid(events, "orders")).items():
        if ev.op == "d":
            continue
        img = ev.image
        bucket = totals[(EPOCH + timedelta(days=img["o_orderdate"]), segment.get(img["o_custkey"], "UNKNOWN"))]
        bucket[0] += 1
        bucket[1] += Decimal(img["o_totalprice"])
    return [(d, s, n, total) for (d, s), (n, total) in sorted(totals.items())]


def expected_dead_letters(events: list[ChangeEvent]) -> list[tuple]:
    """(reason, source LSN) per poison delivery, in the order of Pipeline.dead_letters()."""
    return sorted(((e.reject_reason, e.lsn) for e in events if e.kind == "poison"), key=lambda r: (r[1], r[0]))
