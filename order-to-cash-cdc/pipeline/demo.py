"""`make demo`: a failed task run leaves no trace, the retry applies each event once.

Runs in its own database (O2C_DEMO, recreated each time and kept afterwards
for exploring) with its own topic and sink connector, so the numbers are the
same on every run.
"""

from __future__ import annotations

import time

from pipeline import settings as settings_module
from pipeline import snowflake_db
from pipeline.harness import IsolatedPipeline, Pipeline
from producer.produce import ChangeEvent, initial_scenario, next_batch
from tests import model

WIDTH = 78


def step(n: int, total: int, text: str) -> None:
    print(f"\n[{n}/{total}] {text}")


def detail(text: str) -> None:
    print(f"      {text}")


def describe(events: list[ChangeEvent]) -> str:
    kinds = {k: sum(1 for e in events if e.kind == k) for k in ("change", "duplicate", "late", "poison")}
    return ", ".join(f"{n} {k}" for k, n in kinds.items() if n)


def state(p: Pipeline) -> tuple[list, list, list]:
    return p.orders(), p.customer_history(), p.dead_letters()


def summary(p: Pipeline) -> str:
    orders, history, dead = state(p)
    return (f"core.orders {len(orders)} rows ({sum(1 for o in orders if o[7])} soft-deleted) | "
            f"dim_customer_history {len(history)} versions | dead_letter {len(dead)}")


def yes(flag: bool) -> str:
    return "yes" if flag else "NO"


def main() -> None:
    settings = settings_module.load()
    total = 6
    print("=" * WIDTH)
    print("Order-to-Cash CDC: forced failure, rollback, then exactly-once apply")
    print("=" * WIDTH)
    with snowflake_db.connect(settings) as conn, \
            IsolatedPipeline(settings, conn, database="O2C_DEMO", keep_database=True) as p:
        step(1, total, f"Deployed the pipeline into database {p.database}; topic {p.topic} -> RAW.ORDERS_CDC")

        scenario = initial_scenario(42)
        started = time.monotonic()
        produced = p.produce(scenario.events)
        p.wait_for_landing(produced)
        step(2, total, f"Batch 1: {produced} Debezium events ({describe(scenario.events)})")
        detail(f"landed in RAW.ORDERS_CDC via the Kafka connector in {time.monotonic() - started:.1f}s")
        run = p.run_task()
        detail(f"EXECUTE TASK OPS.APPLY_ORDERS_CDC -> {run.state}")
        detail(summary(p))

        batch2 = next_batch(scenario)
        p.produce(batch2)
        p.wait_for_landing(len(scenario.events))
        before = state(p)
        step(3, total, f"Batch 2: {len(batch2)} more events ({describe(batch2)}) landed; not applied yet")

        p.set_fail_next_run(True)
        step(4, total, "UPDATE OPS.PIPELINE_CONTROL SET FAIL_NEXT_RUN = TRUE; EXECUTE TASK")
        failed = p.run_task()
        detail(f"TASK_HISTORY: {failed.state} ({failed.error_code}) {failed.error_message}")
        after = state(p)
        detail(f"core.orders unchanged:          {yes(after[0] == before[0])}")
        detail(f"dim_customer_history unchanged: {yes(after[1] == before[1])}")
        detail(f"dead_letter unchanged:          {yes(after[2] == before[2])}")
        detail(f"SYSTEM$STREAM_HAS_DATA:         {p.stream_has_data()}  (batch 2 still pending)")

        p.set_fail_next_run(False)
        step(5, total, "UPDATE OPS.PIPELINE_CONTROL SET FAIL_NEXT_RUN = FALSE; EXECUTE TASK")
        retried = p.run_task()
        detail(f"TASK_HISTORY: {retried.state}")
        detail(summary(p))
        events = scenario.events
        detail(f"core.orders equals newest event per key:   {yes(p.orders() == model.expected_orders(events))}")
        detail(f"one SCD2 version per customer change:      "
               f"{yes(p.customer_history() == model.expected_customer_history(events))}")

        p.refresh_mart()
        bookings = p.daily_bookings()
        step(6, total, "ALTER DYNAMIC TABLE MART.DAILY_BOOKINGS REFRESH")
        detail(f"matches totals computed from the {len(events)} events: "
               f"{yes(bookings == model.expected_daily_bookings(events))}")
        detail(f"{'booking_date':<12} {'segment':<11} {'orders':>6} {'gross_bookings':>15}")
        for booking_date, segment, count, gross in bookings[:8]:
            detail(f"{booking_date!s:<12} {segment:<11} {count:>6} {gross:>15,.2f}")
        if len(bookings) > 8:
            detail(f"... {len(bookings) - 8} more rows")

    print("\nExplore it: SELECT * FROM O2C_DEMO.CORE.DIM_CUSTOMER_HISTORY ORDER BY C_CUSTKEY, VALID_FROM;")


if __name__ == "__main__":
    main()
