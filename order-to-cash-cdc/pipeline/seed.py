"""`make seed`: push the seeded scenario through the O2C deployment.

Produces batch 1 to `orders.cdc` (the sink registered by `make up`), waits for
it to land in O2C.RAW.ORDERS_CDC, runs the task once and refreshes the mart.
Safe to repeat: replayed events are recognised by their source LSN, so core
and mart tables end up identical (only the landing table grows).
"""

from __future__ import annotations

from pipeline import settings as settings_module
from pipeline import snowflake_db
from pipeline.harness import Pipeline
from producer.produce import initial_scenario


def main() -> None:
    settings = settings_module.load()
    with snowflake_db.connect(settings) as conn:
        pipeline = Pipeline(settings, conn, settings.database, "orders.cdc")
        scenario = initial_scenario(42)
        before = pipeline.landed()
        produced = pipeline.produce(scenario.events)
        pipeline.wait_for_landing(before + produced)
        run = pipeline.run_task()
        if run.state != "SUCCEEDED":
            raise SystemExit(f"task run {run.state}: {run.error_code} {run.error_message}")
        pipeline.refresh_mart()
        orders = pipeline.orders()
        print(f"Seeded {settings.database}: {produced} events produced to orders.cdc, "
              f"{before + produced} rows in RAW.ORDERS_CDC")
        print(f"  CORE.ORDERS {len(orders)} rows ({sum(1 for o in orders if o[7])} soft-deleted), "
              f"CORE.DIM_CUSTOMER_HISTORY {len(pipeline.customer_history())} versions, "
              f"MART.DAILY_BOOKINGS {len(pipeline.daily_bookings())} rows, "
              f"OPS.DEAD_LETTER {len(pipeline.dead_letters())} rows")


if __name__ == "__main__":
    main()
