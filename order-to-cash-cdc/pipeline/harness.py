"""Drive the pipeline deterministically: produce, wait for landing, run the task.

`IsolatedPipeline` gives a test (or the demo) its own database, topic and sink
connector, built from the same init SQL and connector config as the O2C
deployment, and removes them afterwards. Nothing here sleeps on a schedule:
every wait polls a concrete condition with a bounded timeout.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime

from snowflake.connector import SnowflakeConnection

from pipeline import kafka_connect, snowflake_db
from pipeline.settings import Settings
from producer.produce import ChangeEvent, publish

TASK = "OPS.APPLY_ORDERS_CDC"
TERMINAL_STATES = ("SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED")


@dataclass(frozen=True)
class TaskRun:
    state: str
    error_code: str | None
    error_message: str | None
    query_id: str | None
    scheduled_time: datetime | None


class Pipeline:
    """Operations on one deployed pipeline (a database plus its sink topic)."""

    def __init__(self, settings: Settings, conn: SnowflakeConnection, database: str, topic: str) -> None:
        self.settings = settings
        self.conn = conn
        self.database = database
        self.topic = topic

    # -- helpers --------------------------------------------------------------
    def q(self, sql: str, params: tuple | None = None) -> list[tuple]:
        return snowflake_db.query(self.conn, sql, params)

    def scalar(self, sql: str, params: tuple | None = None):
        return snowflake_db.scalar(self.conn, sql, params)

    def fq(self, name: str) -> str:
        return f"{self.database}.{name}"

    # -- ingest ---------------------------------------------------------------
    def produce(self, events: list[ChangeEvent]) -> int:
        return publish(events, self.topic, self.settings.kafka_bootstrap)

    def landed(self) -> int:
        return self.scalar(f"SELECT COUNT(*) FROM {self.fq('RAW.ORDERS_CDC')}")

    def wait_for_landing(self, expected: int, timeout: float = 180, interval: float = 1.0) -> int:
        """Poll the landing table until it holds `expected` rows (never more)."""
        deadline = time.monotonic() + timeout
        count = self.landed()
        while count < expected and time.monotonic() < deadline:
            time.sleep(interval)
            count = self.landed()
        if count != expected:
            raise TimeoutError(
                f"{self.fq('RAW.ORDERS_CDC')} has {count} rows, expected {expected} within {timeout}s "
                f"(topic {self.topic}; check 'docker compose logs connect')"
            )
        return count

    # -- task -----------------------------------------------------------------
    def _task_runs(self) -> list[TaskRun]:
        rows = self.q(
            f"""SELECT STATE, ERROR_CODE, ERROR_MESSAGE, QUERY_ID, SCHEDULED_TIME
                FROM TABLE({self.database}.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'APPLY_ORDERS_CDC'))
                ORDER BY SCHEDULED_TIME DESC"""
        )
        return [TaskRun(*row) for row in rows if row[0] in TERMINAL_STATES]

    def run_task(self, timeout: float = 120, interval: float = 0.5) -> TaskRun:
        """EXECUTE TASK, then poll TASK_HISTORY() until that run finishes."""
        before = len(self._task_runs())
        self.q(f"EXECUTE TASK {self.fq(TASK)}")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            runs = self._task_runs()
            if len(runs) > before:
                return runs[0]
            time.sleep(interval)
        raise TimeoutError(f"no finished run of {self.fq(TASK)} in TASK_HISTORY() within {timeout}s")

    def set_fail_next_run(self, fail: bool) -> None:
        self.q(
            f"""UPDATE {self.fq('OPS.PIPELINE_CONTROL')}
                SET FAIL_NEXT_RUN = %s, UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                WHERE PIPELINE_NAME = 'orders_cdc'""",
            (fail,),
        )

    def stream_has_data(self) -> bool:
        return bool(self.scalar(f"SELECT SYSTEM$STREAM_HAS_DATA('{self.fq('RAW.ORDERS_CDC_STREAM')}')"))

    def refresh_mart(self) -> None:
        self.q(f"ALTER DYNAMIC TABLE {self.fq('MART.DAILY_BOOKINGS')} REFRESH")

    # -- reads ------------------------------------------------------------------
    def orders(self) -> list[tuple]:
        return self.q(
            f"""SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY,
                       O_CLERK, IS_DELETED, SOURCE_LSN
                FROM {self.fq('CORE.ORDERS')} ORDER BY O_ORDERKEY"""
        )

    def customer_history(self) -> list[tuple]:
        return self.q(
            f"""SELECT C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL, C_MKTSEGMENT, VALID_FROM, VALID_TO,
                       IS_CURRENT, SOURCE_LSN
                FROM {self.fq('CORE.DIM_CUSTOMER_HISTORY')} ORDER BY C_CUSTKEY, SOURCE_LSN"""
        )

    def daily_bookings(self) -> list[tuple]:
        return self.q(
            f"""SELECT BOOKING_DATE, MARKET_SEGMENT, ORDER_COUNT, GROSS_BOOKINGS
                FROM {self.fq('MART.DAILY_BOOKINGS')} ORDER BY BOOKING_DATE, MARKET_SEGMENT"""
        )

    def dead_letters(self) -> list[tuple]:
        return self.q(
            f"""SELECT REASON, RECORD_CONTENT:source:lsn::NUMBER
                FROM {self.fq('OPS.DEAD_LETTER')} ORDER BY 2, 1"""
        )


class IsolatedPipeline:
    """Context manager: a fresh database, topic and sink connector per run."""

    def __init__(self, settings: Settings, conn: SnowflakeConnection, prefix: str = "O2C_T",
                 database: str | None = None, keep_database: bool = False) -> None:
        run_id = uuid.uuid4().hex[:8]
        self.settings = settings
        self.conn = conn
        self.database = (database or f"{prefix}_{run_id}").upper()
        # Topic and connector are unique per run even when the database name is
        # fixed (the demo), so a crashed run can never feed a later one.
        self.topic = f"orders.cdc.{self.database.lower()}.{run_id}"
        self.connector = f"o2c-sink-{self.database.lower().replace('_', '-')}-{run_id}"
        self.keep_database = keep_database
        self.topics = kafka_connect.Topics(settings)
        self.connect = kafka_connect.ConnectClient(settings)

    def __enter__(self) -> Pipeline:
        snowflake_db.deploy(self.conn, self.settings, self.database)
        self.topics.create(self.topic)
        self.connect.put(self.connector, kafka_connect.sink_config(self.settings, self.topic, self.database))
        self.connect.wait_running(self.connector)
        return Pipeline(self.settings, self.conn, self.database, self.topic)

    def __exit__(self, *exc) -> None:
        self.connect.delete(self.connector)
        self.topics.delete(self.topic)
        if not self.keep_database:
            snowflake_db.drop(self.conn, self.database)
