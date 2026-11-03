"""Load the downsampled TLC extract into <database>.RAW.TRIPS.

PUT uploads the Parquet file to an internal stage; COPY INTO matches Parquet
columns to table columns by name and records lineage (file name, row number,
scan time) through INCLUDE_METADATA. COPY keeps per-file load metadata, so
re-running the loader loads 0 new files: the step is idempotent and safe to
retry in CI.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from slim_ci_lab import settings, tlc

STAGE_PATH = "yellow/2024-01/"

RAW_DDL = (
    "CREATE DATABASE IF NOT EXISTS {db}",
    "CREATE SCHEMA IF NOT EXISTS {db}.RAW",
    # USE_LOGICAL_TYPE reads Parquet logical types (TIMESTAMP, DECIMAL) as
    # typed values; it is Snowflake's recommended setting for Parquet loads.
    """CREATE FILE FORMAT IF NOT EXISTS {db}.RAW.TLC_PARQUET
        TYPE = PARQUET
        USE_LOGICAL_TYPE = TRUE""",
    """CREATE STAGE IF NOT EXISTS {db}.RAW.TLC_LANDING
        FILE_FORMAT = (FORMAT_NAME = '{db}.RAW.TLC_PARQUET')
        COMMENT = 'NYC TLC trip files, landed as published'""",
    # Column names follow the TLC data dictionary so MATCH_BY_COLUMN_NAME can
    # map them; renaming and typing happens in dbt staging.
    """CREATE TABLE IF NOT EXISTS {db}.RAW.TRIPS (
        VENDORID INTEGER,
        TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
        TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
        PASSENGER_COUNT INTEGER,
        TRIP_DISTANCE FLOAT,
        RATECODEID INTEGER,
        STORE_AND_FWD_FLAG VARCHAR(1),
        PULOCATIONID INTEGER,
        DOLOCATIONID INTEGER,
        PAYMENT_TYPE INTEGER,
        FARE_AMOUNT FLOAT,
        EXTRA FLOAT,
        MTA_TAX FLOAT,
        TIP_AMOUNT FLOAT,
        TOLLS_AMOUNT FLOAT,
        IMPROVEMENT_SURCHARGE FLOAT,
        TOTAL_AMOUNT FLOAT,
        CONGESTION_SURCHARGE FLOAT,
        AIRPORT_FEE FLOAT,
        _SOURCE_FILE VARCHAR,
        _SOURCE_ROW INTEGER,
        _LOADED_AT TIMESTAMP_LTZ
    ) COMMENT = 'Yellow taxi trips as delivered by NYC TLC (downsampled)'""",
)

COPY_SQL = """COPY INTO {db}.RAW.TRIPS
    FROM @{db}.RAW.TLC_LANDING/{path}
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    INCLUDE_METADATA = (
        _SOURCE_FILE = METADATA$FILENAME,
        _SOURCE_ROW = METADATA$FILE_ROW_NUMBER,
        _LOADED_AT = METADATA$START_SCAN_TIME
    )
    ON_ERROR = ABORT_STATEMENT"""


@dataclass(frozen=True)
class LoadResult:
    files_loaded: int
    rows_loaded: int
    total_rows: int


def ensure_raw_objects(cursor, database: str) -> None:
    for statement in RAW_DDL:
        cursor.execute(statement.format(db=database))


def put_file(cursor, database: str, path: Path, stage_path: str = STAGE_PATH) -> None:
    # OVERWRITE = FALSE keeps the staged file (and its load metadata) stable.
    cursor.execute(
        f"PUT 'file://{path.resolve().as_posix()}' @{database}.RAW.TLC_LANDING/{stage_path} "
        "AUTO_COMPRESS = FALSE OVERWRITE = FALSE"
    )


def copy_into_trips(cursor, database: str, stage_path: str = STAGE_PATH) -> tuple[int, int]:
    """Run COPY INTO and return (files loaded, rows loaded)."""
    rows = cursor.execute(COPY_SQL.format(db=database, path=stage_path)).fetchall()
    columns = [col[0].lower() for col in cursor.description]
    if "rows_loaded" not in columns:  # one row: "Copy executed with 0 files processed."
        return 0, 0
    status, loaded = columns.index("status"), columns.index("rows_loaded")
    done = [row for row in rows if row[status] == "LOADED"]
    return len(done), sum(int(row[loaded]) for row in done)


def load(database: str, path: Path | None = None) -> LoadResult:
    path = path or tlc.sample_file()
    with settings.connect() as conn:
        cursor = conn.cursor()
        ensure_raw_objects(cursor, database)
        put_file(cursor, database, path)
        files, rows = copy_into_trips(cursor, database)
        (total,) = cursor.execute(f"SELECT COUNT(*) FROM {database}.RAW.TRIPS").fetchone()
    return LoadResult(files_loaded=files, rows_loaded=rows, total_rows=int(total))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", default=settings.load().database)
    args = parser.parse_args(argv)
    result = load(args.database)
    print(
        f"{args.database}.RAW.TRIPS: {result.files_loaded} file(s) and {result.rows_loaded} row(s) "
        f"loaded this run; {result.total_rows} row(s) in total"
    )


if __name__ == "__main__":
    main()
