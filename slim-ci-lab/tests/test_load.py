"""Raw load: PUT + COPY INTO <db>.RAW.TRIPS."""

from __future__ import annotations

import pyarrow.compute as pc
import pyarrow.parquet as pq

from slim_ci_lab import loader


def test_raw_row_count_matches_downsampled_file(conn, database, sample_file, first_load):
    expected_rows = pq.ParquetFile(sample_file).metadata.num_rows

    (count, files) = conn.cursor().execute(
        f"SELECT COUNT(*), COUNT(DISTINCT _SOURCE_FILE) FROM {database}.RAW.TRIPS"
    ).fetchone()

    assert first_load.files_loaded == 1
    assert first_load.rows_loaded == expected_rows
    assert (count, files) == (expected_rows, 1)


def test_raw_timestamps_match_source_file(conn, database, sample_file, first_load):
    source = pq.read_table(sample_file, columns=["tpep_pickup_datetime"]).column(0)
    expected = (pc.min(source).as_py(), pc.max(source).as_py())

    actual = conn.cursor().execute(
        f"SELECT MIN(TPEP_PICKUP_DATETIME), MAX(TPEP_PICKUP_DATETIME) FROM {database}.RAW.TRIPS"
    ).fetchone()

    assert actual == expected


def test_copy_rerun_loads_no_new_files(conn, database, sample_file, first_load):
    again = loader.load(database, sample_file)

    assert (again.files_loaded, again.rows_loaded) == (0, 0)
    assert again.total_rows == first_load.total_rows

