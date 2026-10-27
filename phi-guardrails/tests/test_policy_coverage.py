"""The policy-coverage gate and the classification rules around it."""

from __future__ import annotations

import subprocess
import sys

import pytest
from conftest import unique
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from phi_guardrails import coverage
from phi_guardrails.contract import Contract
from phi_guardrails.settings import SAMPLE_ROOT, Settings


def _contract_phi_columns(contract: Contract) -> dict[tuple[str, str], str]:
    return {
        (name, col.name): col.phi_type
        for name, obj in contract.objects.items()
        if obj.is_table
        for col in obj.columns.values()
        if col.phi_type
    }


def test_every_tagged_column_has_a_masking_policy(admin: SnowflakeConnection, deployed: Settings) -> None:
    columns = coverage.scan(admin, deployed.database)
    assert coverage.uncovered(columns) == []
    assert len(columns) == 14  # 12 on PATIENTS (EMAIL included) + START_TS, STOP_TS


def test_tags_match_the_contract(admin: SnowflakeConnection, deployed: Settings, contract: Contract) -> None:
    """Every column the contract classifies as PHI carries exactly that tag
    value, and no other column is tagged."""
    tagged = {(c.table, c.column): c.phi_type for c in coverage.scan(admin, deployed.database)}
    assert tagged == _contract_phi_columns(contract)


def test_every_table_column_is_classified_in_the_contract(
    admin: SnowflakeConnection, deployed: Settings, contract: Contract
) -> None:
    """A new column cannot slip in unclassified: each column of a CLINICAL
    table needs a contract entry (with phi_type, or phi_type: null)."""
    rows = (
        admin.cursor()
        .execute(
            "SELECT TABLE_SCHEMA || '.' || TABLE_NAME, COLUMN_NAME "
            f"FROM {deployed.database}.INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'CLINICAL'"
        )
        .fetchall()
    )
    in_database = set(rows)
    in_contract = {(name, col) for name, obj in contract.objects.items() if obj.is_table for col in obj.columns}
    assert in_database == in_contract


def test_gate_fails_on_a_tagged_but_unprotected_column(
    admin: SnowflakeConnection, deployed: Settings, scratch_schema: str
) -> None:
    """Tag-based masking covers only the data types the tag has policies for.
    An MRN stored as NUMBER and a timestamp stored as TIMESTAMP_TZ keep their
    tags but stay in clear; the gate must name exactly those two columns."""
    db = deployed.database
    schema = scratch_schema.split(".")[1]
    tag = f"{db}.GOVERNANCE.PHI_TYPE"
    cur = admin.cursor()
    cur.execute(
        f"CREATE TABLE {scratch_schema}.LAB_RESULTS ("
        f"MRN NUMBER(10, 0) WITH TAG ({tag} = 'mrn'), "
        f"COLLECTED_AT TIMESTAMP_TZ WITH TAG ({tag} = 'service_date'), "
        f"PATIENT_NAME VARCHAR(100) WITH TAG ({tag} = 'name'), "
        "RESULT VARCHAR(100))"
    )
    cur.execute(
        f"INSERT INTO {scratch_schema}.LAB_RESULTS "
        "SELECT 4711, '2020-03-04 05:06:07 +01:00'::TIMESTAMP_TZ, 'Milo Feil', 'negative'"
    )

    # The leak the gate exists for: the deploy role is not a clinician, so the
    # VARCHAR name is masked for it, but the NUMBER and TIMESTAMP_TZ values are not.
    mrn, name = cur.execute(f"SELECT MRN, PATIENT_NAME FROM {scratch_schema}.LAB_RESULTS").fetchone()
    assert (mrn, name) == (4711, "***MASKED***")

    gaps = {(c.table, c.column, c.data_type) for c in coverage.uncovered(coverage.scan(admin, db))}
    assert gaps == {
        (f"{schema}.LAB_RESULTS", "MRN", "NUMBER"),
        (f"{schema}.LAB_RESULTS", "COLLECTED_AT", "TIMESTAMP_TZ"),
    }

    # The CLI is what CI runs: it must exit non-zero and name the columns.
    result = subprocess.run(
        [sys.executable, "-m", "phi_guardrails.coverage", "--database", db],
        capture_output=True,
        text=True,
        cwd=SAMPLE_ROOT,
        check=False,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert f"{schema}.LAB_RESULTS.MRN" in result.stdout
    assert "2 without a masking policy" in result.stdout


def test_tag_rejects_an_unknown_phi_type(admin: SnowflakeConnection, deployed: Settings, scratch_schema: str) -> None:
    admin.cursor().execute(f"CREATE TABLE {scratch_schema}.NOTES (BODY VARCHAR)")
    with pytest.raises(ProgrammingError) as excinfo:
        admin.cursor().execute(
            f"ALTER TABLE {scratch_schema}.NOTES MODIFY COLUMN BODY "
            f"SET TAG {deployed.database}.GOVERNANCE.PHI_TYPE = 'diagnosis'"
        )
    assert excinfo.value.errno == 391826, excinfo.value  # value not in ALLOWED_VALUES


def test_tag_carries_one_masking_policy_per_data_type(
    admin: SnowflakeConnection, deployed: Settings, scratch_schema: str
) -> None:
    """A second VARCHAR policy cannot be attached to the tag, so there is never
    doubt about which policy masks a text column."""
    policy = f"{scratch_schema}.{unique('SHADOW_MASK')}"
    admin.cursor().execute(f"CREATE MASKING POLICY {policy} AS (val VARCHAR) RETURNS VARCHAR -> val")
    with pytest.raises(ProgrammingError) as excinfo:
        admin.cursor().execute(f"ALTER TAG {deployed.database}.GOVERNANCE.PHI_TYPE SET MASKING POLICY {policy}")
    assert excinfo.value.errno == 391837, excinfo.value


def test_attached_policy_cannot_be_replaced(admin: SnowflakeConnection, scratch_schema: str) -> None:
    """Why the migrations use CREATE ... IF NOT EXISTS and change policies with
    ALTER ... SET BODY: CREATE OR REPLACE on an attached policy fails."""
    cur = admin.cursor()
    cur.execute(f"CREATE TABLE {scratch_schema}.T (V VARCHAR)")
    cur.execute(f"CREATE MASKING POLICY {scratch_schema}.P AS (val VARCHAR) RETURNS VARCHAR -> '***'")
    cur.execute(f"ALTER TABLE {scratch_schema}.T MODIFY COLUMN V SET MASKING POLICY {scratch_schema}.P")
    with pytest.raises(ProgrammingError) as excinfo:
        cur.execute(f"CREATE OR REPLACE MASKING POLICY {scratch_schema}.P AS (val VARCHAR) RETURNS VARCHAR -> val")
    assert excinfo.value.errno == 3531, excinfo.value
