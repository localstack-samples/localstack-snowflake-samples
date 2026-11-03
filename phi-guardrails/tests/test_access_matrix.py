"""The access matrix: one test per cell of access-contract.yml, plus the
behaviours around it (regression detection and tag-only masking)."""

from __future__ import annotations

import pytest
from snowflake.connector import SnowflakeConnection

from phi_guardrails.contract import VISIBLE, Case, Contract, load_contract
from phi_guardrails.drill import temporarily_applied
from phi_guardrails.matrix import Matrix
from phi_guardrails.settings import Settings
from phi_guardrails.synthea import Dataset


@pytest.mark.parametrize("case", load_contract().cases(), ids=lambda c: c.id)
def test_contract_cell(matrix: Matrix, case: Case) -> None:
    failures = matrix.check(case)
    assert not failures, f"{case.id}: " + "; ".join(failures)


def test_matrix_covers_every_cell(contract: Contract) -> None:
    """The generated cases are exactly the contract's cells: 5 objects x 5 personas
    rows cells, plus one column cell per persona that can see an object's rows."""
    expected = sum(
        len(contract.personas) + sum(1 for o in obj.rows.values() if o in VISIBLE) * len(obj.columns)
        for obj in contract.objects.values()
    )
    assert len(contract.cases()) == expected == 174


def test_broken_current_role_policy_is_caught(
    matrix: Matrix, admin: SnowflakeConnection, deployed: Settings, contract: Contract, dataset: Dataset
) -> None:
    """Headline regression: demos/broken-current-role-policy.sql replaces
    IS_ROLE_IN_SESSION('CLINICIAN') with CURRENT_ROLE() = 'CLINICIAN' in the
    string masking policy. The matrix must fail exactly the clinical lead's
    clear text-PHI cells, and nothing else."""
    text_phi = {
        (name, col.name)
        for name, obj in contract.objects.items()
        if obj.is_table
        for col in obj.columns.values()
        if col.phi_type and isinstance(next(r[col.name] for r in dataset.table(name) if r[col.name]), str)
    }
    expected = {
        ("CLINICIAN_LEAD", obj, col)
        for obj, col in text_phi
        if contract.objects[obj].columns[col].access.get("CLINICIAN_LEAD") == "clear"
    }
    # The text PHI columns are all on PATIENTS: two names, SSN, licence and
    # passport numbers, three address parts, ZIP and EMAIL.
    assert {obj for _, obj, _ in expected} == {"CLINICAL.PATIENTS"} and len(expected) == 10

    try:
        with temporarily_applied(admin, deployed.database):
            matrix.observer.invalidate()
            failing = {(case.persona, case.object, case.column) for case, failures in matrix.run() if failures}
    finally:
        matrix.observer.invalidate()
    assert failing == expected


def test_patient_email_is_masked_by_its_tag_alone(admin: SnowflakeConnection, deployed: Settings) -> None:
    """V1.3.0 adds EMAIL with a tag and no policy code: its only masking policy
    is the one the PHI_TYPE tag carries, and GOVERNANCE still has the three
    policies V1.1.0 created."""
    db = deployed.database
    refs = (
        admin.cursor()
        .execute(
            f"SELECT POLICY_NAME, TAG_NAME FROM TABLE({db}.INFORMATION_SCHEMA.POLICY_REFERENCES("
            f"REF_ENTITY_NAME => '{db}.CLINICAL.PATIENTS', REF_ENTITY_DOMAIN => 'table')) "
            "WHERE POLICY_KIND = 'MASKING_POLICY' AND REF_COLUMN_NAME = 'EMAIL'"
        )
        .fetchall()
    )
    assert refs == [("PHI_MASK_STRING", "PHI_TYPE")]
    policies = {row[1] for row in admin.cursor().execute(f"SHOW MASKING POLICIES IN SCHEMA {db}.GOVERNANCE").fetchall()}
    assert policies == {"PHI_MASK_STRING", "PHI_MASK_DATE", "PHI_MASK_TIMESTAMP"}
