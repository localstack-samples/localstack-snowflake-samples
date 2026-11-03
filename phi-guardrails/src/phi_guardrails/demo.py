"""``make demo``: the access matrix, a new PHI column, and a caught regression.

Runs against the sample database (``$PHI_DATABASE``, default ``PHI``) after
``make migrate seed``. The regression drill changes a masking policy and
restores its original body before exiting, even on Ctrl-C.
"""

from __future__ import annotations

from collections import Counter

from phi_guardrails import settings as cfg
from phi_guardrails.contract import Case, Contract, load_contract
from phi_guardrails.drill import BROKEN_POLICY_SQL, STRING_POLICY, temporarily_applied
from phi_guardrails.matrix import Matrix
from phi_guardrails.settings import SAMPLE_ROOT, Settings
from phi_guardrails.synthea import load_dataset

Results = list[tuple[Case, list[str]]]


def heading(text: str) -> None:
    print(f"\n== {text}")


def print_grid(contract: Contract, results: Results) -> Results:
    """One line per persona, one column per object: passed/total cells."""
    passed: Counter = Counter()
    total: Counter = Counter()
    failed = []
    for case, failures in results:
        total[case.persona, case.object] += 1
        if failures:
            failed.append((case, failures))
        else:
            passed[case.persona, case.object] += 1
    objects = list(contract.objects)
    labels = [o.split(".")[1] for o in objects]
    width = max(len(label) for label in labels) + 2
    print(f"   {'persona':<16}" + "".join(f"{label:<{width}}" for label in labels).rstrip())
    for persona in contract.personas:
        cells = []
        for obj in objects:
            p, t = passed[persona, obj], total[persona, obj]
            marker = "ok" if p == t else "FAIL"
            cells.append(f"{f'{marker} {p}/{t}':<{width}}")
        print(f"   {persona:<16}" + "".join(cells).rstrip())
    return failed


def show_email(m: Matrix, settings: Settings) -> None:
    db = settings.database
    with cfg.connect(settings) as conn:
        policy, tag = (
            conn.cursor()
            .execute(
                f"SELECT POLICY_SCHEMA || '.' || POLICY_NAME, TAG_SCHEMA || '.' || TAG_NAME "
                f"FROM TABLE({db}.INFORMATION_SCHEMA.POLICY_REFERENCES(REF_ENTITY_NAME => '{db}.CLINICAL.PATIENTS', "
                "REF_ENTITY_DOMAIN => 'table')) WHERE POLICY_KIND = 'MASKING_POLICY' AND REF_COLUMN_NAME = 'EMAIL'"
            )
            .fetchone()
        )
    print(f"   EMAIL is masked by {policy}, inherited from tag {tag}: no policy was written for it.")
    patient = min(p["PATIENT_ID"] for p in m.oracle.data.patients if p["PRIMARY_FACILITY_ID"] == 101)
    for persona, obj in (
        ("CLINICIAN", "CLINICAL.PATIENTS"),
        ("AUDITOR", "CLINICAL.PATIENTS"),
        ("ANALYST", "ANALYTICS.PATIENT_COHORT"),
    ):
        row = m.observer.observe(persona, m.contract.objects[obj]).rows.get((patient,), {})
        print(f"   {persona:<10} reading {obj:<26} sees EMAIL = {row.get('EMAIL')}")


def drill(m: Matrix, settings: Settings) -> bool:
    """Apply the broken policy, show what the matrix catches, restore; True if all is well."""
    print(f"   {BROKEN_POLICY_SQL.relative_to(SAMPLE_ROOT)} rewrites {STRING_POLICY} with")
    print("   CURRENT_ROLE() = 'CLINICIAN' instead of IS_ROLE_IN_SESSION('CLINICIAN').")
    with cfg.connect(settings) as conn, temporarily_applied(conn, settings.database):
        m.observer.invalidate()
        failed = [(c, f) for c, f in m.run() if f]
        for case, failures in failed:
            print(f"   FAIL {case.persona} {case.object}.{case.column}: {failures[0]}")
        personas = sorted({c.persona for c, _ in failed})
        print(
            f"   {len(failed)} cell(s) failed, for {', '.join(personas) or 'nobody'}: the matrix caught the regression."
        )
    m.observer.invalidate()
    still_failing = [c.id for c, f in m.run([c for c, _ in failed]) if f]
    print(f"   Original policy body restored; the {len(failed)} cell(s) pass again: {not still_failing}")
    return bool(failed) and not still_failing


def main() -> None:
    settings = cfg.load()
    contract = load_contract()
    m = Matrix(settings, contract, load_dataset())
    try:
        heading(
            f"1. Access matrix: access-contract.yml -> {len(contract.cases())} cells, "
            "each queried as the persona's own user"
        )
        failed = print_grid(contract, m.run())
        for case, failures in failed:
            print(f"   FAIL {case.id}: {failures[0]}")
        heading("2. A new PHI column with no new policy code (V1.3.0 adds CLINICAL.PATIENTS.EMAIL)")
        show_email(m, settings)
        heading("3. Regression drill: a broken masking policy")
        drill_ok = drill(m, settings)
    finally:
        m.close()
    if failed or not drill_ok:
        raise SystemExit("The access matrix did not match access-contract.yml; see the FAIL lines above.")


if __name__ == "__main__":
    main()
