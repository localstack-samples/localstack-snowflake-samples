"""The pull request pipeline: state:modified+ with defer, per-PR schemas, the bad-PR catch."""

from __future__ import annotations

import subprocess

from slim_ci_lab import bad_pr_demo, settings
from slim_ci_lab.dbt_runner import apply_patch, copy_project, run_dbt, targets

EMULATOR = settings.is_emulator()
DEMOS = settings.SAMPLE_ROOT / "demos"
CI_SCRIPT = settings.SAMPLE_ROOT / "scripts" / "ci_local.sh"


def run_ci(prod_state, pr_number: str, patch: str) -> subprocess.CompletedProcess[str]:
    """Run scripts/ci_local.sh for one simulated pull request, reusing the session's prod state."""
    prod_target, pr_target = targets()
    env = {
        **prod_state.env,
        "DBT_PR_NUMBER": pr_number,
        "CI_PR_PATCH": str(DEMOS / patch),
        "PROD_STATE": "reuse",
        "STATE_DIR": str(prod_state.state_dir),
        "PROD_TARGET": prod_target,
        "PR_TARGET": pr_target,
    }
    return subprocess.run([str(CI_SCRIPT)], env=env, capture_output=True, text=True, check=False)


def tables_in(conn, database: str, schema: str) -> set[str]:
    rows = conn.cursor().execute(
        f"SELECT table_name FROM {database}.INFORMATION_SCHEMA.TABLES WHERE table_schema = %s",
        (schema,),
    ).fetchall()
    return {name for (name,) in rows}


def test_pr_build_selects_only_the_modified_mart_and_defers_upstream(conn, prod_state):
    db = prod_state.database

    result = run_ci(prod_state, "101", "example-pr.patch")

    assert result.returncode == 0, result.stdout[-4000:]
    if EMULATOR:
        assert "Snowflake credits used: 0" in result.stdout
    # Only the modified model lands in the PR schema; its upstreams were read from prod.
    assert tables_in(conn, db, "PR_101") == {"FCT_DAILY_ZONE_REVENUE"}
    columns = conn.cursor().execute(f"SELECT * FROM {db}.PR_101.FCT_DAILY_ZONE_REVENUE LIMIT 0")
    assert "TIP_RATE" in {col[0] for col in columns.description}
    pr_total, prod_total = conn.cursor().execute(
        f"""SELECT (SELECT SUM(revenue) FROM {db}.PR_101.FCT_DAILY_ZONE_REVENUE),
                   (SELECT SUM(revenue) FROM {db}.MARTS.FCT_DAILY_ZONE_REVENUE)"""
    ).fetchone()
    assert pr_total == prod_total


def test_pr_build_without_defer_fails_with_002003(prod_state, tmp_path):
    project = copy_project(tmp_path / "dbt")
    apply_patch(project, DEMOS / "example-pr.patch")
    env = {**prod_state.env, "DBT_PR_NUMBER": "no_defer"}
    pr_target = ["--target", targets()[1]]
    run_dbt(["run-operation", "drop_pr_schema", *pr_target], project, env)

    result = run_dbt(["run", *pr_target, "--select", "state:modified", "--state", str(prod_state.state_dir)], project, env)

    # Without --defer, ref('int_trips_enriched') points into the empty PR schema.
    assert result.returncode != 0
    assert "002003" in result.stdout
    assert "does not exist or not authorized" in result.stdout


def test_bad_unique_key_passes_once_then_fails_with_100090(database, first_load):
    demo = bad_pr_demo.run(database)

    assert demo.first.returncode == 0, demo.first.stdout[-3000:]
    assert demo.second.returncode != 0
    assert "100090" in demo.second_output
    assert "Duplicate row detected during DML action" in demo.second_output
    assert demo.expected_failure_observed


def test_slim_ci_rejects_bad_unique_key_pr_on_first_ci_run(prod_state):
    # The pipeline clones the modified incremental model from prod, so its
    # very first PR build already runs the MERGE that production would.
    result = run_ci(prod_state, "102", "bad-unique-key.patch")

    assert result.returncode != 0
    assert "100090" in result.stdout
    assert "Duplicate row detected during DML action" in result.stdout
