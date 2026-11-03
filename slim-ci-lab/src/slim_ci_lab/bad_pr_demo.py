"""Demo: a pull request that passes CI once and breaks production on run two.

demos/bad-unique-key.patch changes the incremental model's unique_key to a
non-unique pair. The first run creates the table (CREATE TABLE AS) and every
test passes. The second run is a MERGE, which Snowflake rejects with error
100090 "Duplicate row detected during DML action" because several source
rows match one target row. Running the model twice in CI catches it before
review; this demo exits 0 only when exactly that failure is observed.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from slim_ci_lab import loader, settings
from slim_ci_lab.dbt_runner import apply_patch, copy_project, dbt_env, run_dbt, targets

PATCH = settings.SAMPLE_ROOT / "demos" / "bad-unique-key.patch"
PR_NUMBER = "bad_unique_key"
MODEL = "int_trips_enriched"
EXPECTED_CODE = "100090"
EXPECTED_MESSAGE = "Duplicate row detected during DML action"


@dataclass(frozen=True)
class DemoResult:
    first: subprocess.CompletedProcess[str]
    second: subprocess.CompletedProcess[str]

    @property
    def second_output(self) -> str:
        return self.second.stdout + self.second.stderr

    @property
    def expected_failure_observed(self) -> bool:
        return (
            self.first.returncode == 0
            and self.second.returncode != 0
            and EXPECTED_CODE in self.second_output
            and EXPECTED_MESSAGE in self.second_output
        )


def run(database: str, patch: Path = PATCH, echo: bool = False) -> DemoResult:
    """Apply `patch` to a temporary copy of the project and run the model twice."""
    with tempfile.TemporaryDirectory(prefix="slim-ci-bad-pr-") as tmp:
        project = copy_project(Path(tmp) / "dbt")
        apply_patch(project, patch)
        env = dbt_env(DBT_DATABASE=database, DBT_PR_NUMBER=PR_NUMBER)
        target = ["--target", targets()[1]]
        run_dbt(["run-operation", "drop_pr_schema", *target], project, env)
        _banner(echo, "Run 1: dbt build --select +int_trips_enriched (fresh PR schema)")
        first = run_dbt(["build", *target, "--select", f"+{MODEL}"], project, env, echo=echo)
        _banner(echo, "Run 2: dbt run --select int_trips_enriched (incremental MERGE)")
        second = run_dbt(["run", *target, "--select", MODEL], project, env, echo=echo)
    return DemoResult(first=first, second=second)


def _banner(echo: bool, text: str) -> None:
    if echo:
        print(f"\n=== {text} ===", flush=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Show the second-run MERGE failure of a non-unique unique_key.")
    parser.add_argument("--database", default=settings.load().database)
    args = parser.parse_args(argv)

    print(f"Applying {PATCH.relative_to(settings.SAMPLE_ROOT)} to a temporary copy of the dbt project")
    result = loader.load(args.database)
    print(f"Raw data ready: {result.total_rows} trips in {args.database}.RAW.TRIPS")
    demo = run(args.database, echo=True)

    print("\n=== Result ===")
    print(f"Run 1 exit code: {demo.first.returncode} (expected 0: CREATE TABLE AS, all tests pass)")
    print(f"Run 2 exit code: {demo.second.returncode} (expected non-zero: MERGE with a non-unique key)")
    if demo.expected_failure_observed:
        print(f'Caught on run 2: {EXPECTED_CODE} "{EXPECTED_MESSAGE}".')
        print("A single run would have shipped this bug. Fix: unique_key='trip_id'.")
        print("make ci-local catches it on the first run: CI_PR_PATCH=demos/bad-unique-key.patch make ci-local")
        return
    print(f'Did NOT observe the expected {EXPECTED_CODE} failure on run 2.', file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
