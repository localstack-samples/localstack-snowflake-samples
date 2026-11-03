"""Print the Slim CI summary from dbt's run_results.json and manifest.json.

Also appends it to $GITHUB_STEP_SUMMARY when running in GitHub Actions.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path

from slim_ci_lab import settings

BUILDABLE = {"model", "seed", "snapshot", "test", "unit_test"}


def summarize(run_results: Path, manifest: Path, seconds: int) -> list[str]:
    results = json.loads(run_results.read_text())["results"]
    nodes = json.loads(manifest.read_text())
    all_ids = [uid for uid, node in nodes["nodes"].items() if node["resource_type"] in BUILDABLE]
    all_ids += list(nodes.get("unit_tests", {}))
    built = {r["unique_id"] for r in results}
    statuses = Counter(r["status"] for r in results)
    kinds = Counter(uid.split(".", 1)[0] for uid in built)
    models = sorted(uid.rsplit(".", 1)[-1] for uid in built if uid.startswith(("model.", "snapshot.", "seed.")))
    pr_schema = next((nodes["nodes"][uid]["schema"] for uid in built if uid.startswith("model.")), "-")

    if settings.is_emulator():
        credits = "Snowflake credits used: 0 (Snowflake-Next emulator)"
    else:
        credits = "Snowflake credits used: billable, see SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY"
    return [
        f"PR schema:          {pr_schema}",
        f"Selected:           {', '.join(models) or 'nothing (no modified models)'}",
        "Nodes run:          " + ", ".join(f"{n} {k}" for k, n in sorted(kinds.items())) if kinds else "Nodes run:          0",
        "Results:            " + ", ".join(f"{n} {s}" for s, n in sorted(statuses.items())) if statuses else "Results:            -",
        f"Deferred to prod:   {len(set(all_ids) - built)} of {len(all_ids)} nodes not rebuilt",
        f"Wall-clock:         {seconds // 60}m {seconds % 60:02d}s",
        credits,
    ]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-results", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    args = parser.parse_args(argv)
    lines = summarize(args.run_results, args.manifest, args.seconds)
    print("\n".join(lines))
    step_summary = os.getenv("GITHUB_STEP_SUMMARY")
    if step_summary:
        with Path(step_summary).open("a") as fh:
            fh.write("### Slim CI\n\n```\n" + "\n".join(lines) + "\n```\n")


if __name__ == "__main__":
    main()
