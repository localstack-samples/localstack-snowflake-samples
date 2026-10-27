"""Download (once), verify and load the pinned Synthea extract.

Usage: uv run python scripts/load_synthea.py [--database NAME]

Dataset: Synthea 1K sample CSV export, April 2020 build
  URL     https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_csv_apr2020.zip
  SHA-256 4194b18c11eaedcf0d5d5dd448d8ac9661f14381e2ef9f109215dc42266cd38a
  Cached  .cache/synthea_sample_data_csv_apr2020.zip
Synthea output is fully synthetic. The generator is Apache-2.0; the project
states the data "can be used without concern for legal or privacy restrictions".
The pinned values live in phi_guardrails/synthea.py, which the test oracle shares.
"""

from __future__ import annotations

import argparse

from phi_guardrails import settings as cfg
from phi_guardrails.load import load
from phi_guardrails.synthea import DATASET_SHA256, DATASET_URL


def main() -> None:
    parser = argparse.ArgumentParser(description="Load the pinned Synthea extract.")
    parser.add_argument("--database", help="target database (default: $PHI_DATABASE or PHI)")
    args = parser.parse_args()
    settings = cfg.load()
    if args.database:
        settings = settings.with_database(args.database.upper())
    print(f"Dataset {DATASET_URL}\n  sha256 {DATASET_SHA256}")
    for table, count in load(settings).items():
        print(f"  {settings.database}.{table}: {count} rows")


if __name__ == "__main__":
    main()
